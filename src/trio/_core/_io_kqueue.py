from __future__ import annotations

import errno
import select
import sys
from typing import TYPE_CHECKING, Literal, NoReturn, TypeAlias

import attrs
import outcome

from .. import _core
from ._run import TASK_STATUS_IGNORED, _public
from ._wake_token import WakeToken
from ._wakeup_socketpair import WakeupSocketpair

if TYPE_CHECKING:
    from collections.abc import Callable

    from .._core import TaskStatus
    from .._file_io import _HasFileNo

assert not TYPE_CHECKING or (sys.platform != "linux" and sys.platform != "win32")

EventResult: TypeAlias = "list[select.kevent]"


@attrs.frozen(eq=False)
class _KqueueStatistics:
    tasks_waiting: int
    monitors: int
    backend: Literal["kqueue"] = attrs.field(init=False, default="kqueue")


@attrs.define(eq=False)
class KqueueIOManager:
    _kqueue: select.kqueue = attrs.Factory(select.kqueue)
    # {(ident, filter): the WakeToken of a task blocked in wait_kevent, or a
    # monitor_kevent callback}
    _registered: dict[
        tuple[int, int],
        WakeToken[select.kevent] | Callable[[select.kevent], object],
    ] = attrs.Factory(dict)
    # Keeps track of whether we are running host callbacks in guest mode
    # and, if so, which events have been discarded by those callbacks
    # cancelling waiters or calling notify_closing. Needed in case they
    # have just completed, in which case they will appear to be spurious
    # in process_events.
    _events_pending: bool = False
    _deregistered: set[tuple[int, int]] = attrs.Factory(set)
    _force_wakeup: WakeupSocketpair = attrs.Factory(WakeupSocketpair)
    _force_wakeup_fd: int | None = None

    def __attrs_post_init__(self) -> None:
        force_wakeup_event = select.kevent(
            self._force_wakeup.wakeup_sock,
            select.KQ_FILTER_READ,
            select.KQ_EV_ADD,
        )
        self._kqueue.control([force_wakeup_event], 0)
        self._force_wakeup_fd = self._force_wakeup.wakeup_sock.fileno()

    def statistics(self) -> _KqueueStatistics:
        tasks_waiting = 0
        monitors = 0
        for receiver in self._registered.values():
            if isinstance(receiver, WakeToken):
                tasks_waiting += 1
            else:
                monitors += 1
        return _KqueueStatistics(tasks_waiting=tasks_waiting, monitors=monitors)

    def close(self) -> None:
        self._kqueue.close()
        self._force_wakeup.close()

    def force_wakeup(self) -> None:
        self._force_wakeup.wakeup_thread_and_signal_safe()

    def _deregister(self, key: tuple[int, int]) -> None:
        del self._registered[key]
        if self._events_pending:
            self._deregistered.add(key)

    def get_events(self, timeout: float) -> EventResult:
        # In guest mode, note that we are now processing host callbacks, so
        # need to record any discarded events.
        self._events_pending = True
        # max_events must be > 0 or kqueue gets cranky
        # and we generally want this to be strictly larger than the actual
        # number of events we get, so that we can tell that we've gotten
        # all the events in just 1 call.
        max_events = len(self._registered) + 1
        events = []
        while True:
            batch = self._kqueue.control([], max_events, timeout)
            events += batch
            if len(batch) < max_events:
                break
            else:  # TODO: test this line
                timeout = 0
                # and loop back to the start
        return events

    def process_events(self, events: EventResult) -> None:
        for event in events:
            key = (event.ident, event.filter)
            if event.ident == self._force_wakeup_fd:
                self._force_wakeup.drain()
                continue
            if key in self._deregistered:
                # A host callback deregistered this key after the batch was
                # fetched (see _deregistered above); its receiver is gone and
                # this stale event should just be dropped. Any other missing
                # key would be a bug, so let the lookup below fail loudly.
                continue
            receiver = self._registered[key]
            if isinstance(receiver, WakeToken):
                # A task can only be woken once, so it's done with this key.
                del self._registered[key]
                receiver.reschedule(outcome.Value(event))
            else:
                # A monitor stays registered until monitor_kevent() returns.
                receiver(event)
        self._events_pending = False
        self._deregistered.clear()

    # kevent registration is complicated -- e.g. aio submission can
    # implicitly perform an EV_ADD, and EVFILT_PROC with NOTE_TRACK will
    # automatically register filters for child processes. Earlier revisions
    # had a *very* low-level API, where the caller had responsibility for
    # registering events with the OS kqueue. wait_kevent now takes care of
    # that for one-shot waits. monitor_kevent has not yet been updated
    # similarly, so its callers still need to register events themselves.

    @_public
    def current_kqueue(self) -> select.kqueue:
        """TODO: these are implemented, but are currently more of a sketch than
        anything real. See `#26
        <https://github.com/python-trio/trio/issues/26>`__.
        """
        return self._kqueue

    @_public
    async def monitor_kevent(
        self,
        ident: int,
        filter: int,
        callback: Callable[[select.kevent], object],
        *,
        task_status: TaskStatus[None] = TASK_STATUS_IGNORED,
    ) -> NoReturn:
        """TODO: these are implemented, but are currently more of a sketch than
        anything real. See `#26
        <https://github.com/python-trio/trio/issues/26>`__.

        Calls ``callback`` with each kevent that arrives for ``(ident,
        filter)``, until cancelled. The callback runs inside Trio's run loop,
        not in any task, so it must not block, and should do as little as
        possible: typically just record the event and wake up a task that's
        waiting for it. If it raises, monitoring stops and the exception is
        raised here instead.

        Supports `nursery.start() <trio.Nursery.start>`, reporting itself as
        started once it's listening, which is the point from which it's safe
        to register the kevent with the kernel.
        """
        key = (ident, filter)
        if key in self._registered:
            raise _core.BusyResourceError(
                "attempt to register multiple listeners for same ident/filter pair",
            )
        active = True

        def on_cancel(token: WakeToken[None]) -> None:
            nonlocal active
            active = False
            token.reschedule_cancelled()

        token = WakeToken(on_cancel)

        def deliver(event: select.kevent) -> None:
            nonlocal active
            # Once we're on our way out (cancelled, or the callback raised),
            # any further events are dropped.
            if not active:
                return
            try:
                callback(event)
            except BaseException as exc:
                active = False
                token.reschedule(outcome.Error(exc))

        self._registered[key] = deliver
        try:
            task_status.started()
            await token
        finally:
            del self._registered[key]
        raise AssertionError("Should never get here")

    @_public
    async def wait_kevent(
        self,
        ident: int | _HasFileNo,
        filter: int,
        *,
        fflags: int = 0,
        data: int = 0,
    ) -> select.kevent:
        """Waits for a one-shot kevent to happen.

        This is a low-level function that lets you wait on a specific kevent,
        in case you have a use case not covered by the IO primitives in Trio.

        This registers ``kevent(ident, filter, flags, fflags, data)``, where
        ``flags`` is set to ``select.KQ_EV_ADD | select.KQ_EV_ONESHOT``, and
        waits for it to complete. If cancelled then it is removed with ``flags``
        set to ``select.KQ_EV_DELETE``.

        Args:
            ident: Value used to identify the event. The interpretation depends
                on the filter but it's usually the file descriptor.
            filter: Name of the kernel filter e.g. ``select.KQ_FILTER_READ``.
            fflags: Filter-specific flags.
            data: Filter-specific data.

        Returns:
            The `select.kevent` returned from kqueue.

        Raises:
            BusyResourceError: if another task is waiting for this
                ``(ident, filter)`` pair.
            OSError: if the kqueue rejects the registration (for example,
                `ProcessLookupError` for an ``EVFILT_PROC`` ident that has
                already exited).
        """
        if not isinstance(ident, int):
            ident = ident.fileno()

        key = (ident, filter)
        if key in self._registered:
            raise _core.BusyResourceError(
                "attempt to register multiple listeners for same ident/filter pair",
            )

        def on_cancel(token: WakeToken[select.kevent]) -> None:
            self._deregister(key)
            try:
                event = select.kevent(ident, filter, select.KQ_EV_DELETE)
                self._kqueue.control([event], 0)
            except OSError as exc:
                # kqueue tracks individual fds (*not* the underlying file
                # object, see _io_epoll.py for a long discussion of why this
                # distinction matters), and automatically deregisters an event
                # if the fd is closed. So if kqueue.control says that it
                # doesn't know about this event, it could be because
                # the fd was closed behind our backs. (Too bad we can't ask it
                # to wake us up when this happens, versus discovering it after
                # the fact... oh well, you can't have everything.)
                # FreeBSD reports this using EBADF. macOS uses ENOENT.
                #
                # This can also happen if, in guest mode, the cancellation
                # happens just after the event was fetched by get_events()
                # (which removes it, since it's oneshot) but before it has been
                # processed it in process_events(). This is reported as
                # errno.ENOENT on both platforms.
                if exc.errno in (errno.EBADF, errno.ENOENT):  # pragma: no branch
                    pass
                else:  # pragma: no cover
                    # As far as we know, this branch can't happen.
                    raise
            token.reschedule_cancelled()

        # Register the event before updating _registered in case this throws
        event = select.kevent(
            ident,
            filter,
            select.KQ_EV_ADD | select.KQ_EV_ONESHOT,
            fflags,
            data,
        )
        self._kqueue.control([event], 0)
        token: WakeToken[select.kevent] = WakeToken(on_cancel)
        self._registered[key] = token
        return await token

    @_public
    async def wait_readable(self, fd: int | _HasFileNo) -> None:
        """Block until the kernel reports that the given object is readable.

        On Unix systems, ``fd`` must either be an integer file descriptor,
        or else an object with a ``.fileno()`` method which returns an
        integer file descriptor. Any kind of file descriptor can be passed,
        though the exact semantics will depend on your kernel. For example,
        this probably won't do anything useful for on-disk files.

        On Windows systems, ``fd`` must either be an integer ``SOCKET``
        handle, or else an object with a ``.fileno()`` method which returns
        an integer ``SOCKET`` handle. File descriptors aren't supported,
        and neither are handles that refer to anything besides a
        ``SOCKET``.

        :raises trio.BusyResourceError:
            if another task is already waiting for the given socket to
            become readable.
        :raises trio.ClosedResourceError:
            if another task calls :func:`notify_closing` while this
            function is still working.
        """
        await self.wait_kevent(fd, select.KQ_FILTER_READ)

    @_public
    async def wait_writable(self, fd: int | _HasFileNo) -> None:
        """Block until the kernel reports that the given object is writable.

        See `wait_readable` for the definition of ``fd``.

        :raises trio.BusyResourceError:
            if another task is already waiting for the given socket to
            become writable.
        :raises trio.ClosedResourceError:
            if another task calls :func:`notify_closing` while this
            function is still working.
        """
        await self.wait_kevent(fd, select.KQ_FILTER_WRITE)

    @_public
    def notify_closing(self, fd: int | _HasFileNo) -> None:
        """Notify waiters of the given object that it will be closed.

        Call this before closing a file descriptor (on Unix) or socket (on
        Windows). This will cause any `wait_readable` or `wait_writable`
        calls on the given object to immediately wake up and raise
        `~trio.ClosedResourceError`.

        This doesn't actually close the object – you still have to do that
        yourself afterwards. Also, you want to be careful to make sure no
        new tasks start waiting on the object in between when you call this
        and when it's actually closed. So to close something properly, you
        usually want to do these steps in order:

        1. Explicitly mark the object as closed, so that any new attempts
           to use it will abort before they start.
        2. Call `notify_closing` to wake up any already-existing users.
        3. Actually close the object.

        It's also possible to do them in a different order if that's more
        convenient, *but only if* you make sure not to have any checkpoints in
        between the steps. This way they all happen in a single atomic
        step, so other tasks won't be able to tell what order they happened
        in anyway.
        """
        if not isinstance(fd, int):
            fd = fd.fileno()

        for filter_ in [select.KQ_FILTER_READ, select.KQ_FILTER_WRITE]:
            key = (fd, filter_)
            receiver = self._registered.get(key)

            if receiver is None:
                continue

            if isinstance(receiver, WakeToken):
                event = select.kevent(fd, filter_, select.KQ_EV_DELETE)
                try:
                    self._kqueue.control([event], 0)
                except OSError as e:
                    # kqueue no longer knows about this event. Either the fd
                    # was already closed, which deregisters it automatically
                    # (FreeBSD reports EBADF, macOS ENOENT), or, in guest
                    # mode, the one-shot event was fetched by get_events()
                    # but not yet processed (ENOENT on both platforms).
                    if e.errno not in (errno.EBADF, errno.ENOENT):
                        raise  # pragma: no cover
                exc = _core.ClosedResourceError("another task closed this fd")
                receiver.reschedule(outcome.Error(exc))
                self._deregister(key)
            else:
                # XX this is an interesting example of a case where being able
                # to close a queue would be useful...
                raise NotImplementedError(
                    "can't close an fd that monitor_kevent is using",
                )
