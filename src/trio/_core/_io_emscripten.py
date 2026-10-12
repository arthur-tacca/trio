from __future__ import annotations

import weakref
from typing import TYPE_CHECKING, Literal, TypeAlias

import attrs
from outcome import capture

from ._run import _public

if TYPE_CHECKING:
    from collections.abc import Callable

    from outcome import Outcome

    from .._file_io import _HasFileNo

# Emscripten (e.g. Pyodide, i.e. Python running in a web browser) has no usable
# sockets, no worker threads, and no way to block its single thread while
# waiting for I/O. So this "I/O manager" doesn't do any I/O at all: the wait_*
# functions all raise NotImplementedError. What it does provide is the part of
# guest mode that depends on the platform: waiting for a timeout, which it does
# with a host loop timer rather than by blocking in get_events() on a worker
# thread, and cutting that wait short when force_wakeup() is called.

# Non-empty iff force_wakeup() was called since the last call to get_events()
EventResult: TypeAlias = "list[None]"


@attrs.frozen(eq=False)
class _EmscriptenStatistics:
    backend: Literal["emscripten"] = attrs.field(init=False, default="emscripten")


@attrs.define(eq=False)
class _Wait:
    """A guest-mode wait in progress; see EmscriptenIOManager.wait_for_events.

    The host loop's timer is what keeps this object alive. The I/O manager
    only refers to it weakly, because ``deliver`` leads back to the
    GuestState, and the Runner (which owns the I/O manager) must not keep the
    GuestState alive, directly or indirectly: see the comment above GuestState
    in _run.py.
    """

    io_manager: EmscriptenIOManager
    deliver: Callable[[Outcome[EventResult]], None]
    run_sync_soon: Callable[[Callable[[], object]], object]
    cancel_timer: Callable[[], object] | None = None
    over: bool = False

    def timer_expired(self) -> None:
        if self.over:  # pragma: no cover
            # This timer was cancelled. A well-behaved host shouldn't call
            # us, but it doesn't hurt to be defensive.
            return
        self._end()
        # A timer callback is a fresh host callback of its own, so Trio can
        # run in it directly: no need for another hop.
        self._deliver_events()

    def cut_short(self) -> None:
        # force_wakeup() was called: cancel the timer, and deliver from a host
        # callback of our own rather than inside whatever host callback called
        # us (a promise handler, say), so that Trio doesn't run inside it.
        self._end()
        if self.cancel_timer is not None:
            self.cancel_timer()
        self.run_sync_soon(self._deliver_events)

    def _deliver_events(self) -> None:
        # get_events(0) is called here, when the wait is actually over, rather
        # than when it's cut short: it reports (and clears) the forced wakeup.
        self.deliver(capture(self.io_manager.get_events, 0))

    def _end(self) -> None:
        # Done before delivering, not after, because deliver() runs a guest
        # tick, which may start the next wait.
        self.over = True
        self.io_manager._wait = None


@attrs.define(eq=False)
class EmscriptenIOManager:
    _force_wakeup_pending: bool = False
    # The guest-mode wait in progress, if any. A weak reference: see _Wait.
    _wait: weakref.ref[_Wait] | None = None

    def statistics(self) -> _EmscriptenStatistics:
        return _EmscriptenStatistics()

    def close(self) -> None:
        pass

    def wait_for_events(
        self,
        timeout: float,
        deliver: Callable[[Outcome[EventResult]], None],
        *,
        run_sync_soon_threadsafe: Callable[[Callable[[], object]], object],
        run_sync_later: (
            Callable[[Callable[[], object], float], Callable[[], object]] | None
        ),
    ) -> None:
        """Guest mode: wait for the timeout, or for force_wakeup(), then call
        ``deliver`` on the main thread with the outcome of get_events(0).

        There's nothing to wait for but the timeout, so this asks the host
        loop for a timer (start_guest_run's run_sync_later=); force_wakeup()
        cancels that timer and delivers right away instead.
        """
        assert self._current_wait() is None, "a wait is already in progress"
        assert run_sync_later is not None  # start_guest_run checks this
        wait = _Wait(self, deliver, run_sync_soon_threadsafe)
        wait.cancel_timer = run_sync_later(wait.timer_expired, timeout)
        self._wait = weakref.ref(wait)

    def _current_wait(self) -> _Wait | None:
        return None if self._wait is None else self._wait()

    def force_wakeup(self) -> None:
        self._force_wakeup_pending = True
        wait = self._current_wait()
        if wait is None:
            # No wait in progress (a tick is already scheduled, or running):
            # get_events(0) reports the wakeup when that tick asks.
            return
        wait.cut_short()

    # Return value must be False-y IFF the timeout expired, NOT if any I/O
    # happened or force_wakeup was called. Otherwise it can be anything; gets
    # passed straight through to process_events.
    def get_events(self, timeout: float) -> EventResult:
        if timeout > 0:
            raise NotImplementedError(
                "Trio can't block waiting for events on Emscripten; "
                "use trio.lowlevel.start_guest_run instead of trio.run",
            )
        if self._force_wakeup_pending:
            self._force_wakeup_pending = False
            return [None]
        return []

    def process_events(self, events: EventResult) -> None:
        pass

    @_public
    async def wait_readable(self, fd: int | _HasFileNo) -> None:
        """Not supported on Emscripten, where there is no readiness-based I/O.

        :raises NotImplementedError: always.
        """
        raise NotImplementedError("wait_readable is not supported on Emscripten")

    @_public
    async def wait_writable(self, fd: int | _HasFileNo) -> None:
        """Not supported on Emscripten, where there is no readiness-based I/O.

        :raises NotImplementedError: always.
        """
        raise NotImplementedError("wait_writable is not supported on Emscripten")

    @_public
    def notify_closing(self, fd: int | _HasFileNo) -> None:
        """Does nothing on Emscripten, because `wait_readable` and
        `wait_writable` aren't supported there, so there can never be any
        waiters to notify.
        """
