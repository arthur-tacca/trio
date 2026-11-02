"""Nurseries, and the entry points: the top layer of Trio's core.

See the module docstring of ``_task.py`` for the layering. This module
builds nurseries on top of the runner, and provides `run` and
`start_guest_run`, including the init task that sets up the system and
main-task nurseries for a run.
"""

from __future__ import annotations

import functools
import sys
from typing import TYPE_CHECKING, Any, NoReturn, cast, overload

import attrs
import outcome
from outcome import Error, Outcome, Value
from sniffio import thread_local as sniffio_library

from .._deprecate import warn_deprecated
from .._util import NoPublicConstructor, final, raise_saving_context
from ._cancel_scope import NONSTRICT_EXCEPTIONGROUP_NOTE, CancelScope
from ._cancel_status import CancelReason
from ._exceptions import TrioInternalError
from ._ki import enable_ki_protection
from ._run_context import GLOBAL_RUN_CONTEXT
from ._runner import (
    GuestState,
    Runner,
    setup_runner,
    unrolled_run,
)
from ._task import (
    MISNESTING_ADVICE,
    RetT,
    StatusT,
    Task,
    TaskStatus,
    current_task,
)
from ._traps import cancel_shielded_checkpoint
from ._wake_token import WakeToken

if sys.version_info < (3, 11):
    from exceptiongroup import BaseExceptionGroup


if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Sequence
    from contextlib import AbstractAsyncContextManager
    from types import TracebackType

    from typing_extensions import Unpack

    from .._abc import Clock, Instrument
    from ._runner import EventResult
    from ._task import PosArgT


@final
class _NoStatus(metaclass=NoPublicConstructor):
    """Sentinel for unset TaskStatus._value."""


# This code needs to be read alongside the code from Nursery.start to make
# sense.
@attrs.define(eq=False, repr=False, slots=False)
class _TaskStatus(TaskStatus[StatusT]):
    _old_nursery: Nursery
    _new_nursery: Nursery
    # NoStatus is a sentinel.
    _value: StatusT | type[_NoStatus] = _NoStatus

    def __repr__(self) -> str:
        return f"<Task status object at {id(self):#x}>"

    @overload
    def started(self: _TaskStatus[None]) -> None: ...

    @overload
    def started(self: _TaskStatus[StatusT], value: StatusT) -> None: ...

    def started(self, value: StatusT | None = None) -> None:
        if self._value is not _NoStatus:
            raise RuntimeError("called 'started' twice on the same task status")
        self._value = cast("StatusT", value)  # If None, StatusT == None

        # If the old nursery is cancelled, then quietly quit now; the child
        # will eventually exit on its own, and we don't want to risk moving
        # children that might have propagating Cancelled exceptions into
        # a place with no cancelled cancel scopes to catch them.
        assert self._old_nursery._cancel_status is not None
        if self._old_nursery._cancel_status.effectively_cancelled:
            return

        # Can't be closed, b/c we checked in start() and then _pending_starts
        # should keep it open.
        assert not self._new_nursery._closed

        # Move tasks from the old nursery to the new
        tasks = self._old_nursery._children
        self._old_nursery._children = set()
        for task in tasks:
            task._owner = self._new_nursery
            task._parent_nursery = self._new_nursery
            task._eventual_parent_nursery = None
            self._new_nursery._children.add(task)

        # Move all children of the old nursery's cancel status object
        # to be underneath the new nursery instead. This includes both
        # tasks and child cancel status objects.
        # NB: If the new nursery is cancelled, reparenting a cancel
        # status to be underneath it can invoke an abort_fn, which might
        # do something evil like cancel the old nursery. We thus break
        # everything off from the old nursery before we start attaching
        # anything to the new.
        cancel_status_children = self._old_nursery._cancel_status.children
        # The status sees its tasks as CancelStatus.Members, but they are
        # Tasks: Task._activate_cancel_status is what puts them there.
        cancel_status_tasks = cast(
            "set[Task]", set(self._old_nursery._cancel_status.tasks)
        )
        cancel_status_tasks.discard(self._old_nursery._parent_task)
        for cancel_status in cancel_status_children:
            cancel_status.parent = None
        for task in cancel_status_tasks:
            task._activate_cancel_status(None)
        for cancel_status in cancel_status_children:
            cancel_status.parent = self._new_nursery._cancel_status
        for task in cancel_status_tasks:
            task._activate_cancel_status(self._new_nursery._cancel_status)

        # That should have removed all the children from the old nursery
        assert not self._old_nursery._children

        # And finally, poke the old nursery so it notices that all its
        # children have disappeared and can exit.
        self._old_nursery._check_nursery_closed()


@attrs.define(slots=False)
class NurseryManager:
    """Nursery context manager.

    Note we explicitly avoid @asynccontextmanager and @async_generator
    since they add a lot of extraneous stack frames to exceptions, as
    well as cause problematic behavior with handling of StopIteration
    and StopAsyncIteration.

    """

    strict_exception_groups: bool = True

    @enable_ki_protection
    async def __aenter__(self) -> Nursery:
        self._scope = CancelScope()
        self._scope.__enter__()
        self._nursery = Nursery._create(
            current_task(),
            self._scope,
            self.strict_exception_groups,
        )
        return self._nursery

    @enable_ki_protection
    async def __aexit__(
        self,
        etype: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool:
        new_exc = await self._nursery._nested_child_finished(exc)
        # Tracebacks show the 'raise' line below out of context, so let's give
        # this variable a name that makes sense out of context.
        combined_error_from_nursery = self._scope._close(new_exc)
        if combined_error_from_nursery is None:
            return True
        elif combined_error_from_nursery is exc:
            return False
        else:
            # Copied verbatim from the old MultiErrorCatcher.  Python doesn't
            # allow us to encapsulate this __context__ fixup.
            old_context = combined_error_from_nursery.__context__
            try:
                raise combined_error_from_nursery
            finally:
                _, value, _ = sys.exc_info()
                assert value is combined_error_from_nursery
                value.__context__ = old_context
                # delete references from locals to avoid creating cycles
                # see test_cancel_scope_exit_doesnt_create_cyclic_garbage
                del _, combined_error_from_nursery, value, new_exc

    # make sure these raise errors in static analysis if called
    if not TYPE_CHECKING:

        def __enter__(self) -> NoReturn:
            raise RuntimeError(
                "use 'async with open_nursery(...)', not 'with open_nursery(...)'",
            )

        def __exit__(
            self,
            exc_type: type[BaseException] | None,
            exc_value: BaseException | None,
            traceback: TracebackType | None,
        ) -> NoReturn:  # pragma: no cover
            raise AssertionError("Never called, but should be defined")


def open_nursery(
    strict_exception_groups: bool | None = None,
) -> AbstractAsyncContextManager[Nursery]:
    """Returns an async context manager which must be used to create a
    new `Nursery`.

    It does not block on entry; on exit it blocks until all child tasks
    have exited. If no child tasks are running on exit, it will insert a
    schedule point (but no cancellation point) - equivalent to
    :func:`trio.lowlevel.cancel_shielded_checkpoint`. This means a nursery
    is never the source of a cancellation exception, it only propagates it
    from sub-tasks.

    Args:
      strict_exception_groups (bool): Unless set to False, even a single raised exception
          will be wrapped in an exception group. If not specified, uses the value passed
          to :func:`run`, which defaults to true. Setting it to False will be deprecated
          and ultimately removed in a future version of Trio.

    """
    # only warn if explicitly set to falsy, not if we get it from the global context.
    if strict_exception_groups is not None and not strict_exception_groups:
        warn_deprecated(
            "open_nursery(strict_exception_groups=False)",
            version="0.25.0",
            issue=2929,
            instead=(
                "the default value of True and rewrite exception handlers to handle ExceptionGroups. "
                "See https://trio.readthedocs.io/en/stable/reference-core.html#designing-for-multiple-errors"
            ),
            use_triodeprecationwarning=True,
        )

    if strict_exception_groups is None:
        strict_exception_groups = GLOBAL_RUN_CONTEXT.runner.strict_exception_groups

    return NurseryManager(strict_exception_groups=strict_exception_groups)


def _cancel_ignored(token: WakeToken[RetT]) -> None:
    """A WakeToken on_cancel callback that declines to deliver the cancellation."""


def _abandon_open_nurseries(task: Task, outcome: Outcome[object]) -> Outcome[object]:
    """Deal with ``task`` having exited while still inside one or more of
    its nurseries' ``async with`` blocks (nursery misnesting, see
    MISNESTING_ADVICE).

    Nothing can wait for those nurseries' children any more, so they are
    cancelled and closed, and the task's outcome is replaced with an error
    reporting the misnesting, which might not have been caught anywhere
    else. (The original error, if any, is chained onto the new one.)
    Returns ``outcome`` unchanged if the task left no nursery open.
    """
    if not task._child_nurseries:
        return outcome
    for nursery in task._child_nurseries:
        nursery._parent_task_exited(task)
    try:
        # Raise this, rather than just constructing it, to get a traceback
        # frame included
        raise RuntimeError(
            f"Nursery stack corrupted: Nursery surrounding {task!r} was "
            f"closed before the task exited\n{MISNESTING_ADVICE}",
        )
    except RuntimeError as new_exc:
        if isinstance(outcome, Error):
            new_exc.__context__ = outcome.error
        return Error(new_exc)


@final
class Nursery(metaclass=NoPublicConstructor):
    """A context which may be used to spawn (or cancel) child tasks.

    Not constructed directly, use `open_nursery` instead.

    The nursery will remain open until all child tasks have completed,
    or until it is cancelled, at which point it will cancel all its
    remaining child tasks and close.

    Nurseries ensure the absence of orphaned Tasks, since all running
    tasks will belong to an open Nursery.

    Attributes:
        cancel_scope:
            Creating a nursery also implicitly creates a cancellation scope,
            which is exposed as the :attr:`cancel_scope` attribute. This is
            used internally to implement the logic where if an error occurs
            then ``__aexit__`` cancels all children, but you can use it for
            other things, e.g. if you want to explicitly cancel all children
            in response to some external event.
    """

    def __init__(
        self,
        parent_task: Task,
        cancel_scope: CancelScope,
        strict_exception_groups: bool,
    ) -> None:
        self._parent_task = parent_task
        self._strict_exception_groups = strict_exception_groups
        parent_task._child_nurseries.append(self)
        # the cancel status that children inherit - we take a snapshot, so it
        # won't be affected by any changes in the parent.
        self._cancel_status = parent_task._cancel_status
        # the cancel scope that directly surrounds us; used for cancelling all
        # children.
        self.cancel_scope = cancel_scope
        assert self.cancel_scope._cancel_status is self._cancel_status
        self._children: set[Task] = set()
        self._pending_excs: list[BaseException] = []
        # The "nested child" is how this code refers to the contents of the
        # nursery's 'async with' block, which acts like a child Task in all
        # the ways we can make it.
        self._nested_child_running = True
        # The token the parent is blocked on in __aexit__, waiting for the
        # children to finish, if it is.
        self._parent_aexit_token: WakeToken[None] | None = None
        self._pending_starts = 0
        self._closed = False

    @property
    def child_tasks(self) -> frozenset[Task]:
        """(`frozenset`): Contains all the child :class:`~trio.lowlevel.Task`
        objects which are still running."""
        return frozenset(self._children)

    @property
    def parent_task(self) -> Task:
        "(`~trio.lowlevel.Task`):  The Task that opened this nursery."
        return self._parent_task

    # A nursery is the TaskOwner (see _task.py) of its children: _closed,
    # _child_spawned and _child_finished are the whole of what the runner
    # needs from it.

    def _child_spawned(self, task: Task) -> None:
        self._children.add(task)
        task._parent_nursery = self
        task._activate_cancel_status(self._cancel_status)

    def _child_finished(self, task: Task, outcome: Outcome[object]) -> None:
        self._record_child_finished(task, _abandon_open_nurseries(task, outcome))

    def _record_child_finished(self, task: Task, outcome: Outcome[object]) -> None:
        # The bookkeeping half of _child_finished: forget the child, keep its
        # error (if any) for __aexit__ to raise, and see whether that was the
        # last thing we were waiting for. The main task's owner calls this
        # directly, having dealt with the task's outcome itself.
        self._children.remove(task)
        if self._closed and not hasattr(self, "_pending_excs"):
            # We're abandoned by misnested nurseries, the result of the task is lost.
            return
        if isinstance(outcome, Error):
            self._add_exc(
                outcome.error,
                CancelReason(
                    source="nursery",
                    source_task=repr(task),
                    reason=f"child task raised exception {outcome.error!r}",
                ),
            )
        self._check_nursery_closed()

    def _parent_task_exited(self, task: Task) -> None:
        # Our parent task exited without leaving our 'async with' block
        # first. Nothing can wait for our children any more, so cancel them
        # and close up; _abandon_open_nurseries() raises the misnesting
        # error.
        self.cancel_scope._cancel(
            CancelReason(
                source="nursery",
                reason="Parent Task exited prematurely, abandoning this nursery without exiting it properly.",
                source_task=repr(task),
            )
        )
        self._closed = True

    def _add_exc(self, exc: BaseException, reason: CancelReason | None) -> None:
        self._pending_excs.append(exc)
        self.cancel_scope._cancel(reason)

    def _check_nursery_closed(self) -> None:
        if not any([self._nested_child_running, self._children, self._pending_starts]):
            self._closed = True
            token = self._parent_aexit_token
            if token is not None:
                self._parent_aexit_token = None
                token.reschedule()

    async def _nested_child_finished(
        self,
        nested_child_exc: BaseException | None,
    ) -> BaseException | None:
        # Returns ExceptionGroup instance (or any exception if the nursery is in loose mode
        # and there is just one contained exception) if there are pending exceptions
        if nested_child_exc is not None:
            self._add_exc(
                nested_child_exc,
                reason=CancelReason(
                    source="nursery",
                    source_task=repr(self._parent_task),
                    reason=f"Code block inside nursery contextmanager raised exception {nested_child_exc!r}",
                ),
            )
        self._nested_child_running = False
        self._check_nursery_closed()

        if not self._closed:
            # Being cancelled while we wait for our children changes nothing:
            # they're being cancelled too, and we still have to wait for them.
            # So the token's on_cancel just ignores the request.
            token = WakeToken(_cancel_ignored)
            self._parent_aexit_token = token
            await token
        else:
            # Nothing to wait for, so execute a schedule point, but don't
            # allow us to be cancelled, just like the other branch.  We
            # still need to catch and store non-Cancelled exceptions.
            try:
                await cancel_shielded_checkpoint()
            except BaseException as exc:
                # there's no children to cancel, so don't need to supply cancel reason
                self._add_exc(exc, reason=None)

        popped = self._parent_task._child_nurseries.pop()
        assert popped is self, "Nursery misnesting detected!"
        if self._pending_excs:
            try:
                if not self._strict_exception_groups and len(self._pending_excs) == 1:
                    return self._pending_excs[0]
                exception = BaseExceptionGroup(
                    "Exceptions from Trio nursery",
                    self._pending_excs,
                )
                if not self._strict_exception_groups:
                    exception.add_note(NONSTRICT_EXCEPTIONGROUP_NOTE)
                return exception
            finally:
                # avoid a garbage cycle
                # (see test_locals_destroyed_promptly_on_cancel)
                del self._pending_excs
        return None

    def start_soon(
        self,
        async_fn: Callable[[Unpack[PosArgT]], Awaitable[object]],
        *args: Unpack[PosArgT],
        name: object = None,
    ) -> None:
        """Creates a child task, scheduling ``await async_fn(*args)``.

        If you want to run a function and immediately wait for its result,
        then you don't need a nursery; just use ``await async_fn(*args)``.
        If you want to wait for the task to initialize itself before
        continuing, see :meth:`start`, the other fundamental method for
        creating concurrent tasks in Trio.

        Note that this is *not* an async function and you don't use await
        when calling it. It sets up the new task, but then returns
        immediately, *before* the new task has a chance to do anything.
        New tasks may start running in any order, and at any checkpoint the
        scheduler chooses - at latest when the nursery is waiting to exit.

        It's possible to pass a nursery object into another task, which
        allows that task to start new child tasks in the first task's
        nursery.

        The child task inherits its parent nursery's cancel scopes.

        Args:
            async_fn: An async callable.
            args: Positional arguments for ``async_fn``. If you want
                  to pass keyword arguments, use
                  :func:`functools.partial`.
            name: The name for this task. Only used for
                  debugging/introspection
                  (e.g. ``repr(task_obj)``). If this isn't a string,
                  :meth:`start_soon` will try to make it one. A
                  common use case is if you're wrapping a function
                  before spawning a new task, you might pass the
                  original function as the ``name=`` to make
                  debugging easier.

        Raises:
            RuntimeError: If this nursery is no longer open
                          (i.e. its ``async with`` block has
                          exited).
        """
        self._parent_task._scheduler.spawn(async_fn, args, self, name)

    # Typing changes blocked by https://github.com/python/mypy/pull/17512
    async def start(  # type: ignore[explicit-any]
        self,
        async_fn: Callable[..., Awaitable[object]],
        *args: object,
        name: object = None,
    ) -> Any:
        r"""Creates and initializes a child task.

        Like :meth:`start_soon`, but blocks until the new task has
        finished initializing itself, and optionally returns some
        information from it.

        The ``async_fn`` must accept a ``task_status`` keyword argument,
        and it must make sure that it (or someone) eventually calls
        :meth:`task_status.started() <TaskStatus.started>`.

        The conventional way to define ``async_fn`` is like::

            async def async_fn(arg1, arg2, *, task_status=trio.TASK_STATUS_IGNORED):
                ...  # Caller is blocked waiting for this code to run
                task_status.started()
                ...  # This async code can be interleaved with the caller

        :attr:`trio.TASK_STATUS_IGNORED` is a special global object with
        a do-nothing ``started`` method. This way your function supports
        being called either like ``await nursery.start(async_fn, arg1,
        arg2)`` or directly like ``await async_fn(arg1, arg2)``, and
        either way it can call :meth:`task_status.started() <TaskStatus.started>`
        without worrying about which mode it's in. Defining your function like
        this will make it obvious to readers that it supports being used
        in both modes.

        Before the child calls :meth:`task_status.started() <TaskStatus.started>`,
        it's effectively run underneath the call to :meth:`start`: if it
        raises an exception then that exception is reported by
        :meth:`start`, and does *not* propagate out of the nursery. If
        :meth:`start` is cancelled, then the child task is also
        cancelled.

        When the child calls :meth:`task_status.started() <TaskStatus.started>`,
        it's moved out from underneath :meth:`start` and into the given nursery.

        If the child task passes a value to :meth:`task_status.started(value) <TaskStatus.started>`,
        then :meth:`start` returns this value. Otherwise, it returns ``None``.
        """
        if self._closed:
            raise RuntimeError("Nursery is closed to new arrivals")
        try:
            self._pending_starts += 1
            # wrap internal nursery in try-except to unroll any exceptiongroups
            # to avoid wrapping pre-started() exceptions in an extra ExceptionGroup.
            # See #2611.
            try:
                # set strict_exception_groups = True to make sure we always unwrap
                # *this* nursery's exceptiongroup
                async with open_nursery(strict_exception_groups=True) as old_nursery:
                    task_status: _TaskStatus[object | None] = _TaskStatus(
                        old_nursery,
                        self,
                    )
                    thunk = functools.partial(async_fn, task_status=task_status)
                    task = self._parent_task._scheduler.spawn(
                        thunk,
                        args,
                        old_nursery,
                        name,
                    )
                    task._eventual_parent_nursery = self
                    # Wait for either TaskStatus.started or an exception to
                    # cancel this nursery:
            except BaseExceptionGroup as exc:
                if len(exc.exceptions) == 1:
                    raise_saving_context(exc.exceptions[0])
                raise TrioInternalError(
                    "Internal nursery should not have multiple tasks. This can be "
                    'caused by the user managing to access the "old" nursery in '
                    "`task_status` and spawning tasks in it.",
                ) from exc

            # If we get here, then the child either got reparented or exited
            # normally. The complicated logic is all in TaskStatus.started().
            # (Any exceptions propagate directly out of the above.)
            if task_status._value is _NoStatus:
                raise RuntimeError("child exited without calling task_status.started()")
            return task_status._value
        finally:
            self._pending_starts -= 1
            self._check_nursery_closed()

    def __del__(self) -> None:
        assert not self._children


@final
class _MainTaskOwner:
    """The main task's TaskOwner (see _task.py): its nursery, except that the
    task's outcome is kept for `run` to return (or raise), rather than being
    raised into the nursery as a child's error normally would be.

    The nursery is told that the task finished normally, so that it closes
    as usual. Capturing the outcome here, rather than by wrapping
    ``async_fn`` in a coroutine that captures it, keeps an extra frame out
    of every traceback that passes through the main task.
    """

    def __init__(self, runner: Runner, nursery: Nursery) -> None:
        self._runner = runner
        self._nursery = nursery

    @property
    def _closed(self) -> bool:
        return self._nursery._closed

    def _child_spawned(self, task: Task) -> None:
        self._nursery._child_spawned(task)

    def _child_finished(self, task: Task, outcome: Outcome[object]) -> None:
        self._runner.main_task_outcome = _abandon_open_nurseries(task, outcome)
        self._nursery._record_child_finished(task, Value(None))


################################################################
# run
################################################################


async def init(
    runner: Runner,
    async_fn: Callable[[Unpack[PosArgT]], Awaitable[object]],
    args: tuple[Unpack[PosArgT]],
) -> None:
    """The body of the "<init>" task: the root of the task tree, which sets up
    the nurseries that every other task lives in."""
    # run_sync_soon task runs here:
    async with open_nursery() as run_sync_soon_nursery:
        # All other system tasks run here:
        async with open_nursery() as runner.system_nursery:
            # Only the main task runs here. The cancel scope is how a
            # deferred control-C reaches it: see Runner.deliver_ki().
            with CancelScope() as runner.main_task_scope:
                async with open_nursery() as main_task_nursery:
                    try:
                        runner.main_task = runner.scheduler.spawn(
                            async_fn,
                            args,
                            _MainTaskOwner(runner, main_task_nursery),
                            None,
                        )
                    except BaseException as exc:
                        runner.main_task_outcome = Error(exc)
                        return
                    runner.scheduler.spawn(
                        runner.entry_queue.task,
                        (),
                        run_sync_soon_nursery,
                        "<TrioToken.run_sync_soon task>",
                        system_task=True,
                    )

            # Main task is done; start shutting down system tasks
            runner.system_nursery.cancel_scope._cancel(
                CancelReason(
                    source="shutdown",
                    reason="main task done, shutting down system tasks",
                    source_task=repr(runner.init_task),
                )
            )

        # System nursery is closed; finalize remaining async generators
        await runner.asyncgens.finalize_remaining(runner)

        # There are no more asyncgens, which means no more user-provided
        # code except possibly run_sync_soon callbacks. It's finally safe
        # to stop the run_sync_soon task and exit run().
        run_sync_soon_nursery.cancel_scope._cancel(
            CancelReason(
                source="shutdown",
                reason="main task done, shutting down run_sync_soon callbacks",
                source_task=repr(runner.init_task),
            )
        )


def run(
    async_fn: Callable[[Unpack[PosArgT]], Awaitable[RetT]],
    *args: Unpack[PosArgT],
    clock: Clock | None = None,
    instruments: Sequence[Instrument] = (),
    restrict_keyboard_interrupt_to_checkpoints: bool = False,
    strict_exception_groups: bool = True,
) -> RetT:
    """Run a Trio-flavored async function, and return the result.

    Calling::

       run(async_fn, *args)

    is the equivalent of::

       await async_fn(*args)

    except that :func:`run` can (and must) be called from a synchronous
    context.

    This is Trio's main entry point. Almost every other function in Trio
    requires that you be inside a call to :func:`run`.

    Args:
      async_fn: An async function.

      args: Positional arguments to be passed to *async_fn*. If you need to
          pass keyword arguments, then use :func:`functools.partial`.

      clock: ``None`` to use the default system-specific monotonic clock;
          otherwise, an object implementing the :class:`trio.abc.Clock`
          interface, like (for example) a :class:`trio.testing.MockClock`
          instance.

      instruments (list of :class:`trio.abc.Instrument` objects): Any
          instrumentation you want to apply to this run. This can also be
          modified during the run; see :ref:`instrumentation`.

      restrict_keyboard_interrupt_to_checkpoints (bool): What happens if the
          user hits control-C while :func:`run` is running? If this argument
          is False (the default), then you get the standard Python behavior: a
          :exc:`KeyboardInterrupt` exception will immediately interrupt
          whatever task is running. If no task is running, or the running
          code is protected against :exc:`KeyboardInterrupt`, then Trio
          instead cancels the main task, as if a cancel scope around it had
          been cancelled, and raises :exc:`KeyboardInterrupt` from
          :func:`run` once everything has shut down. Alternatively, if you
          set this argument to True, then control-C always takes that second
          route, so it only ever interrupts your code at :ref:`checkpoints
          <checkpoints>`, as a :exc:`Cancelled` exception.

          Either way, a second control-C while the first is still being
          handled is absorbed, except that with the default setting it still
          raises :exc:`KeyboardInterrupt` immediately if it arrives while
          unprotected code is running, like any other control-C.

          The default behavior is nice because it means that even if you
          accidentally write an infinite loop that never executes any
          checkpoints, then you can still break out of it using control-C.
          The alternative behavior is nice if you're paranoid about a
          :exc:`KeyboardInterrupt` at just the wrong place leaving your
          program in an inconsistent state, because it means that you only
          have to worry about :exc:`KeyboardInterrupt` at the exact same
          places where you already have to worry about :exc:`Cancelled`.

          This setting has no effect if your program has registered a custom
          SIGINT handler, or if :func:`run` is called from anywhere but the
          main thread (this is a Python limitation), or if you use
          :func:`open_signal_receiver` to catch SIGINT.

      strict_exception_groups (bool): Unless set to False, nurseries will always wrap
          even a single raised exception in an exception group. This can be overridden
          on the level of individual nurseries. Setting it to False will be deprecated
          and ultimately removed in a future version of Trio.

    Returns:
      Whatever ``async_fn`` returns.

    Raises:
      TrioInternalError: if an unexpected error is encountered inside Trio's
          internal machinery. This is a bug and you should `let us know
          <https://github.com/python-trio/trio/issues>`__.

      Anything else: if ``async_fn`` raises an exception, then :func:`run`
          propagates it.

    """
    if strict_exception_groups is not None and not strict_exception_groups:
        warn_deprecated(
            "trio.run(..., strict_exception_groups=False)",
            version="0.25.0",
            issue=2929,
            instead=(
                "the default value of True and rewrite exception handlers to handle ExceptionGroups. "
                "See https://trio.readthedocs.io/en/stable/reference-core.html#designing-for-multiple-errors"
            ),
            use_triodeprecationwarning=True,
        )

    __tracebackhide__ = True

    runner = setup_runner(
        clock,
        instruments,
        restrict_keyboard_interrupt_to_checkpoints,
        strict_exception_groups,
    )

    prev_library, sniffio_library.name = sniffio_library.name, "trio"
    try:
        gen = unrolled_run(runner, init, (runner, async_fn, args))
        # Need to send None in the first time.
        next_send: EventResult = None  # type: ignore[assignment]
        while True:
            try:
                timeout = gen.send(next_send)
            except StopIteration:
                break
            next_send = runner.io_manager.get_events(timeout)
    finally:
        sniffio_library.name = prev_library
    # Inlined copy of runner.main_task_outcome.unwrap() to avoid
    # cluttering every single Trio traceback with an extra frame.
    if isinstance(runner.main_task_outcome, Value):
        return cast("RetT", runner.main_task_outcome.value)
    elif isinstance(runner.main_task_outcome, Error):
        raise runner.main_task_outcome.error
    else:  # pragma: no cover
        raise AssertionError(runner.main_task_outcome)


def start_guest_run(  # type: ignore[explicit-any]
    async_fn: Callable[..., Awaitable[RetT]],
    *args: object,
    run_sync_soon_threadsafe: Callable[[Callable[[], object]], object],
    done_callback: Callable[[outcome.Outcome[RetT]], object],
    run_sync_soon_not_threadsafe: (
        Callable[[Callable[[], object]], object] | None
    ) = None,
    host_uses_signal_set_wakeup_fd: bool = False,
    clock: Clock | None = None,
    instruments: Sequence[Instrument] = (),
    restrict_keyboard_interrupt_to_checkpoints: bool = False,
    strict_exception_groups: bool = True,
) -> None:
    """Start a "guest" run of Trio on top of some other "host" event loop.

    Each host loop can only have one guest run at a time.

    You should always let the Trio run finish before stopping the host loop;
    if not, it may leave Trio's internal data structures in an inconsistent
    state. You might be able to get away with it if you immediately exit the
    program, but it's safest not to go there in the first place.

    Generally, the best way to do this is wrap this in a function that starts
    the host loop and then immediately starts the guest run, and then shuts
    down the host when the guest run completes.

    Once :func:`start_guest_run` returns successfully, the guest run
    has been set up enough that you can invoke sync-colored Trio
    functions such as :func:`~trio.current_time`, :func:`spawn_system_task`,
    and :func:`current_trio_token`. If a `~trio.TrioInternalError` occurs
    during this early setup of the guest run, it will be raised out of
    :func:`start_guest_run`.  All other errors, including all errors
    raised by the *async_fn*, will be delivered to your
    *done_callback* at some point after :func:`start_guest_run` returns
    successfully.

    Args:

      run_sync_soon_threadsafe: An arbitrary callable, which will be passed a
         function as its sole argument::

            def my_run_sync_soon_threadsafe(fn):
                ...

         This callable should schedule ``fn()`` to be run by the host on its
         next pass through its loop. **Must support being called from
         arbitrary threads.**

      done_callback: An arbitrary callable::

            def my_done_callback(run_outcome):
                ...

         When the Trio run has finished, Trio will invoke this callback to let
         you know. The argument is an `outcome.Outcome`, reporting what would
         have been returned or raised by `trio.run`. This function can do
         anything you want, but commonly you'll want it to shut down the
         host loop, unwrap the outcome, etc.

      run_sync_soon_not_threadsafe: Like ``run_sync_soon_threadsafe``, but
         will only be called from inside the host loop's main thread.
         Optional, but if your host loop allows you to implement this more
         efficiently than ``run_sync_soon_threadsafe`` then passing it will
         make things a bit faster.

      host_uses_signal_set_wakeup_fd (bool): Pass `True` if your host loop
         uses `signal.set_wakeup_fd`, and `False` otherwise. For more details,
         see :ref:`guest-run-implementation`.

    For the meaning of other arguments, see `trio.run`.

    """
    if strict_exception_groups is not None and not strict_exception_groups:
        warn_deprecated(
            "trio.start_guest_run(..., strict_exception_groups=False)",
            version="0.25.0",
            issue=2929,
            instead=(
                "the default value of True and rewrite exception handlers to handle ExceptionGroups. "
                "See https://trio.readthedocs.io/en/stable/reference-core.html#designing-for-multiple-errors"
            ),
            use_triodeprecationwarning=True,
        )

    runner = setup_runner(
        clock,
        instruments,
        restrict_keyboard_interrupt_to_checkpoints,
        strict_exception_groups,
    )
    # In guest mode the loop's I/O wait runs on a worker thread while the host
    # loop's thread can change deadlines and reschedule tasks, so the deadlines
    # and the run queue need a LoopWaker (see _task.py) to tell the loop to
    # come back and recompute its timeout: the runner is that waker.
    runner.scheduler.deadlines.loop_waker = runner
    runner.scheduler.loop_waker = runner
    runner.guest_tick_scheduled = True

    if run_sync_soon_not_threadsafe is None:
        run_sync_soon_not_threadsafe = run_sync_soon_threadsafe

    guest_state = GuestState(
        runner=runner,
        run_sync_soon_threadsafe=run_sync_soon_threadsafe,
        run_sync_soon_not_threadsafe=run_sync_soon_not_threadsafe,
        done_callback=done_callback,
        unrolled_run_gen=unrolled_run(
            runner,
            init,
            (runner, async_fn, args),
            host_uses_signal_set_wakeup_fd=host_uses_signal_set_wakeup_fd,
        ),
    )

    # Run a few ticks of the guest run synchronously, so that by the
    # time we return, the system nursery exists and callers can use
    # spawn_system_task. We don't actually run any user code during
    # this time, so it shouldn't be possible to get an exception here,
    # except for a TrioInternalError.
    next_send = cast(
        "EventResult",
        None,
    )  # First iteration must be `None`, every iteration after that is EventResult
    for _tick in range(5):  # expected need is 2 iterations + leave some wiggle room
        if runner.system_nursery is not None:
            # We're initialized enough to switch to async guest ticks
            break
        try:
            timeout = guest_state.unrolled_run_gen.send(next_send)
        except StopIteration:  # pragma: no cover
            raise TrioInternalError(
                "Guest runner exited before system nursery was initialized",
            ) from None
        if timeout != 0:  # pragma: no cover
            guest_state.unrolled_run_gen.throw(
                TrioInternalError(
                    "Guest runner blocked before system nursery was initialized",
                ),
            )
        # next_send should be the return value of
        # IOManager.get_events() if no I/O was waiting, which is
        # platform-dependent. We don't actually check for I/O during
        # this init phase because no one should be expecting any yet.
        if sys.platform == "win32":
            next_send = 0
        else:
            next_send = []
    else:  # pragma: no cover
        guest_state.unrolled_run_gen.throw(
            TrioInternalError(
                "Guest runner yielded too many times before "
                "system nursery was initialized",
            ),
        )

    guest_state.unrolled_run_next_send = Value(next_send)
    run_sync_soon_not_threadsafe(guest_state.guest_tick)
