"""Tasks and the scheduler: the task layer of Trio's core.

The core is layered, top to bottom, as

    _run.py            nurseries, run() and start_guest_run()
    _runner.py         the run loop and the per-run objects around it
    _traps.py          the yield points: checkpoints, detaching, and the
                       wait_task_rescheduled shim
    _wake_token.py     WakeToken, what a task sleeps on
    _cancel_scope.py   CancelScope, the user's handle on the cancellation tree
    _task.py           tasks and the Scheduler
    _cancel_status.py  the cancellation tree and its deadlines
    _run_context.py    the thread-local record of the current run

with _instrumentation.py standalone below this module. Each module imports
only from the ones below it, so the runner knows nothing of nurseries,
this module nothing of nurseries, cancel scopes, wake tokens or the loop,
and the cancellation tree nothing of scopes or tasks: where a lower layer
has to call up, it does so through a contract it defines and the upper
layer implements (`TaskOwner` and `CancelTarget` here, `CancelStatus.Scope`,
`CancelStatus.Member`, `Deadlines.Scope` and `LoopWaker` in
``_cancel_status.py``), and a Task's parent nursery is a slot that only
the nursery layer reads. This module owns the run's task-level state, the
`Scheduler` (the run queue, the living tasks, the deadlines, the clock and
the instruments, with spawning and reaping), which the runner creates and
drives and which every task refers to.
"""

from __future__ import annotations

import functools
import gc
import itertools
from collections import deque
from contextvars import copy_context
from math import inf
from typing import (
    TYPE_CHECKING,
    Any,
    Final,
    Protocol,
    overload,
)

import attrs
from outcome import Error

from .._util import NoPublicConstructor, coroutine_or_error, final
from ._cancel_status import CancelReason, CancelStatus, Deadlines, LoopWaker
from ._run_context import GLOBAL_RUN_CONTEXT

if TYPE_CHECKING:
    import contextvars
    import types
    from collections.abc import Awaitable, Callable, Iterator

    from outcome import Outcome
    from typing_extensions import TypeVar, TypeVarTuple, Unpack

    from .._abc import Clock
    from ._instrumentation import Instruments
    from ._run import Nursery

    PosArgT = TypeVarTuple("PosArgT")
    StatusT = TypeVar("StatusT", default=None)
    StatusT_contra = TypeVar("StatusT_contra", contravariant=True, default=None)
    BaseExcT = TypeVar("BaseExcT", bound=BaseException)
else:
    from typing import TypeVar

    StatusT = TypeVar("StatusT")
    StatusT_contra = TypeVar("StatusT_contra", contravariant=True)

RetT = TypeVar("RetT")


# Decorator to mark methods public. This does nothing by itself, but
# trio/_tools/gen_exports.py looks for it.
def _public(fn: RetT) -> RetT:
    return fn


MISNESTING_ADVICE = """
This is probably a bug in your code, that has caused Trio's internal state to
become corrupted. We'll do our best to recover, but from now on there are
no guarantees.

Typically this is caused by one of the following:
  - yielding within a generator or async generator that's opened a cancel
    scope or nursery (unless the generator is a @contextmanager or
    @asynccontextmanager); see https://github.com/python-trio/trio/issues/638
  - manually calling __enter__ or __exit__ on a trio.CancelScope, or
    __aenter__ or __aexit__ on the object returned by trio.open_nursery();
    doing so correctly is difficult and you should use @[async]contextmanager
    instead, or maybe [Async]ExitStack
  - using [Async]ExitStack to interleave the entries/exits of cancel scopes
    and/or nurseries in a way that couldn't be achieved by some nesting of
    'with' and 'async with' blocks
  - using the low-level coroutine object protocol to execute some parts of
    an async function in a different cancel scope/nursery context than
    other parts
If you don't believe you're doing any of these things, please file a bug:
https://github.com/python-trio/trio/issues/new
"""


################################################################
# The task_status protocol (nursery.start() lives in _run.py)
################################################################


class TaskStatus(Protocol[StatusT_contra]):
    """The interface provided by :meth:`Nursery.start()` to the spawned task.

    This is provided via the ``task_status`` keyword-only parameter.
    """

    @overload
    def started(self: TaskStatus[None]) -> None: ...

    @overload
    def started(self, value: StatusT_contra) -> None: ...

    def started(self, value: StatusT_contra | None = None) -> None:
        """Tasks call this method to indicate that they have initialized.

        See `nursery.start() <trio.Nursery.start>` for more information.
        """


################################################################
# What the layers above implement for this one
################################################################


class TaskOwner(Protocol):
    """What a task belongs to, as the runner sees it.

    A task's owner is told when the task has been spawned and when it has
    finished, and says whether it is still accepting tasks. Normally the
    owner is the task's nursery; the runner is the owner of the root
    ("<init>") task, and the main task's owner is a wrapper around its
    nursery that keeps the task's outcome for `trio.run` to return (see
    ``_run.py``). Defining the contract down here, in the layer that calls
    it, is what lets tasks be spawned and reaped without this layer or the
    runner ever naming a nursery.

    The members are underscore-prefixed because the classes implementing
    them are public, and the contract is not part of their user-facing API.
    """

    @property
    def _closed(self) -> bool:
        """True once the owner has stopped accepting new tasks."""
        ...

    def _child_spawned(self, task: Task) -> None:
        """Called with a newly created task, before the task has run or any
        instrument has seen it: the owner records it, and puts it under
        whatever cancel scopes it inherits (for a nursery, by activating
        the nursery's cancel status for it)."""
        ...

    def _child_finished(self, task: Task, outcome: Outcome[object]) -> None:
        """Called once the task's coroutine has finished with ``outcome`` (or
        the task has been permanently detached), after its exit callbacks
        have run and it has been removed from its cancel scope and from
        the set of living tasks."""
        ...


class CancelTarget(Protocol):
    """What a sleeping task hands a cancellation to: the WakeToken it is
    awaiting, as this layer sees it.

    When a task's cancel status becomes cancelled while the task is
    blocked, the task calls ``_request_cancel`` on whatever it is sleeping
    on, at most once per sleep (``_cancel_requested`` says whether it has).
    What happens next is the token's business: see WakeToken.on_cancel.

    Like TaskOwner's, the members are underscore-prefixed because WakeToken
    is public and they are not part of its user-facing API.
    """

    @property
    def _cancel_requested(self) -> bool: ...

    def _request_cancel(self, reason: CancelReason) -> None: ...


################################################################
# Task and friends
################################################################


@final
@attrs.define(eq=False, repr=False)
class Task(metaclass=NoPublicConstructor):  # type: ignore[explicit-any]
    # Who the task reports to; see TaskOwner.
    _owner: TaskOwner
    # The run the task belongs to, for it and for anything it blocks on or
    # enters to reach the run queue, the deadlines and the clock without the
    # thread-local; see Scheduler.
    _scheduler: Scheduler
    coro: types.CoroutineType[Any, Outcome[object], Any]  # type: ignore[explicit-any]
    name: str
    context: contextvars.Context
    _counter: int = attrs.field(init=False, factory=itertools.count().__next__)
    _ki_protected: bool

    # Invariant:
    # - for unscheduled tasks, _next_send_fn and _next_send are both None
    # - for scheduled tasks, _next_send_fn(_next_send) resumes the task;
    #   usually _next_send_fn is self.coro.send and _next_send is an
    #   Outcome. When recovering from a foreign await, _next_send_fn is
    #   self.coro.throw and _next_send is an exception. _next_send_fn
    #   will effectively be at the top of every task's call stack, so
    #   it should be written in C if you don't want to pollute Trio
    #   tracebacks with extraneous frames.
    # - for scheduled tasks, custom_sleep_data is None
    # - _sleeping_on is what the task is blocked on (a WakeToken, awaited
    #   directly or via wait_task_rescheduled), seen as the CancelTarget it
    #   is to this layer, or None once the task has been rescheduled
    # Tasks start out unscheduled.
    _next_send_fn: Callable[[Any], object] | None = None  # type: ignore[explicit-any]
    _next_send: Outcome[Any] | BaseException | None = None  # type: ignore[explicit-any]
    _sleeping_on: CancelTarget | None = None
    custom_sleep_data: Any = None  # type: ignore[explicit-any]

    # The nurseries this task is in and contains. These are for introspection
    # and nursery.start(): only the nursery layer reads them (this layer and
    # the runner report to the owner, above, instead). None for the root task.
    _parent_nursery: Nursery | None = None
    _child_nurseries: list[Nursery] = attrs.Factory(list)
    _eventual_parent_nursery: Nursery | None = None

    # See add_task_exit_callback(); None once the task has exited.
    _exit_callbacks: list[Callable[[Task], object]] | None = attrs.Factory(list)

    # these are counts of how many cancel/schedule points this task has
    # executed, for assert{_no,}_checkpoints
    # XX maybe these should be exposed as part of a statistics() method?
    _cancel_points: int = 0
    _schedule_points: int = 0

    def __repr__(self) -> str:
        return f"<Task {self.name!r} at {id(self):#x}>"

    @property
    def parent_nursery(self) -> Nursery | None:
        """The nursery this task is inside (or None if this is the "init"
        task).

        Example use case: drawing a visualization of the task tree in a
        debugger.

        """
        return self._parent_nursery

    @property
    def eventual_parent_nursery(self) -> Nursery | None:
        """The nursery this task will be inside after it calls
        ``task_status.started()``.

        If this task has already called ``started()``, or if it was not
        spawned using `nursery.start() <trio.Nursery.start>`, then
        its `eventual_parent_nursery` is ``None``.

        """
        return self._eventual_parent_nursery

    @property
    def child_nurseries(self) -> list[Nursery]:
        """The nurseries this task contains.

        This is a list, with outer nurseries before inner nurseries.

        """
        return list(self._child_nurseries)

    def iter_await_frames(self) -> Iterator[tuple[types.FrameType, int]]:
        """Iterates recursively over the coroutine-like objects this
        task is waiting on, yielding the frame and line number at each
        frame.

        This is similar to `traceback.walk_stack` in a synchronous
        context. Note that `traceback.walk_stack` returns frames from
        the bottom of the call stack to the top, while this function
        starts from `Task.coro <trio.lowlevel.Task.coro>` and works it
        way down.

        Example usage: extracting a stack trace::

            import traceback

            def print_stack_for_task(task):
                ss = traceback.StackSummary.extract(task.iter_await_frames())
                print("".join(ss.format()))

        """
        # Ignore static typing as we're doing lots of dynamic introspection
        coro: Any = self.coro  # type: ignore[explicit-any]
        while coro is not None:
            if hasattr(coro, "cr_frame"):
                # A real coroutine
                if cr_frame := coro.cr_frame:  # None if the task has finished
                    yield cr_frame, cr_frame.f_lineno
                coro = coro.cr_await
            elif hasattr(coro, "gi_frame"):
                # A generator decorated with @types.coroutine
                if gi_frame := coro.gi_frame:  # pragma: no branch
                    yield gi_frame, gi_frame.f_lineno  # pragma: no cover
                coro = coro.gi_yieldfrom
            elif coro.__class__.__name__ in [
                "async_generator_athrow",
                "async_generator_asend",
            ]:
                # cannot extract the generator directly, see https://github.com/python/cpython/issues/76991
                # we can however use the gc to look through the object
                for referent in gc.get_referents(coro):
                    if hasattr(referent, "ag_frame"):  # pragma: no branch
                        yield referent.ag_frame, referent.ag_frame.f_lineno
                        coro = referent.ag_await
                        break
                else:  # pragma: no cover
                    # either cpython changed or we are running on an alternative python implementation
                    return
            else:  # pragma: no cover
                return

    ################
    # Cancellation
    ################

    # The CancelStatus object that is currently active for this task.
    # Don't change this directly; instead, use _activate_cancel_status().
    # This can be None, but only in the init task.
    _cancel_status: CancelStatus = attrs.field(default=None, repr=False)

    def _activate_cancel_status(self, cancel_status: CancelStatus | None) -> None:
        if self._cancel_status is not None:
            self._cancel_status._tasks.remove(self)
        self._cancel_status = cancel_status  # type: ignore[assignment]
        if self._cancel_status is not None:
            self._cancel_status._tasks.add(self)
            if self._cancel_status.effectively_cancelled:
                self._attempt_delivery_of_any_pending_cancel()

    def _attempt_delivery_of_any_pending_cancel(self) -> None:
        # If the task is sleeping, hand the cancellation to the token it's
        # sleeping on (see CancelTarget). Its on_cancel either delivers it
        # (by rescheduling the token with it), or declines, in which case the
        # token will be rescheduled by "someone" later. Either way, we only
        # request cancellation once per blocking call.
        token = self._sleeping_on
        if token is None or token._cancel_requested:
            return
        if not self._cancel_status.effectively_cancelled:
            return

        reason = self._cancel_status._scope._cancel_reason
        if reason is None:
            reason = CancelReason(source="unknown", reason="misnesting")
        token._request_cancel(reason)


################################################################
# The scheduler
################################################################


@attrs.define(eq=False)
class Scheduler:
    """The run's task-level state: the run queue and the set of living
    tasks, the deadlines, the clock and the instruments, plus spawning and
    reaping tasks.

    There is one per run. The runner (``_runner.py``) creates it and drives
    it: the loop that takes tasks off the run queue and waits for I/O lives
    there. Every task refers to its run's scheduler (``Task._scheduler``),
    so that a task, a cancel scope entered in it, a WakeToken it sleeps on
    or a nursery it opened can reach the run without going through the
    thread-local ``GLOBAL_RUN_CONTEXT``.
    """

    clock: Clock
    deadlines: Deadlines
    instruments: Instruments
    # The context that system tasks start from.
    system_context: contextvars.Context
    run_queue: deque[Task] = attrs.Factory(deque)
    tasks: set[Task] = attrs.Factory(set)
    # Woken when the first task becomes runnable, if set: see LoopWaker.
    loop_waker: LoopWaker | None = None
    # If the run goes idle for this long (in real seconds), the loop calls
    # clock._autojump(). Set by MockClock (its autojump_threshold) and read
    # by the loop: the one piece of the loop's idle handling that lives
    # outside the loop, here so that the clock needn't know the runner.
    clock_autojump_threshold: float = inf

    def spawn(
        self,
        async_fn: Callable[[Unpack[PosArgT]], Awaitable[object]],
        args: tuple[Unpack[PosArgT]],
        owner: TaskOwner,
        name: object,
        *,
        system_task: bool = False,
        context: contextvars.Context | None = None,
    ) -> Task:
        ######
        # Make sure the owner is in working order
        ######

        if owner._closed:
            raise RuntimeError("Nursery is closed to new arrivals")

        ######
        # Propagate contextvars
        ######
        if context is None:
            context = self.system_context.copy() if system_task else copy_context()

        ######
        # Call the function and get the coroutine object, while giving helpful
        # errors for common mistakes.
        ######
        # TypeVarTuple passed into ParamSpec function confuses Mypy.
        coro = context.run(coroutine_or_error, async_fn, *args)  # type: ignore[arg-type]

        if name is None:
            name = async_fn
        if isinstance(name, functools.partial):
            name = name.func
        if not isinstance(name, str):
            try:
                name = f"{name.__module__}.{name.__qualname__}"  # type: ignore[attr-defined]
            except AttributeError:
                name = repr(name)

        # very old Cython versions (<0.29.24) has the attribute, but with a value of None
        if getattr(coro, "cr_frame", None) is None:
            # This async function is implemented in C or Cython
            async def python_wrapper(orig_coro: Awaitable[RetT]) -> RetT:
                return await orig_coro

            coro = python_wrapper(coro)
        assert coro.cr_frame is not None, "Coroutine frame should exist"  # type: ignore[attr-defined]

        ######
        # Set up the Task object
        ######
        task = Task._create(
            coro=coro,
            owner=owner,
            scheduler=self,
            name=name,
            context=context,
            ki_protected=system_task,
        )

        self.tasks.add(task)
        owner._child_spawned(task)

        if "task_spawned" in self.instruments:
            self.instruments.call("task_spawned", task)
        # Special case: normally next_send should be an Outcome, but for the
        # very first send we have to send a literal unboxed None.
        self.enqueue(task, None)
        return task

    def enqueue(
        self,
        task: Task,
        next_send: Outcome[object] | BaseException | None,
    ) -> None:
        # Make a task runnable, sending it next_send when it next runs. This
        # is the low-level operation behind WakeToken.reschedule(): anything
        # that wakes a sleeping task goes through its token (reschedule()
        # included), so that the token's bookkeeping stays in step. The other
        # callers here are starting a task, or waking one that isn't sleeping
        # on a token at all.
        assert task._next_send_fn is None
        task._next_send_fn = task.coro.send
        task._next_send = next_send
        task._sleeping_on = None
        task.custom_sleep_data = None
        if not self.run_queue and self.loop_waker is not None:
            self.loop_waker.wake()
        self.run_queue.append(task)
        if "task_scheduled" in self.instruments:
            self.instruments.call("task_scheduled", task)

    def task_exited(self, task: Task, outcome: Outcome[object]) -> None:
        # Run the task's exit callbacks (see add_task_exit_callback). Setting
        # the list to None first is what marks the task as exited.
        exit_callbacks = task._exit_callbacks
        task._exit_callbacks = None
        assert exit_callbacks is not None
        for callback in exit_callbacks:
            callback(task)

        if (
            task._cancel_status is not None
            and task._cancel_status.abandoned_by_misnesting
            and task._cancel_status.parent is None
        ):
            # The cancel scope surrounding this task's nursery was closed
            # before the task exited. Force the task to exit with an error,
            # since the error might not have been caught elsewhere. See the
            # comments in CancelStatus.close(). (The matching check for a
            # nursery that the task left open is made by the nursery that
            # owns the task, in the layer that knows what a nursery is.)
            try:
                # Raise this, rather than just constructing it, to get a
                # traceback frame included
                raise RuntimeError(
                    "Cancel scope stack corrupted: Cancel scope surrounding "
                    f"{task!r} was closed before the task exited\n{MISNESTING_ADVICE}",
                )
            except RuntimeError as new_exc:
                if isinstance(outcome, Error):
                    new_exc.__context__ = outcome.error
                outcome = Error(new_exc)

        task._activate_cancel_status(None)
        self.tasks.remove(task)
        task._owner._child_finished(task, outcome)

        if "task_exited" in self.instruments:
            self.instruments.call("task_exited", task)


################################################################
# Other public API functions
################################################################


class _TaskStatusIgnored(TaskStatus[object]):
    def __repr__(self) -> str:
        return "TASK_STATUS_IGNORED"

    def started(self, value: object = None) -> None:
        pass


TASK_STATUS_IGNORED: Final[TaskStatus[object]] = _TaskStatusIgnored()


def add_task_exit_callback(task: Task, callback: Callable[[Task], object]) -> None:
    """Arrange for ``callback(task)`` to be called when ``task`` exits.

    The callback runs inside Trio's run loop, with :exc:`KeyboardInterrupt`
    protection, as soon as the task's coroutine has finished and before the
    task's parent nursery is told that it's done. It must not block, and it
    must not raise: an exception from it would crash the whole run.

    This is a low-level hook for bookkeeping that has to happen no matter how
    a task exits, such as breaking a parking lot that the task was holding
    (see :func:`add_parking_lot_breaker`). It's not a general-purpose
    "finally" clause: regular code should use ``try``/``finally`` or a
    context manager inside the task instead.

    The same callback can be registered any number of times for the same
    task, and is then called once per registration; each registration can
    be withdrawn with :func:`remove_task_exit_callback`.

    Raises:
      RuntimeError: if the task has already exited.

    """
    if task._exit_callbacks is None:
        raise RuntimeError(f"{task!r} has already exited")
    task._exit_callbacks.append(callback)


def remove_task_exit_callback(task: Task, callback: Callable[[Task], object]) -> None:
    """Withdraw one registration made by :func:`add_task_exit_callback`.

    Raises:
      RuntimeError: if the callback is not registered for the task. (This
          includes the case where the task has already exited, since by
          then the callback has been called and the registration is gone.)

    """
    try:
        if task._exit_callbacks is None:
            raise ValueError
        task._exit_callbacks.remove(callback)
    except ValueError:
        raise RuntimeError(
            f"{callback!r} is not registered as an exit callback for {task!r}",
        ) from None


def current_task() -> Task:
    """Return the :class:`Task` object representing the current task.

    Returns:
      Task: the :class:`Task` that called :func:`current_task`.

    """

    try:
        return GLOBAL_RUN_CONTEXT.task
    except AttributeError:
        raise RuntimeError("must be called from async context") from None


def current_effective_deadline() -> float:
    """Returns the current effective deadline for the current task.

    This function examines all the cancellation scopes that are currently in
    effect (taking into account shielding), and returns the deadline that will
    expire first.

    One example of where this might be is useful is if your code is trying to
    decide whether to begin an expensive operation like an RPC call, but wants
    to skip it if it knows that it can't possibly complete in the available
    time. Another example would be if you're using a protocol like gRPC that
    `propagates timeout information to the remote peer
    <http://www.grpc.io/docs/guides/concepts.html#deadlines>`__; this function
    gives a way to fetch that information so you can send it along.

    If this is called in a context where a cancellation is currently active
    (i.e., a blocking call will immediately raise :exc:`Cancelled`), then
    returned deadline is ``-inf``. If it is called in a context where no
    scopes have a deadline set, it returns ``inf``.

    Returns:
        float: the effective deadline, as an absolute time.

    """
    return current_task()._cancel_status.effective_deadline()


def in_trio_task() -> bool:
    """Check whether we are in a Trio task.
    This returns `True` if and only if :func:`~trio.lowlevel.current_task` will succeed.

    See also the discussion of differing ways of :ref:`detecting Trio <trio_contexts>`.
    """
    return hasattr(GLOBAL_RUN_CONTEXT, "task")
