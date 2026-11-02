"""Tasks, cancellation and the scheduler: the bottom layer of Trio's core.

The core is layered, top to bottom, as

    _run.py           nurseries, run() and start_guest_run()
    _runner.py        the run loop and the per-run objects around it
    _traps.py         the yield points: checkpoints, detaching, and the
                      wait_task_rescheduled shim
    _wake_token.py    WakeToken, what a task sleeps on
    _task.py          tasks, cancel scopes, deadlines, and the Scheduler
    _run_context.py   the thread-local record of the current run

with _instrumentation.py standalone below this module. Each module imports
only from the ones below it, so the runner knows nothing of nurseries,
and this module nothing of nurseries, wake tokens or the loop: where a
lower layer has to call up, it does so through a contract defined down
here that the upper layer implements (`TaskOwner`, `CancelTarget`,
`LoopWaker`), and a Task's parent nursery is a slot that only the nursery
layer reads. This module owns the run's task-level state, the `Scheduler`
(the run queue, the living tasks, the deadlines, the clock and the
instruments, with spawning and reaping), which the runner creates and
drives and which every task refers to.
"""

from __future__ import annotations

import functools
import gc
import itertools
import sys
from collections import deque
from contextlib import contextmanager
from contextvars import copy_context
from heapq import heapify, heappop, heappush
from math import inf, isnan
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
from ._concat_tb import concat_tb
from ._exceptions import Cancelled, CancelReasonLiteral
from ._ki import enable_ki_protection
from ._run_context import GLOBAL_RUN_CONTEXT

if sys.version_info < (3, 11):
    from exceptiongroup import BaseExceptionGroup


if TYPE_CHECKING:
    import contextvars
    import types
    from collections.abc import Awaitable, Callable, Iterator
    from types import TracebackType

    from outcome import Outcome
    from typing_extensions import Self, TypeVar, TypeVarTuple, Unpack

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


def _current_time() -> float:
    # For the cancel scope properties, which can be used from outside any
    # task: without a task in hand, the clock is reached through the
    # thread-local.
    return GLOBAL_RUN_CONTEXT.scheduler.clock.current_time()


DEADLINE_HEAP_MIN_PRUNE_THRESHOLD: Final = 1000

# Used to track if an exceptiongroup can be collapsed
NONSTRICT_EXCEPTIONGROUP_NOTE = 'This is a "loose" ExceptionGroup, and may be collapsed by Trio if it only contains one exception - typically after `Cancelled` has been stripped from it. Note this has consequences for exception handling, and strict_exception_groups=True is recommended.'


# Decorator to mark methods public. This does nothing by itself, but
# trio/_tools/gen_exports.py looks for it.
def _public(fn: RetT) -> RetT:
    return fn


################################################################
# CancelScope and friends
################################################################


def collapse_exception_group(
    excgroup: BaseExceptionGroup[BaseException],
) -> BaseException:
    """Recursively collapse any single-exception groups into that single contained
    exception.

    """
    exceptions = list(excgroup.exceptions)
    modified = False
    for i, exc in enumerate(exceptions):
        if isinstance(exc, BaseExceptionGroup):
            new_exc = collapse_exception_group(exc)
            if new_exc is not exc:
                modified = True
                exceptions[i] = new_exc

    if (
        len(exceptions) == 1
        and isinstance(excgroup, BaseExceptionGroup)
        and NONSTRICT_EXCEPTIONGROUP_NOTE in getattr(excgroup, "__notes__", ())
    ):
        exceptions[0].__traceback__ = concat_tb(
            excgroup.__traceback__,
            exceptions[0].__traceback__,
        )
        return exceptions[0]
    elif modified:
        return excgroup.derive(exceptions)
    else:
        return excgroup


@attrs.define(eq=False)
class Deadlines:
    """A container of deadlined cancel scopes.

    Only contains scopes with non-infinite deadlines that are currently
    attached to at least one task.

    """

    # Heap of (deadline, id(CancelScope), CancelScope)
    _heap: list[tuple[float, int, CancelScope]] = attrs.Factory(list)
    # Count of active deadlines (those that haven't been changed)
    _active: int = 0
    # Woken whenever the next deadline changes, if set: see LoopWaker.
    loop_waker: LoopWaker | None = None

    def add(self, deadline: float, cancel_scope: CancelScope) -> None:
        heappush(self._heap, (deadline, id(cancel_scope), cancel_scope))
        self._active += 1

    def remove(self, deadline: float, cancel_scope: CancelScope) -> None:
        self._active -= 1

    def change(self, cancel_scope: CancelScope, old: float, new: float) -> None:
        """Move ``cancel_scope``'s registered deadline from ``old`` to ``new``
        (either may be ``inf``, meaning no deadline)."""
        if self.loop_waker is not None:
            old_next_deadline = self.next_deadline()
        if old != inf:
            self.remove(old, cancel_scope)
        if new != inf:
            self.add(new, cancel_scope)
        if self.loop_waker is not None and old_next_deadline != self.next_deadline():
            self.loop_waker.wake()

    def next_deadline(self) -> float:
        while self._heap:
            deadline, _, cancel_scope = self._heap[0]
            if deadline == cancel_scope._registered_deadline:
                return deadline
            else:
                # This entry is stale; discard it and try again
                heappop(self._heap)
        return inf

    def _prune(self) -> None:
        # In principle, it's possible for a cancel scope to toggle back and
        # forth repeatedly between the same two deadlines, and end up with
        # lots of stale entries that *look* like they're still active, because
        # their deadline is correct, but in fact are redundant. So when
        # pruning we have to eliminate entries with the wrong deadline, *and*
        # eliminate duplicates.
        seen = set()
        pruned_heap = []
        for deadline, tiebreaker, cancel_scope in self._heap:
            if deadline == cancel_scope._registered_deadline:
                if cancel_scope in seen:
                    continue
                seen.add(cancel_scope)
                pruned_heap.append((deadline, tiebreaker, cancel_scope))
        # See test_cancel_scope_deadline_duplicates for a test that exercises
        # this assert:
        assert len(pruned_heap) == self._active
        heapify(pruned_heap)
        self._heap = pruned_heap

    def expire(self, now: float) -> bool:
        did_something = False
        while self._heap and self._heap[0][0] <= now:
            deadline, _, cancel_scope = heappop(self._heap)
            if deadline == cancel_scope._registered_deadline:
                did_something = True
                # This implicitly calls self.remove(), so we don't need to
                # decrement _active here
                cancel_scope._cancel(CancelReason(source="deadline"))
        # If we've accumulated too many stale entries, then prune the heap to
        # keep it under control. (We only do this occasionally in a batch, to
        # keep the amortized cost down)
        if len(self._heap) > self._active * 2 + DEADLINE_HEAP_MIN_PRUNE_THRESHOLD:
            self._prune()
        return did_something


@attrs.define
class CancelReason:
    """Attached to a :class:`CancelScope` upon cancellation with details of the source of the
    cancellation, which is then used to construct the string in a :exc:`Cancelled`.
    Users can pass a ``reason`` str to :meth:`CancelScope.cancel` to set it.

    Not publicly exported or documented.
    """

    source: CancelReasonLiteral
    source_task: str | None = None
    reason: str | None = None


@attrs.define(eq=False)
class CancelStatus:
    """Tracks the cancellation status for a contiguous extent
    of code that will become cancelled, or not, as a unit.

    Each task has at all times a single "active" CancelStatus whose
    cancellation state determines whether checkpoints executed in that
    task raise Cancelled. Each 'with CancelScope(...)' context is
    associated with a particular CancelStatus.  When a task enters
    such a context, a CancelStatus is created which becomes the active
    CancelStatus for that task; when the 'with' block is exited, the
    active CancelStatus for that task goes back to whatever it was
    before.

    CancelStatus objects are arranged in a tree whose structure
    mirrors the lexical nesting of the cancel scope contexts.  When a
    CancelStatus becomes cancelled, it notifies all of its direct
    children, who become cancelled in turn (and continue propagating
    the cancellation down the tree) unless they are shielded. (There
    will be at most one such child except in the case of a
    CancelStatus that immediately encloses a nursery.) At the leaves
    of this tree are the tasks themselves, which get woken up to deliver
    an abort when their direct parent CancelStatus becomes cancelled.

    You can think of CancelStatus as being responsible for the
    "plumbing" of cancellations as opposed to CancelScope which is
    responsible for the origination of them.

    """

    # Our associated cancel scope. Can be any object with attributes
    # `deadline`, `shield`, and `cancel_called`, but in current usage
    # is always a CancelScope object. Must not be None.
    _scope: CancelScope = attrs.field(alias="scope")

    # True iff the tasks in self._tasks should receive cancellations
    # when they checkpoint. Always True when scope.cancel_called is True;
    # may also be True due to a cancellation propagated from our
    # parent.  Unlike scope.cancel_called, this does not necessarily stay
    # true once it becomes true. For example, we might become
    # effectively cancelled due to the cancel scope two levels out
    # becoming cancelled, but then the cancel scope one level out
    # becomes shielded so we're not effectively cancelled anymore.
    effectively_cancelled: bool = False

    # The CancelStatus whose cancellations can propagate to us; we
    # become effectively cancelled when they do, unless scope.shield
    # is True.  May be None (for the outermost CancelStatus in a call
    # to trio.run(), briefly during TaskStatus.started(), or during
    # recovery from misnesting of cancel scopes).
    _parent: CancelStatus | None = attrs.field(default=None, repr=False, alias="parent")

    # All of the CancelStatuses that have this CancelStatus as their parent.
    _children: set[CancelStatus] = attrs.field(factory=set, init=False, repr=False)

    # Tasks whose cancellation state is currently tied directly to
    # the cancellation state of this CancelStatus object. Don't modify
    # this directly; instead, use Task._activate_cancel_status().
    # Invariant: all(task._cancel_status is self for task in self._tasks)
    _tasks: set[Task] = attrs.field(factory=set, init=False, repr=False)

    # Set to True on still-active cancel statuses that are children
    # of a cancel status that's been closed. This is used to permit
    # recovery from misnested cancel scopes (well, at least enough
    # recovery to show a useful traceback).
    abandoned_by_misnesting: bool = attrs.field(default=False, init=False, repr=False)

    def __attrs_post_init__(self) -> None:
        if self._parent is not None:
            self._parent._children.add(self)
            self.recalculate()

    # parent/children/tasks accessors are used by TaskStatus.started()

    @property
    def parent(self) -> CancelStatus | None:
        return self._parent

    @parent.setter
    def parent(self, parent: CancelStatus | None) -> None:
        if self._parent is not None:
            self._parent._children.remove(self)
        self._parent = parent
        if self._parent is not None:
            self._parent._children.add(self)
            self.recalculate()

    @property
    def children(self) -> frozenset[CancelStatus]:
        return frozenset(self._children)

    @property
    def tasks(self) -> frozenset[Task]:
        return frozenset(self._tasks)

    def encloses(self, other: CancelStatus | None) -> bool:
        """Returns true if this cancel status is a direct or indirect
        parent of cancel status *other*, or if *other* is *self*.
        """
        while other is not None:
            if other is self:
                return True
            other = other.parent
        return False

    def close(self) -> None:
        self.parent = None  # now we're not a child of self.parent anymore
        if self._tasks or self._children:
            # Cancel scopes weren't exited in opposite order of being
            # entered. CancelScope._close() deals with raising an error
            # if appropriate; our job is to leave things in a reasonable
            # state for unwinding our dangling children. We choose to leave
            # this part of the CancelStatus tree unlinked from everyone
            # else, cancelled, and marked so that exiting a CancelScope
            # within the abandoned subtree doesn't affect the active
            # CancelStatus. Note that it's possible for us to get here
            # without CancelScope._close() raising an error, if a
            # nursery's cancel scope is closed within the nursery's
            # nested child and no other cancel scopes are involved,
            # but in that case task_exited() will deal with raising
            # the error.
            self._mark_abandoned()

            # Since our CancelScope is about to forget about us, and we
            # have no parent anymore, there's nothing left to call
            # recalculate(). So, we can stay cancelled by setting
            # effectively_cancelled and updating our children.
            self.effectively_cancelled = True
            for task in self._tasks:
                task._attempt_delivery_of_any_pending_cancel()
            for child in self._children:
                child.recalculate()

    @property
    def parent_cancellation_is_visible_to_us(self) -> bool:
        return (
            self._parent is not None
            and not self._scope.shield
            and self._parent.effectively_cancelled
        )

    def recalculate(self) -> None:
        # This does a depth-first traversal over this and descendent cancel
        # statuses, to ensure their state is up-to-date. It's basically a
        # recursive algorithm, but we use an explicit stack to avoid any
        # issues with stack overflow.
        todo = [self]
        while todo:
            current = todo.pop()
            new_state = (
                current._scope.cancel_called
                or current.parent_cancellation_is_visible_to_us
            )
            if new_state != current.effectively_cancelled:
                if (
                    current._scope._cancel_reason is None
                    and current.parent_cancellation_is_visible_to_us
                ):
                    assert current._parent is not None
                    current._scope._cancel_reason = (
                        current._parent._scope._cancel_reason
                    )
                current.effectively_cancelled = new_state
                if new_state:
                    for task in current._tasks:
                        task._attempt_delivery_of_any_pending_cancel()
                todo.extend(current._children)

    def _mark_abandoned(self) -> None:
        self.abandoned_by_misnesting = True
        for child in self._children:
            child._mark_abandoned()

    def effective_deadline(self) -> float:
        if self.effectively_cancelled:
            return -inf
        if self._parent is None or self._scope.shield:
            return self._scope.deadline
        return min(self._scope.deadline, self._parent.effective_deadline())


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


@final
@attrs.define(eq=False, repr=False)
class CancelScope:
    """A *cancellation scope*: the link between a unit of cancellable
    work and Trio's cancellation system.

    A :class:`CancelScope` becomes associated with some cancellable work
    when it is used as a context manager surrounding that work::

        cancel_scope = trio.CancelScope()
        ...
        with cancel_scope:
            await long_running_operation()

    Inside the ``with`` block, a cancellation of ``cancel_scope`` (via
    a call to its :meth:`cancel` method or via the expiry of its
    :attr:`deadline`) will immediately interrupt the
    ``long_running_operation()`` by raising :exc:`Cancelled` at its
    next :ref:`checkpoint <checkpoints>`.

    The context manager ``__enter__`` returns the :class:`CancelScope`
    object itself, so you can also write ``with trio.CancelScope() as
    cancel_scope:``.

    If a cancel scope becomes cancelled before entering its ``with`` block,
    the :exc:`Cancelled` exception will be raised at the first
    checkpoint inside the ``with`` block. This allows a
    :class:`CancelScope` to be created in one :ref:`task <tasks>` and
    passed to another, so that the first task can later cancel some work
    inside the second.

    Cancel scopes are not reusable or reentrant; that is, each cancel
    scope can be used for at most one ``with`` block.  (You'll get a
    :exc:`RuntimeError` if you violate this rule.)

    The :class:`CancelScope` constructor takes initial values for the
    cancel scope's :attr:`deadline` and :attr:`shield` attributes; these
    may be freely modified after construction, whether or not the scope
    has been entered yet, and changes take immediate effect.
    """

    _cancel_status: CancelStatus | None = attrs.field(default=None, init=False)
    _has_been_entered: bool = attrs.field(default=False, init=False)
    # The run's deadline registry, from the task that entered the scope;
    # None before entering and after exiting, when there's nothing to
    # register.
    _deadlines: Deadlines | None = attrs.field(default=None, init=False)
    _registered_deadline: float = attrs.field(default=inf, init=False)
    _cancel_called: bool = attrs.field(default=False, init=False)
    cancelled_caught: bool = attrs.field(default=False, init=False)

    _cancel_reason: CancelReason | None = attrs.field(default=None, init=False)

    # Constructor arguments:
    _relative_deadline: float = attrs.field(
        default=inf,
        kw_only=True,
        alias="relative_deadline",
    )
    _deadline: float = attrs.field(default=inf, kw_only=True, alias="deadline")
    _shield: bool = attrs.field(default=False, kw_only=True, alias="shield")

    def __attrs_post_init__(self) -> None:
        if isnan(self._deadline):
            raise ValueError("deadline must not be NaN")
        if isnan(self._relative_deadline):
            raise ValueError("relative deadline must not be NaN")
        if self._relative_deadline < 0:
            raise ValueError("timeout must be non-negative")
        if self._relative_deadline != inf and self._deadline != inf:
            raise ValueError(
                "Cannot specify both a deadline and a relative deadline",
            )

    @enable_ki_protection
    def __enter__(self) -> Self:
        task = current_task()
        if self._has_been_entered:
            raise RuntimeError(
                "Each CancelScope may only be used for a single 'with' block",
            )
        self._has_been_entered = True
        self._deadlines = task._scheduler.deadlines

        if self._relative_deadline != inf:
            assert self._deadline == inf
            self._deadline = _current_time() + self._relative_deadline
            self._relative_deadline = inf

        if _current_time() >= self._deadline:
            self._cancel(CancelReason(source="deadline"))
        with self._might_change_registered_deadline():
            self._cancel_status = CancelStatus(scope=self, parent=task._cancel_status)
            task._activate_cancel_status(self._cancel_status)
        return self

    def _close(self, exc: BaseException | None) -> BaseException | None:
        if self._cancel_status is None:
            new_exc = RuntimeError(
                f"Cancel scope stack corrupted: attempted to exit {self!r} "
                "which had already been exited",
            )
            new_exc.__context__ = exc
            return new_exc
        scope_task = current_task()
        if scope_task._cancel_status is not self._cancel_status:
            # Cancel scope misnesting: this cancel scope isn't the most
            # recently opened by this task (that's still open). That is,
            # our assumptions about context managers forming a stack
            # have been violated. Try and make the best of it.
            if self._cancel_status.abandoned_by_misnesting:
                # We are an inner cancel scope that was still active when
                # some outer scope was closed. The closure of that outer
                # scope threw an error, so we don't need to throw another
                # one; it would just confuse the traceback.
                pass
            elif not self._cancel_status.encloses(scope_task._cancel_status):
                # This task isn't even indirectly contained within the
                # cancel scope it's trying to close. Raise an error
                # without changing any state.
                new_exc = RuntimeError(
                    f"Cancel scope stack corrupted: attempted to exit {self!r} "
                    f"from unrelated {scope_task!r}\n{MISNESTING_ADVICE}",
                )
                new_exc.__context__ = exc
                return new_exc
            else:
                # Otherwise, there's some inner cancel scope(s) that
                # we're abandoning by closing this outer one.
                # CancelStatus.close() will take care of the plumbing;
                # we just need to make sure we don't let the error
                # pass silently.
                new_exc = RuntimeError(
                    f"Cancel scope stack corrupted: attempted to exit {self!r} "
                    f"in {scope_task!r} that's still within its child {scope_task._cancel_status._scope!r}\n{MISNESTING_ADVICE}",
                )
                new_exc.__context__ = exc
                exc = new_exc
                scope_task._activate_cancel_status(self._cancel_status.parent)
        else:
            scope_task._activate_cancel_status(self._cancel_status.parent)
        if (
            exc is not None
            and self._cancel_status.effectively_cancelled
            and not self._cancel_status.parent_cancellation_is_visible_to_us
        ) or (
            scope_task._cancel_status is not self._cancel_status
            and self._cancel_status.abandoned_by_misnesting
        ):
            if isinstance(exc, Cancelled):
                self.cancelled_caught = True
                exc = None
            elif isinstance(exc, BaseExceptionGroup):
                matched, exc = exc.split(Cancelled)
                if matched:
                    self.cancelled_caught = True

                if exc:
                    exc = collapse_exception_group(exc)

        self._cancel_status.close()
        with self._might_change_registered_deadline():
            self._cancel_status = None
        self._deadlines = None
        return exc

    @enable_ki_protection
    def __exit__(
        self,
        etype: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool:
        # NB: NurseryManager calls _close() directly rather than __exit__(),
        # so __exit__() must be just _close() plus this logic for adapting
        # the exception-filtering result to the context manager API.

        # Tracebacks show the 'raise' line below out of context, so let's give
        # this variable a name that makes sense out of context.
        remaining_error_after_cancel_scope = self._close(exc)
        if remaining_error_after_cancel_scope is None:
            return True
        elif remaining_error_after_cancel_scope is exc:
            return False
        else:
            # Copied verbatim from the old MultiErrorCatcher.  Python doesn't
            # allow us to encapsulate this __context__ fixup.
            old_context = remaining_error_after_cancel_scope.__context__
            try:
                raise remaining_error_after_cancel_scope
            finally:
                _, value, _ = sys.exc_info()
                assert value is remaining_error_after_cancel_scope
                value.__context__ = old_context
                # delete references from locals to avoid creating cycles
                # see test_cancel_scope_exit_doesnt_create_cyclic_garbage
                # Note: still relevant
                del remaining_error_after_cancel_scope, value, _, exc

    def __repr__(self) -> str:
        if self._cancel_status is not None:
            binding = "active"
        elif self._has_been_entered:
            binding = "exited"
        else:
            binding = "unbound"

        if self._cancel_called:
            state = ", cancelled"
        elif self._deadline == inf:
            state = ""
        else:
            try:
                now = _current_time()
            except AttributeError:  # not inside a run
                state = ""
            else:
                state = ", deadline is {:.2f} seconds {}".format(
                    abs(self._deadline - now),
                    "from now" if self._deadline >= now else "ago",
                )

        return f"<trio.CancelScope at {id(self):#x}, {binding}{state}>"

    @contextmanager
    @enable_ki_protection
    def _might_change_registered_deadline(self) -> Iterator[None]:
        try:
            yield
        finally:
            old = self._registered_deadline
            if self._cancel_status is None or self._cancel_called:
                new = inf
            else:
                new = self._deadline
            if old != new:
                self._registered_deadline = new
                # Only an entered scope can have a registered deadline, and
                # entering is what gives us the registry.
                assert self._deadlines is not None
                self._deadlines.change(self, old, new)

    @property
    def deadline(self) -> float:
        """Read-write, :class:`float`. An absolute time on the current
        run's clock at which this scope will automatically become
        cancelled. You can adjust the deadline by modifying this
        attribute, e.g.::

           # I need a little more time!
           cancel_scope.deadline += 30

        Note that for efficiency, the core run loop only checks for
        expired deadlines every once in a while. This means that in
        certain cases there may be a short delay between when the clock
        says the deadline should have expired, and when checkpoints
        start raising :exc:`~trio.Cancelled`. This is a very obscure
        corner case that you're unlikely to notice, but we document it
        for completeness. (If this *does* cause problems for you, of
        course, then `we want to know!
        <https://github.com/python-trio/trio/issues>`__)

        Defaults to :data:`math.inf`, which means "no deadline", though
        this can be overridden by the ``deadline=`` argument to
        the :class:`~trio.CancelScope` constructor.
        """
        if self._relative_deadline != inf:
            assert self._deadline == inf
            raise RuntimeError(
                "Unentered relative cancel scope does not have an absolute deadline."
            )
        return self._deadline

    @deadline.setter
    def deadline(self, new_deadline: float) -> None:
        if isnan(new_deadline):
            raise ValueError("deadline must not be NaN")
        if self._relative_deadline != inf:
            assert self._deadline == inf
            raise RuntimeError(
                "Unentered relative cancel scope does not have an absolute deadline."
            )
        with self._might_change_registered_deadline():
            self._deadline = float(new_deadline)

    @property
    def relative_deadline(self) -> float:
        """Read-write, :class:`float`. The number of seconds remaining until this
        scope's deadline, relative to the current time.

        Defaults to :data:`math.inf` ("no deadline"). Must be non-negative.

        When modified
        Before entering: sets the deadline relative to when the scope enters.
        After entering: sets a new deadline relative to the current time.

        Raises:
          RuntimeError: if trying to read or modify an unentered scope with an absolute deadline, i.e. when :attr:`is_relative` is ``False``.
        """
        if self._has_been_entered:
            return self._deadline - _current_time()
        elif self._deadline != inf:
            assert self._relative_deadline == inf
            raise RuntimeError(
                "Unentered non-relative cancel scope does not have a relative deadline",
            )
        return self._relative_deadline

    @relative_deadline.setter
    def relative_deadline(self, new_relative_deadline: float) -> None:
        if isnan(new_relative_deadline):
            raise ValueError("relative deadline must not be NaN")
        if new_relative_deadline < 0:
            raise ValueError("relative deadline must be non-negative")
        if self._has_been_entered:
            with self._might_change_registered_deadline():
                self._deadline = _current_time() + float(new_relative_deadline)
        elif self._deadline != inf:
            assert self._relative_deadline == inf
            raise RuntimeError(
                "Unentered non-relative cancel scope does not have a relative deadline",
            )
        else:
            self._relative_deadline = new_relative_deadline

    @property
    def is_relative(self) -> bool | None:
        """Returns None after entering. Returns False if both deadline and
        relative_deadline are inf."""
        assert not (self._deadline != inf and self._relative_deadline != inf)
        if self._has_been_entered:
            return None
        return self._relative_deadline != inf

    @property
    def shield(self) -> bool:
        """Read-write, :class:`bool`, default :data:`False`. So long as
        this is set to :data:`True`, then the code inside this scope
        will not receive :exc:`~trio.Cancelled` exceptions from scopes
        that are outside this scope. They can still receive
        :exc:`~trio.Cancelled` exceptions from (1) this scope, or (2)
        scopes inside this scope. You can modify this attribute::

           with trio.CancelScope() as cancel_scope:
               cancel_scope.shield = True
               # This cannot be interrupted by any means short of
               # killing the process:
               await sleep(10)

               cancel_scope.shield = False
               # Now this can be cancelled normally:
               await sleep(10)

        Defaults to :data:`False`, though this can be overridden by the
        ``shield=`` argument to the :class:`~trio.CancelScope` constructor.
        """
        return self._shield

    @shield.setter
    @enable_ki_protection
    def shield(self, new_value: bool) -> None:
        if not isinstance(new_value, bool):
            raise TypeError("shield must be a bool")
        self._shield = new_value
        if self._cancel_status is not None:
            self._cancel_status.recalculate()

    @enable_ki_protection
    def _cancel(self, cancel_reason: CancelReason | None) -> None:
        """Internal sources of cancellation should use this instead of :meth:`cancel`
        in order to set a more detailed :class:`CancelReason`
        Helper or high-level functions can use `cancel`.
        """
        if self._cancel_called:
            return

        if self._cancel_reason is None:
            self._cancel_reason = cancel_reason

        with self._might_change_registered_deadline():
            self._cancel_called = True

        if self._cancel_status is not None:
            self._cancel_status.recalculate()

    @enable_ki_protection
    def cancel(self, reason: str | None = None) -> None:
        """Cancels this scope immediately.

        The optional ``reason`` argument accepts a string, which will be attached to
        any resulting :exc:`Cancelled` exception to help you understand where that
        cancellation is coming from and why it happened.

        This method is idempotent, i.e., if the scope was already
        cancelled then this method silently does nothing.
        """
        try:
            source_task = repr(current_task())
        except RuntimeError:
            source_task = None
        self._cancel(
            CancelReason(reason=reason, source="explicit", source_task=source_task)
        )

    @property
    def cancel_called(self) -> bool:
        """Readonly :class:`bool`. Records whether cancellation has been
        requested for this scope, either by an explicit call to
        :meth:`cancel` or by the deadline expiring.

        This attribute being True does *not* necessarily mean that the
        code within the scope has been, or will be, affected by the
        cancellation. For example, if :meth:`cancel` was called after
        the last checkpoint in the ``with`` block, when it's too late to
        deliver a :exc:`~trio.Cancelled` exception, then this attribute
        will still be True.

        This attribute is mostly useful for debugging and introspection.
        If you want to know whether or not a chunk of code was actually
        cancelled, then :attr:`cancelled_caught` is usually more
        appropriate.
        """
        if (  # noqa: SIM102  # collapsible-if but this way is nicer
            self._cancel_status is not None or not self._has_been_entered
        ):
            # Scope is active or not yet entered: make sure cancel_called
            # is true if the deadline has passed. This shouldn't
            # be able to actually change behavior, since we check for
            # deadline expiry on scope entry and at every checkpoint,
            # but it makes the value returned by cancel_called more
            # closely match expectations.
            if not self._cancel_called and _current_time() >= self._deadline:
                self._cancel(CancelReason(source="deadline"))
        return self._cancel_called


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


class LoopWaker(Protocol):
    """How the run loop is told that what it is waiting for has changed.

    The loop sleeps in an I/O wait whose timeout is computed from the next
    deadline and from whether any task is runnable. Normally neither can
    change while it sleeps, because every change is made by a task, and
    tasks only run between waits. In guest mode, though, the wait runs on a
    worker thread while the host loop's thread can change deadlines and
    reschedule tasks; so there the deadlines and the run queue are given a
    waker to call when the next deadline changes or the first task becomes
    runnable, which makes the loop come back and recompute its timeout. (In
    normal mode they have none.) The runner implements it: see Runner.wake.
    """

    def wake(self) -> None: ...


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
