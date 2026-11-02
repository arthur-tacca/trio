"""The cancellation tree and its deadlines: the layer of Trio's core below tasks.

See the module docstring of ``_task.py`` for the layering. A CancelStatus is
one node of the tree of the cancel scopes that are currently entered, with
the tasks whose cancellation state it decides hanging off it, and Deadlines
is the registry of the scopes' deadlines. Neither names a cancel scope or a
task, which live above this module: what they need from those they get
through the contracts defined here (`CancelStatus.Scope`,
`CancelStatus.Member` and `Deadlines.Scope`), in the same way that
``_task.py`` defines `TaskOwner` for the nursery layer. `LoopWaker`, the
contract the run loop implements for guest mode, is defined here too,
because the deadlines are its lowest user.
"""

from __future__ import annotations

from heapq import heapify, heappop, heappush
from math import inf
from typing import TYPE_CHECKING, Final, Protocol

import attrs

if TYPE_CHECKING:
    from ._exceptions import CancelReasonLiteral

DEADLINE_HEAP_MIN_PRUNE_THRESHOLD: Final = 1000


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
class Deadlines:
    """A container of deadlined cancel scopes.

    Only contains scopes with non-infinite deadlines that are currently
    attached to at least one task.

    """

    class Scope(Protocol):
        """A cancel scope as the deadline registry sees it: something that
        registers its deadline here (see add, remove and change), that says
        which deadline it currently has registered, so that a stale heap
        entry can be told from a live one, and that is cancelled when that
        deadline passes. CancelScope implements it; the members are
        underscore-prefixed because CancelScope is public and they are not
        part of its user-facing API."""

        @property
        def _registered_deadline(self) -> float: ...

        def _cancel(self, cancel_reason: CancelReason | None) -> None: ...

    # Heap of (deadline, id(scope), scope)
    _heap: list[tuple[float, int, Scope]] = attrs.Factory(list)
    # Count of active deadlines (those that haven't been changed)
    _active: int = 0
    # Woken whenever the next deadline changes, if set: see LoopWaker.
    loop_waker: LoopWaker | None = None

    def add(self, deadline: float, cancel_scope: Scope) -> None:
        heappush(self._heap, (deadline, id(cancel_scope), cancel_scope))
        self._active += 1

    def remove(self, deadline: float, cancel_scope: Scope) -> None:
        self._active -= 1

    def change(self, cancel_scope: Scope, old: float, new: float) -> None:
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

    class Scope(Protocol):
        """The cancel scope a status is the plumbing for, as the status sees
        it: where its policy comes from. The status reads whether the scope
        has been cancelled, whether it shields what is inside it from
        cancellations outside, and its deadline; and when a cancellation
        reaches the scope from an enclosing one, the status records that
        scope's reason on this one. CancelScope implements it;
        ``_cancel_reason`` is underscore-prefixed because CancelScope is
        public and it is not part of its user-facing API."""

        _cancel_reason: CancelReason | None

        @property
        def shield(self) -> bool: ...

        @property
        def cancel_called(self) -> bool: ...

        @property
        def deadline(self) -> float: ...

    class Member(Protocol):
        """A task whose cancellation state is tied to a status: a member of
        the extent of code the status tracks (see ``_tasks``). The status
        tells it when a cancellation may have become deliverable to it.
        Task implements it."""

        def _attempt_delivery_of_any_pending_cancel(self) -> None: ...

    # Our associated cancel scope (see Scope); in current usage always a
    # CancelScope object. Must not be None.
    _scope: Scope = attrs.field(alias="scope")

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
    # the cancellation state of this CancelStatus object (see Member).
    # Don't modify this directly; instead, use Task._activate_cancel_status().
    # Invariant: all(task._cancel_status is self for task in self._tasks)
    _tasks: set[Member] = attrs.field(factory=set, init=False, repr=False)

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
    def tasks(self) -> frozenset[Member]:
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
