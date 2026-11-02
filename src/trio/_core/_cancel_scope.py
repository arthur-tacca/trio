"""Cancel scopes: the layer of Trio's core above tasks.

See the module docstring of ``_task.py`` for the layering. A CancelScope is
the user's handle on a node of the cancellation tree (``_cancel_status.py``):
entering it creates a CancelStatus under the current task's and makes that
the task's active status, and the scope's deadline, shield and cancel() are
the policy the status reads from it. This module sits above ``_task.py``
because entering and exiting a scope need the current task; the status and
the deadline registry below never name a scope, they see it through the
contracts they define (`CancelStatus.Scope` and `Deadlines.Scope`), which
CancelScope satisfies.
"""

from __future__ import annotations

import sys
from contextlib import contextmanager
from math import inf, isnan
from typing import TYPE_CHECKING

import attrs

from .._util import final
from ._cancel_status import CancelReason, CancelStatus
from ._concat_tb import concat_tb
from ._exceptions import Cancelled
from ._ki import enable_ki_protection
from ._run_context import GLOBAL_RUN_CONTEXT
from ._task import MISNESTING_ADVICE, current_task

if sys.version_info < (3, 11):
    from exceptiongroup import BaseExceptionGroup


if TYPE_CHECKING:
    from collections.abc import Iterator
    from types import TracebackType

    from typing_extensions import Self

    from ._cancel_status import Deadlines


def _current_time() -> float:
    # For the cancel scope properties, which can be used from outside any
    # task: without a task in hand, the clock is reached through the
    # thread-local.
    return GLOBAL_RUN_CONTEXT.scheduler.clock.current_time()


# Used to track if an exceptiongroup can be collapsed


NONSTRICT_EXCEPTIONGROUP_NOTE = 'This is a "loose" ExceptionGroup, and may be collapsed by Trio if it only contains one exception - typically after `Cancelled` has been stripped from it. Note this has consequences for exception handling, and strict_exception_groups=True is recommended.'


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
