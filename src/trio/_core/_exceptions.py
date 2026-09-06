from __future__ import annotations

import enum
import sys
from functools import partial
from typing import TYPE_CHECKING, Literal, TypeAlias

import attrs

from trio._util import NoPublicConstructor, final

if sys.version_info < (3, 11):
    from exceptiongroup import BaseExceptionGroup

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import TracebackType

    from typing_extensions import Self

CancelReasonLiteral: TypeAlias = Literal[
    "KeyboardInterrupt",
    "deadline",
    "explicit",
    "nursery",
    "shutdown",
    "unknown",
]


class TrioInternalError(Exception):
    """Raised by :func:`run` if we encounter a bug in Trio, or (possibly) a
    misuse of one of the low-level :mod:`trio.lowlevel` APIs.

    This should never happen! If you get this error, please file a bug.

    Unfortunately, if you get this error it also means that all bets are off –
    Trio doesn't know what is going on and its normal invariants may be void.
    (For example, we might have "lost track" of a task. Or lost track of all
    tasks.) Again, though, this shouldn't happen.

    """


class RunFinishedError(RuntimeError):
    """Raised by `trio.from_thread.run` and similar functions if the
    corresponding call to :func:`trio.run` has already finished.

    """


class WouldBlock(Exception):
    """Raised by ``X_nowait`` functions if ``X`` would block."""


@final
@attrs.define(eq=False, kw_only=True)
class Cancelled(BaseException, metaclass=NoPublicConstructor):
    """Raised by blocking calls if the surrounding scope has been cancelled.

    You should let this exception propagate, to be caught by the relevant
    cancel scope. To remind you of this, it inherits from :exc:`BaseException`
    instead of :exc:`Exception`, just like :exc:`KeyboardInterrupt` and
    :exc:`SystemExit` do. This means that if you write something like::

       try:
           ...
       except Exception:
           ...

    then this *won't* catch a :exc:`Cancelled` exception.

    You cannot raise :exc:`Cancelled` yourself. Attempting to do so
    will produce a :exc:`TypeError`. Use :meth:`cancel_scope.cancel()
    <trio.CancelScope.cancel>` instead.

    .. note::

       In the US it's also common to see this word spelled "canceled", with
       only one "l". This is a `recent
       <https://books.google.com/ngrams/graph?content=canceled%2Ccancelled&year_start=1800&year_end=2000&corpus=5&smoothing=3&direct_url=t1%3B%2Ccanceled%3B%2Cc0%3B.t1%3B%2Ccancelled%3B%2Cc0>`__
       and `US-specific
       <https://books.google.com/ngrams/graph?content=canceled%2Ccancelled&year_start=1800&year_end=2000&corpus=18&smoothing=3&share=&direct_url=t1%3B%2Ccanceled%3B%2Cc0%3B.t1%3B%2Ccancelled%3B%2Cc0>`__
       innovation, and even in the US both forms are still commonly used. So
       for consistency with the rest of the world and with "cancellation"
       (which always has two "l"s), Trio uses the two "l" spelling
       everywhere.

    """

    source: CancelReasonLiteral = "unknown"
    # repr(Task), so as to avoid gc troubles from holding a reference
    source_task: str | None = None
    reason: str | None = None

    def __str__(self) -> str:
        return (
            f"cancelled due to {self.source}"
            + ("" if self.reason is None else f" with reason {self.reason!r}")
            + ("" if self.source_task is None else f" from task {self.source_task}")
        )

    def __reduce__(self) -> tuple[Callable[[], Cancelled], tuple[()]]:
        # The `__reduce__` tuple does not support directly passing kwargs, and the
        # kwargs are required so we can't use the third item for adding to __dict__,
        # so we use partial.
        return (
            partial(
                Cancelled._create,
                source=self.source,
                source_task=self.source_task,
                reason=self.reason,
            ),
            (),
        )

    if TYPE_CHECKING:
        # for type checking on internal code
        @classmethod
        def _create(
            cls,
            *,
            source: CancelReasonLiteral = "unknown",
            source_task: str | None = None,
            reason: str | None = None,
        ) -> Self: ...


class BusyResourceError(Exception):
    """Raised when a task attempts to use a resource that some other task is
    already using, and this would lead to bugs and nonsense.

    For example, if two tasks try to send data through the same socket at the
    same time, Trio will raise :class:`BusyResourceError` instead of letting
    the data get scrambled.

    """


class ClosedResourceError(Exception):
    """Raised when attempting to use a resource after it has been closed.

    Note that "closed" here means that *your* code closed the resource,
    generally by calling a method with a name like ``close`` or ``aclose``, or
    by exiting a context manager. If a problem arises elsewhere – for example,
    because of a network failure, or because a remote peer closed their end of
    a connection – then that should be indicated by a different exception
    class, like :exc:`BrokenResourceError` or an :exc:`OSError` subclass.

    """


class BrokenResourceError(Exception):
    """Raised when an attempt to use a resource fails due to external
    circumstances.

    For example, you might get this if you try to send data on a stream where
    the remote side has already closed the connection.

    You *don't* get this error if *you* closed the resource – in that case you
    get :class:`ClosedResourceError`.

    This exception's ``__cause__`` attribute will often contain more
    information about the underlying error.

    """


class EndOfChannel(Exception):
    """Raised when trying to receive from a :class:`trio.abc.ReceiveChannel`
    that has no more data to receive.

    This is analogous to an "end-of-file" condition, but for channels.

    """


class _Auto(enum.Enum):
    AUTO = enum.auto()


_AUTO = _Auto.AUTO


@final
class preserve_ambient_exception:
    """A context manager that stops a `Cancelled` raised in its body from
    masking an exception that was already propagating.

    Use it to surround the contents of a ``finally:`` block, or the body of
    an ``__exit__`` or ``__aexit__`` method, when that cleanup code contains
    :ref:`checkpoints <checkpoints>`. If the enclosing scope has been
    cancelled, then those checkpoints will raise `Cancelled`, and normally
    that new `Cancelled` would replace whatever exception the ``finally:``
    block or ``__aexit__`` method was invoked to handle. In many cases the
    original exception is the more interesting one, and it would otherwise
    be lost.

    When entered, this context manager records the "ambient" exception: the
    exception currently being handled, as reported by :func:`sys.exc_info`.
    If the body then raises `Cancelled` (or an :exc:`BaseExceptionGroup`
    containing nothing but `Cancelled` exceptions), and there was an ambient
    exception, the `Cancelled` is suppressed so that the ambient exception
    continues to propagate. In every other case the body's exception
    propagates unchanged.

    For example, this async context manager does some asynchronous cleanup
    and wants the exception from its body to win over any cancellation that
    interrupts the cleanup::

       @asynccontextmanager
       async def my_cm():
           try:
               yield
           finally:
               with trio.lowlevel.preserve_ambient_exception():
                   await cleanup()

    :class:`trio.abc.AsyncResource` uses this in its default ``__aexit__``
    implementation, passing the ``exc_value`` argument explicitly.

    Args:
      ambient: The exception that is being handled. If omitted, it is looked
          up automatically with :func:`sys.exc_info` when the context manager
          is entered. Pass ``None`` to indicate that no exception is being
          handled, in which case the context manager does nothing.

    .. note::

       Suppressing the `Cancelled` means that the cancel scope it belongs to
       never sees it, so that scope's
       :attr:`~trio.CancelScope.cancelled_caught` won't be set by it. The
       scope remains cancelled, so the next checkpoint will raise `Cancelled`
       again. If the ambient exception is caught somewhere between the
       cleanup code and the cancel scope, the cancellation is therefore
       delayed until that next checkpoint rather than lost.

    """

    __slots__ = ("_ambient", "_captured")

    def __init__(
        self, ambient: BaseException | Literal[_Auto.AUTO] | None = _AUTO
    ) -> None:
        self._ambient = ambient
        self._captured: BaseException | None = None

    def __enter__(self) -> Self:
        if self._ambient is _AUTO:
            self._captured = sys.exc_info()[1]
        else:
            self._captured = self._ambient
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        captured, self._captured = self._captured, None
        if exc_value is None or captured is None:
            return False
        if isinstance(exc_value, BaseExceptionGroup):
            # The group is suppressed only if it contains nothing but Cancelled
            # exceptions (possibly nested inside further groups). Note that
            # BaseExceptionGroup.subgroup()/split() with a predicate would also
            # test the group nodes themselves, so we match by type instead.
            return exc_value.split(Cancelled)[1] is None
        return isinstance(exc_value, Cancelled)
