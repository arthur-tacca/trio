from __future__ import annotations

import signal
from typing import TypeVar

import outcome
import pytest

from ... import _core
from ...testing import assert_checkpoints, wait_all_tasks_blocked
from .._wake_token import WakeToken
from .tutil import check_sequence_matches

T = TypeVar("T")
D = TypeVar("D")


def ki_self() -> None:
    signal.raise_signal(signal.SIGINT)


def no_cancel(token: WakeToken[T, D]) -> None:  # pragma: no cover
    raise AssertionError("on_cancel should not have been called")


def cancel_now(token: WakeToken[T, D]) -> None:
    token.reschedule_cancelled()


class Lot:
    """A stand-in for something that keeps track of its waiters."""

    def __init__(self) -> None:
        self.waiting: set[WakeToken[None, Lot]] = set()


async def test_wake_token_data() -> None:
    # No data: it's None, and so is its type parameter
    plain: WakeToken[int] = WakeToken(no_cancel)
    assert plain.data is None

    # Data is handed to on_cancel through the token, with its type
    lot = Lot()

    def on_cancel(token: WakeToken[None, Lot]) -> None:
        token.data.waiting.remove(token)
        token.reschedule_cancelled()

    with _core.CancelScope() as scope:
        scope.cancel()
        token = WakeToken(on_cancel, lot)
        assert token.data is lot
        lot.waiting.add(token)
        with pytest.raises(_core.Cancelled):
            await token
    assert not lot.waiting

    # And it can be reassigned, e.g. when a waiter is moved somewhere else
    other = Lot()
    token2 = WakeToken(on_cancel, lot)
    token2.data = other
    assert token2.data is other


async def test_wake_token_reschedule_without_argument() -> None:
    # A token whose value type is None can be woken without an outcome
    token = WakeToken(no_cancel)
    record: list[object] = []

    async def waiter() -> None:
        # mypy objects to using an awaited None, but that's the value under test
        record.append(await token)  # type: ignore[func-returns-value]

    async with _core.open_nursery() as nursery:
        nursery.start_soon(waiter)
        await wait_all_tasks_blocked()
        token.reschedule()
    assert record == [None]

    # ...and so can one whose value type merely includes None
    token2: WakeToken[int | None] = WakeToken(no_cancel)

    async def waiter2() -> None:
        record.append(await token2)

    async with _core.open_nursery() as nursery:
        nursery.start_soon(waiter2)
        await wait_all_tasks_blocked()
        token2.reschedule()
    assert record == [None, None]


async def test_wake_token_reschedule_rejects_non_outcomes() -> None:
    # Untyped callers get a clear error, rather than the sleeping task
    # choking on whatever they passed
    token = WakeToken(no_cancel)

    async def waiter() -> None:
        await token

    async with _core.open_nursery() as nursery:
        nursery.start_soon(waiter)
        await wait_all_tasks_blocked()
        with pytest.raises(TypeError, match=r"needs an outcome\.Outcome"):
            token.reschedule(None)  # type: ignore[call-overload]
        with pytest.raises(TypeError, match=r"needs an outcome\.Outcome"):
            token.reschedule(42)  # type: ignore[call-overload]
        token.reschedule()


async def test_wake_token_task_accessor() -> None:
    token = WakeToken(no_cancel)
    with pytest.raises(RuntimeError, match="hasn't been awaited yet"):
        _ = token.task

    seen: list[_core.Task] = []

    async def waiter() -> None:
        seen.append(_core.current_task())
        await token

    async with _core.open_nursery() as nursery:
        nursery.start_soon(waiter)
        await wait_all_tasks_blocked()
        assert token.task is seen[0]
        token.reschedule()
    # Still available after the wake-up, for whoever wants to know who it was
    assert token.task is seen[0]


async def test_wake_token_on_cancel_is_reassignable() -> None:
    record: list[str] = []

    def first(token: WakeToken[None]) -> None:  # pragma: no cover
        record.append("first")
        token.reschedule_cancelled()

    def second(token: WakeToken[None]) -> None:
        record.append("second")
        token.reschedule_cancelled()

    with _core.CancelScope() as scope:
        token = WakeToken(first)
        token.on_cancel = second
        scope.cancel()
        with pytest.raises(_core.Cancelled):
            await token
    assert record == ["second"]


async def test_wake_token_value_and_error() -> None:
    record: list[object] = []

    async def waiter(token: WakeToken[object]) -> None:
        record.append("sleeping")
        try:
            record.append(await token)
        except KeyError as exc:
            record.append(exc)

    t1: WakeToken[object] = WakeToken(no_cancel)
    t2: WakeToken[object] = WakeToken(no_cancel)
    assert repr(t1).endswith("not awaited>")
    async with _core.open_nursery() as nursery:
        nursery.start_soon(waiter, t1)
        nursery.start_soon(waiter, t2)
        await wait_all_tasks_blocked()
        assert record == ["sleeping", "sleeping"]
        assert "awaited by" in repr(t1)
        err = KeyError("boom")
        t1.reschedule(outcome.Value(1))
        t2.reschedule(outcome.Error(err))
        assert "rescheduled" in repr(t1)
        await wait_all_tasks_blocked()
    # Both waiters wake in the same scheduler pass, in either order.
    check_sequence_matches(record, ["sleeping", "sleeping", {1, err}])


async def test_wake_token_is_a_checkpoint() -> None:
    token = WakeToken(cancel_now)
    _core.current_trio_token().run_sync_soon(token.reschedule)
    with assert_checkpoints():
        await token


async def test_wake_token_cancel_immediately() -> None:
    record: list[object] = []

    def on_cancel(token: WakeToken[object]) -> None:
        record.append(token)
        assert "cancellation requested" in repr(token)
        token.reschedule_cancelled()

    with _core.CancelScope() as scope:
        scope.cancel()
        token: WakeToken[object] = WakeToken(on_cancel)
        with pytest.raises(_core.Cancelled):
            await token
    assert record == [token]


async def test_wake_token_cancel_deferred() -> None:
    trio_token = _core.current_trio_token()
    record: list[str] = []

    def on_cancel(token: WakeToken[object]) -> None:
        record.append("cancel")
        trio_token.run_sync_soon(token.reschedule_cancelled)

    with _core.CancelScope() as scope:
        scope.cancel()
        token: WakeToken[object] = WakeToken(on_cancel)
        with pytest.raises(_core.Cancelled):
            await token
    assert record == ["cancel"]


async def test_wake_token_cancel_declined_then_value() -> None:
    trio_token = _core.current_trio_token()

    def on_cancel(token: WakeToken[str]) -> None:
        # Decline the cancellation: the operation "completes" a little later.
        trio_token.run_sync_soon(token.reschedule, outcome.Value("late"))

    with _core.CancelScope() as scope:
        scope.cancel()
        token: WakeToken[str] = WakeToken(on_cancel)
        assert await token == "late"
        # The cancellation is still available to raise by hand.
        with pytest.raises(_core.Cancelled):
            raise token.get_cancelled_exception()


async def test_wake_token_misuse() -> None:
    token = WakeToken(cancel_now)
    with pytest.raises(RuntimeError, match="before the token was awaited"):
        token.reschedule()
    with pytest.raises(RuntimeError, match="before any cancellation was requested"):
        token.get_cancelled_exception()

    _core.current_trio_token().run_sync_soon(token.reschedule)
    await token
    with pytest.raises(RuntimeError, match="only be awaited once"):
        await token
    with pytest.raises(RuntimeError, match="more than once"):
        token.reschedule()


async def test_wake_token_cannot_be_shared_between_tasks() -> None:
    token = WakeToken(cancel_now)

    async def waiter() -> None:
        await token

    async with _core.open_nursery() as nursery:
        nursery.start_soon(waiter)
        await wait_all_tasks_blocked()
        with pytest.raises(RuntimeError, match="only be awaited once"):
            await token
        token.reschedule()


def test_wake_token_deferred_ki_is_a_cancellation() -> None:
    record: list[str] = []

    @_core.enable_ki_protection
    async def main() -> None:
        # We're KI-protected, so this cancels the main task rather than
        # raising here, and we see it as soon as we next sleep.
        ki_self()

        def on_cancel(token: WakeToken[None]) -> None:
            record.append("cancel")
            exc = token.get_cancelled_exception()
            assert exc.source == "KeyboardInterrupt"
            token.reschedule(outcome.Error(exc))

        token = WakeToken(on_cancel)
        with pytest.raises(_core.Cancelled):
            await token
        record.append("after")

    with pytest.raises(KeyboardInterrupt):
        _core.run(main)
    assert record == ["cancel", "after"]


def test_wake_token_deferred_ki_survives_a_declined_cancel() -> None:
    @_core.enable_ki_protection
    async def main() -> None:
        ki_self()

        def on_cancel(token: WakeToken[str]) -> None:
            # Decline, without ever calling get_cancelled_exception().
            token.reschedule(outcome.Value("ignored"))

        token: WakeToken[str] = WakeToken(on_cancel)
        assert await token == "ignored"
        # Nothing was lost: the task is still cancelled.
        with pytest.raises(_core.Cancelled) as excinfo:
            await _core.checkpoint()
        assert excinfo.value.source == "KeyboardInterrupt"

    with pytest.raises(KeyboardInterrupt):
        _core.run(main)


async def test_wake_token_cancelled_exception_can_be_taken_repeatedly() -> None:
    def on_cancel(token: WakeToken[None]) -> None:
        exc1 = token.get_cancelled_exception()
        exc2 = token.get_cancelled_exception()
        assert isinstance(exc1, _core.Cancelled)
        assert isinstance(exc2, _core.Cancelled)
        # A fresh but equivalent exception each time
        assert exc1 is not exc2
        assert (exc1.source, exc1.reason) == (exc2.source, exc2.reason)
        token.reschedule(outcome.Error(exc1))

    with _core.CancelScope() as scope:
        scope.cancel()
        token = WakeToken(on_cancel)
        with pytest.raises(_core.Cancelled):
            await token
        # Still available after delivery
        with pytest.raises(_core.Cancelled):
            raise token.get_cancelled_exception()


async def test_wake_token_declining_after_taking_the_exception() -> None:
    with _core.CancelScope() as scope:
        scope.cancel()

        def on_cancel(token: WakeToken[str]) -> None:
            # Taking the exception doesn't oblige us to deliver it now...
            assert isinstance(token.get_cancelled_exception(), _core.Cancelled)
            token.reschedule(outcome.Value("declined"))

        token: WakeToken[str] = WakeToken(on_cancel)
        assert await token == "declined"
        # ...but the task is still cancelled.
        with pytest.raises(_core.Cancelled):
            await _core.checkpoint()


# The legacy wait_task_rescheduled()/reschedule() API is a shim over WakeToken.


async def test_public_reschedule_routes_through_the_token() -> None:
    record: list[int] = []
    sleeping: list[_core.Task] = []

    async def waiter() -> None:
        sleeping.append(_core.current_task())
        token: WakeToken[int] = WakeToken(no_cancel)
        record.append(await token)

    async with _core.open_nursery() as nursery:
        nursery.start_soon(waiter)
        await wait_all_tasks_blocked()
        (task,) = sleeping
        _core.reschedule(task, outcome.Value(7))
        # Already woken: the task is no longer blocked on anything.
        with pytest.raises(RuntimeError, match="not blocked"):
            _core.reschedule(task, outcome.Value(8))
    assert record == [7]
    with pytest.raises(RuntimeError, match="not blocked"):
        _core.reschedule(_core.current_task())


async def test_legacy_abort_may_peek_then_succeed() -> None:
    def abort(raise_cancel: _core.RaiseCancelT) -> _core.Abort:
        with pytest.raises(_core.Cancelled):
            raise_cancel()  # legacy code may inspect before deciding
        return _core.Abort.SUCCEEDED

    with _core.CancelScope() as scope:
        scope.cancel()
        with pytest.raises(_core.Cancelled):
            await _core.wait_task_rescheduled(abort)


async def test_legacy_raise_cancel_can_be_called_repeatedly() -> None:
    task = _core.current_task()
    trio_token = _core.current_trio_token()
    saved: list[_core.RaiseCancelT] = []

    def abort(raise_cancel: _core.RaiseCancelT) -> _core.Abort:
        saved.append(raise_cancel)
        trio_token.run_sync_soon(_core.reschedule, task, outcome.Value("woken"))
        return _core.Abort.FAILED

    with _core.CancelScope() as scope:
        scope.cancel()
        assert await _core.wait_task_rescheduled(abort) == "woken"
        seen = set()
        for _ in range(3):  # e.g. from_thread.check_cancelled() polls like this
            with pytest.raises(_core.Cancelled) as excinfo:
                saved[0]()
            seen.add((excinfo.value.source, excinfo.value.reason))
        assert len(seen) == 1  # re-created exceptions are equivalent


def test_legacy_abort_sees_deferred_ki_as_cancellation() -> None:
    @_core.enable_ki_protection
    async def main() -> None:
        ki_self()

        def abort(raise_cancel: _core.RaiseCancelT) -> _core.Abort:
            with pytest.raises(_core.Cancelled) as excinfo:
                raise_cancel()
            assert excinfo.value.source == "KeyboardInterrupt"
            # Trio then calls raise_cancel again to build the outcome, and
            # gets an equivalent exception.
            return _core.Abort.SUCCEEDED

        with pytest.raises(_core.Cancelled) as excinfo:
            await _core.wait_task_rescheduled(abort)
        assert excinfo.value.source == "KeyboardInterrupt"

    with pytest.raises(KeyboardInterrupt):
        _core.run(main)
