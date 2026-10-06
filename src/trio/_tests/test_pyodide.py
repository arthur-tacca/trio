# Tests for trio.pyodide's promise support. These run on any platform, using
# fakes for the bits of Pyodide and JavaScript that wait_promise touches;
# tests/pyodide/ exercises the real thing (including fetch) inside Pyodide.
from __future__ import annotations

import sys
import types
from typing import TYPE_CHECKING

import pytest

import trio
import trio.testing

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from types import ModuleType


class FakeJsError(Exception):
    """A JavaScript Error as seen from Python: Pyodide makes those exceptions."""

    def __init__(self, message: str, name: str = "Error") -> None:
        super().__init__(message)
        self.message = message
        self.name = name

    @classmethod
    def new(cls, message: str) -> FakeJsError:
        return cls(message)


def fake_run_js(source: str) -> Callable[[FakeAbortController, str, str], None]:
    """Python stand-in for the JavaScript helper trio._pyodide creates."""
    if "controller.abort" in source:
        return lambda controller, name, message: controller.abort(
            FakeJsError(message, name)
        )
    raise NotImplementedError(source)  # pragma: no cover


class FakeSignal:
    def __init__(self) -> None:
        self.aborted = False
        self.reason: FakeJsError | None = None
        self.listeners: list[Callable[[object], None]] = []


class FakeAbortController:
    def __init__(self) -> None:
        self.signal = FakeSignal()

    @classmethod
    def new(cls) -> FakeAbortController:
        return cls()

    def abort(self, reason: FakeJsError | None = None) -> None:
        if self.signal.aborted:
            return
        if reason is None:
            reason = FakeJsError("This operation was aborted", "AbortError")
        self.signal.aborted = True
        self.signal.reason = reason
        for listener in self.signal.listeners:
            listener(reason)


class FakePromise:
    """Settles asynchronously, like a real promise: callbacks run as
    run_sync_soon jobs, which stand in for JavaScript microtasks."""

    def __init__(self, respects: FakeAbortController | None = None) -> None:
        self.callbacks: list[
            tuple[Callable[[object], object], Callable[[object], object]]
        ] = []
        self.outcome: tuple[str, object] | None = None
        if respects is not None:
            respects.signal.listeners.append(self.reject)

    def then(
        self,
        on_fulfilled: Callable[[object], object],
        on_rejected: Callable[[object], object],
    ) -> FakePromise:
        self.callbacks.append((on_fulfilled, on_rejected))
        if self.outcome is not None:
            self._deliver()
        return self

    def resolve(self, value: object) -> None:
        self._settle("fulfilled", value)

    def reject(self, reason: object) -> None:
        self._settle("rejected", reason)

    def _settle(self, kind: str, value: object) -> None:
        if self.outcome is not None:
            return
        self.outcome = (kind, value)
        if self.callbacks:
            self._deliver()

    def _deliver(self) -> None:
        callbacks, self.callbacks = self.callbacks, []
        assert self.outcome is not None
        kind, value = self.outcome

        def deliver() -> None:
            for on_fulfilled, on_rejected in callbacks:
                (on_fulfilled if kind == "fulfilled" else on_rejected)(value)

        trio.lowlevel.current_trio_token().run_sync_soon(deliver)


@pytest.fixture(scope="module")
def trio_pyodide() -> Iterator[ModuleType]:
    """Import trio.pyodide with fake 'js' and 'pyodide.ffi' modules."""
    if sys.platform == "emscripten":  # pragma: no cover
        import trio.pyodide

        yield trio.pyodide
        return

    fake_js = types.ModuleType("js")
    fake_js.Error = FakeJsError  # type: ignore[attr-defined]
    fake_js.AbortController = FakeAbortController  # type: ignore[attr-defined]
    fake_pyodide = types.ModuleType("pyodide")
    fake_ffi = types.ModuleType("pyodide.ffi")
    fake_ffi.to_js = lambda obj, **kwargs: obj  # type: ignore[attr-defined]
    fake_code = types.ModuleType("pyodide.code")
    fake_code.run_js = fake_run_js  # type: ignore[attr-defined]
    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setitem(sys.modules, "js", fake_js)
        monkeypatch.setitem(sys.modules, "pyodide", fake_pyodide)
        monkeypatch.setitem(sys.modules, "pyodide.ffi", fake_ffi)
        monkeypatch.setitem(sys.modules, "pyodide.code", fake_code)
        monkeypatch.delitem(sys.modules, "trio._pyodide", raising=False)
        monkeypatch.delitem(sys.modules, "trio.pyodide", raising=False)
        import trio.pyodide

        try:
            yield trio.pyodide
        finally:
            sys.modules.pop("trio.pyodide", None)
            sys.modules.pop("trio._pyodide", None)
            for name in ("pyodide", "_pyodide"):
                if hasattr(trio, name):
                    delattr(trio, name)


async def test_fulfilled(trio_pyodide: ModuleType) -> None:
    promise = FakePromise()
    record: list[object] = []

    async def waiter() -> None:
        record.append(await trio_pyodide.wait_promise(promise))

    async with trio.open_nursery() as nursery:
        nursery.start_soon(waiter)
        await trio.testing.wait_all_tasks_blocked()
        assert record == []
        promise.resolve("value")
    assert record == ["value"]


async def test_already_settled(trio_pyodide: ModuleType) -> None:
    promise = FakePromise()
    promise.resolve(42)
    assert await trio_pyodide.wait_promise(promise) == 42


async def test_rejected_with_js_error(trio_pyodide: ModuleType) -> None:
    promise = FakePromise()
    error = FakeJsError("fetch failed", "TypeError")
    promise.reject(error)
    with pytest.raises(FakeJsError) as excinfo:
        await trio_pyodide.wait_promise(promise)
    assert excinfo.value is error


async def test_rejected_with_non_error(trio_pyodide: ModuleType) -> None:
    promise = FakePromise()
    promise.reject("just a string")
    with pytest.raises(
        trio_pyodide.JsPromiseRejected, match="just a string"
    ) as excinfo:
        await trio_pyodide.wait_promise(promise)
    assert excinfo.value.reason == "just a string"


async def test_cancellation_aborts_and_surfaces_when_the_promise_rejects(
    trio_pyodide: ModuleType,
    autojump_clock: trio.testing.MockClock,
) -> None:
    controller = FakeAbortController()
    # like fetch: rejects with the signal's reason when the signal is aborted
    promise = FakePromise(respects=controller)
    with trio.move_on_after(1) as cancel_scope:
        await trio_pyodide.wait_promise(promise, abort_controller=controller)
    assert cancel_scope.cancelled_caught
    assert controller.signal.aborted
    assert controller.signal.reason is not None
    assert controller.signal.reason.name == trio_pyodide.CANCELLED_ERROR_NAME


async def test_cancellation_waits_for_the_promise(
    trio_pyodide: ModuleType,
    autojump_clock: trio.testing.MockClock,
) -> None:
    # The promise ignores the abort signal, so cancelling the task can't
    # interrupt it: the task keeps waiting, and gets the eventual value.
    controller = FakeAbortController()
    promise = FakePromise()
    record: list[object] = []

    async def waiter() -> None:
        with trio.move_on_after(1) as cancel_scope:
            record.append(
                await trio_pyodide.wait_promise(promise, abort_controller=controller)
            )
        record.append(("cancelled_caught", cancel_scope.cancelled_caught))

    async with trio.open_nursery() as nursery:
        nursery.start_soon(waiter)
        await trio.sleep(2)
        assert controller.signal.aborted
        assert record == []
        promise.resolve("late value")
        await trio.testing.wait_all_tasks_blocked()
    # The operation completed, so no Cancelled was raised inside the scope
    assert record == ["late value", ("cancelled_caught", False)]


async def test_cancellation_without_abort_controller(
    trio_pyodide: ModuleType,
    autojump_clock: trio.testing.MockClock,
) -> None:
    promise = FakePromise()
    record: list[object] = []

    async def waiter() -> None:
        with trio.move_on_after(1):
            record.append(await trio_pyodide.wait_promise(promise))

    async with trio.open_nursery() as nursery:
        nursery.start_soon(waiter)
        await trio.sleep(2)
        assert record == []
        promise.resolve("value")
    assert record == ["value"]


async def test_cancelled_then_rejected_with_something_else(
    trio_pyodide: ModuleType,
    autojump_clock: trio.testing.MockClock,
) -> None:
    # If the operation fails for its own reasons after we asked it to abort,
    # that failure is what gets raised; the cancellation waits for the next
    # checkpoint.
    controller = FakeAbortController()
    promise = FakePromise()
    error = FakeJsError("disk on fire", "TypeError")

    async def waiter() -> None:
        with trio.move_on_after(1):
            await trio_pyodide.wait_promise(promise, abort_controller=controller)

    with pytest.RaisesGroup(pytest.RaisesExc(check=lambda exc: exc is error)):
        async with trio.open_nursery() as nursery:
            nursery.start_soon(waiter)
            await trio.sleep(2)
            assert controller.signal.aborted
            promise.reject(error)


async def test_foreign_abort_is_an_error(trio_pyodide: ModuleType) -> None:
    # Somebody else aborting the controller isn't a Trio cancellation
    controller = FakeAbortController()
    promise = FakePromise(respects=controller)

    async def waiter() -> None:
        with pytest.raises(FakeJsError, match="This operation was aborted") as excinfo:
            await trio_pyodide.wait_promise(promise, abort_controller=controller)
        assert excinfo.value.name == "AbortError"

    async with trio.open_nursery() as nursery:
        nursery.start_soon(waiter)
        await trio.testing.wait_all_tasks_blocked()
        controller.abort()


async def test_cancelled_and_foreign_abort(
    trio_pyodide: ModuleType,
    autojump_clock: trio.testing.MockClock,
) -> None:
    # A plain AbortError after we've asked for an abort still counts as our
    # cancellation
    controller = FakeAbortController()
    promise = FakePromise()
    with trio.move_on_after(1) as cancel_scope:

        async def abort_from_js() -> None:
            # JavaScript doesn't care about Trio's cancellation
            with trio.CancelScope(shield=True):
                await trio.sleep(2)
            promise.reject(FakeJsError("This operation was aborted", "AbortError"))

        async with trio.open_nursery() as nursery:
            nursery.start_soon(abort_from_js)
            await trio_pyodide.wait_promise(promise, abort_controller=controller)
    assert cancel_scope.cancelled_caught
