# Tests for trio.pyodide. These run on any platform, using fakes for the bits
# of Pyodide and JavaScript that it touches; tests/pyodide/ exercises the real
# thing (including fetch) inside Pyodide.
from __future__ import annotations

import operator
import sys
import types
from typing import TYPE_CHECKING

import pytest

import trio
import trio.lowlevel
import trio.testing

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from types import ModuleType


class FakeJsError(Exception):
    """A JavaScript Error as seen from Python: Pyodide makes those exceptions."""

    def __init__(
        self, message: str, name: str = "Error", stack: str | None = None
    ) -> None:
        super().__init__(message)
        self.message = message
        self.name = name
        self.stack = stack

    @property
    def js_error(self) -> FakeJsError:
        # pyodide.ffi.JsException.js_error: the JavaScript error itself
        return self

    @classmethod
    def new(cls, message: str) -> FakeJsError:
        return cls(message)


class FakeProxy:
    def __init__(self, fn: Callable[..., object]) -> None:  # type: ignore[explicit-any]
        self.fn = fn
        self.destroyed = False

    def __call__(self, *args: object) -> object:
        assert not self.destroyed, "called a destroyed proxy"
        return self.fn(*args)

    def destroy(self) -> None:
        assert not self.destroyed, "proxy destroyed twice"
        self.destroyed = True


class FakeSignal:
    def __init__(self) -> None:
        self.aborted = False
        self.reason: FakeJsError | None = None
        self.listeners: list[Callable[[object], None]] = []

    def addEventListener(self, event: str, listener: Callable[[object], None]) -> None:
        assert event == "abort"
        self.listeners.append(listener)

    def removeEventListener(
        self, event: str, listener: Callable[[object], None]
    ) -> None:
        assert event == "abort"
        self.listeners.remove(listener)


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
        for listener in list(self.signal.listeners):
            listener(reason)


class FakePromise:
    """Settles asynchronously, like a real promise: callbacks run as
    run_sync_soon jobs, which stand in for JavaScript microtasks."""

    def __init__(self, respects: FakeSignal | None = None) -> None:
        self.callbacks: list[
            tuple[Callable[[object], object], Callable[[object], object]]
        ] = []
        self.outcome: tuple[str, object] | None = None
        if respects is not None:
            respects.listeners.append(self.reject)

    @staticmethod
    def new(
        executor: Callable[
            [Callable[[object], None], Callable[[object], None]], object
        ],
    ) -> FakePromise:
        promise = FakePromise()
        executor(promise.resolve, promise.reject)
        return promise

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


def fake_run_js(source: str) -> Callable[..., object]:  # type: ignore[explicit-any]
    """Python stand-ins for the JavaScript helpers trio._pyodide creates."""
    if "controller.abort" in source:
        return lambda controller, name, message: controller.abort(
            FakeJsError(message, name)
        )
    if "error.stack" in source:
        return lambda name, message, stack: FakeJsError(message, name, stack)
    if "stream.cancel" in source:
        return lambda stream: None
    if "Reflect.apply" in source:

        def make_call_in_js(  # type: ignore[explicit-any]
            settle: Callable[[int, bool, object], None],
        ) -> Callable[..., None]:
            def call_in_js(
                target: object, name: str | None, args: tuple[object, ...], token: int
            ) -> None:
                try:
                    function = target if name is None else getattr(target, name)
                    assert callable(function)
                    result = function(*args)
                except BaseException as exc:
                    promise = FakePromise()
                    promise.reject(exc)
                else:
                    if isinstance(result, FakePromise):
                        promise = result
                    else:
                        promise = FakePromise()
                        promise.resolve(result)
                promise.then(
                    lambda value: settle(token, True, value),
                    lambda reason: settle(token, False, reason),
                )

            return call_in_js

        return make_call_in_js
    if "throw reason" in source:

        def throw(reason: object) -> None:
            assert isinstance(reason, BaseException)
            raise reason

        return throw
    raise NotImplementedError(source)  # pragma: no cover


@pytest.fixture(scope="module")
def trio_pyodide() -> Iterator[ModuleType]:
    """Import trio.pyodide with fake 'js' and 'pyodide' modules."""
    if sys.platform == "emscripten":  # pragma: no cover
        import trio.pyodide

        yield trio.pyodide
        return

    fake_js = types.ModuleType("js")
    fake_js.Error = FakeJsError  # type: ignore[attr-defined]
    fake_js.AbortController = FakeAbortController  # type: ignore[attr-defined]
    fake_js.Promise = FakePromise  # type: ignore[attr-defined]
    fake_pyodide = types.ModuleType("pyodide")
    fake_ffi = types.ModuleType("pyodide.ffi")
    fake_ffi.create_proxy = FakeProxy  # type: ignore[attr-defined]
    fake_ffi.JsException = FakeJsError  # type: ignore[attr-defined]
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


################################################################
# call, call_method, wait_promise
################################################################


async def test_call_sync_function(trio_pyodide: ModuleType) -> None:
    assert await trio_pyodide.call(operator.add, 1, 2) == 3


async def test_call_async_function(trio_pyodide: ModuleType) -> None:
    promise = FakePromise()
    record: list[object] = []

    async def waiter() -> None:
        record.append(await trio_pyodide.call(lambda: promise))

    async with trio.open_nursery() as nursery:
        nursery.start_soon(waiter)
        await trio.testing.wait_all_tasks_blocked()
        assert record == []
        promise.resolve("value")
    assert record == ["value"]


async def test_call_sync_throw(trio_pyodide: ModuleType) -> None:
    error = FakeJsError("not a function", "TypeError")

    def throws() -> None:
        raise error

    with pytest.raises(FakeJsError) as excinfo:
        await trio_pyodide.call(throws)
    assert excinfo.value is error


async def test_call_rejected_with_non_error(trio_pyodide: ModuleType) -> None:
    promise = FakePromise()
    promise.reject("just a string")
    with pytest.raises(
        trio_pyodide.JsPromiseRejected, match="just a string"
    ) as excinfo:
        await trio_pyodide.call(lambda: promise)
    assert excinfo.value.reason == "just a string"


async def test_call_method(trio_pyodide: ModuleType) -> None:
    class Thing:
        name = "thing"

        def hello(self, greeting: str) -> str:
            return f"{greeting} from {self.name}"

    assert await trio_pyodide.call_method(Thing(), "hello", "hi") == "hi from thing"


async def test_call_cancellation(
    trio_pyodide: ModuleType,
    autojump_clock: trio.testing.MockClock,
) -> None:
    # like fetch: rejects with the signal's reason when the signal is aborted
    with trio.move_on_after(1) as cancel_scope:
        with trio_pyodide.abort_signal() as signal:
            await trio_pyodide.call(lambda: FakePromise(respects=signal))
    assert cancel_scope.cancelled_caught
    assert signal.reason is not None
    assert signal.reason.name == trio_pyodide.CANCELLED_ERROR_NAME


async def test_wait_promise(trio_pyodide: ModuleType) -> None:
    promise = FakePromise()
    promise.resolve(42)
    assert await trio_pyodide.wait_promise(promise) == 42

    promise = FakePromise()
    error = FakeJsError("fetch failed", "TypeError")
    promise.reject(error)
    with pytest.raises(FakeJsError) as excinfo:
        await trio_pyodide.wait_promise(promise)
    assert excinfo.value is error


async def test_cancellation_aborts_and_surfaces_when_the_promise_rejects(
    trio_pyodide: ModuleType,
    autojump_clock: trio.testing.MockClock,
) -> None:
    with trio.move_on_after(1) as cancel_scope:
        with trio_pyodide.abort_signal() as signal:
            await trio_pyodide.wait_promise(FakePromise(respects=signal))
    assert cancel_scope.cancelled_caught
    assert signal.aborted
    assert signal.reason is not None
    assert signal.reason.name == trio_pyodide.CANCELLED_ERROR_NAME


async def test_cancellation_waits_for_the_promise(
    trio_pyodide: ModuleType,
    autojump_clock: trio.testing.MockClock,
) -> None:
    # The promise ignores the abort signal, so cancelling the task can't
    # interrupt it: the task keeps waiting, and gets the eventual value.
    promise = FakePromise()
    record: list[object] = []
    signals: list[FakeSignal] = []

    async def waiter() -> None:
        with trio.move_on_after(1) as cancel_scope:
            with trio_pyodide.abort_signal() as signal:
                signals.append(signal)
                record.append(await trio_pyodide.call(lambda: promise))
        record.append(("cancelled_caught", cancel_scope.cancelled_caught))

    async with trio.open_nursery() as nursery:
        nursery.start_soon(waiter)
        await trio.sleep(2)
        assert signals[0].aborted
        assert record == []
        promise.resolve("late value")
        await trio.testing.wait_all_tasks_blocked()
    # The operation completed, so no Cancelled was raised inside the scope
    assert record == ["late value", ("cancelled_caught", False)]


async def test_cancellation_outside_an_abort_signal_block(
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
    promise = FakePromise()
    error = FakeJsError("disk on fire", "TypeError")
    signals: list[FakeSignal] = []

    async def waiter() -> None:
        with trio.move_on_after(1):
            with trio_pyodide.abort_signal() as signal:
                signals.append(signal)
                await trio_pyodide.wait_promise(promise)

    with pytest.RaisesGroup(pytest.RaisesExc(check=lambda exc: exc is error)):
        async with trio.open_nursery() as nursery:
            nursery.start_soon(waiter)
            await trio.sleep(2)
            assert signals[0].aborted
            promise.reject(error)


async def test_foreign_abort_is_an_error(trio_pyodide: ModuleType) -> None:
    # The operation being aborted on the JavaScript side, for its own
    # reasons, isn't a Trio cancellation
    promise = FakePromise()

    async def waiter() -> None:
        with trio_pyodide.abort_signal():
            with pytest.raises(
                FakeJsError, match="This operation was aborted"
            ) as excinfo:
                await trio_pyodide.wait_promise(promise)
            assert excinfo.value.name == "AbortError"

    async with trio.open_nursery() as nursery:
        nursery.start_soon(waiter)
        await trio.testing.wait_all_tasks_blocked()
        promise.reject(FakeJsError("This operation was aborted", "AbortError"))


async def test_cancelled_and_foreign_abort(
    trio_pyodide: ModuleType,
    autojump_clock: trio.testing.MockClock,
) -> None:
    # A plain AbortError after we've asked for an abort still counts as our
    # cancellation
    promise = FakePromise()
    with trio.move_on_after(1) as cancel_scope:

        async def abort_from_js() -> None:
            # JavaScript doesn't care about Trio's cancellation
            with trio.CancelScope(shield=True):
                await trio.sleep(2)
            promise.reject(FakeJsError("This operation was aborted", "AbortError"))

        async with trio.open_nursery() as nursery:
            nursery.start_soon(abort_from_js)
            with trio_pyodide.abort_signal():
                await trio_pyodide.wait_promise(promise)
    assert cancel_scope.cancelled_caught


async def test_abort_signal_aborts_on_exit(trio_pyodide: ModuleType) -> None:
    with trio_pyodide.abort_signal() as signal:
        assert isinstance(signal, FakeSignal)
        assert not signal.aborted
    assert signal.aborted
    assert signal.reason is not None
    assert signal.reason.name == trio_pyodide.CANCELLED_ERROR_NAME


async def test_abort_signal_nesting(
    trio_pyodide: ModuleType,
    autojump_clock: trio.testing.MockClock,
) -> None:
    with trio_pyodide.abort_signal() as outer:
        with trio.move_on_after(1):
            with trio_pyodide.abort_signal() as inner:
                await trio_pyodide.call(lambda: FakePromise(respects=inner))
        # The cancellation aborted the innermost block's signal only
        assert inner.aborted
        assert not outer.aborted
    assert outer.aborted


async def test_abort_signal_not_inherited_by_child_tasks(
    trio_pyodide: ModuleType,
    autojump_clock: trio.testing.MockClock,
) -> None:
    promise = FakePromise()

    async def child() -> None:
        with trio.move_on_after(1):
            await trio_pyodide.call(lambda: promise)

    with trio_pyodide.abort_signal() as signal:
        async with trio.open_nursery() as nursery:
            nursery.start_soon(child)
            await trio.sleep(2)
            # The child was cancelled, but it had no signal of its own, so
            # the parent's wasn't touched
            assert not signal.aborted
            promise.resolve(None)
    assert signal.aborted


def test_abort_signal_needs_a_task(trio_pyodide: ModuleType) -> None:
    with pytest.raises(RuntimeError, match="async context"):
        with trio_pyodide.abort_signal():
            pass  # pragma: no cover


################################################################
# callable_from_js
################################################################


def js_awaits(promise: object, record: list[tuple[str, object]]) -> None:
    """Stand-in for JavaScript awaiting the promise."""
    assert isinstance(promise, FakePromise)
    promise.then(
        lambda value: record.append(("fulfilled", value)),
        lambda reason: record.append(("rejected", reason)),
    )


async def test_callable_from_js_result(trio_pyodide: ModuleType) -> None:
    async def double(x: int) -> int:
        await trio.lowlevel.checkpoint()
        return x * 2

    record: list[tuple[str, object]] = []
    start = trio_pyodide.callable_from_js(double)
    js_awaits(start(21), record)
    js_awaits(start(4), record)
    await trio.testing.wait_all_tasks_blocked()
    assert sorted(record, key=repr) == [("fulfilled", 42), ("fulfilled", 8)]


async def test_callable_from_js_keyword_arguments(trio_pyodide: ModuleType) -> None:
    async def greet(name: str, *, greeting: str = "hi") -> str:
        await trio.lowlevel.checkpoint()
        return f"{greeting} {name}"

    record: list[tuple[str, object]] = []
    js_awaits(trio_pyodide.callable_from_js(greet)("bob", greeting="hello"), record)
    await trio.testing.wait_all_tasks_blocked()
    assert record == [("fulfilled", "hello bob")]


async def test_callable_from_js_exception(trio_pyodide: ModuleType) -> None:
    async def boom() -> None:
        await trio.lowlevel.checkpoint()
        raise ValueError("boom")

    record: list[tuple[str, object]] = []
    js_awaits(trio_pyodide.callable_from_js(boom)(), record)
    # ...and the run survives it, which a system task raising would not allow
    await trio.testing.wait_all_tasks_blocked()
    [(kind, error)] = record
    assert kind == "rejected"
    assert isinstance(error, FakeJsError)
    assert (error.name, error.message) == ("ValueError", "boom")
    assert error.stack is not None
    assert error.stack.startswith("Traceback (most recent call last):")
    assert 'raise ValueError("boom")' in error.stack


async def test_callable_from_js_js_error_passes_through(
    trio_pyodide: ModuleType,
) -> None:
    # A JavaScript error raised in Python goes back to JavaScript as itself
    error = FakeJsError("nope", "RangeError")

    async def boom() -> None:
        await trio.lowlevel.checkpoint()
        raise error

    record: list[tuple[str, object]] = []
    js_awaits(trio_pyodide.callable_from_js(boom)(), record)
    await trio.testing.wait_all_tasks_blocked()
    assert record == [("rejected", error)]


async def sleep_until_aborted(
    trio_pyodide: ModuleType,
    signal: object = None,
) -> None:
    with trio_pyodide.cancel_on_abort(signal):
        await trio.sleep_forever()


async def test_callable_from_js_abort_from_js(trio_pyodide: ModuleType) -> None:
    controller = FakeAbortController()
    record: list[tuple[str, object]] = []
    start = trio_pyodide.callable_from_js(sleep_until_aborted)
    js_awaits(start(trio_pyodide, signal=controller.signal), record)
    await trio.testing.wait_all_tasks_blocked()
    assert record == []
    assert len(controller.signal.listeners) == 1
    controller.abort()
    await trio.testing.wait_all_tasks_blocked()
    assert record == [("rejected", controller.signal.reason)]
    # the abort listener was cleaned up
    assert controller.signal.listeners == []


async def test_callable_from_js_already_aborted(trio_pyodide: ModuleType) -> None:
    controller = FakeAbortController()
    controller.abort()
    record: list[tuple[str, object]] = []
    start = trio_pyodide.callable_from_js(sleep_until_aborted)
    js_awaits(start(trio_pyodide, signal=controller.signal), record)
    await trio.testing.wait_all_tasks_blocked()
    assert record == [("rejected", controller.signal.reason)]


def test_callable_from_js_cancelled_when_the_run_ends(
    trio_pyodide: ModuleType,
) -> None:
    controller = FakeAbortController()
    promises: list[object] = []

    async def main() -> None:
        start = trio_pyodide.callable_from_js(sleep_until_aborted)
        promises.append(start(trio_pyodide, signal=controller.signal))
        promises.append(start(trio_pyodide))
        await trio.testing.wait_all_tasks_blocked()
        # Returning ends the run, which cancels the system tasks on its way out

    trio.run(main)
    assert len(promises) == 2
    for promise in promises:
        assert isinstance(promise, FakePromise)
        assert promise.outcome is not None
        kind, error = promise.outcome
        assert kind == "rejected"
        assert isinstance(error, FakeJsError)
        assert error.name == trio_pyodide.CANCELLED_ERROR_NAME
    assert controller.signal.listeners == []


def test_callable_from_js_without_a_run(trio_pyodide: ModuleType) -> None:
    start = trio_pyodide.callable_from_js(trio.sleep)
    promise = start(0)
    assert isinstance(promise, FakePromise)
    assert promise.outcome is not None
    kind, error = promise.outcome
    assert kind == "rejected"
    assert isinstance(error, FakeJsError)
    assert error.name == "RuntimeError"


################################################################
# cancel_on_abort
################################################################


async def test_cancel_on_abort(
    trio_pyodide: ModuleType,
    autojump_clock: trio.testing.MockClock,
) -> None:
    controller = FakeAbortController()
    record: list[object] = []

    async def worker() -> None:
        with trio_pyodide.cancel_on_abort(controller.signal) as scope:
            await trio.sleep_forever()
        record.append(scope.cancelled_caught)  # not reached: the reason is raised

    with pytest.RaisesGroup(
        pytest.RaisesExc(check=lambda exc: exc is controller.signal.reason)
    ):
        async with trio.open_nursery() as nursery:
            nursery.start_soon(worker)
            await trio.sleep(1)
            assert len(controller.signal.listeners) == 1
            controller.abort()
    assert record == []
    assert controller.signal.listeners == []


async def test_cancel_on_abort_already_aborted(trio_pyodide: ModuleType) -> None:
    controller = FakeAbortController()
    controller.abort()
    with pytest.raises(FakeJsError) as excinfo:
        with trio_pyodide.cancel_on_abort(controller.signal):
            await trio.sleep_forever()
    assert excinfo.value is controller.signal.reason


async def test_cancel_on_abort_without_signal(trio_pyodide: ModuleType) -> None:
    with trio_pyodide.cancel_on_abort(None) as scope:
        await trio.lowlevel.checkpoint()
    assert not scope.cancelled_caught


async def test_cancel_on_abort_own_deadline(
    trio_pyodide: ModuleType,
    autojump_clock: trio.testing.MockClock,
) -> None:
    # A cancellation that isn't the abort is just a cancellation
    controller = FakeAbortController()
    with trio_pyodide.cancel_on_abort(controller.signal) as scope:
        scope.deadline = trio.current_time() + 1
        await trio.sleep_forever()
    assert scope.cancelled_caught
    assert not controller.signal.aborted
    assert controller.signal.listeners == []
