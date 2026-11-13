# Tests for trio.pyodide. These run on any platform, using fakes for the bits
# of Pyodide and JavaScript that it touches; tests/pyodide/ exercises the real
# thing (including fetch) inside Pyodide.
from __future__ import annotations

import operator
import sys
import types
from typing import TYPE_CHECKING, ClassVar

import pytest

import trio
import trio.lowlevel
import trio.testing
from trio._core._io_emscripten import EmscriptenIOManager
from trio._core._tests.test_guest_mode_emscripten import (
    FakeBrowserLoop,
    enter_emscripten_mode,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Sequence
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


def soon(fn: Callable[[], object]) -> None:
    """Call fn in a moment, the way a browser delivers events from its loop."""
    trio.lowlevel.current_trio_token().run_sync_soon(fn)


class FakeArrayBuffer:
    """The data of a binary WebSocket message, as Python sees it."""

    def __init__(self, data: bytes) -> None:
        self._data = data

    def to_bytes(self) -> bytes:
        return self._data


class FakeWebSocket:
    """A JavaScript WebSocket, with the server's part played by the test.

    ``script`` says what happens to each new one: "accept"; "hang" (nothing);
    "refuse", the way a browser reports a failed connection (an error event,
    then a close event); "refuse-like-node", the way Node 22 does (an error
    event with a message, and nothing else); or "accept-and-close".
    """

    script: ClassVar[str] = "accept"
    instances: ClassVar[list[FakeWebSocket]] = []

    def __init__(self, url: str, protocols: Sequence[str] = ()) -> None:
        # Every handler it's ever been given
        self.handlers_seen: list[FakeProxy] = []
        self.url = url
        self.protocols = list(protocols)
        self.readyState = 0
        self.protocol = ""
        self.binaryType = "blob"
        self.bufferedAmount = 0
        self.onopen: FakeProxy | None = None
        self.onmessage: FakeProxy | None = None
        self.onerror: FakeProxy | None = None
        self.onclose: FakeProxy | None = None
        self.sent: list[object] = []
        self.close_calls: list[tuple[int | None, str | None]] = []
        self.handlers_at_close: tuple[FakeProxy | None, ...] | None = None
        self.replies_to_close = True
        FakeWebSocket.instances.append(self)

    def __setattr__(self, name: str, value: object) -> None:
        if name.startswith("on") and value is not None:
            assert isinstance(value, FakeProxy)
            self.handlers_seen.append(value)
        super().__setattr__(name, value)

    @classmethod
    def new(cls, url: str, protocols: Sequence[str] = ()) -> FakeWebSocket:
        if "://" not in url:
            raise FakeJsError(f"The URL '{url}' is invalid.", "SyntaxError")
        ws = cls(url, protocols)
        if cls.script != "hang":
            soon(
                {
                    "accept": ws.server_accept,
                    "refuse": ws.server_lost,
                    "refuse-like-node": ws.server_refuse_like_node,
                    "accept-and-close": ws.server_accept_and_close,
                }[cls.script],
            )
        return ws

    @property
    def handlers(self) -> tuple[FakeProxy | None, ...]:
        return (self.onopen, self.onmessage, self.onerror, self.onclose)

    def _fire(self, name: str, **attributes: object) -> None:
        handler = getattr(self, "on" + name)
        if handler is not None:
            handler(types.SimpleNamespace(type=name, **attributes))

    # What the page does

    def send(self, data: object) -> None:
        # A real one throws if it's still connecting, and quietly discards
        # the data if it's closing or closed. Neither should ever be asked for.
        assert self.readyState == 1
        self.sent.append(data)

    def close(self, code: int | None = None, reason: str | None = None) -> None:
        self.close_calls.append((code, reason))
        self.handlers_at_close = self.handlers
        if self.readyState == 0:
            # This is Node's behaviour, the more awkward one: an error event
            # from inside close(), and then no close event, ever.
            self.readyState = 2
            self._fire(
                "error",
                message="Connection was closed before it was established.",
            )
        elif self.readyState == 1:
            self.readyState = 2
            if self.replies_to_close:
                # Servers echo the close code and reason
                soon(
                    lambda: self.server_close(
                        1005 if code is None else code,
                        reason or "",
                    ),
                )

    # What the server (or the network) does

    def server_accept(self) -> None:
        if self.readyState != 0:  # pragma: no cover
            return
        self.readyState = 1
        if self.protocols:
            self.protocol = self.protocols[-1]
        self._fire("open")

    def server_send(self, data: str | bytes) -> None:
        assert self.readyState == 1
        self._fire(
            "message",
            data=data if isinstance(data, str) else FakeArrayBuffer(data),
        )

    def server_close(
        self,
        code: int = 1000,
        reason: str = "",
        was_clean: bool = True,
    ) -> None:
        self.readyState = 3
        self._fire("close", code=code, reason=reason, wasClean=was_clean)

    def server_lost(self) -> None:
        # The connection breaks, or can't be made: an error event that says
        # nothing, then a close event.
        self.readyState = 3
        self._fire("error")
        self._fire("close", code=1006, reason="", wasClean=False)

    def server_refuse_like_node(self) -> None:
        self._fire("error", message="Received network error or non-101 status code.")

    def server_accept_and_close(self) -> None:
        self.server_accept()
        self.server_close(4000, "busy")


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
    fake_js.WebSocket = FakeWebSocket  # type: ignore[attr-defined]
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
# run
################################################################


def test_run_result(monkeypatch: pytest.MonkeyPatch, trio_pyodide: ModuleType) -> None:
    enter_emscripten_mode(monkeypatch)
    host = FakeBrowserLoop()
    monkeypatch.setattr(EmscriptenIOManager, "host", host)

    async def main(x: int) -> int:
        await trio.sleep(0.01)
        return x * 2

    promise = trio_pyodide.run(main, 21)
    assert isinstance(promise, FakePromise)
    assert promise.outcome is None  # the run has only just started
    host.run_until(lambda: promise.outcome is not None)
    assert promise.outcome == ("fulfilled", 42)


def test_run_exception(
    monkeypatch: pytest.MonkeyPatch, trio_pyodide: ModuleType
) -> None:
    enter_emscripten_mode(monkeypatch)
    host = FakeBrowserLoop()
    monkeypatch.setattr(EmscriptenIOManager, "host", host)

    async def main() -> None:
        await trio.lowlevel.checkpoint()
        raise ValueError("nope")

    promise = trio_pyodide.run(main)
    assert isinstance(promise, FakePromise)
    host.run_until(lambda: promise.outcome is not None)
    assert promise.outcome is not None
    kind, error = promise.outcome
    assert kind == "rejected"
    assert isinstance(error, FakeJsError)
    assert error.name == "ValueError"
    assert error.message == "nope"
    assert error.stack is not None
    assert "ValueError: nope" in error.stack


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


################################################################
# connect_websocket
################################################################


@pytest.fixture
def websockets(monkeypatch: pytest.MonkeyPatch) -> list[FakeWebSocket]:
    """The fake WebSockets made during the test, which get accepted unless the
    test changes FakeWebSocket.script."""
    monkeypatch.setattr(FakeWebSocket, "script", "accept")
    monkeypatch.setattr(FakeWebSocket, "instances", [])
    return FakeWebSocket.instances


def assert_released(fake: FakeWebSocket) -> None:
    """The connection has let go of the WebSocket: no event from it can reach
    Python any more, and its proxies have been destroyed."""
    assert fake.handlers == (None, None, None, None)
    assert len(fake.handlers_seen) == 4
    assert all(handler.destroyed for handler in fake.handlers_seen)


async def test_websocket_send_and_recv(
    trio_pyodide: ModuleType,
    websockets: list[FakeWebSocket],
) -> None:
    async with trio_pyodide.connect_websocket("wss://example.com/chat") as ws:
        (fake,) = websockets
        assert fake.url == "wss://example.com/chat"
        assert fake.protocols == []
        assert fake.binaryType == "arraybuffer"
        assert ws.state is trio_pyodide.WebSocketState.OPEN
        assert ws.subprotocol is None
        assert ws.close_code is None
        assert ws.close_reason is None
        assert repr(ws) == (
            "<trio.pyodide.WebSocketConnection OPEN 'wss://example.com/chat'>"
        )

        await ws.send("héllo")
        await ws.send(b"\x00\xff")
        await ws.send(bytearray(b"array"))
        await ws.send(memoryview(b"view"))
        sent = ["héllo", b"\x00\xff", bytearray(b"array"), memoryview(b"view")]
        assert fake.sent == sent

        fake.server_send("héllo yourself")
        fake.server_send(b"\x00\xff")
        assert await ws.recv() == "héllo yourself"
        message = await ws.recv()
        assert isinstance(message, bytes)
        assert message == b"\x00\xff"

    # Leaving the block closed it, normally
    assert fake.close_calls == [(1000, "")]
    assert ws.state is trio_pyodide.WebSocketState.CLOSED
    assert ws.close_code == 1000
    assert ws.close_reason == ""
    assert_released(fake)
    with pytest.raises(trio_pyodide.ConnectionClosedOK):
        await ws.recv()
    with pytest.raises(trio_pyodide.ConnectionClosedOK):
        await ws.send("too late")
    assert fake.sent == sent


def test_websocket_connection_has_no_public_constructor(
    trio_pyodide: ModuleType,
) -> None:
    with pytest.raises(TypeError, match="no public constructor"):
        trio_pyodide.WebSocketConnection(object(), 10)


async def test_websocket_subprotocols(
    trio_pyodide: ModuleType,
    websockets: list[FakeWebSocket],
) -> None:
    async with trio_pyodide.connect_websocket(
        "ws://example.com",
        subprotocols=("chat.v1", "chat.v2"),
    ) as ws:
        (fake,) = websockets
        assert fake.protocols == ["chat.v1", "chat.v2"]
        # The server's choice
        assert ws.subprotocol == "chat.v2"

    with pytest.raises(TypeError, match="subprotocols must be a sequence"):
        async with trio_pyodide.connect_websocket(
            "ws://example.com",
            subprotocols="chat.v1",
        ):
            raise AssertionError("shouldn't connect")  # pragma: no cover
    assert len(websockets) == 1


async def test_websocket_decode_and_text_flags(
    trio_pyodide: ModuleType,
    websockets: list[FakeWebSocket],
) -> None:
    async with trio_pyodide.connect_websocket("ws://example.com") as ws:
        (fake,) = websockets
        for _ in range(2):
            fake.server_send("text")
            fake.server_send(b"binary")
        assert await ws.recv(decode=False) == b"text"
        assert await ws.recv(decode=True) == "binary"
        assert await ws.recv(decode=True) == "text"
        assert await ws.recv(decode=False) == b"binary"

        await ws.send("as binary", text=False)
        await ws.send(b"as text", text=True)
        await ws.send(memoryview(b"view as text"), text=True)
        await ws.send("text anyway", text=True)
        await ws.send(b"binary anyway", text=False)
        assert fake.sent == [
            b"as binary",
            "as text",
            "view as text",
            "text anyway",
            b"binary anyway",
        ]
        with pytest.raises(TypeError, match="str or bytes-like, not list"):
            await ws.send(["not", "a", "message"])


async def test_websocket_messages_wait_to_be_received(
    trio_pyodide: ModuleType,
    websockets: list[FakeWebSocket],
) -> None:
    async with trio_pyodide.connect_websocket("ws://example.com") as ws:
        (fake,) = websockets
        expected = [str(i) for i in range(1000)]
        for message in expected:
            fake.server_send(message)
        assert [await ws.recv() for _ in expected] == expected


async def test_websocket_recv_cancellation_loses_nothing(
    trio_pyodide: ModuleType,
    websockets: list[FakeWebSocket],
    autojump_clock: trio.testing.MockClock,
) -> None:
    async with trio_pyodide.connect_websocket("ws://example.com") as ws:
        (fake,) = websockets
        with trio.move_on_after(1) as scope:
            await ws.recv()
        assert scope.cancelled_caught
        fake.server_send("after the timeout")
        assert await ws.recv() == "after the timeout"


async def test_websocket_iteration(
    trio_pyodide: ModuleType,
    websockets: list[FakeWebSocket],
) -> None:
    async with trio_pyodide.connect_websocket("ws://example.com") as ws:
        (fake,) = websockets
        fake.server_send("one")
        fake.server_send(b"two")
        soon(lambda: fake.server_close(1000, "that's all"))
        # Ends when the connection is closed normally
        assert [message async for message in ws] == ["one", b"two"]
        assert ws.close_code == 1000
        assert ws.close_reason == "that's all"
        await ws.wait_closed()
    # It was closed already, so leaving the block didn't close it again
    assert fake.close_calls == []
    assert_released(fake)


async def test_websocket_closed_by_server_with_an_error_code(
    trio_pyodide: ModuleType,
    websockets: list[FakeWebSocket],
) -> None:
    async with trio_pyodide.connect_websocket("ws://example.com") as ws:
        (fake,) = websockets
        fake.server_send("last words")
        fake.server_close(4001, "go away")
        assert ws.state is trio_pyodide.WebSocketState.CLOSED
        assert_released(fake)
        # What arrived before it closed can still be received
        assert await ws.recv() == "last words"
        with pytest.raises(trio_pyodide.ConnectionClosedError) as excinfo:
            await ws.recv()
        exc = excinfo.value
        assert (exc.code, exc.reason, exc.was_clean) == (4001, "go away", True)
        assert str(exc) == "code 4001 (go away)"
        assert (ws.close_code, ws.close_reason) == (4001, "go away")
        with pytest.raises(trio_pyodide.ConnectionClosedError):
            await ws.send("hello?")
        with pytest.raises(trio_pyodide.ConnectionClosedError):
            async for _ in ws:
                raise AssertionError("no more messages")  # pragma: no cover


@pytest.mark.parametrize(
    ("code", "was_clean", "ok"),
    [
        (1000, True, True),  # normal closure
        (1001, True, True),  # going away
        (1005, True, True),  # the close frame had no code
        (1000, False, False),  # ...but the closing handshake didn't complete
        (1002, True, False),
        (4000, True, False),
        (1006, False, False),
    ],
)
async def test_websocket_closed_ok_or_error(
    trio_pyodide: ModuleType,
    websockets: list[FakeWebSocket],
    code: int,
    was_clean: bool,
    ok: bool,
) -> None:
    async with trio_pyodide.connect_websocket("ws://example.com") as ws:
        (fake,) = websockets
        # A task that's waiting when it happens gets woken
        soon(lambda: fake.server_close(code, "", was_clean))
        with pytest.raises(trio_pyodide.ConnectionClosed) as excinfo:
            await ws.recv()
        expected = (
            trio_pyodide.ConnectionClosedOK
            if ok
            else trio_pyodide.ConnectionClosedError
        )
        assert type(excinfo.value) is expected


async def test_websocket_connection_lost(
    trio_pyodide: ModuleType,
    websockets: list[FakeWebSocket],
) -> None:
    async with trio_pyodide.connect_websocket("ws://example.com") as ws:
        (fake,) = websockets
        fake.server_send("made it")
        fake.server_lost()
        assert await ws.recv() == "made it"
        with pytest.raises(trio_pyodide.ConnectionClosedError) as excinfo:
            await ws.recv()
        assert excinfo.value.code == 1006
        assert not excinfo.value.was_clean
        assert str(excinfo.value) == "code 1006, without a closing handshake"
    assert_released(fake)


@pytest.mark.parametrize(
    ("script", "detail"),
    [
        ("refuse", ""),
        ("refuse-like-node", ": Received network error or non-101 status code."),
    ],
)
async def test_websocket_handshake_fails(
    trio_pyodide: ModuleType,
    websockets: list[FakeWebSocket],
    monkeypatch: pytest.MonkeyPatch,
    script: str,
    detail: str,
) -> None:
    monkeypatch.setattr(FakeWebSocket, "script", script)
    with pytest.raises(trio_pyodide.InvalidHandshake) as excinfo:
        async with trio_pyodide.connect_websocket("ws://nowhere.example"):
            raise AssertionError("shouldn't connect")  # pragma: no cover
    assert str(excinfo.value) == (
        "couldn't open a WebSocket connection to 'ws://nowhere.example'" + detail
    )
    (fake,) = websockets
    assert_released(fake)


async def test_websocket_invalid_uri(
    trio_pyodide: ModuleType,
    websockets: list[FakeWebSocket],
) -> None:
    # Whatever the browser throws from the constructor comes straight through
    with pytest.raises(FakeJsError, match="is invalid"):
        async with trio_pyodide.connect_websocket("not a uri"):
            raise AssertionError("shouldn't connect")  # pragma: no cover
    assert websockets == []


async def test_websocket_open_timeout(
    trio_pyodide: ModuleType,
    websockets: list[FakeWebSocket],
    monkeypatch: pytest.MonkeyPatch,
    autojump_clock: trio.testing.MockClock,
) -> None:
    monkeypatch.setattr(FakeWebSocket, "script", "hang")
    start = trio.current_time()
    with pytest.raises(TimeoutError, match="opening handshake"):
        async with trio_pyodide.connect_websocket(
            "ws://slow.example",
            open_timeout=3,
        ):
            raise AssertionError("shouldn't connect")  # pragma: no cover
    assert trio.current_time() - start == 3
    (fake,) = websockets
    # The attempt was abandoned by closing the WebSocket, and the handlers
    # had gone before that could fire anything at them
    assert fake.close_calls == [(None, None)]
    assert fake.handlers_at_close == (None, None, None, None)
    assert_released(fake)


async def test_websocket_connect_cancelled(
    trio_pyodide: ModuleType,
    websockets: list[FakeWebSocket],
    monkeypatch: pytest.MonkeyPatch,
    autojump_clock: trio.testing.MockClock,
) -> None:
    monkeypatch.setattr(FakeWebSocket, "script", "hang")
    with trio.move_on_after(30) as scope:
        async with trio_pyodide.connect_websocket(
            "ws://slow.example",
            open_timeout=None,
        ):
            raise AssertionError("shouldn't connect")  # pragma: no cover
    assert scope.cancelled_caught
    (fake,) = websockets
    assert fake.close_calls == [(None, None)]
    assert fake.handlers_at_close == (None, None, None, None)
    assert_released(fake)


async def test_websocket_closed_straight_after_opening(
    trio_pyodide: ModuleType,
    websockets: list[FakeWebSocket],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The handshake succeeded, so this isn't a failure to connect, even
    # though the connection has already closed by the time we get it
    monkeypatch.setattr(FakeWebSocket, "script", "accept-and-close")
    async with trio_pyodide.connect_websocket("ws://example.com") as ws:
        assert ws.state is trio_pyodide.WebSocketState.CLOSED
        with pytest.raises(trio_pyodide.ConnectionClosedError) as excinfo:
            await ws.recv()
        assert (excinfo.value.code, excinfo.value.reason) == (4000, "busy")


async def test_websocket_send_waits_for_the_browser(
    trio_pyodide: ModuleType,
    websockets: list[FakeWebSocket],
    autojump_clock: trio.testing.MockClock,
) -> None:
    async with trio_pyodide.connect_websocket("ws://example.com") as ws:
        (fake,) = websockets

        # Cancelling a send before it starts sends nothing...
        with trio.CancelScope() as scope:
            scope.cancel()
            await ws.send("cancelled")
        assert scope.cancelled_caught
        assert fake.sent == []

        # ...and nor does cancelling one that's waiting for the browser to
        # get through what it already has queued
        fake.bufferedAmount = 2**20 + 1
        with trio.move_on_after(1) as scope:
            await ws.send("cancelled")
        assert scope.cancelled_caught
        assert fake.sent == []

        async with trio.open_nursery() as nursery:
            nursery.start_soon(ws.send, "first")
            await trio.testing.wait_all_tasks_blocked()
            nursery.start_soon(ws.send, "second")
            await trio.testing.wait_all_tasks_blocked()
            assert fake.sent == []
            fake.bufferedAmount = 2**20
        # Once there was room they went, in the order they were asked for
        assert fake.sent == ["first", "second"]


async def test_websocket_send_while_closing(
    trio_pyodide: ModuleType,
    websockets: list[FakeWebSocket],
) -> None:
    async with trio_pyodide.connect_websocket("ws://example.com") as ws:
        (fake,) = websockets
        # The server has started closing the connection
        fake.readyState = 2
        assert ws.state is trio_pyodide.WebSocketState.CLOSING

        async def send_too_late() -> None:
            # It waits to see how the connection ends, to report that
            with pytest.raises(trio_pyodide.ConnectionClosedOK):
                await ws.send("too late")

        async with trio.open_nursery() as nursery:
            nursery.start_soon(send_too_late)
            await trio.testing.wait_all_tasks_blocked()
            fake.server_close(1001, "restarting")
        assert fake.sent == []


async def test_websocket_aclose_with_code_and_reason(
    trio_pyodide: ModuleType,
    websockets: list[FakeWebSocket],
) -> None:
    async with trio_pyodide.connect_websocket("ws://example.com") as ws:
        (fake,) = websockets
        # Browsers refuse these, so we do, before closing anything
        with pytest.raises(ValueError, match="1000, or a code from 3000 to 4999"):
            await ws.aclose(1001)
        with pytest.raises(ValueError, match="at most 123 bytes"):
            await ws.aclose(reason="é" * 62)
        assert ws.state is trio_pyodide.WebSocketState.OPEN
        assert fake.close_calls == []

        reason = "é" * 61
        await ws.aclose(3001, reason)
        assert fake.close_calls == [(3001, reason)]
        assert ws.state is trio_pyodide.WebSocketState.CLOSED
        assert (ws.close_code, ws.close_reason) == (3001, reason)
        assert_released(fake)
        # Closing with a code that isn't one of the normal ones counts as an
        # error afterwards, as it does in the websockets library
        with pytest.raises(trio_pyodide.ConnectionClosedError):
            await ws.recv()

        # Closing again does nothing
        await ws.aclose()
        await ws.aclose(4000, "again")
    assert fake.close_calls == [(3001, reason)]


async def test_websocket_aclose_while_server_is_closing(
    trio_pyodide: ModuleType,
    websockets: list[FakeWebSocket],
) -> None:
    async with trio_pyodide.connect_websocket("ws://example.com") as ws:
        (fake,) = websockets
        fake.readyState = 2
        async with trio.open_nursery() as nursery:
            # The browser ignores our close(), since it's closing already;
            # we wait for it to finish all the same
            nursery.start_soon(ws.aclose, 3000, "ours")
            await trio.testing.wait_all_tasks_blocked()
            assert fake.close_calls == [(3000, "ours")]
            assert ws.state is trio_pyodide.WebSocketState.CLOSING
            fake.server_close(1000, "theirs")
        # We didn't get to send our code, so it doesn't count against this
        # being a normal closure
        with pytest.raises(trio_pyodide.ConnectionClosedOK) as excinfo:
            await ws.recv()
        assert excinfo.value.reason == "theirs"


async def test_websocket_close_timeout(
    trio_pyodide: ModuleType,
    websockets: list[FakeWebSocket],
    autojump_clock: trio.testing.MockClock,
) -> None:
    async with trio_pyodide.connect_websocket(
        "ws://example.com",
        close_timeout=2,
    ) as ws:
        (fake,) = websockets
        fake.replies_to_close = False
        start = trio.current_time()
    assert trio.current_time() - start == 2
    assert fake.close_calls == [(1000, "")]
    # The browser is still waiting for the server...
    assert fake.readyState == 2
    # ...but we aren't: for us, it has closed abnormally
    assert ws.state is trio_pyodide.WebSocketState.CLOSED
    assert (ws.close_code, ws.close_reason) == (1006, "")
    assert_released(fake)
    with pytest.raises(trio_pyodide.ConnectionClosedError) as excinfo:
        await ws.recv()
    assert not excinfo.value.was_clean
    # Whatever the browser does with it later doesn't reach us
    fake.server_close(1000)
    assert ws.close_code == 1006


async def test_websocket_aclose_cancelled(
    trio_pyodide: ModuleType,
    websockets: list[FakeWebSocket],
    autojump_clock: trio.testing.MockClock,
) -> None:
    async with trio_pyodide.connect_websocket(
        "ws://example.com",
        close_timeout=None,
    ) as ws:
        (fake,) = websockets
        fake.replies_to_close = False
        with trio.move_on_after(5) as scope:
            await ws.aclose()
        assert scope.cancelled_caught
        # Like any Trio resource, it's closed regardless
        assert fake.close_calls == [(1000, "")]
        assert ws.state is trio_pyodide.WebSocketState.CLOSED
        assert ws.close_code == 1006
        assert_released(fake)


async def test_websocket_closed_when_the_block_raises(
    trio_pyodide: ModuleType,
    websockets: list[FakeWebSocket],
) -> None:
    with pytest.raises(KeyError, match="oops"):
        async with trio_pyodide.connect_websocket("ws://example.com") as ws:
            raise KeyError("oops")
    (fake,) = websockets
    assert fake.close_calls == [(1000, "")]
    assert ws.state is trio_pyodide.WebSocketState.CLOSED
    assert_released(fake)
