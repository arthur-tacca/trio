"""Trio support for Pyodide (Python in the browser): starting a Trio run as a
guest of the browser's event loop, calling JavaScript async functions from
Trio tasks, calling Trio async functions from JavaScript, an HTTP client
built on the browser's ``fetch``, and a WebSocket client built on the
browser's ``WebSocket``.

This only works on Emscripten; see the "Running Trio in the browser with
Pyodide" section of the docs.

"""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING

# Type checkers skip the rest of this module on other platforms, like they do
# for the platform-specific I/O managers; it can't be imported there anyway.
assert not TYPE_CHECKING or sys.platform == "emscripten"

import enum
import itertools
import json
import math
import traceback
from contextlib import asynccontextmanager, contextmanager
from typing import Final, NamedTuple

import js
from outcome import Outcome, Value
from pyodide.code import run_js
from pyodide.ffi import JsException, create_proxy, to_js

import trio

from ._abc import AsyncResource, ReceiveStream
from ._util import NoPublicConstructor, final

if TYPE_CHECKING:
    from collections.abc import (
        AsyncIterator,
        Awaitable,
        Callable,
        Iterator,
        Mapping,
        Sequence,
    )

    from typing_extensions import Self

    from ._core._traps import Abort, RaiseCancelT
    from .abc import Clock, Instrument

# The convention for cancellation: when a Trio task waiting on a JavaScript
# operation is cancelled, we abort its AbortController with a JavaScript Error
# whose name is this. Anything that respects the AbortSignal then rejects its
# promise with that error, and when it arrives back here, we turn it into the
# task's Cancelled. In the other direction, a Trio task started from
# JavaScript that gets cancelled by Trio rejects its promise with the same
# kind of error.
CANCELLED_ERROR_NAME: Final = "TrioCancelled"
_CANCELLED_MESSAGE: Final = "the Trio task waiting for this operation was cancelled"
_BLOCK_ENDED_MESSAGE: Final = (
    "the trio.pyodide.abort_signal() block that made this signal has ended"
)


class JsPromiseRejected(Exception):
    """Raised by `call`, `call_method` and `wait_promise` when a promise is
    rejected with something that isn't a JavaScript ``Error`` (JavaScript
    ``Error`` objects are raised directly, as `pyodide.ffi.JsException`).

    The value the promise was rejected with is available as ``.reason``.

    """

    def __init__(self, reason: object) -> None:
        super().__init__(repr(reason))
        self.reason = reason


class _Rejected(NamedTuple):
    reason: object


################################################################
# Starting a Trio run
################################################################


def run(
    async_fn: Callable[..., Awaitable[object]],
    *args: object,
    clock: Clock | None = None,
    instruments: Sequence[Instrument] = (),
    restrict_keyboard_interrupt_to_checkpoints: bool = False,
    strict_exception_groups: bool = True,
) -> object:
    """Start a Trio run as a guest of the browser's event loop, and return a
    JavaScript ``Promise`` for its result.

    This is the Pyodide counterpart of `trio.run`, which can't work in a
    browser because it would block the page's only thread. Like `trio.run`,
    it runs ``async_fn(*args)`` as the main task, and the result is the
    value it returns or the exception it raises: the promise is fulfilled
    with the return value (converted with `pyodide.ffi.to_js`), or rejected
    with the exception, converted the same way as for `callable_from_js`.
    Unlike `trio.run`, it returns at once, and the run proceeds as the
    browser runs the callbacks Trio asks for.

    Await the promise from JavaScript (``await pyodide.runPython(...)`` hands
    it back as a promise), or from Python code running under asyncio, such as
    the top level of ``pyodide.runPythonAsync``::

        result = await trio.pyodide.run(main)

    For the keyword arguments, see `trio.run`. Only one Trio run can be
    active at a time. If you need to supply your own host loop glue or done
    callback, use `trio.lowlevel.start_guest_run` instead; on Emscripten it
    needs neither.

    """
    resolvers: list[object] = []
    promise = js.Promise.new(
        lambda resolve, reject: resolvers.extend((resolve, reject))
    )
    resolve, reject = resolvers

    def done(outcome: Outcome[object]) -> None:
        try:
            result = outcome.unwrap()
        except BaseException as exc:
            reject(_error_from_exception(exc))
        else:
            resolve(to_js(result))

    trio.lowlevel.start_guest_run(
        async_fn,
        *args,
        done_callback=done,
        clock=clock,
        instruments=instruments,
        restrict_keyboard_interrupt_to_checkpoints=restrict_keyboard_interrupt_to_checkpoints,
        strict_exception_groups=strict_exception_groups,
    )
    return promise


################################################################
# Calling JavaScript from Trio
################################################################

# Tasks waiting for a JavaScript operation to settle, by token
_waiting: dict[int, trio.lowlevel.Task] = {}
_tokens = itertools.count()


def _settle(token: int, fulfilled: bool, value: object) -> None:
    # Called from JavaScript when a promise settles. Promise callbacks run as
    # microtasks on the host loop, between guest ticks, so this is the "host
    # wakes a Trio task" path.
    task = _waiting.pop(token, None)
    if task is None:  # pragma: no cover
        return
    trio.lowlevel.reschedule(task, Value(value if fulfilled else _Rejected(value)))


# The JavaScript half of call() and call_method(). It's important that the
# promise never crosses into Python: Pyodide converts every promise it hands to
# Python into an asyncio.Future (a PyodideFuture), and if that goes back to
# JavaScript it's a different promise, whose rejections are wrapped in
# PythonError and which can't carry a non-Error rejection at all. So we call
# the function here, attach the handlers here, and only hand the settled value
# to Python, through the one permanent proxy of _settle. A synchronous throw
# becomes a rejection, so that _settle always runs as a microtask, after the
# task has gone to sleep.
_call_in_js = run_js(
    """
    (settle) => (target, name, args, token) => {
        let result;
        try {
            if (name == null) {
                result = Reflect.apply(target, undefined, args);
            } else {
                if (typeof target[name] !== "function") {
                    throw new TypeError(String(name) + " is not a method of " + String(target));
                }
                result = Reflect.apply(target[name], target, args);
            }
        } catch (error) {
            result = Promise.reject(error);
        }
        Promise.resolve(result).then(
            (value) => settle(token, true, value),
            (reason) => settle(token, false, reason),
        );
    }
    """,
)(create_proxy(_settle))

# Aborts an AbortController with our cancellation error. The error is made in
# JavaScript, because an Error object that has been through Python comes back
# wrapped in a PythonError.
_abort_with_cancellation = run_js(
    """
    (controller, name, message) => {
        const error = new Error(message);
        error.name = name;
        controller.abort(error);
    }
    """,
)


def _is_abort(reason: object) -> bool:
    return getattr(reason, "name", None) in (CANCELLED_ERROR_NAME, "AbortError")


################################################################
# Cancellation: abort_signal()
################################################################

# The AbortControllers of the abort_signal() blocks each task is inside,
# innermost last. This is keyed by task rather than kept in a context
# variable, because context variables are copied into child tasks, and a
# child cancelled on its own must not abort its parent's operations.
_controllers: dict[trio.lowlevel.Task, list[object]] = {}


def _current_controller() -> object:
    try:
        task = trio.lowlevel.current_task()
    except RuntimeError:
        return None
    stack = _controllers.get(task)
    return stack[-1] if stack else None


@contextmanager
def abort_signal() -> Iterator[object]:
    """Make an ``AbortSignal`` that cancelling the task aborts.

    Use it as a context manager, and pass the signal it gives you to the
    JavaScript operations you call inside the block::

        with trio.pyodide.abort_signal() as signal:
            result = await trio.pyodide.call(js.someAsyncFunction, arg, signal)

    If the task is cancelled while it's waiting, inside the block, for a
    JavaScript operation (with `call`, `call_method` or `wait_promise`), the
    signal is aborted, with a JavaScript ``Error`` named ``"TrioCancelled"``
    (`CANCELLED_ERROR_NAME`). An operation that respects the signal then
    rejects its promise, and the task's `~trio.Cancelled` is raised once that
    rejection arrives. An operation that ignores the signal runs to
    completion, the task gets its value, and the cancellation is delivered
    at the next checkpoint. An ``AbortError`` that arrives while the task
    isn't cancelled is raised like any other rejection.

    Leaving the block aborts the signal too, if that hasn't happened yet, so
    nothing you gave the signal to outlives the block. Blocks nest: an
    operation waited for inside the innermost block uses that block's
    signal. The signal belongs to the task that made it, and isn't inherited
    by tasks started inside the block.

    """
    task = trio.lowlevel.current_task()
    controller = js.AbortController.new()
    _controllers.setdefault(task, []).append(controller)
    try:
        yield controller.signal
    finally:
        stack = _controllers[task]
        popped = stack.pop()
        assert popped is controller
        if not stack:
            del _controllers[task]
        if not controller.signal.aborted:
            _abort_with_cancellation(
                controller,
                CANCELLED_ERROR_NAME,
                _BLOCK_ENDED_MESSAGE,
            )


async def _wait_for_settlement(token: int, abort_controller: object) -> object:
    raise_cancel: RaiseCancelT | None = None

    def abort_fn(raise_cancel_: RaiseCancelT) -> Abort:
        nonlocal raise_cancel
        raise_cancel = raise_cancel_
        if abort_controller is not None:
            _abort_with_cancellation(
                abort_controller,
                CANCELLED_ERROR_NAME,
                _CANCELLED_MESSAGE,
            )
        # Cancellation surfaces when the promise settles, not before
        return trio.lowlevel.Abort.FAILED

    result = await trio.lowlevel.wait_task_rescheduled(abort_fn)
    if isinstance(result, _Rejected):
        if raise_cancel is not None and _is_abort(result.reason):
            raise_cancel()
        if isinstance(result.reason, BaseException):
            raise result.reason
        raise JsPromiseRejected(result.reason)
    return result


async def _call(
    target: object,
    name: str | None,
    args: tuple[object, ...],
    abort_controller: object,
) -> object:
    token = next(_tokens)
    _waiting[token] = trio.lowlevel.current_task()
    try:
        _call_in_js(target, name, to_js(args), token)
    except BaseException:
        del _waiting[token]
        raise
    return await _wait_for_settlement(token, abort_controller)


async def call(function: object, *args: object) -> object:
    """Call a JavaScript function and wait for its result.

    The function is called in JavaScript with the given arguments (converted
    with `pyodide.ffi.to_js`), and if it returns a promise, that's waited
    for. The result is converted to Python the usual way, so a JavaScript
    object arrives as a `pyodide.ffi.JsProxy`.

    If the function throws, or its promise is rejected, with a JavaScript
    ``Error``, that's raised (it arrives as a `pyodide.ffi.JsException`);
    anything else it's rejected with is wrapped in `JsPromiseRejected`.

    JavaScript promises can't be cancelled, so if the task waiting here is
    cancelled, it keeps waiting until the promise settles. To let the
    cancellation interrupt the operation, make the call inside an
    `abort_signal` block and give the operation the block's signal.

    This calls the function as a plain function. For a method, which needs
    its object as ``this``, use `call_method`.

    """
    return await _call(function, None, args, _current_controller())


async def call_method(obj: object, name: str, *args: object) -> object:
    """Call a method of a JavaScript object and wait for its result.

    Like `call`, but calls ``obj[name](*args)`` with ``obj`` as ``this``.
    (Passing ``obj.name`` to `call` wouldn't work: a method taken from a
    `pyodide.ffi.JsProxy` loses its object when it's handed back to
    JavaScript.)

    """
    return await _call(obj, name, args, _current_controller())


async def wait_promise(promise: object) -> object:
    """Wait for a JavaScript promise that you already have, and return the
    value it was fulfilled with.

    Prefer `call` or `call_method` where you can, because by the time a
    promise reaches Python, Pyodide has turned it into an `asyncio.Future`:
    this function works through that future's ``then`` method, and as of
    Pyodide 0.28 the conversion itself fails for a promise that's rejected
    with something that isn't an ``Error``. Rejections and cancellation
    otherwise behave as for `call`.

    Don't ``await`` a promise directly from a Trio task: Pyodide's ``await``
    support for promises is built on asyncio and won't work.

    """
    token = next(_tokens)
    _waiting[token] = trio.lowlevel.current_task()
    try:
        promise.then(
            lambda value: _settle(token, True, value),
            lambda reason: _settle(token, False, reason),
        )
    except BaseException:
        del _waiting[token]
        raise
    return await _wait_for_settlement(token, _current_controller())


################################################################
# Calling Trio from JavaScript
################################################################

_make_error = run_js(
    """
    (name, message, stack) => {
        const error = new Error(message);
        error.name = name;
        if (stack != null) {
            error.stack = stack;
        }
        return error;
    }
    """,
)


def _error_from_exception(exc: BaseException) -> object:
    if isinstance(exc, JsException):
        # A JavaScript error that passed through Python, such as an abort
        # reason raised by cancel_on_abort: hand JavaScript the original
        return exc.js_error
    return _make_error(
        type(exc).__name__,
        str(exc),
        "".join(traceback.format_exception(exc)),
    )


# Raises a JavaScript value in Python, as a JsException wrapping it
_throw = run_js("(reason) => { throw reason; }")


@contextmanager
def cancel_on_abort(signal: object) -> Iterator[trio.CancelScope]:
    """Cancel the block when a JavaScript ``AbortSignal`` is aborted.

    This is the converse of `abort_signal`: a cancel scope that a signal
    JavaScript controls cancels. Use it in a Trio function that JavaScript
    calls (see `callable_from_js`), to let the caller cancel it::

        async def fetch_user(user_id, signal=None):
            with trio.pyodide.cancel_on_abort(signal):
                ...

    Aborting the signal cancels the scope, which raises `~trio.Cancelled`
    at the block's next checkpoint, as any cancel scope does; a signal that
    is already aborted cancels it straight away. When the block ends because
    of that cancellation, the signal's abort reason (the JavaScript error it
    was aborted with) is raised, as a `pyodide.ffi.JsException`, so that a
    function called from JavaScript rejects its promise with the reason, the
    way an aborted ``fetch`` does. Catch it if you want something else to
    happen. ``signal`` may be ``None``, for callers that didn't pass one, in
    which case the block is an ordinary cancel scope.

    The context manager yields the `~trio.CancelScope`, so you can also give
    it a deadline, or cancel it yourself.

    """
    on_abort = None
    with trio.CancelScope() as scope:
        if signal is not None:
            if signal.aborted:
                scope.cancel()
            else:
                on_abort = create_proxy(lambda _event: scope.cancel())
                signal.addEventListener("abort", on_abort)
        try:
            yield scope
        finally:
            if on_abort is not None:
                signal.removeEventListener("abort", on_abort)
                on_abort.destroy()
    if scope.cancelled_caught and signal is not None and signal.aborted:
        _throw(signal.reason)


def callable_from_js(
    async_fn: Callable[..., Awaitable[object]],
) -> Callable[..., object]:
    """Return a function that JavaScript can call to run ``async_fn``.

    Each call starts ``async_fn(*args, **kwargs)`` as a Trio system task and
    immediately returns a JavaScript ``Promise``, which is fulfilled with the
    task's return value (converted with `pyodide.ffi.to_js`) or rejected if
    it raises. Exceptions are delivered to the JavaScript caller only: a
    rejection is a JavaScript ``Error`` whose ``name`` is the Python
    exception type's name, and whose ``stack`` is the Python traceback, or,
    for a `pyodide.ffi.JsException`, the JavaScript error it wraps.
    (JavaScript passes keyword arguments with ``callKwargs``.)

    To let JavaScript cancel the call, have ``async_fn`` take an
    ``AbortSignal`` as an argument and use `cancel_on_abort` with it; the
    promise is then rejected with the signal's abort reason.

    The task belongs to no nursery of yours: JavaScript chose to make the
    call, so JavaScript owns its lifetime, through such a signal. The only
    other thing that cancels it is the end of the Trio run, which cancels
    every system task; the promise is then rejected with an ``Error`` named
    ``"TrioCancelled"``. A call made while no Trio run is active is rejected
    with a ``RuntimeError``. Like any system task, it runs with
    :exc:`KeyboardInterrupt` protection enabled.

    The returned function is a plain Python function. To let JavaScript keep
    hold of it, assign it to a JavaScript object (``js.myApp.doThing =
    ...``), or wrap it with `pyodide.ffi.create_proxy`.
    """

    def start(*args: object, **kwargs: object) -> object:
        resolvers: list[object] = []
        promise = js.Promise.new(
            lambda resolve, reject: resolvers.extend((resolve, reject))
        )
        resolve, reject = resolvers

        async def run() -> None:
            try:
                result = await async_fn(*args, **kwargs)
            except trio.Cancelled:
                reject(_make_error(CANCELLED_ERROR_NAME, _CANCELLED_MESSAGE, None))
                raise
            except BaseException as exc:
                reject(_error_from_exception(exc))
                if not isinstance(exc, Exception):
                    raise
                return
            resolve(to_js(result))

        try:
            trio.lowlevel.spawn_system_task(run, name=async_fn)
        except RuntimeError as exc:
            # No Trio run is active, or it has finished
            reject(_error_from_exception(exc))
        return promise

    return start


################################################################
# fetch
################################################################


def _check_not_aborted(abort_controller: object) -> None:
    # Once a request has been aborted (because a task was cancelled while
    # waiting on it), nothing more can be read from it; the browser would
    # reject with the stale abort reason, which would be confusing.
    if abort_controller.signal.aborted:
        raise trio.BrokenResourceError("this request was aborted")


# Cancels a ReadableStream (or a reader of one), ignoring the result
_cancel_stream = run_js("(stream) => { stream.cancel().catch(() => {}); }")


@final
class ResponseBody(ReceiveStream):
    """The body of a `Response`, as a `~trio.abc.ReceiveStream`."""

    def __init__(self, js_response: object, abort_controller: object) -> None:
        self._js_response = js_response
        self._abort_controller = abort_controller
        self._reader: object = None
        self._buffer = b""
        self._eof = False
        self._closed = False

    async def receive_some(self, max_bytes: int | None = None) -> bytes:
        if self._closed:
            raise trio.ClosedResourceError
        if max_bytes is not None and max_bytes < 1:
            raise ValueError("max_bytes must be >= 1")
        if self._buffer:
            chunk = self._buffer
        elif self._eof or self._js_response.body is None:
            await trio.lowlevel.checkpoint()
            return b""
        else:
            _check_not_aborted(self._abort_controller)
            if self._reader is None:
                # getReader() locks the stream, so only do it once we're
                # actually going to read
                self._reader = self._js_response.body.getReader()
            result = await _call(self._reader, "read", (), self._abort_controller)
            if self._closed:
                raise trio.ClosedResourceError
            if result.done:
                self._eof = True
                return b""
            chunk = result.value.to_bytes()
        if max_bytes is not None and len(chunk) > max_bytes:
            chunk, self._buffer = chunk[:max_bytes], chunk[max_bytes:]
        else:
            self._buffer = b""
        return chunk

    async def aclose(self) -> None:
        if not self._closed:
            self._closed = True
            self._buffer = b""
            if not self._eof and self._js_response.body is not None:
                # Tell the browser we're not going to read the rest
                _cancel_stream(
                    (
                        self._reader
                        if self._reader is not None
                        else self._js_response.body
                    ),
                )
        await trio.lowlevel.checkpoint()


@final
class Response:
    """The response to a `fetch` request.

    The status line and headers are available immediately; the body can be
    read as a whole with `text`, `json` or `bytes`, or streamed through
    `body`. Use it as an async context manager (or call `aclose`) to make
    sure the body is released if you don't read it to the end.

    """

    def __init__(self, js_response: object, abort_controller: object) -> None:
        self._js_response = js_response
        self._abort_controller = abort_controller
        self.status: int = js_response.status
        self.status_text: str = js_response.statusText
        self.ok: bool = js_response.ok
        self.url: str = js_response.url
        self.redirected: bool = js_response.redirected
        self.headers: dict[str, str] = dict(js_response.headers.entries())
        self.body: ResponseBody = ResponseBody(js_response, abort_controller)

    def __repr__(self) -> str:
        return (
            f"<trio.pyodide.Response [{self.status} {self.status_text}] {self.url!r}>"
        )

    async def text(self) -> str:
        """Read the whole body, decoded as text."""
        _check_not_aborted(self._abort_controller)
        return await _call(self._js_response, "text", (), self._abort_controller)

    async def json(self) -> object:
        """Read the whole body and parse it as JSON."""
        return json.loads(await self.text())

    async def bytes(self) -> bytes:
        """Read the whole body as bytes."""
        _check_not_aborted(self._abort_controller)
        buffer = await _call(
            self._js_response, "arrayBuffer", (), self._abort_controller
        )
        return buffer.to_bytes()

    async def aclose(self) -> None:
        """Release the body, if it hasn't been read to the end."""
        await self.body.aclose()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.aclose()


async def fetch(
    url: str,
    *,
    method: str = "GET",
    headers: Mapping[str, str] | None = None,
    body: bytes | bytearray | memoryview | str | None = None,
    **options: object,
) -> Response:
    """Make an HTTP request using the browser's ``fetch``, and return a
    `Response` once the status line and headers have arrived.

    Cancelling the task (with a timeout, say) aborts the request, both while
    waiting for the response and while reading its body; see `call` for the
    details. Any other keyword arguments are passed through to ``fetch`` as
    request options, for example ``credentials="include"``.

    Network failures are raised as `pyodide.ffi.JsException`, like the
    ``TypeError`` that ``fetch`` itself rejects with.

    """
    if "signal" in options:
        raise TypeError(
            "fetch() doesn't take a signal; cancel the Trio task instead",
        )
    abort_controller = js.AbortController.new()
    init = js.Object.new()
    init.method = method
    init.signal = abort_controller.signal
    if headers is not None:
        init.headers = to_js(dict(headers), dict_converter=js.Object.fromEntries)
    if body is not None:
        init.body = (
            to_js(body) if isinstance(body, (bytes, bytearray, memoryview)) else body
        )
    for key, value in options.items():
        setattr(init, key, value)
    js_response = await _call(js.fetch, None, (url, init), abort_controller)
    return Response(js_response, abort_controller)


################################################################
# WebSockets
################################################################

# This follows the connection API of the 'websockets' library's Trio
# implementation (websockets.trio), as far as a browser allows, so that the
# same program can use that library natively and this module in a browser.
# It's a parallel implementation rather than a port: the browser does the
# protocol itself, and only lets a script see whole messages and four events.

# send() waits until the browser has fewer than this many bytes queued before
# handing it another message. Browsers don't say when their queue drains, so
# waiting means polling, and a generous mark keeps the polling rare.
_SEND_HIGH_WATER_MARK: Final = 2**20
_SEND_POLL_INTERVAL: Final = 0.01

# RFC 6455 close codes. The "OK" ones are normal closure, going away, and the
# stand-in for a close frame that didn't carry a code; 1006 is the stand-in
# for a connection that ended without a close frame at all.
_OK_CLOSE_CODES: Final = frozenset({1000, 1001, 1005})
_ABNORMAL_CLOSURE: Final = 1006


class WebSocketState(enum.IntEnum):
    """The state of a `WebSocketConnection`. The values are those of a
    JavaScript ``WebSocket``'s ``readyState``."""

    CONNECTING = 0
    OPEN = 1
    CLOSING = 2
    CLOSED = 3


class InvalidHandshake(Exception):
    """Raised by `connect_websocket` when the connection can't be made: the
    server couldn't be reached, or it didn't accept the WebSocket handshake.

    Browsers deliberately don't tell scripts which of those it was, or why
    (their developer console usually does), so there are no details here
    beyond whatever message the JavaScript runtime offers.

    """


class ConnectionClosed(Exception):
    """Raised when sending to, or receiving from, a `WebSocketConnection`
    that has been closed. It's always one of the two subclasses,
    `ConnectionClosedOK` or `ConnectionClosedError`.

    The attributes are what the browser reports when a connection closes:

    * ``code``: the close code from the server's close frame; 1005 if its
      close frame had none, and 1006 if the connection ended without one
    * ``reason``: the reason from the server's close frame, often empty
    * ``was_clean``: whether both sides completed the closing handshake

    """

    def __init__(self, code: int, reason: str, was_clean: bool) -> None:
        super().__init__(code, reason, was_clean)
        self.code = code
        self.reason = reason
        self.was_clean = was_clean

    def __str__(self) -> str:
        text = f"code {self.code}"
        if self.reason:
            text += f" ({self.reason})"
        if not self.was_clean:
            text += ", without a closing handshake"
        return text


class ConnectionClosedOK(ConnectionClosed):
    """The connection was closed normally: the closing handshake completed,
    with a close code of 1000 (normal closure) or 1001 (going away), or
    without one."""


class ConnectionClosedError(ConnectionClosed):
    """The connection was closed with any other close code, or was lost
    without a closing handshake."""


@final
class WebSocketConnection(AsyncResource, metaclass=NoPublicConstructor):
    """A WebSocket connection, as made by `connect_websocket`.

    Send messages with `send`, and receive them with `recv` or by iterating
    over the connection with ``async for``. A message is a `str` (a WebSocket
    text message) or a `bytes` (a binary one).

    """

    def __init__(self, js_websocket: object, close_timeout: float | None) -> None:
        self._ws = js_websocket
        self._close_timeout = math.inf if close_timeout is None else close_timeout
        # Messages wait here until recv() takes them. There's no limit,
        # because a browser can't be asked to stop receiving.
        channels = trio.open_memory_channel[str | bytes](math.inf)
        self._send_channel, self._receive_channel = channels
        self._send_lock = trio.Lock()
        self._opened = False
        self._error_message = ""
        # Set once the handshake has succeeded or failed
        self._handshake_done = trio.Event()
        # (code, reason, was_clean) once the connection is over. That's
        # usually when the browser says it has closed, but see _finish().
        self._close_info: tuple[int, str, bool] | None = None
        self._closed = trio.Event()
        # The code we asked the browser to close with, if we closed first
        self._sent_close_code: int | None = None

        js_websocket.binaryType = "arraybuffer"
        self._handlers = {
            "onopen": create_proxy(self._on_open),
            "onmessage": create_proxy(self._on_message),
            "onerror": create_proxy(self._on_error),
            "onclose": create_proxy(self._on_close),
        }
        for name, handler in self._handlers.items():
            setattr(js_websocket, name, handler)

    def __repr__(self) -> str:
        return f"<trio.pyodide.WebSocketConnection {self.state.name} {self._ws.url!r}>"

    # The event handlers. JavaScript calls these from the browser's event
    # loop, between Trio's ticks, so like any host code they may only use
    # Trio's synchronous functions.

    def _on_open(self, event: object) -> None:
        self._opened = True
        self._handshake_done.set()

    def _on_message(self, event: object) -> None:
        data = event.data
        if not isinstance(data, str):
            data = data.to_bytes()
        self._send_channel.send_nowait(data)

    def _on_error(self, event: object) -> None:
        if self._opened:
            # An open connection broke. A close event always follows, and
            # says all that the browser is willing to (code 1006).
            return
        # The connection couldn't be made. Browsers follow this with a close
        # event that adds nothing, and Node (as of version 22) with no event
        # at all, so this is where a connection attempt ends. Browsers say
        # nothing about what went wrong; some other runtimes give a message.
        message = getattr(event, "message", None)
        if isinstance(message, str):
            self._error_message = message
        self._finish(_ABNORMAL_CLOSURE, "", False)

    def _on_close(self, event: object) -> None:
        self._finish(event.code, event.reason, event.wasClean)

    def _finish(self, code: int, reason: str, was_clean: bool) -> None:
        # The connection is over, as far as this object is concerned. Usually
        # that's because the browser said so (a close event), but we also get
        # here by giving up: on a connection attempt that failed, timed out or
        # was cancelled, or on a closing handshake that the server didn't
        # complete in time.
        #
        # Either way, the browser has nothing more to tell us that we'd act
        # on, so this lets go of the event handlers. That means JavaScript
        # never calls into this object after the connection is finished, and
        # in particular never after the 'async with' block that owns it has
        # exited, whatever the browser does with the socket afterwards.
        if self._close_info is not None:
            return
        self._close_info = (code, reason, was_clean)
        handlers, self._handlers = self._handlers, {}
        for name, handler in handlers.items():
            setattr(self._ws, name, None)
            handler.destroy()
        self._send_channel.close()
        self._handshake_done.set()
        self._closed.set()

    def _abandon(self) -> None:
        # Give up on the connection without a closing handshake. The handlers
        # go first, because close() can fire an error event synchronously
        # (Node does when the connection hasn't been made yet).
        self._finish(_ABNORMAL_CLOSURE, "", False)
        self._ws.close()

    def _close_exc(self) -> ConnectionClosed:
        assert self._close_info is not None
        code, reason, was_clean = self._close_info
        ok = (
            was_clean
            and code in _OK_CLOSE_CODES
            and (
                self._sent_close_code is None
                or self._sent_close_code in _OK_CLOSE_CODES
            )
        )
        exc_type = ConnectionClosedOK if ok else ConnectionClosedError
        return exc_type(code, reason, was_clean)

    @property
    def state(self) -> WebSocketState:
        """The connection's state, a `WebSocketState`. This is here for
        completeness; normally you'd just call `send` or `recv`, and handle
        `ConnectionClosed`."""
        if self._close_info is not None:
            return WebSocketState.CLOSED
        return WebSocketState(self._ws.readyState)

    @property
    def subprotocol(self) -> str | None:
        """The subprotocol the server selected, or ``None`` if it didn't
        select one."""
        return self._ws.protocol or None

    @property
    def close_code(self) -> int | None:
        """The close code, as for `ConnectionClosed`; ``None`` until the
        connection is closed."""
        return None if self._close_info is None else self._close_info[0]

    @property
    def close_reason(self) -> str | None:
        """The close reason, as for `ConnectionClosed`; ``None`` until the
        connection is closed."""
        return None if self._close_info is None else self._close_info[1]

    async def recv(self, decode: bool | None = None) -> str | bytes:
        """Receive the next message: a `str` for a text message, or a `bytes`
        for a binary one.

        Pass ``decode=True`` to get a binary message as a `str` too (decoded
        as UTF-8), or ``decode=False`` to get a text message as `bytes`.

        Once the connection has closed, and the messages that arrived before
        that have all been received, this raises `ConnectionClosed`:
        `ConnectionClosedOK` after a normal closure, and
        `ConnectionClosedError` otherwise.

        Cancelling this is safe: no message is lost, and the next call
        returns the next message. Messages that arrive while nobody is
        receiving are kept, without limit, because a browser can't be told to
        stop reading from a WebSocket.

        """
        try:
            message = await self._receive_channel.receive()
        except trio.EndOfChannel:
            raise self._close_exc() from None
        if decode is True and not isinstance(message, str):
            return message.decode()
        if decode is False and isinstance(message, str):
            return message.encode()
        return message

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> str | bytes:
        # Iteration ends when the connection is closed normally, and raises
        # ConnectionClosedError if it ends any other way.
        try:
            return await self.recv()
        except ConnectionClosedOK:
            raise StopAsyncIteration from None

    async def send(
        self,
        message: str | bytes | bytearray | memoryview,
        *,
        text: bool | None = None,
    ) -> None:
        """Send a message: a `str` as a text message, and `bytes` (or a
        `bytearray` or `memoryview`) as a binary one.

        Pass ``text=True`` to send bytes that hold UTF-8 as a text message,
        or ``text=False`` to send a `str` as a binary message.

        If the browser already has a lot of data queued for this connection,
        this first waits for it to send some of that. Cancelling this is
        safe: if it raises `~trio.Cancelled`, the message has not been sent,
        and otherwise all of it has been handed to the browser.

        Raises `ConnectionClosed` if the connection is closed, like `recv`.

        """
        data: object
        if isinstance(message, str):
            data = message if text is not False else to_js(message.encode())
        elif isinstance(message, (bytes, bytearray, memoryview)):
            data = str(message, "utf-8") if text else to_js(message)
        else:
            raise TypeError(
                f"a message must be a str or bytes-like, not {type(message).__name__}",
            )
        # The lock keeps tasks that are waiting to send in order. Taking it
        # is also this function's checkpoint; after that, nothing below can
        # be interrupted between deciding to send and having sent.
        async with self._send_lock:
            while True:
                if (
                    self._close_info is not None
                    or self._ws.readyState != WebSocketState.OPEN
                ):
                    # Closed, or closing, in which case wait to see how
                    await self._closed.wait()
                    raise self._close_exc()
                if self._ws.bufferedAmount <= _SEND_HIGH_WATER_MARK:
                    break
                await trio.sleep(_SEND_POLL_INTERVAL)
            self._ws.send(data)

    async def aclose(self, code: int = 1000, reason: str = "") -> None:
        """Close the connection: start the closing handshake, sending the
        server ``code`` and ``reason``, and wait for it to finish.

        If the server hasn't completed the handshake after ``close_timeout``
        seconds (see `connect_websocket`), or this is cancelled, the wait is
        abandoned and the connection counts as closed abnormally, with close
        code 1006; the browser finishes with the socket in its own time. So
        the connection is always closed when this returns or raises.

        Browsers only allow scripts the close code 1000 (normal closure) and
        the codes from 3000 to 4999, and a reason of at most 123 bytes;
        anything else is a `ValueError`. Closing a connection that's already
        closed, or closing, has no further effect.

        """
        if code != 1000 and not 3000 <= code <= 4999:
            raise ValueError(
                "a browser can only close a WebSocket with code 1000, or a code "
                f"from 3000 to 4999, not {code}",
            )
        if len(reason.encode()) > 123:
            raise ValueError("a close reason can be at most 123 bytes of UTF-8")
        if self._close_info is None:
            if self._ws.readyState == WebSocketState.OPEN:
                self._sent_close_code = code
            # (This does nothing if the browser is already closing it.)
            self._ws.close(code, reason)
        try:
            with trio.move_on_after(self._close_timeout):
                await self._closed.wait()
        finally:
            self._finish(_ABNORMAL_CLOSURE, "", False)

    async def wait_closed(self) -> None:
        """Wait until the connection is closed."""
        await self._closed.wait()


@asynccontextmanager
async def connect_websocket(
    uri: str,
    *,
    subprotocols: Sequence[str] | None = None,
    open_timeout: float | None = 10,
    close_timeout: float | None = 10,
) -> AsyncIterator[WebSocketConnection]:
    """Connect to a WebSocket server, using the browser's ``WebSocket``.

    This is an async context manager, giving a `WebSocketConnection`::

        async with trio.pyodide.connect_websocket("wss://example.com/chat") as ws:
            await ws.send("hello")
            reply = await ws.recv()

    The connection is closed when the block exits, as if by
    `WebSocketConnection.aclose`.

    Args:
      uri: The server's URI, such as ``wss://example.com/chat``. The browser
          is what interprets it, so one without a scheme is taken relative to
          the page's.
      subprotocols: The subprotocols to offer the server, in order of
          preference. Its choice is the connection's ``subprotocol``.
      open_timeout: How long to wait for the connection to be made, in
          seconds; ``None`` waits indefinitely.
      close_timeout: How long `WebSocketConnection.aclose` waits for the
          server to complete the closing handshake, in seconds; ``None``
          waits indefinitely.

    Raises:
      InvalidHandshake: if the connection can't be made.
      TimeoutError: if that takes longer than ``open_timeout``.
      pyodide.ffi.JsException: if the browser won't even try, because the URI
          or subprotocols are invalid, or because its security policy forbids
          the connection (an insecure ``ws`` URI from a page loaded over
          HTTPS, for example).

    Cancelling the wait for the connection abandons the attempt.

    The API follows the `websockets <https://websockets.readthedocs.io/>`__
    library's Trio implementation, ``websockets.trio``, so that a program can
    use that natively and this in a browser. It's a subset, because a browser
    keeps much of the protocol to itself. It doesn't let a script send or
    see pings, set HTTP headers, choose extensions or limits, or use a proxy
    of its own choosing; it doesn't report why a handshake failed; it only
    sends and delivers whole messages; and it won't close with code 1011
    when the block exits with an exception, as that library does.

    """
    if isinstance(subprotocols, str):
        raise TypeError("subprotocols must be a sequence of str, not a str")
    if subprotocols:
        js_websocket = js.WebSocket.new(uri, to_js(list(subprotocols)))
    else:
        js_websocket = js.WebSocket.new(uri)
    connection = WebSocketConnection._create(js_websocket, close_timeout)
    try:
        with trio.fail_after(math.inf if open_timeout is None else open_timeout):
            await connection._handshake_done.wait()
    except BaseException as exc:
        # Closing a WebSocket that's still connecting abandons the attempt
        connection._abandon()
        if isinstance(exc, trio.TooSlowError):
            raise TimeoutError("timed out during opening handshake") from exc
        raise
    if not connection._opened:
        message = f"couldn't open a WebSocket connection to {uri!r}"
        if connection._error_message:
            message += f": {connection._error_message}"
        raise InvalidHandshake(message)
    try:
        yield connection
    finally:
        await connection.aclose()
