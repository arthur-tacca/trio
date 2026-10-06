"""Trio support for Pyodide (Python in the browser): calling JavaScript async
functions from Trio tasks, calling Trio async functions from JavaScript, and
an HTTP client built on the browser's ``fetch``.

This only works on Emscripten, with Trio running in guest mode on top of the
JavaScript event loop; see the "Running Trio in the browser with Pyodide"
section of the docs.

"""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING

# Type checkers skip the rest of this module on other platforms, like they do
# for the platform-specific I/O managers; it can't be imported there anyway.
assert not TYPE_CHECKING or sys.platform == "emscripten"

import itertools
import json
import traceback
from typing import Final, NamedTuple

import js
from outcome import Value
from pyodide.code import run_js
from pyodide.ffi import create_proxy, to_js

import trio

from ._abc import ReceiveStream
from ._util import final

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Mapping

    from typing_extensions import Self

    from ._core._traps import Abort, RaiseCancelT

# The convention for cancellation: when a Trio task waiting on a JavaScript
# operation is cancelled, we abort its AbortController with a JavaScript Error
# whose name is this. Anything that respects the AbortSignal then rejects its
# promise with that error, and when it arrives back here, we turn it into the
# task's Cancelled. In the other direction, a Trio task started from
# JavaScript that gets cancelled by Trio rejects its promise with the same
# kind of error.
CANCELLED_ERROR_NAME: Final = "TrioCancelled"
_CANCELLED_MESSAGE: Final = "the Trio task waiting for this operation was cancelled"


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


async def call(
    function: object,
    *args: object,
    abort_controller: object = None,
) -> object:
    """Call a JavaScript function and wait for its result.

    The function is called in JavaScript with the given arguments (converted
    with `pyodide.ffi.to_js`), and if it returns a promise, that's waited
    for. The result is converted to Python the usual way, so a JavaScript
    object arrives as a `pyodide.ffi.JsProxy`.

    If the function throws, or its promise is rejected, with a JavaScript
    ``Error``, that's raised (it arrives as a `pyodide.ffi.JsException`);
    anything else it's rejected with is wrapped in `JsPromiseRejected`.

    JavaScript promises can't be cancelled, so if the task waiting here is
    cancelled, it keeps waiting until the promise settles. But if you pass an
    ``AbortController`` (the JavaScript object) whose signal the operation
    respects, then cancelling the task aborts it, with a JavaScript ``Error``
    named ``"TrioCancelled"``. If the promise is then rejected because of that
    abort, the task's `~trio.Cancelled` is raised here. If the promise is
    fulfilled anyway, the value is returned as usual, and the cancellation is
    delivered at the task's next checkpoint.

    This calls the function as a plain function. For a method, which needs
    its object as ``this``, use `call_method`.

    """
    return await _call(function, None, args, abort_controller)


async def call_method(
    obj: object,
    name: str,
    *args: object,
    abort_controller: object = None,
) -> object:
    """Call a method of a JavaScript object and wait for its result.

    Like `call`, but calls ``obj[name](*args)`` with ``obj`` as ``this``.
    (Passing ``obj.name`` to `call` wouldn't work: a method taken from a
    `pyodide.ffi.JsProxy` loses its object when it's handed back to
    JavaScript.)

    """
    return await _call(obj, name, args, abort_controller)


async def wait_promise(promise: object, *, abort_controller: object = None) -> object:
    """Wait for a JavaScript promise that you already have, and return the
    value it was fulfilled with.

    Prefer `call` or `call_method` where you can, because by the time a
    promise reaches Python, Pyodide has turned it into an `asyncio.Future`:
    this function works through that future's ``then`` method, and as of
    Pyodide 0.28 the conversion itself fails for a promise that's rejected
    with something that isn't an ``Error``. Rejections, cancellation and
    ``abort_controller`` otherwise behave as for `call`.

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
    return await _wait_for_settlement(token, abort_controller)


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
    return _make_error(
        type(exc).__name__,
        str(exc),
        "".join(traceback.format_exception(exc)),
    )


def callable_from_js(
    nursery: trio.Nursery,
    async_fn: Callable[..., Awaitable[object]],
) -> Callable[..., object]:
    """Return a function that JavaScript can call to run ``async_fn``.

    Each call starts ``async_fn(*args)`` as a task in ``nursery``, and
    immediately returns a JavaScript ``Promise``, which is fulfilled with the
    task's return value (converted with `pyodide.ffi.to_js`) or rejected if
    it raises. Exceptions are delivered to the JavaScript caller only; they
    don't crash the nursery. A rejection is a JavaScript ``Error`` whose
    ``name`` is the Python exception type's name, and whose ``stack`` is the
    Python traceback.

    JavaScript can pass an ``AbortSignal`` as the ``signal`` keyword argument
    (with ``callKwargs``, since it's a keyword argument). Aborting it cancels
    the task, and the promise is rejected with the signal's reason. If the
    task is cancelled by Trio instead, say because the nursery is cancelled,
    the promise is rejected with an ``Error`` named ``"TrioCancelled"``.

    The returned function is a plain Python function. To let JavaScript keep
    hold of it, assign it to a JavaScript object (``js.myApp.doThing =
    ...``), or wrap it with `pyodide.ffi.create_proxy`.

    """

    def start(*args: object, signal: object = None) -> object:
        resolvers: list[object] = []
        promise = js.Promise.new(
            lambda resolve, reject: resolvers.extend((resolve, reject))
        )
        resolve, reject = resolvers

        async def run() -> None:
            on_abort = None
            with trio.CancelScope() as cancel_scope:
                if signal is not None:
                    if signal.aborted:
                        cancel_scope.cancel()
                    else:
                        on_abort = create_proxy(lambda _event: cancel_scope.cancel())
                        signal.addEventListener("abort", on_abort)
                try:
                    result = await async_fn(*args)
                except trio.Cancelled:
                    if signal is not None and signal.aborted:
                        reject(signal.reason)
                    else:
                        reject(
                            _make_error(CANCELLED_ERROR_NAME, _CANCELLED_MESSAGE, None)
                        )
                    raise
                except BaseException as exc:
                    reject(_error_from_exception(exc))
                    if not isinstance(exc, Exception):
                        raise
                    return
                finally:
                    if on_abort is not None:
                        signal.removeEventListener("abort", on_abort)
                        on_abort.destroy()
                resolve(to_js(result))

        try:
            nursery.start_soon(run)
        except BaseException as exc:
            # e.g. the nursery has already closed
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
            result = await call_method(
                self._reader,
                "read",
                abort_controller=self._abort_controller,
            )
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
        return await call_method(
            self._js_response,
            "text",
            abort_controller=self._abort_controller,
        )

    async def json(self) -> object:
        """Read the whole body and parse it as JSON."""
        return json.loads(await self.text())

    async def bytes(self) -> bytes:
        """Read the whole body as bytes."""
        _check_not_aborted(self._abort_controller)
        buffer = await call_method(
            self._js_response,
            "arrayBuffer",
            abort_controller=self._abort_controller,
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
    js_response = await call(js.fetch, url, init, abort_controller=abort_controller)
    return Response(js_response, abort_controller)
