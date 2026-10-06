"""Trio support for Pyodide (Python in the browser): awaiting JavaScript
promises, and an HTTP client built on the browser's ``fetch``.

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

import json
from typing import Final, NamedTuple

import js
from outcome import Value
from pyodide.code import run_js
from pyodide.ffi import to_js

import trio

from ._abc import ReceiveStream
from ._util import final

if TYPE_CHECKING:
    from collections.abc import Mapping

    from typing_extensions import Self

    from ._core._traps import Abort, RaiseCancelT

# The convention for cancellation: when a Trio task waiting on a promise is
# cancelled, we abort the AbortController with a JS Error whose name is this.
# Anything that respects the AbortSignal then rejects its promise with that
# error, and when it arrives back here, we turn it into the task's Cancelled.
CANCELLED_ERROR_NAME: Final = "TrioCancelled"


class JsPromiseRejected(Exception):
    """Raised by `wait_promise` when a promise is rejected with something that
    isn't a JavaScript ``Error`` (JavaScript ``Error`` objects are raised
    directly, as `pyodide.ffi.JsException`).

    The value the promise was rejected with is available as ``.reason``.

    """

    def __init__(self, reason: object) -> None:
        super().__init__(repr(reason))
        self.reason = reason


class _Rejected(NamedTuple):
    reason: object


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


async def wait_promise(promise: object, *, abort_controller: object = None) -> object:
    """Wait for a JavaScript promise to settle, and return the value it was
    fulfilled with.

    If the promise is rejected with a JavaScript ``Error``, that's raised (it
    arrives as a `pyodide.ffi.JsException`); if it's rejected with anything
    else, `JsPromiseRejected` is raised.

    JavaScript promises can't be cancelled, so if the task waiting here is
    cancelled, it keeps waiting until the promise settles. But if you pass an
    ``AbortController`` (the JavaScript object) whose signal the operation
    behind the promise respects, then cancelling the task aborts it, with a
    JavaScript ``Error`` named ``"TrioCancelled"``. If the promise is then
    rejected because of that abort, the task's `~trio.Cancelled` is raised
    here. If the promise is fulfilled anyway, the value is returned as usual,
    and the cancellation is delivered at the task's next checkpoint.

    Don't ``await`` a promise directly from a Trio task: Pyodide's ``await``
    support for promises is built on asyncio and won't work.

    """
    task = trio.lowlevel.current_task()
    raise_cancel: RaiseCancelT | None = None

    # These run as microtasks on the host loop, between guest ticks, so this
    # is the "host wakes a Trio task" path. Pyodide's JsProxy.then wrapper
    # takes care of the handlers' lifetimes.
    def on_fulfilled(value: object) -> None:
        trio.lowlevel.reschedule(task, Value(value))

    def on_rejected(reason: object) -> None:
        trio.lowlevel.reschedule(task, Value(_Rejected(reason)))

    promise.then(on_fulfilled, on_rejected)

    def abort_fn(raise_cancel_: RaiseCancelT) -> Abort:
        nonlocal raise_cancel
        raise_cancel = raise_cancel_
        if abort_controller is not None:
            _abort_with_cancellation(
                abort_controller,
                CANCELLED_ERROR_NAME,
                "the Trio task waiting for this operation was cancelled",
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


def _check_not_aborted(abort_controller: object) -> None:
    # Once a request has been aborted (because a task was cancelled while
    # waiting on it), nothing more can be read from it; the browser would
    # reject with the stale abort reason, which would be confusing.
    if abort_controller.signal.aborted:
        raise trio.BrokenResourceError("this request was aborted")


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
            result = await wait_promise(
                self._reader.read(),
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
                # Tell the browser we're not going to read the rest. The
                # result doesn't matter, but the promise needs a rejection
                # handler, or the host complains about an unhandled rejection.
                stream = (
                    self._reader if self._reader is not None else self._js_response.body
                )
                stream.cancel().catch(lambda _: None)
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
        return await wait_promise(
            self._js_response.text(),
            abort_controller=self._abort_controller,
        )

    async def json(self) -> object:
        """Read the whole body and parse it as JSON."""
        return json.loads(await self.text())

    async def bytes(self) -> bytes:
        """Read the whole body as bytes."""
        _check_not_aborted(self._abort_controller)
        buffer = await wait_promise(
            self._js_response.arrayBuffer(),
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
    waiting for the response and while reading its body; see `wait_promise`
    for the details. Any other keyword arguments are passed through to
    ``fetch`` as request options, for example ``credentials="include"``.

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
    js_response = await wait_promise(
        js.fetch(url, init),
        abort_controller=abort_controller,
    )
    return Response(js_response, abort_controller)
