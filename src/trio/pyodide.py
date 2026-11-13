"""Trio APIs for Pyodide (Python in the browser): starting a Trio run as a
guest of the browser's event loop, calling JavaScript async functions from
Trio, calling Trio async functions from JavaScript, an HTTP client built on
the browser's ``fetch``, and a WebSocket client built on the browser's
``WebSocket``. Only available on Emscripten; see :ref:`guest-run-emscripten`.

"""

import sys
from typing import TYPE_CHECKING

assert not TYPE_CHECKING or sys.platform == "emscripten"

from ._pyodide import (
    CANCELLED_ERROR_NAME as CANCELLED_ERROR_NAME,
    ConnectionClosed as ConnectionClosed,
    ConnectionClosedError as ConnectionClosedError,
    ConnectionClosedOK as ConnectionClosedOK,
    InvalidHandshake as InvalidHandshake,
    JsPromiseRejected as JsPromiseRejected,
    Response as Response,
    ResponseBody as ResponseBody,
    WebSocketConnection as WebSocketConnection,
    WebSocketState as WebSocketState,
    abort_signal as abort_signal,
    call as call,
    call_method as call_method,
    callable_from_js as callable_from_js,
    cancel_on_abort as cancel_on_abort,
    connect_websocket as connect_websocket,
    fetch as fetch,
    run as run,
    wait_promise as wait_promise,
)
