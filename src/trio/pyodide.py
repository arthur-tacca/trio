"""Trio APIs for Pyodide (Python in the browser): calling JavaScript async
functions from Trio, calling Trio async functions from JavaScript, and an HTTP
client built on the browser's ``fetch``. Only available on Emscripten; see
:ref:`guest-run-emscripten`.

"""

import sys
from typing import TYPE_CHECKING

assert not TYPE_CHECKING or sys.platform == "emscripten"

from ._pyodide import (
    CANCELLED_ERROR_NAME as CANCELLED_ERROR_NAME,
    JsPromiseRejected as JsPromiseRejected,
    Response as Response,
    ResponseBody as ResponseBody,
    call as call,
    call_method as call_method,
    callable_from_js as callable_from_js,
    fetch as fetch,
    wait_promise as wait_promise,
)
