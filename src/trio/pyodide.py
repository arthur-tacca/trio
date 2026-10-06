"""Trio APIs for Pyodide (Python in the browser): an HTTP client built on the
browser's ``fetch``, and a way to wait for JavaScript promises. Only available
on Emscripten; see :ref:`guest-run-emscripten`.

"""

import sys
from typing import TYPE_CHECKING

assert not TYPE_CHECKING or sys.platform == "emscripten"

from ._pyodide import (
    CANCELLED_ERROR_NAME as CANCELLED_ERROR_NAME,
    JsPromiseRejected as JsPromiseRejected,
    Response as Response,
    ResponseBody as ResponseBody,
    fetch as fetch,
    wait_promise as wait_promise,
)
