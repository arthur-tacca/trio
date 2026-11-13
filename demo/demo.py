"""Runs inside Pyodide, in the page: starts Trio as a guest of the browser's
event loop and drives the demo."""

from __future__ import annotations

from typing import Any

import js
import trio
import trio.pyodide
from pyodide.ffi import JsException

# --- the demo ---------------------------------------------------------------


def element(id: str) -> Any:
    return js.document.getElementById(id)


def add_trace_line(list_id: str, start_time: float, message: str) -> None:
    """Add a line to one of the page's lists, with the time since start_time."""
    elapsed = trio.current_time() - start_time
    item = js.document.createElement("li")
    item.textContent = f"{elapsed:4.1f} s  {message}"
    element(list_id).appendChild(item)


class NurseryDemo:
    """The Start and Cancel buttons: five sleeping tasks in one nursery."""

    def __init__(self, spawner: trio.Nursery) -> None:
        # The page's long-lived nursery, which hosts each run of the demo
        self._spawner = spawner
        # The demo's own nursery, while a run is in progress
        self._nursery: trio.Nursery | None = None
        self._running = False
        self._start_time = 0.0

    def start(self) -> None:
        """Start a run, unless one is already in progress."""
        if self._running:
            return
        self._running = True
        self._spawner.start_soon(self._run)

    def cancel(self) -> None:
        """Cancel the running nursery, if there is one."""
        if self._nursery is None:
            return
        self._trace("cancel requested")
        self._nursery.cancel_scope.cancel()

    def _trace(self, message: str) -> None:
        add_trace_line("trace", self._start_time, message)

    async def _run(self) -> None:
        element("trace").replaceChildren()
        self._start_time = trio.current_time()
        try:
            self._trace("opening the nursery")
            async with trio.open_nursery() as nursery:
                self._nursery = nursery
                for n in range(1, 6):
                    nursery.start_soon(self._sleeper, n)
                self._trace(
                    "all five tasks spawned; the nursery is now waiting for them"
                )
            if nursery.cancel_scope.cancelled_caught:
                self._trace("nursery finished: it was cancelled")
            else:
                self._trace("nursery finished: all tasks completed")
        finally:
            self._nursery = None
            self._running = False

    async def _sleeper(self, n: int) -> None:
        self._trace(f"task {n} started, sleeping for {n} s")
        try:
            await trio.sleep(n)
        except trio.Cancelled:
            self._trace(f"task {n} cancelled")
            raise
        self._trace(f"task {n} finished")


async def fetch_hello() -> None:
    response = await trio.pyodide.fetch("hello.txt")
    element("fetch-out").textContent = (
        f"HTTP {response.status}: {await response.text()}"
    )


async def fetch_cancelled() -> None:
    output = element("fetch-out")
    # Stream the largest file to hand, the Trio wheel, whatever it is called today
    async with await trio.pyodide.fetch("wheels/index.json") as index:
        wheels = await index.json()
    assert isinstance(wheels, list)
    trio_wheel = next(name for name in wheels if name.startswith("trio-"))
    received = 0
    total = "?"
    with trio.move_on_after(0.3) as scope:
        async with await trio.pyodide.fetch(f"wheels/{trio_wheel}") as response:
            total = response.headers.get("content-length", "?")
            # Read slowly, in small pieces, so the timeout hits mid-stream
            while chunk := await response.body.receive_some(32 * 1024):
                received += len(chunk)
                output.textContent = f"streaming... {received} of {total} bytes"
                await trio.sleep(0.1)
    if scope.cancelled_caught:
        output.textContent = (
            f"timed out after 0.3 s with {received} of {total} bytes read; "
            "Cancelled was raised inside the read, and the request was aborted"
        )
    else:
        output.textContent = f"finished before the timeout: {received} bytes"


class WebSocketDemo:
    """The Connect, Send and Disconnect buttons: one WebSocket connection at a
    time, with a task that reports whatever the server sends."""

    def __init__(self, spawner: trio.Nursery) -> None:
        # The page's long-lived nursery, as for NurseryDemo
        self._spawner = spawner
        # From Connect until the connection has ended. Cancelling it is how
        # Disconnect works, whether we're connected or still connecting.
        self._cancel_scope: trio.CancelScope | None = None
        # The connection, once it has been made
        self._connection: trio.pyodide.WebSocketConnection | None = None
        self._start_time = 0.0

    def connect(self, url: str) -> None:
        """Connect to the server, unless a connection is already under way."""
        if self._cancel_scope is not None:
            return
        self._cancel_scope = trio.CancelScope()
        self._spawner.start_soon(self._run, url, self._cancel_scope)

    def send(self, text: str) -> None:
        """Send a message, if there's a connection to send it on."""
        if self._connection is not None:
            self._spawner.start_soon(self._send, self._connection, text)

    def disconnect(self) -> None:
        """Cancel the connection, or the attempt to make one, if there is one."""
        if self._cancel_scope is None or self._cancel_scope.cancel_called:
            return
        self._trace("disconnect requested")
        self._cancel_scope.cancel()

    def _trace(self, message: str) -> None:
        add_trace_line("ws-trace", self._start_time, message)

    async def _run(self, url: str, cancel_scope: trio.CancelScope) -> None:
        element("ws-trace").replaceChildren()
        self._start_time = trio.current_time()
        self._trace(f"connecting to {url}")
        try:
            with cancel_scope:
                async with trio.pyodide.connect_websocket(url) as ws:
                    self._connection = ws
                    self._trace("connected")
                    # This loop ends when the server closes the connection
                    # normally. (An echo server may well send a greeting of
                    # its own before it echoes anything.)
                    async for message in ws:
                        self._trace(f"received {message!r:.200}")
                    reason = f" ({ws.close_reason})" if ws.close_reason else ""
                    self._trace(f"closed by the server: code {ws.close_code}{reason}")
            # Leaving the block closed the connection, if it wasn't already
            if cancel_scope.cancelled_caught:
                self._trace("disconnected: the connection's task was cancelled")
        except trio.pyodide.ConnectionClosedError as exc:
            # Closed with some other code than a normal one, or just lost
            self._trace(f"closed with an error: {exc}")
        except trio.pyodide.InvalidHandshake:
            # Browsers don't tell scripts why; their console usually does
            self._trace("couldn't connect")
        except TimeoutError:
            self._trace("couldn't connect: timed out")
        except JsException as exc:
            # The browser wouldn't even try: not a WebSocket address, say
            self._trace(f"couldn't connect: {exc.message}")
        finally:
            self._connection = None
            self._cancel_scope = None

    async def _send(
        self,
        ws: trio.pyodide.WebSocketConnection,
        text: str,
    ) -> None:
        try:
            await ws.send(text)
        except trio.pyodide.ConnectionClosed:
            # Too late. The task in _run reports how the connection ended.
            return
        self._trace(f"sent {text!r}")


async def double(x: int) -> int:
    await trio.sleep(0.5)  # pretend to work
    return x * 2


async def main() -> None:
    async with trio.open_nursery() as nursery:
        demo = NurseryDemo(nursery)
        websocket = WebSocketDemo(nursery)

        # Expose things for the buttons. Synchronous Trio calls are fine from
        # JavaScript event handlers; async ones go through callable_from_js.
        js.startNursery = demo.start
        js.cancelNursery = demo.cancel
        js.connectWebSocket = websocket.connect
        js.sendWebSocket = websocket.send
        js.disconnectWebSocket = websocket.disconnect
        js.fetchHello = trio.pyodide.callable_from_js(fetch_hello)
        js.fetchCancelled = trio.pyodide.callable_from_js(fetch_cancelled)
        js.trioDouble = trio.pyodide.callable_from_js(double)
        for button_id in (
            "start-btn",
            "cancel-btn",
            "fetch-btn",
            "fetch-cancel-btn",
            "ws-connect-btn",
            "ws-send-btn",
            "ws-disconnect-btn",
            "js-call-btn",
        ):
            element(button_id).disabled = False
        element("status").textContent = "Trio is running"
        await trio.sleep_forever()


def report_end(text: str) -> None:
    element("status").textContent = text


# Start Trio as a guest of the browser's event loop. main() runs until the
# page goes away, so the promise only ever settles if something goes wrong.
trio.pyodide.run(main).then(
    lambda value: report_end(f"Trio finished: {value}"),
    lambda error: report_end(f"Trio failed: {error}"),
)
