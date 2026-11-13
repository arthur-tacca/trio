"""Test for trio.pyodide's WebSocket client, connect_websocket.

This runs *inside* Pyodide, driven by run_tests.mjs, against the WebSocket
endpoints of the server in test_server.mjs (under js.TEST_BASE_URL).
"""

from __future__ import annotations

import sys
import time
from typing import TYPE_CHECKING

import js
import trio
from pyodide.ffi import JsException
from trio.pyodide import (
    ConnectionClosed,
    ConnectionClosedError,
    ConnectionClosedOK,
    InvalidHandshake,
    WebSocketConnection,
    WebSocketState,
    connect_websocket,
)

if TYPE_CHECKING:
    from outcome import Outcome


async def how_it_closed(ws: WebSocketConnection) -> ConnectionClosed:
    """Receive from a connection that has nothing left to give."""
    try:
        message = await ws.recv()
    except ConnectionClosed as exc:
        closed = exc
    else:
        raise AssertionError(f"expected ConnectionClosed, got {message!r}")
    assert ws.state is WebSocketState.CLOSED
    assert (ws.close_code, ws.close_reason) == (closed.code, closed.reason)
    return closed


async def trio_main() -> dict[str, object]:
    base = str(js.TEST_BASE_URL).replace("http", "ws", 1) + "/ws"
    closed_port = str(js.TEST_CLOSED_URL).replace("http", "ws", 1)
    results: dict[str, object] = {}

    def progress(label: str) -> None:
        # Printed as we go, so a hang shows where it happened
        print(" ...", label)

    # --- sending and receiving ------------------------------------------------
    progress("sending and receiving")
    async with connect_websocket(base + "/echo") as ws:
        assert ws.state is WebSocketState.OPEN
        assert ws.subprotocol is None
        assert (ws.close_code, ws.close_reason) == (None, None)
        await ws.send("héllo wörld ✓")
        assert await ws.recv() == "héllo wörld ✓"
        await ws.send(b"\x00\x01\xfe\xff")
        echo = await ws.recv()
        assert type(echo) is bytes, type(echo)
        assert echo == b"\x00\x01\xfe\xff"
        await ws.send(bytearray(b"array"))
        assert await ws.recv() == b"array"
        await ws.send(memoryview(b"view"))
        assert await ws.recv() == b"view"
        # Empty messages are still messages
        await ws.send("")
        assert await ws.recv() == ""
        await ws.send(b"")
        assert await ws.recv() == b""
        # Choosing the kind of message, and the type it's received as
        await ws.send("as binary", text=False)
        assert await ws.recv() == b"as binary"
        await ws.send(b"as text", text=True)
        assert await ws.recv() == "as text"
        await ws.send("text as bytes")
        assert await ws.recv(decode=False) == b"text as bytes"
        await ws.send(b"binary as str")
        assert await ws.recv(decode=True) == "binary as str"
    assert ws.state is WebSocketState.CLOSED
    assert (ws.close_code, ws.close_reason) == (1000, "")
    assert type(await how_it_closed(ws)) is ConnectionClosedOK
    try:
        await ws.send("too late")
    except ConnectionClosedOK:
        pass
    else:
        raise AssertionError("send on a closed connection should raise")
    results["echo"] = "text, binary and empty messages; text= and decode="

    async with connect_websocket(
        base + "/echo",
        subprotocols=["chat.v1", "chat.v2"],
    ) as ws:
        assert ws.subprotocol == "chat.v2", ws.subprotocol
    results["subprotocol"] = ws.subprotocol

    # --- messages wait until they're received -----------------------------------
    progress("queueing")
    expected = [str(i) for i in range(500)]
    async with connect_websocket(base + "/echo") as ws:
        for message in expected:
            await ws.send(message)
        # Give the echoes time to arrive while nobody is receiving
        await trio.sleep(0.3)
        assert [await ws.recv() for _ in expected] == expected
    results["messages queued while nobody was receiving"] = len(expected)

    megabyte = bytes(range(256)) * 4096
    received = 0
    async with connect_websocket(base + "/echo") as ws:
        async with trio.open_nursery() as nursery:

            @nursery.start_soon
            async def send_a_lot() -> None:
                for _ in range(8):
                    await ws.send(megabyte)

            for _ in range(8):
                echo = await ws.recv()
                assert echo == megabyte
                received += len(echo)
    results["large messages"] = f"{received} bytes echoed"

    # --- iteration, and the ways a connection ends --------------------------------
    progress("closing")
    async with connect_websocket(base + "/burst") as ws:
        messages = [message async for message in ws]
    assert messages == ["one", b"two", "three"], messages
    assert (ws.close_code, ws.close_reason) == (1000, "that's all")
    results["iteration"] = f"{messages}, then closed with 1000 ({ws.close_reason})"

    for query, exc_type, code, reason in [
        ("?code=1001&reason=restarting", ConnectionClosedOK, 1001, "restarting"),
        ("", ConnectionClosedOK, 1005, ""),
        ("?code=4001&reason=go+away", ConnectionClosedError, 4001, "go away"),
    ]:
        async with connect_websocket(base + "/close" + query) as ws:
            exc = await how_it_closed(ws)
            assert type(exc) is exc_type, exc
            assert (exc.code, exc.reason, exc.was_clean) == (code, reason, True), exc
            try:
                async for _ in ws:
                    raise AssertionError("nothing was sent")
            except ConnectionClosedError:
                assert exc_type is ConnectionClosedError
            else:
                assert exc_type is ConnectionClosedOK
        results[f"closed by the server with {code}"] = f"{exc_type.__name__}: {exc}"

    async with connect_websocket(base + "/vanish") as ws:
        assert await ws.recv() == "about to vanish"
        exc = await how_it_closed(ws)
        assert type(exc) is ConnectionClosedError, exc
        assert (exc.code, exc.was_clean) == (1006, False), exc
    results["connection lost"] = f"ConnectionClosedError: {exc}"

    async with connect_websocket(base + "/echo") as ws:
        for bad_code in (999, 1001, 1011, 2999, 5000):
            try:
                await ws.aclose(bad_code)
            except ValueError:
                pass
            else:
                raise AssertionError(f"close code {bad_code} should be refused")
        try:
            await ws.aclose(reason="x" * 124)
        except ValueError:
            pass
        else:
            raise AssertionError("a 124-byte reason should be refused")
        assert ws.state is WebSocketState.OPEN
        await ws.aclose(3001, "finished early")
        assert ws.state is WebSocketState.CLOSED
        assert (ws.close_code, ws.close_reason) == (3001, "finished early")
        await ws.aclose()
        assert type(await how_it_closed(ws)) is ConnectionClosedError
    results["closed by us with 3001"] = f"{ws.close_code} ({ws.close_reason})"

    ws = None
    try:
        async with connect_websocket(base + "/echo") as ws:
            raise KeyError("oops")
    except KeyError:
        pass
    assert ws is not None
    assert (ws.state, ws.close_code) == (WebSocketState.CLOSED, 1000)
    results["closed when the block raises"] = True

    # --- cancellation and timeouts -----------------------------------------------
    progress("cancellation and timeouts")
    async with connect_websocket(base + "/echo") as ws:
        with trio.move_on_after(0.1) as scope:
            await ws.recv()
        assert scope.cancelled_caught
        await ws.send("still here")
        assert await ws.recv() == "still here"
    results["recv cancelled, then used again"] = True

    start = trio.current_time()
    try:
        async with connect_websocket(base + "/never", open_timeout=0.3):
            raise AssertionError("shouldn't connect")
    except TimeoutError as timeout:
        elapsed = trio.current_time() - start
        assert 0.25 <= elapsed <= 2, elapsed
        results["open_timeout"] = f"TimeoutError after {elapsed:.2f} s: {timeout}"
    else:
        raise AssertionError("expected TimeoutError")

    with trio.move_on_after(0.2) as scope:
        async with connect_websocket(base + "/never", open_timeout=None):
            raise AssertionError("shouldn't connect")
    assert scope.cancelled_caught
    results["connect cancelled"] = True

    async with connect_websocket(base + "/ignore-close", close_timeout=0.3) as ws:
        start = trio.current_time()
    elapsed = trio.current_time() - start
    assert 0.25 <= elapsed <= 2, elapsed
    assert (ws.state, ws.close_code) == (WebSocketState.CLOSED, 1006)
    exc = await how_it_closed(ws)
    assert type(exc) is ConnectionClosedError, exc
    results["close_timeout"] = f"gave up after {elapsed:.2f} s: {exc}"

    # --- failing to connect --------------------------------------------------------
    progress("failing to connect")
    for label, uri in [
        ("nothing listening", closed_port + "/"),
        ("handshake rejected", base + "/reject"),
    ]:
        try:
            async with connect_websocket(uri):
                raise AssertionError("shouldn't connect")
        except InvalidHandshake as invalid:
            # (The message has the port in it, which changes every time)
            results[f"connect: {label}"] = "InvalidHandshake: " + str(invalid).replace(
                uri,
                "...",
            )
        else:
            raise AssertionError("expected InvalidHandshake")

    # A URI that the WebSocket constructor itself refuses. (It has to have a
    # scheme: a browser takes one without as relative to the page's.)
    error_name = None
    try:
        async with connect_websocket("ftp://example.com/"):
            raise AssertionError("shouldn't connect")
    except JsException as js_error:
        error_name = js_error.name
    assert error_name == "SyntaxError", error_name
    results["connect: invalid uri"] = f"JsException: {error_name}"

    # --- concurrency ---------------------------------------------------------------
    progress("concurrency")
    finished = []

    async def chat(name: str) -> None:
        async with connect_websocket(base + "/echo") as ws:
            for i in range(20):
                await ws.send(f"{name} {i}")
                assert await ws.recv() == f"{name} {i}"
        finished.append(name)

    async with trio.open_nursery() as nursery:
        for name in "abc":
            nursery.start_soon(chat, name)
    assert sorted(finished) == ["a", "b", "c"], finished
    results["concurrent connections"] = len(finished)
    return results


started = time.perf_counter()


def done_callback(outcome: Outcome[dict[str, object]]) -> None:
    try:
        results = outcome.unwrap()
    except BaseException:
        import traceback

        traceback.print_exc()
        js.process.exitCode = 1
        return
    finally:
        js.stopServer()
    print("guest run finished after", round(time.perf_counter() - started, 3), "s")
    for key, value in results.items():
        print(f"  {key}: {value}")
    print("WEBSOCKET TEST PASSED")


assert sys.platform == "emscripten"
trio.lowlevel.start_guest_run(trio_main, done_callback=done_callback)
