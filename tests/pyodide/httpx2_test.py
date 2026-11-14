"""Test for the proof-of-concept httpx2 transport, trio._pyodide_httpx2.

Like the other tests here, this runs inside Pyodide, driven by run_tests.mjs,
against the server in test_server.mjs. Unlike them, it also runs natively::

    node tests/pyodide/run_tests.mjs --native=python3 httpx2_test.py

There it uses httpx2 just as it comes, over real sockets, with none of our
code involved. The point is that both runs make the same assertions, except
in the places marked NATIVE, which are the things a browser doesn't let a
script do. So this is one httpx2 program that behaves the same way under Trio
on both platforms.

It needs more installing than the other tests; see README.rst. Without
httpx2, it reports that it was skipped.
"""

from __future__ import annotations

import os
import sys
import time
from contextlib import contextmanager
from types import SimpleNamespace
from typing import TYPE_CHECKING

import trio
import trio.testing

try:
    import httpcore2
    import httpx2
    import wsproto
    from httpx2.websockets import (
        WebSocketDisconnect,
        WebSocketNetworkError,
        WebSocketUpgradeError,
    )
    from wsproto.events import BytesMessage, CloseConnection, Ping, Pong, TextMessage
except ImportError as exc:
    missing: ImportError | None = exc
else:
    missing = None

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Iterator

    from outcome import Outcome

NATIVE = sys.platform != "emscripten"
# Pyodide runs in other JavaScript runtimes too, and they don't have all of
# a browser's rules
IN_BROWSER = False

if NATIVE:
    BASE = os.environ["TEST_BASE_URL"]
    CLOSED = os.environ["TEST_CLOSED_URL"]
else:
    import js

    if missing is None:
        from trio._pyodide_httpx2 import BrowserTransport

    IN_BROWSER = hasattr(js, "document")
    BASE = str(js.TEST_BASE_URL)
    CLOSED = str(js.TEST_CLOSED_URL)

WS = BASE.replace("http", "ws", 1) + "/ws"


def make_client() -> httpx2.AsyncClient:
    """The one thing a program has to do differently in a browser."""
    if NATIVE:
        return httpx2.AsyncClient()
    return httpx2.AsyncClient(transport=BrowserTransport())


@contextmanager
def raises(expected: type[BaseException]) -> Iterator[SimpleNamespace]:
    caught = SimpleNamespace(value=None)
    try:
        yield caught
    except expected as exc:
        caught.value = exc
    else:
        raise AssertionError(f"{expected.__name__} wasn't raised")


async def server_saw(client: httpx2.AsyncClient, entry: str) -> None:
    """Wait for the server to have logged something that only it can see."""
    with trio.move_on_after(3):
        while True:
            log = (await client.get(BASE + "/log")).json()
            if entry in log:
                return
            await trio.sleep(0.02)
    raise AssertionError(f"the server never logged {entry!r}; it has {log}")


################################################################
# HTTP
################################################################


async def http_requests(client: httpx2.AsyncClient) -> str:
    response = await client.get(BASE + "/hello")
    assert response.status_code == 200
    assert response.text == "hello from node"
    assert response.headers["content-type"] == "text/plain"
    assert response.url == BASE + "/hello"
    assert response.request.method == "GET"
    assert response.is_closed

    response = await client.post(
        BASE + "/echo", headers={"X-Test": "a header"}, content="a bødy ✓"
    )
    assert response.text == "POST a header a bødy ✓"

    response = await client.put(BASE + "/echo-raw", json={"answer": 42})
    assert response.json() == {"answer": 42}

    response = await client.get(BASE + "/json", params={"unused": "1"})
    assert response.json() == {"answer": 42, "list": [1, 2, 3]}

    response = await client.get(BASE + "/nowhere")
    assert response.status_code == 404
    assert response.text == "nope"
    with raises(httpx2.HTTPStatusError):
        response.raise_for_status()

    # httpx2 lets an address say ws where it means http, and the reverse
    response = await client.get(WS[: -len("/ws")] + "/hello")
    assert response.text == "hello from node"
    return "GET, POST, PUT, JSON, 404"


async def http_bodies(client: httpx2.AsyncClient) -> str:
    # Every byte value, in a body big enough to arrive in several pieces
    payload = bytes(range(256)) * 1200
    response = await client.post(BASE + "/echo-raw", content=payload)
    assert response.content == payload

    # A compressed response is decoded once, by whoever gets to it first
    response = await client.get(BASE + "/gzip")
    assert response.text == "squashed " * 50
    if NATIVE:
        assert response.headers["content-encoding"] == "gzip"
    else:
        assert "content-encoding" not in response.headers
        assert "content-length" not in response.headers

    # A response that's read as it arrives
    async with client.stream("GET", BASE + "/stream") as response:
        assert response.status_code == 200
        assert not response.is_closed
        pieces = [text async for text in response.aiter_text()]
    assert "".join(pieces) == "chunk0\nchunk1\nchunk2\nchunk3\nchunk4\n"
    assert len(pieces) > 1, pieces
    return f"{len(payload)} bytes each way, gzip, a stream in {len(pieces)} pieces"


async def http_redirects(client: httpx2.AsyncClient) -> str:
    response = await client.get(BASE + "/redirect")
    if NATIVE:
        assert response.status_code == 302
        assert response.headers["location"] == "/hello"
    else:
        # A browser follows redirects itself, asked to or not
        assert response.status_code == 200
        assert response.text == "hello from node"

    response = await client.get(BASE + "/redirect", follow_redirects=True)
    assert response.status_code == 200
    assert response.text == "hello from node"
    if NATIVE:
        assert [r.status_code for r in response.history] == [302]
        assert response.url == BASE + "/hello"
    else:
        # ...and doesn't say where it has been
        assert response.history == []
        assert response.url == BASE + "/redirect"
    return "followed by httpx2" if NATIVE else "followed by the browser"


async def http_cookies(client: httpx2.AsyncClient) -> str:
    async with make_client() as other:
        other.cookies.set("sent", "1")
        response = await other.get(BASE + "/cookies")
        if IN_BROWSER:
            # Cookies are a browser's own business: it won't send a script's,
            # and doesn't show a script the server's
            assert response.text == "none"
            assert not response.cookies
            return "kept to itself by the browser"
        assert response.text == "sent=1"
        assert response.cookies["flavour"] == "oat"
        response = await other.get(BASE + "/cookies")
        assert sorted(response.text.split("; ")) == ["flavour=oat", "sent=1"]
        return "sent, and remembered"


async def http_failures(client: httpx2.AsyncClient) -> str:
    with raises(httpx2.ConnectError):
        await client.get(CLOSED + "/")
    with raises(httpx2.UnsupportedProtocol) as caught:
        await client.get("ftp://example.com/")
    assert "unsupported protocol 'ftp://'" in str(caught.value)
    # (a browser would take this one as relative to the page's address)
    with raises(httpx2.UnsupportedProtocol) as caught:
        await client.get("example.com/hello")
    assert "missing an 'http://' or 'https://' protocol" in str(caught.value)

    # A server that takes too long to answer
    with raises(httpx2.TimeoutException) as caught:
        await client.get(BASE + "/slow?timeout", timeout=0.2)
    # (a browser doesn't say when it has connected, only when it has headers)
    slow = httpx2.ReadTimeout if NATIVE else httpx2.ConnectTimeout
    assert type(caught.value) is slow, caught.value
    await server_saw(client, "abandoned /slow?timeout")

    # A server that stops part of the way through a response
    received = b""
    async with client.stream("GET", BASE + "/stall", timeout=0.3) as response:
        with raises(httpx2.ReadTimeout):
            async for chunk in response.aiter_bytes():
                received += chunk
    assert received == b"first\n"
    await server_saw(client, "abandoned /stall")
    return f"ConnectError, {slow.__name__}, ReadTimeout"


async def http_cancellation(client: httpx2.AsyncClient) -> str:
    # Trio's cancellation gives up on a request...
    start = trio.current_time()
    with trio.move_on_after(0.2) as scope:
        await client.get(BASE + "/slow?cancelled")
    assert scope.cancelled_caught
    assert trio.current_time() - start < 0.6
    await server_saw(client, "abandoned /slow?cancelled")

    # ...and on the rest of a response...
    async with client.stream("GET", BASE + "/stall?cancelled") as response:
        with trio.move_on_after(0.2) as scope:
            await response.aread()
        assert scope.cancelled_caught
        await server_saw(client, "abandoned /stall?cancelled")

    # ...and so does closing a response without reading all of it
    async with client.stream("GET", BASE + "/stream?closed") as response:
        chunks = response.aiter_bytes()
        assert await chunks.__anext__() == b"chunk0\n"
        await chunks.aclose()
    await server_saw(client, "abandoned /stream?closed")

    # The client is none the worse for any of it
    assert (await client.get(BASE + "/hello")).text == "hello from node"
    return "the server saw each request dropped"


async def http_concurrency(client: httpx2.AsyncClient) -> str:
    replies = {}

    async def ask(i: int) -> None:
        response = await client.post(
            BASE + "/echo", headers={"x-test": str(i)}, content="x"
        )
        replies[i] = response.text

    async with trio.open_nursery() as nursery:
        for i in range(10):
            nursery.start_soon(ask, i)
    assert replies == {i: f"POST {i} x" for i in range(10)}
    return f"{len(replies)} requests at once"


################################################################
# WebSockets
################################################################


async def ws_messages(client: httpx2.AsyncClient) -> str:
    async with client.websocket(
        WS + "/echo", subprotocols=["chat.v1", "chat.v2"]
    ) as ws:
        assert ws.subprotocol == "chat.v2"
        assert ws.response is not None
        assert ws.response.status_code == 101
        await ws.send_text("héllo ✓")
        assert await ws.receive_text() == "héllo ✓"
        await ws.send_bytes(b"\x00\x01\xfe\xff")
        assert await ws.receive_bytes() == b"\x00\x01\xfe\xff"
        await ws.send_json({"answer": 42})
        assert await ws.receive_json() == {"answer": 42}
        await ws.send_text("")
        assert await ws.receive_text() == ""

        # Nothing to receive: httpx2's own timeout, then Trio's
        with raises(TimeoutError):
            await ws.receive_text(timeout=0.2)
        with trio.move_on_after(0.2) as scope:
            await ws.receive_text()
        assert scope.cancelled_caught
        await ws.send_text("still working")
        assert await ws.receive_text() == "still working"

    # An address may be given the HTTP way, and no subprotocol is no trouble
    async with client.websocket(BASE + "/ws/echo") as ws:
        assert ws.subprotocol is None
        await ws.send_text("by http address")
        assert await ws.receive_text() == "by http address"
    return "text, bytes, JSON, a subprotocol, timeouts"


async def ws_big_messages(client: httpx2.AsyncClient) -> str:
    # Just inside the session's default limit of 64 KiB...
    big = "é" * 32000
    async with client.websocket(WS + "/echo") as ws:
        await ws.send_text(big)
        assert await ws.receive_text() == big
        # ...and outside it. The session enforces this, and ends the connection
        await ws.send_bytes(bytes(70000))
        with raises(WebSocketDisconnect) as caught:
            await ws.receive_bytes()
        assert caught.value.code == 1009, caught.value

    payload = bytes(range(256)) * 1200
    async with client.websocket(WS + "/echo", max_message_size_bytes=2**20) as ws:
        await ws.send_bytes(payload)
        assert await ws.receive_bytes() == payload
    return f"{len(big.encode())} bytes, a refusal of 70000, {len(payload)} with a higher limit"


async def ws_pings(client: httpx2.AsyncClient) -> str:
    async with client.websocket(WS + "/echo") as ws:
        answered = await ws.ping()
        with trio.fail_after(1):
            await answered.wait()

    # The session's keepalive: it pings, and gives up on a connection that
    # doesn't answer one in time
    async with client.websocket(
        WS + "/echo",
        keepalive_ping_interval_seconds=0.1,
        keepalive_ping_timeout_seconds=0.3,
    ) as ws:
        await trio.sleep(0.8)
        await ws.send_text("still here")
        assert await ws.receive_text() == "still here"
    return "a ping, and several keepalive pings, all answered"


async def ws_closed_by_server(client: httpx2.AsyncClient) -> str:
    async with client.websocket(WS + "/burst") as ws:
        assert await ws.receive_text() == "one"
        assert await ws.receive_bytes() == b"two"
        assert await ws.receive_text() == "three"
        with raises(WebSocketDisconnect) as caught:
            await ws.receive_text()
        assert (caught.value.code, caught.value.reason) == (1000, "that's all")

    async with client.websocket(WS + "/close?code=4001&reason=go+away") as ws:
        with raises(WebSocketDisconnect) as caught:
            await ws.receive_text()
        assert (caught.value.code, caught.value.reason) == (4001, "go away")

    # A close frame with no code in it
    async with client.websocket(WS + "/close") as ws:
        with raises(WebSocketDisconnect) as caught:
            await ws.receive_text()
        assert caught.value.code == 1005, caught.value
        assert not caught.value.reason

    # No close frame at all: the connection is just gone
    async with client.websocket(WS + "/vanish") as ws:
        assert await ws.receive_text() == "about to vanish"
        with raises(WebSocketNetworkError):
            await ws.receive_text()
    return "codes 1000, 4001 and none; and a lost connection"


async def ws_closed_by_client(client: httpx2.AsyncClient) -> str:
    async with client.websocket(WS + "/echo?default") as ws:
        pass
    await server_saw(client, "closed /ws/echo?default 1000 ")

    async with client.websocket(WS + "/echo?custom") as ws:
        await ws.close(4000, "all done")
    await server_saw(client, "closed /ws/echo?custom 4000 all done")

    # A browser only lets a script use 1000, and 3000 to 4999
    async with client.websocket(WS + "/echo?away") as ws:
        await ws.close(1001, "leaving")
    sent = 1001 if NATIVE else 1000
    await server_saw(client, f"closed /ws/echo?away {sent} leaving")

    # Leaving while the server is in full flow
    async with client.websocket(WS + "/flood?flow") as ws:
        for _ in range(5):
            assert await ws.receive_text() == "flood " * 20
    await server_saw(client, "closed /ws/flood?flow 1000 ")

    # A server that never answers a close frame doesn't hold anything up
    start = trio.current_time()
    async with client.websocket(WS + "/ignore-close") as ws:
        pass
    assert trio.current_time() - start < 1
    return f"codes 1000, 4000 and 1001 (sent as {sent}); mid-flood; unanswered"


async def ws_failures(client: httpx2.AsyncClient) -> str:
    with raises(httpx2.ConnectError):
        async with client.websocket(CLOSED.replace("http", "ws", 1) + "/"):
            raise AssertionError("connected to nothing")
    with raises(httpx2.UnsupportedProtocol):
        async with client.websocket("ftp://example.com/"):
            raise AssertionError("connected by FTP")

    # A server that refuses the handshake. A browser doesn't say what it said
    refused = WebSocketUpgradeError if NATIVE else httpx2.ConnectError
    with raises(refused) as caught:
        async with client.websocket(WS + "/reject"):
            raise AssertionError("connected despite a 403")
    if NATIVE:
        assert caught.value.response.status_code == 403

    # A server that never answers the handshake
    start = trio.current_time()
    with raises(httpx2.TimeoutException) as caught:
        async with client.websocket(WS + "/never", timeout=0.3):
            raise AssertionError("connected to silence")
    slow = httpx2.ReadTimeout if NATIVE else httpx2.ConnectTimeout
    assert type(caught.value) is slow, caught.value
    assert trio.current_time() - start < 1

    # Giving up on a handshake, with Trio's cancellation
    with trio.move_on_after(0.2) as scope:
        async with client.websocket(WS + "/never"):
            raise AssertionError("connected to silence")
    assert scope.cancelled_caught
    return f"ConnectError, {refused.__name__}, {slow.__name__}"


async def ws_concurrency(client: httpx2.AsyncClient) -> str:
    finished = []

    async def chat(name: str) -> None:
        async with client.websocket(WS + "/echo") as ws:
            for i in range(20):
                await ws.send_text(f"{name} {i}")
                assert await ws.receive_text() == f"{name} {i}"
        finished.append(name)

    async def talk_and_listen() -> None:
        # One task sending while another receives, on one connection
        async with client.websocket(WS + "/echo") as ws:

            async def send_all() -> None:
                for i in range(50):
                    await ws.send_text(str(i))

            async with trio.open_nursery() as nursery:
                nursery.start_soon(send_all)
                for i in range(50):
                    assert await ws.receive_text() == str(i)
        finished.append("both ways")

    async with trio.open_nursery() as nursery:
        for name in "abc":
            nursery.start_soon(chat, name)
        nursery.start_soon(talk_and_listen)
    assert sorted(finished) == ["a", "b", "both ways", "c"], finished
    return "four connections at once"


################################################################
# The transport's own stream, which httpx2's session reads and writes. These
# go behind the session's back, to ask things of the stream that the session
# itself never does.
################################################################


async def open_stream(path: str) -> tuple[httpx2.Response, object]:
    request = httpx2.Request("GET", WS + path, headers={"upgrade": "websocket"})
    response = await BrowserTransport().handle_async_request(request)
    assert response.status_code == 101
    return response, response.extensions["network_stream"]


async def read_events(
    stream: object, session: wsproto.connection.Connection, max_bytes: int = 4096
) -> list[object]:
    """Read from the stream until it amounts to something: a whole message,
    some other kind of frame, or the end."""
    events: list[object] = []
    while True:
        data = await stream.read(max_bytes)
        if not data:
            return [*events, "end of stream"]
        assert len(data) <= max_bytes
        session.receive_data(data)
        events += session.events()
        if events and getattr(events[-1], "message_finished", True):
            return events


async def stream_details(client: httpx2.AsyncClient) -> str:
    def new_session() -> wsproto.connection.Connection:
        return wsproto.connection.Connection(wsproto.ConnectionType.CLIENT)

    # A message sent in several frames, and frames that arrive in pieces
    response, stream = await open_stream("/echo")
    session = new_session()
    frames = session.send(TextMessage(data="in ", message_finished=False))
    frames += session.send(TextMessage(data="three ", message_finished=False))
    frames += session.send(TextMessage(data="frames"))
    await stream.write(frames[:5])
    await stream.write(frames[5:])
    events = await read_events(stream, session, max_bytes=3)
    assert "".join(event.data for event in events) == "in three frames", events
    frames = session.send(BytesMessage(data=b"bi", message_finished=False))
    frames += session.send(BytesMessage(data=b"nary"))
    await stream.write(frames)
    events = await read_events(stream, session)
    assert b"".join(event.data for event in events) == b"binary", events

    # Only one task may read at a time. A ping written meanwhile is answered,
    # which takes interrupting the reader.
    async with trio.open_nursery() as nursery:

        async def read_pong() -> None:
            assert await read_events(stream, session) == [Pong(payload=b"p")]

        nursery.start_soon(read_pong)
        await trio.testing.wait_all_tasks_blocked()
        with raises(trio.BusyResourceError):
            await stream.read(4096)
        await stream.write(session.send(Ping(payload=b"p")))

    # Closing: with a code a browser won't send, and then more than once
    await stream.write(session.send(CloseConnection(code=1011, reason="oops")))
    await stream.aclose()
    await stream.aclose()
    await response.aclose()
    assert await stream.read(4096) == b""
    await server_saw(client, "closed /ws/echo 1000 oops")
    # After that, writing fails the way the session expects of a closed stream
    with raises(httpcore2.WriteError):
        await stream.write(new_session().send(TextMessage(data="too late")))

    # The server closes first
    response, stream = await open_stream("/close?code=4002&reason=enough")
    session = new_session()
    assert await read_events(stream, session) == [
        CloseConnection(code=4002, reason="enough")
    ]
    # A ping can't be answered any more, and is ignored...
    await stream.write(new_session().send(Ping(payload=b"late")))
    assert await read_events(stream, session) == ["end of stream"]
    # ...and a message has nowhere to go
    with raises(httpcore2.WriteError):
        await stream.write(new_session().send(TextMessage(data="anyone there?")))
    # Closing the response is enough to close the stream
    await response.aclose()
    assert await stream.read(4096) == b""

    # Messages still arriving once the session has sent its close frame are
    # dropped. It isn't listening by then, and a server may not send them.
    response, stream = await open_stream("/flood")
    session = new_session()
    assert (await read_events(stream, session))[0].data == "flood " * 20
    await stream.write(session.send(CloseConnection(code=1000)))
    async with trio.open_nursery() as nursery:

        async def read_to_the_end() -> None:
            assert await stream.read(4096) == b""

        nursery.start_soon(read_to_the_end)
        await trio.sleep(0.05)
        await stream.aclose()
    await response.aclose()

    # A response that's read like any other, with no session at all
    response = await client.get(
        BASE + "/ws/echo?unused", headers={"upgrade": "websocket"}
    )
    assert response.status_code == 101
    assert response.content == b""
    await server_saw(client, "closed /ws/echo?unused 1000 ")

    # A connection that the browser won't even attempt: a subprotocol has to
    # be a single word
    request = httpx2.Request(
        "GET",
        WS + "/echo",
        headers={"upgrade": "websocket", "sec-websocket-protocol": "two words"},
    )
    with raises(httpx2.ConnectError):
        await BrowserTransport().handle_async_request(request)

    # httpx2's WebSocket code needs httpcore2, which it doesn't ask for here
    sys.modules["httpcore2"] = None  # (makes importing it fail)
    try:
        with raises(ImportError) as caught:
            await open_stream("/echo")
        assert "httpcore2" in str(caught.value)
    finally:
        sys.modules["httpcore2"] = httpcore2
    return "fragments, pieces, one reader, closing, late messages"


################################################################


async def trio_main() -> dict[str, str]:
    scenarios: list[Callable[[httpx2.AsyncClient], Awaitable[str]]] = [
        http_requests,
        http_bodies,
        http_redirects,
        http_cookies,
        http_failures,
        http_cancellation,
        http_concurrency,
        ws_messages,
        ws_big_messages,
        ws_pings,
        ws_closed_by_server,
        ws_closed_by_client,
        ws_failures,
        ws_concurrency,
    ]
    if not NATIVE:
        scenarios.append(stream_details)
    results = {}
    async with make_client() as client:
        for scenario in scenarios:
            # Printed as we go, so a hang shows where it happened
            print(" ...", scenario.__name__)
            with trio.fail_after(30):
                results[scenario.__name__] = await scenario(client)
    return results


started = time.perf_counter()


def report(results: dict[str, str]) -> None:
    print("run finished after", round(time.perf_counter() - started, 3), "s")
    for key, value in results.items():
        print(f"  {key}: {value}")
    where = "natively, with httpx2's own transport" if NATIVE else "in Pyodide"
    print(f"HTTPX2 TEST PASSED ({where})")


def done_callback(outcome: Outcome[dict[str, str]]) -> None:
    try:
        results = outcome.unwrap()
    except BaseException:
        import traceback

        traceback.print_exc()
        js.process.exitCode = 1
        return
    finally:
        js.stopServer()
    report(results)


if missing is not None:
    print(f"HTTPX2 TEST SKIPPED: {missing} (see README.rst for what to install)")
    if not NATIVE:
        js.stopServer()
elif NATIVE:
    report(trio.run(trio_main))
else:
    trio.lowlevel.start_guest_run(trio_main, done_callback=done_callback)
