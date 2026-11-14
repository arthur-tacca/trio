"""A proof of concept: `httpx2 <https://github.com/pydantic/httpx2>`__ for
Trio in the browser, HTTP and WebSockets both.

httpx2 already works under Trio on other platforms. On Emscripten it can't
open sockets, so it needs a transport that goes through the browser, and the
one it comes with (``httpx2-jsfetch``) only works under asyncio: it awaits
JavaScript promises directly, which Pyodide turns into asyncio futures. It
doesn't do WebSockets at all. `BrowserTransport` is a transport for Trio,
built on `trio.pyodide.fetch` and `trio.pyodide.connect_websocket`::

    import httpx2
    from trio._pyodide_httpx2 import BrowserTransport

    async with httpx2.AsyncClient(transport=BrowserTransport()) as client:
        response = await client.get("https://example.com/")
        async with client.websocket("wss://example.com/chat") as ws:
            await ws.send_text("hello")

With that one argument, a program written for httpx2 runs under Trio both
natively and in a browser. tests/pyodide/httpx2_test.py is such a program: it
makes the same assertions in both places, apart from the differences listed
on `BrowserTransport`.

This module is here so that the whole experiment is in one place, and it
doesn't belong in Trio. httpx2 depends on Trio and not the other way round,
so its real home would be httpx2 itself or a package of its own. That's why
it has a private name, why nothing else in Trio imports it, and why Trio
doesn't depend on httpx2: to use this, install httpx2 yourself.

"""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING

# As in _pyodide.py: type checkers skip the rest of this module on other
# platforms, where it can't be imported anyway.
assert not TYPE_CHECKING or sys.platform == "emscripten"

import math
from contextlib import AsyncExitStack
from typing import AnyStr

import httpx2
from pyodide.ffi import JsException

import trio

from ._pyodide import ConnectionClosed, InvalidHandshake, connect_websocket, fetch
from ._util import ConflictDetector

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    import wsproto.connection

    from ._pyodide import Response, WebSocketConnection

# A custom User-Agent makes a browser ask the server's permission before a
# cross-origin request (a CORS preflight), and many servers would refuse it.
# httpx2 sets one on every request, so it's left out, as httpx2-jsfetch does;
# the browser sends its own. The other headers that httpx2 adds by itself
# (Host, Connection, Accept-Encoding, Content-Length) are ones a browser
# doesn't let a script set, and it ignores them without complaint.
_UNSENT_HEADERS = frozenset({"user-agent"})

# The close codes that a browser lets a script close a WebSocket with
_NORMAL_CLOSURE = 1000
_APPLICATION_CLOSE_CODES = range(3000, 5000)
# The codes a browser reports when the server's close frame had no code in
# it, and when there was no close frame at all
_NO_STATUS_RECEIVED = 1005
_ABNORMAL_CLOSURE = 1006
# A WebSocket close frame with nothing in it, as a server sends it
_EMPTY_CLOSE_FRAME = b"\x88\x00"


def _address(request: httpx2.Request, *, websocket: bool) -> str:
    """A request's URL, with the scheme a browser wants for it. httpx2 takes
    http and ws to mean the same thing; a browser's fetch only takes the
    first, and its WebSocket only the second."""
    scheme = request.url.scheme
    if scheme not in ("http", "https", "ws", "wss"):
        # The errors, and the wording, that httpx2 has on other platforms.
        # Left to itself a browser would take an address with no scheme to be
        # relative to the page's address, which nobody using httpx2 expects.
        if not scheme:
            raise httpx2.UnsupportedProtocol(
                "Request URL is missing an 'http://' or 'https://' protocol.",
            )
        raise httpx2.UnsupportedProtocol(
            f"Request URL has an unsupported protocol '{scheme}://'.",
        )
    secure = "s" if scheme in ("https", "wss") else ""
    return str(request.url.copy_with(scheme=("ws" if websocket else "http") + secure))


def _timeouts(request: httpx2.Request) -> tuple[float, float]:
    """A request's connect and read timeouts, in seconds; infinity for none."""
    timeouts = request.extensions.get("timeout") or {}
    connect, read = timeouts.get("connect"), timeouts.get("read")
    return (
        math.inf if connect is None else connect,
        math.inf if read is None else read,
    )


class BrowserTransport(httpx2.AsyncBaseTransport):
    """An httpx2 transport for Trio on Pyodide: requests are made by the
    browser's ``fetch``, and WebSocket connections by its ``WebSocket``. Pass
    one to `httpx2.AsyncClient`, as its ``transport``. It has no settings and
    holds no resources, so one can be shared, and needn't be closed.

    Cancelling a Trio task that's waiting for a response, or reading one,
    aborts the request.

    A browser does a lot of what an HTTP client usually does for itself, and
    doesn't let a script see or change it. So some things work differently
    from httpx2 on other platforms. For HTTP they are the same as with
    httpx2's own transport for Emscripten, except for the last:

    * A browser follows redirects itself. ``follow_redirects`` makes no
      difference, and the response is the final one, with no ``history`` and
      with the ``url`` that was asked for.
    * A request's body is sent all at once, not streamed, and a browser
      won't send some headers at all, among them ``User-Agent``.
    * Cookies are the browser's own business. It doesn't send the ones a
      script asks for, or show a script the ones a server sets, so httpx2's
      cookies do nothing. (This is only so in a browser; other JavaScript
      runtimes, such as Node.js, have no such rules.)
    * There are only two timeouts. ``connect`` is how long the response's
      headers may take to arrive, so a server that's slow to answer gives
      `httpx2.ConnectTimeout` where a socket would give `httpx2.ReadTimeout`;
      ``read`` is how long each part of the body may take.
    * A browser decodes a compressed response itself. The body is what
      httpx2 would have given, but the ``Content-Encoding`` and
      ``Content-Length`` headers, which no longer describe it, are removed.

    For WebSockets (which need the wsproto, anyio and httpcore2 packages, as
    httpx2's WebSocket support does), a browser runs the protocol itself, and
    lets a script send and receive whole messages:

    * A handshake carries nothing of the client's but the address and the
      subprotocols: no headers, cookies or authentication. The handshake
      response that the session reports is made up, and has only a status of
      101 and the subprotocol that was chosen.
    * A browser doesn't say why a connection couldn't be made, so a rejected
      handshake is `httpx2.ConnectError`, without the server's response. As
      with HTTP, there's one timeout for the handshake, ``connect``.
    * The session's pings never reach the server; they're answered here.
      The browser sends pings of its own, and answers the server's.
    * A script may only close a connection with code 1000, or one from 3000
      to 4999. If the session uses any other, the server gets 1000 (with the
      reason unchanged).
    * A browser can't be made to stop reading, so the session's
      ``queue_size`` doesn't hold the server back; messages wait here.

    """

    async def handle_async_request(self, request: httpx2.Request) -> httpx2.Response:
        if request.headers.get("upgrade", "").lower() == "websocket":
            return await self._open_websocket(request)
        return await self._fetch(request)

    async def _fetch(self, request: httpx2.Request) -> httpx2.Response:
        assert isinstance(request.stream, httpx2.AsyncByteStream)
        url = _address(request, websocket=False)
        # Only some browsers can stream a request's body, and only over
        # HTTP/2, so it's collected first.
        body = b"".join([chunk async for chunk in request.stream])
        headers = {
            name: value
            for name, value in request.headers.items()
            if name.lower() not in _UNSENT_HEADERS
        }
        connect_timeout, read_timeout = _timeouts(request)
        try:
            with trio.fail_after(connect_timeout):
                response = await fetch(
                    url,
                    method=request.method,
                    headers=headers,
                    # (fetch won't take a body for a GET, not even an empty one)
                    body=body or None,
                )
        except trio.TooSlowError as exc:
            raise httpx2.ConnectTimeout("Request timed out") from exc
        except JsException as exc:
            raise httpx2.ConnectError(str(exc.message)) from exc
        response_headers = dict(response.headers)
        if "content-encoding" in response_headers:
            # The browser has decoded the body already. Left alone, this
            # header would have httpx2 decode it a second time.
            del response_headers["content-encoding"]
            response_headers.pop("content-length", None)
        return httpx2.Response(
            response.status,
            headers=response_headers,
            stream=_FetchBody(response, read_timeout),
        )

    async def _open_websocket(self, request: httpx2.Request) -> httpx2.Response:
        try:
            import httpcore2  # noqa: F401
        except ImportError as exc:
            raise ImportError(
                "httpx2's WebSocket support uses the httpcore2 package, which "
                "httpx2 doesn't install on Emscripten; it needs installing too",
            ) from exc
        url = _address(request, websocket=True)
        offered = request.headers.get("sec-websocket-protocol", "")
        subprotocols = [name.strip() for name in offered.split(",") if name.strip()]
        connect_timeout, _ = _timeouts(request)
        exit_stack = AsyncExitStack()
        try:
            connection = await exit_stack.enter_async_context(
                connect_websocket(
                    url,
                    subprotocols=subprotocols or None,
                    open_timeout=(
                        None if connect_timeout == math.inf else connect_timeout
                    ),
                    # Like httpx2 on a socket, don't wait for the server to
                    # answer a close frame; the browser sees to the rest. (A
                    # session closes its connection shielded from
                    # cancellation, so a wait here couldn't be interrupted.)
                    close_timeout=0,
                ),
            )
        except TimeoutError as exc:
            raise httpx2.ConnectTimeout("Request timed out") from exc
        except InvalidHandshake as exc:
            raise httpx2.ConnectError(str(exc)) from exc
        except JsException as exc:
            # The browser wouldn't even try: subprotocols it won't offer, say,
            # or an address that its security policy rules out
            raise httpx2.ConnectError(str(exc.message)) from exc
        stream = _WebSocketStream(connection, exit_stack)
        # There's no handshake response to pass on (a browser keeps it to
        # itself), so this is the one the session checks for: status 101, and
        # the subprotocol. The session finds its connection in the extension.
        headers: dict[str, str] = {}
        if connection.subprotocol is not None:
            headers["sec-websocket-protocol"] = connection.subprotocol
        return httpx2.Response(
            101,
            headers=headers,
            stream=_UpgradedBody(stream),
            extensions={"network_stream": stream},
        )


class _FetchBody(httpx2.AsyncByteStream):
    """The body of a fetch response, as httpx2 reads it."""

    def __init__(self, response: Response, read_timeout: float) -> None:
        self._response = response
        self._read_timeout = read_timeout

    async def __aiter__(self) -> AsyncIterator[bytes]:
        while True:
            try:
                with trio.fail_after(self._read_timeout):
                    chunk = await self._response.body.receive_some()
            except trio.TooSlowError as exc:
                raise httpx2.ReadTimeout("Request timed out") from exc
            except JsException as exc:
                raise httpx2.ReadError(str(exc.message)) from exc
            except trio.BrokenResourceError as exc:
                # A read was cancelled earlier (by a timeout, for one), which
                # aborted the request
                raise httpx2.ReadError(str(exc)) from exc
            if not chunk:
                return
            yield chunk

    async def aclose(self) -> None:
        await self._response.aclose()


class _UpgradedBody(httpx2.AsyncByteStream):
    """The body of the response to a WebSocket handshake. There isn't one,
    but closing the response has to close the connection, as it does when
    httpx2 has a socket."""

    def __init__(self, stream: _WebSocketStream) -> None:
        self._stream = stream

    async def __aiter__(self) -> AsyncIterator[bytes]:
        yield b""

    async def aclose(self) -> None:
        await self._stream.aclose()


def _take_message(parts: list[AnyStr]) -> AnyStr:
    message = parts[0][:0].join(parts)
    parts.clear()
    return message


class _WebSocketStream:
    """What httpx2's WebSocket session takes for its connection to the
    server.

    The session expects a stream of bytes, which is what an HTTP client has
    once a server has accepted an upgrade, and it runs the WebSocket protocol
    over that itself, reading and writing frames. A browser has no such
    stream to give. It runs the protocol too, and lets a script see whole
    messages.

    So this plays the server's part of the protocol to the session, in
    memory. A message that the browser delivers is put back into a frame for
    the session to read, and a frame that the session writes is turned back
    into a message for the browser to send. (httpx2 does the same thing
    itself, in its ASGIWebSocketTransport, to stand in front of an ASGI
    application.)

    """

    def __init__(
        self,
        connection: WebSocketConnection,
        exit_stack: AsyncExitStack,
    ) -> None:
        import wsproto

        self._connection = connection
        # Closing this leaves connect_websocket's block
        self._exit_stack = exit_stack
        # The server that the session thinks it's talking to
        self._server: wsproto.connection.Connection = wsproto.connection.Connection(
            wsproto.ConnectionType.SERVER,
        )
        # Frames that the session hasn't read yet
        self._unread = bytearray()
        # Set while read() is waiting for the browser, so that write() can
        # interrupt it when it has made a frame itself
        self._recv_scope: trio.CancelScope | None = None
        self._read_conflict = ConflictDetector(
            "another task is reading from this WebSocket",
        )
        # The parts, so far, of a message that the session is sending in
        # several frames
        self._text_parts: list[str] = []
        self._bytes_parts: list[bytes] = []
        # What the session's close frame said, once it has written one
        self._close_request: tuple[int, str] | None = None
        # Nothing more is coming from the browser
        self._ended = False
        self._closed = False

    # read() and write() have the signatures of httpcore2's network streams,
    # which is what this stands in for. The session never passes a timeout.

    async def read(
        self,
        max_bytes: int,
        timeout: float | None = None,  # noqa: ASYNC109
    ) -> bytes:
        from wsproto.connection import ConnectionState
        from wsproto.events import BytesMessage, TextMessage

        with self._read_conflict:
            if self._unread:
                await trio.lowlevel.checkpoint()
            while not self._unread:
                if self._ended or self._closed:
                    await trio.lowlevel.checkpoint()
                    return b""
                # Cancelling recv() loses nothing, so write() can use that to
                # wake this up. Any other cancellation passes straight through.
                with trio.CancelScope() as self._recv_scope:
                    try:
                        message = await self._connection.recv()
                    except ConnectionClosed as closed:
                        self._ended = True
                        self._unread += self._closing_frame(closed)
                    else:
                        # Once the session has sent its close frame it's not
                        # listening any more, and a server may not send it
                        # messages, so anything still arriving is dropped.
                        if self._server.state is ConnectionState.OPEN:
                            self._unread += self._server.send(
                                (
                                    TextMessage(data=message)
                                    if isinstance(message, str)
                                    else BytesMessage(data=message)
                                ),
                            )
                self._recv_scope = None
            data = bytes(self._unread[:max_bytes])
            del self._unread[:max_bytes]
            return data

    def _closing_frame(self, closed: ConnectionClosed) -> bytes:
        """The close frame to give the session, when the browser says that
        the connection has closed; nothing, if a frame isn't how it ended."""
        from wsproto.connection import ConnectionState
        from wsproto.events import CloseConnection

        if closed.code == _ABNORMAL_CLOSURE or self._server.state not in (
            ConnectionState.OPEN,
            ConnectionState.REMOTE_CLOSING,
        ):
            # No close frame arrived, so there's none to pass on. The stream
            # just ends, which is what a session sees when a socket is lost.
            return b""
        if closed.code == _NO_STATUS_RECEIVED:
            # The server's close frame had no code in it. wsproto would write
            # 1000 into the frame, so this one is written by hand.
            self._server.send(CloseConnection(code=_NORMAL_CLOSURE))
            return _EMPTY_CLOSE_FRAME
        return self._server.send(
            CloseConnection(code=closed.code, reason=closed.reason or None),
        )

    async def write(
        self,
        buffer: bytes,
        timeout: float | None = None,  # noqa: ASYNC109
    ) -> None:
        import httpcore2
        from wsproto.connection import ConnectionState
        from wsproto.events import BytesMessage, CloseConnection, Ping, TextMessage

        await trio.lowlevel.checkpoint_if_cancelled()
        if self._closed:
            raise httpcore2.WriteError("the WebSocket connection has been closed")
        self._server.receive_data(buffer)
        sent = False
        # All of the events are taken out before acting on any, because
        # sending a message can wait, and read() uses the protocol object too.
        for event in list(self._server.events()):
            if isinstance(event, TextMessage):
                self._text_parts.append(event.data)
                if event.message_finished:
                    await self._send(_take_message(self._text_parts))
                    sent = True
            elif isinstance(event, BytesMessage):
                self._bytes_parts.append(bytes(event.data))
                if event.message_finished:
                    await self._send(_take_message(self._bytes_parts))
                    sent = True
            elif isinstance(event, Ping):
                # The session checking that the server is still there. A
                # browser does that itself and doesn't let a script send a
                # ping, so the only way to answer is from here.
                if self._server.state is ConnectionState.OPEN:
                    self._unread += self._server.send(event.response())
                    if self._recv_scope is not None:
                        self._recv_scope.cancel()
            elif isinstance(event, CloseConnection):
                self._close_request = (event.code, event.reason or "")
        if not sent:
            await trio.lowlevel.cancel_shielded_checkpoint()

    async def _send(self, message: str | bytes) -> None:
        import httpcore2

        try:
            await self._connection.send(message)
        except ConnectionClosed as closed:
            # The session's own word for a connection that has gone
            raise httpcore2.WriteError(
                f"the WebSocket connection is closed: {closed}",
            ) from closed

    async def aclose(self) -> None:
        if self._closed:
            await trio.lowlevel.checkpoint()
            return
        self._closed = True
        code, reason = self._close_request or (_NORMAL_CLOSURE, "")
        if code != _NORMAL_CLOSURE and code not in _APPLICATION_CLOSE_CODES:
            code = _NORMAL_CLOSURE
        try:
            await self._connection.aclose(code, reason)
        finally:
            await self._exit_stack.aclose()
