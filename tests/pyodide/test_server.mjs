// The local server that the tests talk to: HTTP for trio.pyodide.fetch, and
// WebSocket (on the same port, under /ws/) for trio.pyodide.connect_websocket.
// See README.rst.
import http from "node:http";
import { createHash } from "node:crypto";
import { gzipSync } from "node:zlib";
// Pyodide's own Node.js package depends on this one, so it's already installed
import { WebSocketServer } from "ws";

// What a test can't see from its own side: the requests whose client went
// away before the response was complete, and how each WebSocket connection
// was closed. Tests read it back from /log. An entry names the address that
// was asked for, so a test tells its own apart by adding a query string.
const log = [];

function handleHttp(req, res) {
  const path = req.url.split("?")[0];
  res.on("close", () => {
    if (!res.writableFinished) log.push(`abandoned ${req.url}`);
  });
  // For tests that run in a real browser, where the page comes from elsewhere
  res.setHeader("access-control-allow-origin", "*");
  res.setHeader("access-control-expose-headers", "*");
  if (req.method === "OPTIONS") {
    res.setHeader("access-control-allow-methods", "*");
    res.setHeader("access-control-allow-headers", "*");
    res.end();
  } else if (path === "/hello") {
    res.setHeader("content-type", "text/plain");
    res.end("hello from node");
  } else if (path === "/echo") {
    let body = "";
    req.on("data", (chunk) => (body += chunk));
    req.on("end", () => res.end(`${req.method} ${req.headers["x-test"]} ${body}`));
  } else if (path === "/echo-raw") {
    // The request's body, byte for byte
    res.setHeader("content-type", "application/octet-stream");
    req.pipe(res);
  } else if (path === "/json") {
    res.setHeader("content-type", "application/json");
    res.end(JSON.stringify({ answer: 42, list: [1, 2, 3] }));
  } else if (path === "/gzip") {
    res.setHeader("content-type", "text/plain");
    res.setHeader("content-encoding", "gzip");
    res.end(gzipSync("squashed ".repeat(50)));
  } else if (path === "/redirect") {
    res.writeHead(302, { location: "/hello" });
    res.end();
  } else if (path === "/cookies") {
    // Says which cookies came with the request, and sets one
    res.setHeader("set-cookie", "flavour=oat");
    res.end(req.headers.cookie ?? "none");
  } else if (path === "/slow") {
    const timer = setTimeout(() => res.end("slow done"), 1000);
    req.on("close", () => clearTimeout(timer));
  } else if (path === "/stream") {
    res.writeHead(200);
    let i = 0;
    const interval = setInterval(() => {
      res.write(`chunk${i++}\n`);
      if (i === 5) { clearInterval(interval); res.end(); }
    }, 40);
    req.on("close", () => clearInterval(interval));
  } else if (path === "/stall") {
    // Start a response, and never finish it
    res.writeHead(200);
    res.write("first\n");
  } else if (path === "/log") {
    res.setHeader("content-type", "application/json");
    res.end(JSON.stringify(log));
  } else {
    res.statusCode = 404;
    res.end("nope");
  }
}

// WebSocket endpoints that behave: these are handled by the 'ws' library
function handleWebSocket(ws, req) {
  const url = new URL(req.url, "http://localhost");
  ws.on("close", (code, reason) => log.push(`closed ${req.url} ${code} ${reason}`));
  if (url.pathname === "/ws/echo") {
    ws.on("message", (data, isBinary) => ws.send(data, { binary: isBinary }));
  } else if (url.pathname === "/ws/close") {
    // Close straight away, with the code and reason asked for (or without)
    const code = url.searchParams.get("code");
    if (code === null) ws.close();
    else ws.close(Number(code), url.searchParams.get("reason") ?? "");
  } else if (url.pathname === "/ws/burst") {
    ws.send("one");
    ws.send(Buffer.from("two"));
    ws.send("three");
    ws.close(1000, "that's all");
  } else if (url.pathname === "/ws/vanish") {
    // Drop the connection without a closing handshake
    ws.send("about to vanish", () => ws.terminate());
  } else if (url.pathname === "/ws/flood") {
    // Send without a pause, for as long as the connection lasts
    const interval = setInterval(() => ws.send("flood ".repeat(20)), 2);
    ws.on("close", () => clearInterval(interval));
  } else {
    ws.close(4404, "no such endpoint");
  }
}

// WebSocket endpoints that misbehave: these are done by hand
function handleUpgrade(wss, req, socket, head) {
  const path = req.url.split("?")[0];
  if (path === "/ws/never") {
    // Never answer the opening handshake
  } else if (path === "/ws/reject") {
    socket.end("HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
  } else if (path === "/ws/ignore-close") {
    // Accept the connection, then ignore everything, close frames included
    const accept = createHash("sha1")
      .update(req.headers["sec-websocket-key"] + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11")
      .digest("base64");
    socket.write(
      "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" +
        `Sec-WebSocket-Accept: ${accept}\r\n\r\n`,
    );
  } else {
    wss.handleUpgrade(req, socket, head, (ws) => wss.emit("connection", ws, req));
  }
}

// Starts the server on a free port. It doesn't keep the process alive by
// itself, but its open connections do, until stop() is called.
export async function startTestServer() {
  const server = http.createServer(handleHttp);
  const wss = new WebSocketServer({
    noServer: true,
    handleProtocols: (protocols) => (protocols.has("chat.v2") ? "chat.v2" : false),
  });
  wss.on("connection", handleWebSocket);
  const sockets = new Set();
  server.on("connection", (socket) => {
    sockets.add(socket);
    socket.on("close", () => sockets.delete(socket));
  });
  server.on("upgrade", (req, socket, head) => {
    socket.on("error", () => {});
    handleUpgrade(wss, req, socket, head);
  });
  await new Promise((done) => server.listen(0, "127.0.0.1", done));
  server.unref();
  // A port that nothing is listening on, for tests of failing to connect
  const unused = http.createServer();
  await new Promise((done) => unused.listen(0, "127.0.0.1", done));
  const closedPort = unused.address().port;
  await new Promise((done) => unused.close(done));
  return {
    baseUrl: `http://127.0.0.1:${server.address().port}`,
    closedUrl: `http://127.0.0.1:${closedPort}`,
    stop() {
      server.close();
      for (const socket of sockets) socket.destroy();
    },
  };
}
