Trio on Pyodide tests
=====================

These check that Trio works on Emscripten, by running small Trio programs
inside `Pyodide <https://pyodide.org/>`__ under Node.js:

- ``smoke_test.py``: guest mode on top of the JavaScript event loop (sleeping,
  nurseries, cancellation, waking Trio from JavaScript).
- ``pyodide_test.py``: ``trio.pyodide``, i.e. calling JavaScript async
  functions from Trio, calling Trio from JavaScript, and the ``fetch``-based
  HTTP client.
- ``websocket_test.py``: ``trio.pyodide.connect_websocket``.
- ``httpx2_test.py``: the proof-of-concept transport for `httpx2
  <https://github.com/pydantic/httpx2>`__, ``trio._pyodide_httpx2``, for both
  HTTP and WebSockets.

All but the first talk to a local HTTP and WebSocket server,
``test_server.mjs``, which the runner starts for them.

They aren't part of the regular test suite. To run them::

    # Pyodide's Node.js package (which brings the "ws" package the server uses)
    npm install pyodide
    # Trio's (pure Python) dependencies, in a directory Pyodide can mount
    python -m pip install --target pyodide-deps attrs sortedcontainers idna outcome sniffio
    # Run all the *_test.py files, or name the ones you want
    node tests/pyodide/run_tests.mjs
    node tests/pyodide/run_tests.mjs pyodide_test.py

The runner mounts ``src/`` and ``pyodide-deps/`` into Pyodide's file system;
pass ``--src=...`` or ``--deps=...`` to use different directories.

``httpx2_test.py`` reports that it was skipped unless httpx2 is in that
directory too, with what its WebSocket support needs. On Emscripten, httpx2
imports ``httpx2-jsfetch``, which pip only installs by itself when it's
running there, so it has to be named::

    python -m pip install --target pyodide-deps "httpx2[ws]" httpx2-jsfetch

That test is also an ordinary program. It runs under any Python, from 3.11,
that has Trio's dependencies and ``httpx2[ws]`` installed, and there it uses
httpx2 just as it comes, over real sockets::

    node tests/pyodide/run_tests.mjs --native=python3 httpx2_test.py

Both ways it makes the same assertions, apart from the few that are marked
in the file as depending on the platform: the things that a browser doesn't
let a script do. That's the point of the transport: one httpx2 program for
Trio, natively and in a browser.
