Trio on Pyodide tests
=====================

These check that Trio works on Emscripten, by running small Trio programs
inside `Pyodide <https://pyodide.org/>`__ under Node.js:

- ``smoke_test.py``: guest mode on top of the JavaScript event loop (sleeping,
  nurseries, cancellation, waking Trio from JavaScript).
- ``fetch_test.py``: ``trio.pyodide``, i.e. waiting for JavaScript promises
  and the ``fetch``-based HTTP client, against a local HTTP server that the
  runner starts.

They aren't part of the regular test suite. To run them::

    # Pyodide's Node.js package
    npm install pyodide
    # Trio's (pure Python) dependencies, in a directory Pyodide can mount
    python -m pip install --target pyodide-deps attrs sortedcontainers idna outcome sniffio
    # Run all the *_test.py files, or name the ones you want
    node tests/pyodide/run_tests.mjs
    node tests/pyodide/run_tests.mjs fetch_test.py

The runner mounts ``src/`` and ``pyodide-deps/`` into Pyodide's file system;
pass ``--src=...`` or ``--deps=...`` to use different directories.
