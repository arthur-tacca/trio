Trio on Pyodide smoke test
==========================

This checks that Trio's guest mode works on Emscripten, by running a small
Trio program inside `Pyodide <https://pyodide.org/>`__ under Node.js.
It isn't part of the regular test suite. To run it::

    # Pyodide's Node.js package
    npm install pyodide
    # Trio's (pure Python) dependencies, in a directory Pyodide can mount
    python -m pip install --target pyodide-deps attrs sortedcontainers idna outcome sniffio
    # Run it
    node tests/pyodide/run_smoke_test.mjs

The script mounts ``src/`` and ``pyodide-deps/`` into Pyodide's file system;
pass ``--src=...`` or ``--deps=...`` to use different directories.
