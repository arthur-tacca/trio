# Trio in the browser with Pyodide

A static page that runs [Trio](https://trio.readthedocs.io/) inside
[Pyodide](https://pyodide.org/) as a guest of the browser's event loop, using
the Emscripten support on this branch. It is published with GitHub Pages at
https://arthur-tacca.github.io/trio/ by the "Pyodide demo" workflow, which
rebuilds the Trio wheel from the branch on every push.

The page shows concurrent tasks with `trio.sleep`, cancelling a task from a
button, `trio.pyodide.fetch` with a Trio timeout aborting a streaming fetch,
and JavaScript awaiting a Trio async function.

## Files

- `index.html`, `demo.js`: load Pyodide from the jsDelivr CDN, unpack the wheels
  into the Pyodide filesystem, then run `demo.py`.
- `demo.py`: the guest-mode glue (`run_sync_soon` and `run_sync_later` on top
  of `setTimeout`) and the Trio tasks behind the page.
- `build_wheels.py`: builds the Trio wheel from this checkout and downloads its
  pure-Python dependencies into `wheels/`, with a `wheels/index.json` listing
  them. The `wheels/` directory is generated and not committed.
- `hello.txt`: a small file for the fetch button.

## Run locally

From the repository root:

    python -m pip install build
    python demo/build_wheels.py
    python -m http.server 8000 --directory demo

then open http://localhost:8000/. To load Pyodide from somewhere other than
the CDN, for instance a local copy served next to the page, open
`index.html?pyodide=/pyodide/` (any URL ending in a slash works).

## Publishing

`.github/workflows/pyodide-demo.yml` runs on every push to the
`pyodide-support` branch, and on demand from the Actions tab. It runs
`build_wheels.py` and publishes the `demo/` directory. One-time setup in the
repository settings: under Pages, set the source to "GitHub Actions". On a
fork, workflows must first be enabled from the Actions tab.
