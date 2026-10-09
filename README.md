# Trio running in Pyodide

A static web page that runs [Trio](https://trio.readthedocs.io/) inside
[Pyodide](https://pyodide.org/) as a guest of the browser's event loop, using
the experimental Emscripten support on the `pyodide-support` branch of this
repository. It is published with GitHub Pages at
https://arthur-tacca.github.io/trio/.

This `gh-pages` branch holds only the demo. The code it demonstrates lives on
the `pyodide-support` branch.

The page shows concurrent tasks with `trio.sleep`, cancelling a task from a
button, `trio.pyodide.fetch` with a Trio timeout aborting a streaming fetch,
and JavaScript awaiting a Trio async function.

## Files

- `index.html`, `demo.js`: load Pyodide from the jsDelivr CDN, unpack the wheels
  into the Pyodide filesystem, then run `demo.py`.
- `demo.py`: the guest-mode glue (`run_sync_soon` and `run_sync_later` on top
  of `setTimeout`) and the Trio tasks behind the page.
- `wheels/`: Trio built from the `pyodide-support` branch plus its pure-Python
  dependencies. `wheels/index.json` lists them.
- `.nojekyll`: tells GitHub Pages to serve the files as they are.

## Run locally

Any static file server works:

    python3 -m http.server 8000

then open http://localhost:8000/. To use a local copy of Pyodide instead of
the CDN, serve it alongside the page and open `index.html?pyodide=/pyodide/`.

## Publishing

In the repository settings, under Pages, choose "Deploy from a branch", pick
`gh-pages` and the `/ (root)` folder. Pages builds run through GitHub Actions,
so on a fork the workflows must first be enabled from the Actions tab.

## Rebuild the Trio wheel

From a checkout of the `pyodide-support` branch:

    python -m pip install build
    python -m build --wheel

then copy `dist/trio-*.whl` into `wheels/` on this branch, update the Trio
entry in `wheels/index.json` if the file name changed, and commit.
