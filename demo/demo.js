// Loads Pyodide, installs Trio and its dependencies from the wheels directory,
// and hands over to demo.py, which starts Trio in guest mode.
const PYODIDE_VERSION = "314.0.7";
const params = new URLSearchParams(location.search);
// ?pyodide=/pyodide/ lets a local checkout use a local copy instead of the CDN
const indexURL = params.get("pyodide") ?? `https://cdn.jsdelivr.net/pyodide/v${PYODIDE_VERSION}/full/`;

const $ = (id) => document.getElementById(id);
const status = (text) => { $("status").textContent = text; };

try {
  status("Loading Pyodide…");
  const { loadPyodide } = await import(indexURL + "pyodide.mjs");
  const pyodide = await loadPyodide({ indexURL });

  status("Installing Trio…");
  const wheels = await (await fetch("wheels/index.json")).json();
  for (const name of wheels) {
    const buffer = await (await fetch(`wheels/${name}`)).arrayBuffer();
    pyodide.unpackArchive(buffer, "zip", { extractDir: "/wheels" });
  }
  pyodide.runPython('import sys; sys.path.insert(0, "/wheels")');

  status("Starting Trio…");
  pyodide.runPython(await (await fetch("demo.py")).text());

  // Buttons. The Python side put these functions on globalThis.
  $("start-btn").onclick = () => globalThis.startNursery();
  $("cancel-btn").onclick = () => globalThis.cancelNursery();
  $("fetch-btn").onclick = () => globalThis.fetchHello();
  $("fetch-cancel-btn").onclick = () => globalThis.fetchCancelled();
  $("js-call-btn").onclick = async () => {
    $("js-call-out").textContent = "waiting…";
    const result = await globalThis.trioDouble(21);
    $("js-call-out").textContent = `JavaScript got ${result} back from Trio`;
  };
} catch (error) {
  status(`Failed: ${error}`);
  throw error;
}
