// Runs smoke_test.py inside Pyodide under Node.js. See README.rst.
import { loadPyodide } from "pyodide";
import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(here, "..", "..");
const args = Object.fromEntries(
  process.argv.slice(2).map((arg) => arg.replace(/^--/, "").split("=")),
);
const srcDir = resolve(args.src ?? resolve(repoRoot, "src"));
const depsDir = resolve(args.deps ?? resolve(repoRoot, "pyodide-deps"));

const t0 = Date.now();
const pyodide = await loadPyodide();
pyodide.mountNodeFS("/trio_src", srcDir);
pyodide.mountNodeFS("/deps", depsDir);
console.log(`pyodide ${pyodide.version} loaded in ${Date.now() - t0} ms`);
pyodide.runPython('import sys; sys.path[:0] = ["/trio_src", "/deps"]');
pyodide.runPython(readFileSync(resolve(here, "smoke_test.py"), "utf8"));
// The guest run is now in progress; Node keeps running until Trio's last
// timer has fired, and then exits.
process.on("exit", () => console.log(`node exiting ${Date.now() - t0} ms after start`));
