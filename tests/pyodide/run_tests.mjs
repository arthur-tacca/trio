// Runs the *_test.py files in this directory inside Pyodide under Node.js,
// each in its own process, with a local server (test_server.mjs) for them to
// talk to. With --native=PYTHON it runs them with that Python instead, for
// the tests that can do both. See README.rst.
import { spawn, spawnSync } from "node:child_process";
import { readFileSync, readdirSync } from "node:fs";
import { delimiter, dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { startTestServer } from "./test_server.mjs";

const here = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(here, "..", "..");
const positional = process.argv.slice(2).filter((arg) => !arg.startsWith("--"));
const options = Object.fromEntries(
  process.argv.slice(2).filter((arg) => arg.startsWith("--")).map((arg) => arg.slice(2).split("=")),
);
const srcDir = resolve(options.src ?? resolve(repoRoot, "src"));
const depsDir = resolve(options.deps ?? resolve(repoRoot, "pyodide-deps"));

if (options.file === undefined) {
  // Parent: run each test file in a child process, so each gets a fresh Pyodide.
  const files = positional.length ? positional : readdirSync(here).filter((f) => f.endsWith("_test.py")).sort();
  let failed = 0;
  for (const file of files) {
    console.log(`\n=== ${file}`);
    const args = [fileURLToPath(import.meta.url), `--file=${file}`, `--src=${srcDir}`, `--deps=${depsDir}`];
    if (options.native !== undefined) args.push(`--native=${options.native}`);
    const result = spawnSync(process.execPath, args, { stdio: "inherit" });
    if (result.status !== 0) failed += 1;
  }
  console.log(`\n${files.length - failed} of ${files.length} test files passed`);
  process.exitCode = failed ? 1 : 0;
} else if (options.native !== undefined) {
  // Child, outside Pyodide: a server, then the test file under an ordinary
  // Python, which is told where the server is through its environment.
  const server = await startTestServer();
  const python = spawn(options.native, [resolve(here, options.file)], {
    stdio: "inherit",
    env: {
      ...process.env,
      PYTHONPATH: [srcDir, process.env.PYTHONPATH].filter(Boolean).join(delimiter),
      TEST_BASE_URL: server.baseUrl,
      TEST_CLOSED_URL: server.closedUrl,
    },
  });
  python.on("error", (error) => {
    console.error(`couldn't run ${options.native}: ${error.message}`);
    server.stop();
    process.exitCode = 1;
  });
  python.on("exit", (code) => {
    server.stop();
    process.exitCode = code ?? 1;
  });
} else {
  // Child: a server for the tests to talk to, then the test file itself.
  const server = await startTestServer();
  globalThis.TEST_BASE_URL = server.baseUrl;
  globalThis.TEST_CLOSED_URL = server.closedUrl;
  globalThis.stopServer = server.stop;

  const { loadPyodide } = await import("pyodide");
  const t0 = Date.now();
  const pyodide = await loadPyodide();
  pyodide.mountNodeFS("/trio_src", srcDir);
  pyodide.mountNodeFS("/deps", depsDir);
  console.log(`pyodide ${pyodide.version} loaded in ${Date.now() - t0} ms`);
  pyodide.runPython('import sys; sys.path[:0] = ["/trio_src", "/deps"]');
  pyodide.runPython(readFileSync(resolve(here, options.file), "utf8"));
  // The guest run is now in progress; Node keeps running until Trio's last
  // timer has fired, and then exits.
  process.on("exit", () => console.log(`node exiting ${Date.now() - t0} ms after start`));
}
