# The *_test.py files here are scripts for run_tests.mjs to run inside Pyodide,
# not pytest tests: they import ``js`` and ``pyodide``, which only exist there.
collect_ignore_glob = ["*_test.py"]
