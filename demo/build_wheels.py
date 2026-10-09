"""Collect the wheels that the demo page installs into Pyodide.

Builds Trio from this checkout and downloads its pure-Python dependencies into
``demo/wheels/``, then writes ``demo/wheels/index.json`` listing them. Needs
the ``build`` package (``python -m pip install build``).

pip evaluates dependency markers for the interpreter running it, so run this
with a Python new enough not to need Trio's backports (3.11 or later) and not
on Windows, which would pull in cffi.
"""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

DEMO = Path(__file__).resolve().parent
WHEELS = DEMO / "wheels"


def main() -> None:
    if WHEELS.exists():
        shutil.rmtree(WHEELS)
    WHEELS.mkdir()
    with tempfile.TemporaryDirectory() as dist:
        subprocess.run(
            [
                sys.executable,
                "-m",
                "build",
                "--wheel",
                "--outdir",
                dist,
                str(DEMO.parent),
            ],
            check=True,
        )
        (trio_wheel,) = Path(dist).glob("trio-*.whl")
        # Pyodide can only use pure-Python wheels, so resolve Trio's
        # dependencies as if for an interpreter that accepts nothing else.
        subprocess.run(
            [
                sys.executable,
                "-m",
                "pip",
                "download",
                "--only-binary=:all:",
                "--implementation=py",
                "--abi=none",
                "--platform=any",
                "--dest",
                str(WHEELS),
                str(trio_wheel),
            ],
            check=True,
        )
    names = sorted(path.name for path in WHEELS.glob("*.whl"))
    (WHEELS / "index.json").write_text(json.dumps(names, indent=2) + "\n")
    print(f"Collected {len(names)} wheels into {WHEELS}")


if __name__ == "__main__":
    main()
