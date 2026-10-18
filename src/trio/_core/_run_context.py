from __future__ import annotations

import threading
from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:
    from .._abc import Clock
    from ._runner import Runner
    from ._task import Deadlines, Task


class RunContext(threading.local):
    """Per-thread record of the run in progress, if any.

    ``runner`` and ``task`` are the current run and task. ``clock`` and
    ``deadlines`` are the two pieces of the runner that the task/cancellation
    layer needs, published here so that it doesn't have to know about the
    runner itself.
    """

    runner: Runner
    task: Task
    clock: Clock
    deadlines: Deadlines


GLOBAL_RUN_CONTEXT: Final = RunContext()
