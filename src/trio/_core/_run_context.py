from __future__ import annotations

import threading
from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:
    from typing import Any

    from ._local import RunVar
    from ._runner import Runner
    from ._task import Scheduler, Task


class RunContext(threading.local):
    """Per-thread record of the run in progress, if any.

    ``runner`` and ``task`` are the current run and task. ``runner`` is for
    the public synchronous accessors (``current_time()`` and the like, which
    have no task in hand); everything else reaches the run through its
    task, except for two services published here for the modules below the
    runner that have no task to go through either: ``scheduler``, for the
    clock (a cancel scope's deadline can be read and set from outside any
    task) and for what MockClock sets and wakes, and ``run_locals``, the
    values of this run's RunVars.
    """

    runner: Runner
    task: Task
    scheduler: Scheduler
    run_locals: dict[RunVar[Any], object]  # type: ignore[explicit-any]


GLOBAL_RUN_CONTEXT: Final = RunContext()
