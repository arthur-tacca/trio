from __future__ import annotations

import threading
from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:
    from ._run import Runner, Task


class RunContext(threading.local):
    runner: Runner
    task: Task


GLOBAL_RUN_CONTEXT: Final = RunContext()


def current_task() -> Task:
    """Return the :class:`Task` object representing the current task.

    Returns:
      Task: the :class:`Task` that called :func:`current_task`.

    """

    try:
        return GLOBAL_RUN_CONTEXT.task
    except AttributeError:
        raise RuntimeError("must be called from async context") from None
