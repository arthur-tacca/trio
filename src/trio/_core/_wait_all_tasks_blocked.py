from __future__ import annotations

from typing import Protocol, runtime_checkable

from ._ki import enable_ki_protection
from ._run_context import GLOBAL_RUN_CONTEXT


@runtime_checkable
class SupportsWaitAllTasksBlocked(Protocol):
    """The capability :func:`wait_all_tasks_blocked` needs from the run's
    clock. :class:`trio.testing.TestingClock` provides it; a custom clock
    may too, including by delegating to a ``TestingClock``."""

    async def wait_all_tasks_blocked(self, cushion: float = 0.0) -> None: ...


@enable_ki_protection
async def wait_all_tasks_blocked(cushion: float = 0.0) -> None:
    """Block until there are no runnable tasks.

    This is useful in testing code when you want to give other tasks a
    chance to "settle down". The calling task is blocked, and doesn't wake
    up until all other tasks are also blocked for at least ``cushion``
    seconds. (Setting a non-zero ``cushion`` is intended to handle cases
    like two tasks talking to each other over a local socket, where we want
    to ignore the potential brief moment between a send and receive when all
    tasks are blocked.)

    Note that ``cushion`` is measured in *real* time, not the Trio clock
    time.

    If there are multiple tasks blocked in :func:`wait_all_tasks_blocked`,
    then the one with the shortest ``cushion`` is the one woken (and this
    task becoming unblocked resets the timers for the remaining tasks). If
    there are multiple tasks that have exactly the same ``cushion``, then
    all are woken.

    Knowing when the run has gone idle is the clock's job, so this needs a
    clock that implements it -- in practice :class:`trio.testing.TestingClock`.
    Pass ``clock=trio.testing.TestingClock(rate=1.0)`` to :func:`trio.run` if
    you want real-time behaviour.

    You should also consider :class:`trio.testing.Sequencer`, which provides
    a more explicit way to control execution ordering within a test, and
    will often produce more readable tests.

    """
    try:
        clock = GLOBAL_RUN_CONTEXT.runner.clock
    except AttributeError:
        raise RuntimeError("must be called from async context") from None
    if not isinstance(clock, SupportsWaitAllTasksBlocked):
        raise TypeError(
            f"wait_all_tasks_blocked() needs a clock that supports it "
            f"(see SupportsWaitAllTasksBlocked), but this run installed "
            f"{type(clock).__name__}. Pass "
            f"clock=trio.testing.TestingClock(rate=1.0) to trio.run().",
        )
    await clock.wait_all_tasks_blocked(cushion)
