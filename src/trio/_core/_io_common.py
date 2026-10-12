from __future__ import annotations

import copy
from functools import partial
from typing import TYPE_CHECKING, TypeVar

import outcome

from .. import _core
from ._thread_cache import start_thread_soon

if TYPE_CHECKING:
    from collections.abc import Callable

    from ._io_epoll import EpollWaiters
    from ._io_windows import AFDWaiters


# Utility function shared between _io_epoll and _io_windows
def wake_all(waiters: EpollWaiters | AFDWaiters, exc: BaseException) -> None:
    try:
        current_task = _core.current_task()
    except RuntimeError:
        current_task = None
    raise_at_end = False
    for attr_name in ["read_task", "write_task"]:
        task = getattr(waiters, attr_name)
        if task is not None:
            if task is current_task:
                raise_at_end = True
            else:
                _core.reschedule(task, outcome.Error(copy.copy(exc)))
            setattr(waiters, attr_name, None)
    if raise_at_end:
        raise exc


EventsT = TypeVar("EventsT")


def wait_for_events_in_thread(
    get_events: Callable[[float], EventsT],
    timeout: float,
    deliver: Callable[[outcome.Outcome[EventsT]], None],
    run_sync_soon_threadsafe: Callable[[Callable[[], object]], object],
) -> None:
    """The guest-mode wait, for I/O managers whose get_events() blocks: call
    it on a worker thread, and hand its outcome to ``deliver`` on the main
    thread. force_wakeup() makes get_events() return early, which ends the
    wait.

    The I/O managers' wait_for_events() methods are called by
    GuestState.guest_tick in _run.py; see there for the contract.
    """

    def get_events_in_thread() -> EventsT:
        return get_events(timeout)

    def deliver_from_thread(events_outcome: outcome.Outcome[EventsT]) -> None:
        run_sync_soon_threadsafe(partial(deliver, events_outcome))

    start_thread_soon(get_events_in_thread, deliver_from_thread)
