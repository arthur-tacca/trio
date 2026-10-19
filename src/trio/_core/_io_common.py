from __future__ import annotations

import copy
from typing import TYPE_CHECKING

import outcome

if TYPE_CHECKING:
    from ._io_epoll import EpollWaiters
    from ._io_windows import AFDWaiters
    from ._wake_token import WakeToken


# Utility function shared between _io_epoll and _io_windows
def wake_all(
    waiters: EpollWaiters | AFDWaiters,
    exc: BaseException,
    *,
    own: WakeToken[None] | None = None,
) -> None:
    """Wake all the tasks waiting in ``waiters`` with (a copy of) ``exc``, and
    clear ``waiters``.

    ``own`` is the token that the calling task has just registered in
    ``waiters`` but not awaited yet, if any. That one can't be woken, so
    instead ``exc`` is raised here, once the others have been dealt with.
    """
    raise_at_end = False
    for attr_name in ["read_waiter", "write_waiter"]:
        token = getattr(waiters, attr_name)
        if token is not None:
            if token is own:
                raise_at_end = True
            else:
                token.reschedule(outcome.Error(copy.copy(exc)))
            setattr(waiters, attr_name, None)
    if raise_at_end:
        raise exc
