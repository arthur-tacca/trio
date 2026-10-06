from __future__ import annotations

import weakref
from typing import TYPE_CHECKING, Literal, TypeAlias

import attrs

from ._run import _public

if TYPE_CHECKING:
    from collections.abc import Callable

    from .._file_io import _HasFileNo

# Emscripten (e.g. Pyodide, i.e. Python running in a web browser) has no usable
# sockets, no worker threads, and no way to block its single thread while
# waiting for I/O. So this "I/O manager" doesn't do any I/O at all: the wait_*
# functions all raise NotImplementedError. What it does provide is the
# bookkeeping that guest mode needs in order to run Trio on top of the
# browser's event loop:
#
# - force_wakeup() asks the host loop to run a guest tick as soon as possible,
#   via a callback that GuestState registers with set_guest_wakeup().
# - get_events() then reports whether such a forced wakeup happened, so that
#   the run loop can tell "woken up early" apart from "timeout expired".
#
# Timeouts are handled by host loop timers instead of a worker thread; see
# GuestState.guest_tick in _run.py.

# Non-empty iff force_wakeup() was called since the last call to get_events()
EventResult: TypeAlias = "list[None]"


@attrs.frozen(eq=False)
class _EmscriptenStatistics:
    backend: Literal["emscripten"] = attrs.field(init=False, default="emscripten")


@attrs.define(eq=False)
class EmscriptenIOManager:
    _force_wakeup_pending: bool = False
    # A weak reference to the GuestState method that schedules a guest tick
    # immediately. It must be weak: the Runner must not keep the GuestState
    # alive, directly or indirectly (see the comment above GuestState in
    # _run.py).
    _guest_wakeup: weakref.WeakMethod[Callable[[], None]] | None = None

    def statistics(self) -> _EmscriptenStatistics:
        return _EmscriptenStatistics()

    def close(self) -> None:
        pass

    def set_guest_wakeup(self, guest_wakeup: Callable[[], None]) -> None:
        """Register the (bound method) callback that force_wakeup() should use
        to schedule a guest tick as soon as possible."""
        self._guest_wakeup = weakref.WeakMethod(guest_wakeup)

    def force_wakeup(self) -> None:
        self._force_wakeup_pending = True
        if self._guest_wakeup is not None:
            guest_wakeup = self._guest_wakeup()
            if guest_wakeup is not None:
                guest_wakeup()

    # Return value must be False-y IFF the timeout expired, NOT if any I/O
    # happened or force_wakeup was called. Otherwise it can be anything; gets
    # passed straight through to process_events.
    def get_events(self, timeout: float) -> EventResult:
        if timeout > 0:
            raise NotImplementedError(
                "Trio can't block waiting for events on Emscripten; "
                "use trio.lowlevel.start_guest_run instead of trio.run",
            )
        if self._force_wakeup_pending:
            self._force_wakeup_pending = False
            return [None]
        return []

    def process_events(self, events: EventResult) -> None:
        pass

    @_public
    async def wait_readable(self, fd: int | _HasFileNo) -> None:
        """Not supported on Emscripten, where there is no readiness-based I/O.

        :raises NotImplementedError: always.
        """
        raise NotImplementedError("wait_readable is not supported on Emscripten")

    @_public
    async def wait_writable(self, fd: int | _HasFileNo) -> None:
        """Not supported on Emscripten, where there is no readiness-based I/O.

        :raises NotImplementedError: always.
        """
        raise NotImplementedError("wait_writable is not supported on Emscripten")

    @_public
    def notify_closing(self, fd: int | _HasFileNo) -> None:
        """Does nothing on Emscripten, because `wait_readable` and
        `wait_writable` aren't supported there, so there can never be any
        waiters to notify.
        """
