from __future__ import annotations

from typing import TYPE_CHECKING

import attrs

from .. import _core

if TYPE_CHECKING:
    from collections.abc import Callable

    from ._run import Task
    from ._traps import Abort, RaiseCancelT


@attrs.define(eq=False)
class EmscriptenWakeup:
    """Replacement for `WakeupSocketpair` on Emscripten.

    The entry queue needs some way for `TrioToken.run_sync_soon` to wake up the
    task that runs the queued jobs. Normally that's a socketpair, because it
    has to work from other threads and from signal handlers. Emscripten has no
    socketpair, but also no other threads, so all that's needed is to
    reschedule the waiting task from the host loop's thread.

    That can't be done directly from `wakeup_thread_and_signal_safe`, though,
    because it isn't only called from host callbacks between ticks. Pyodide
    runs Trio's SIGINT handler at an arbitrary bytecode boundary when it's
    given an "interrupt buffer", and the garbage collector runs Trio's async
    generator finalizer wherever it happens to fire; both end up here, possibly
    in the middle of the run loop's own bookkeeping. Writing to a socketpair is
    safe at such a moment, because the wake is only acted on later, when
    get_events() sees it; rescheduling a task is not. So this does the
    equivalent: it sets a flag and asks the host loop to call it back, which
    is a JavaScript scheduling call that touches no Trio state, and it
    reschedules the task from that callback, which runs with nothing of Trio's
    on the stack. `start_guest_run` provides the host loop's
    ``run_sync_soon_threadsafe`` for this.

    """

    _pending: bool = False
    _waiting_task: Task | None = None
    _host_run_sync_soon: Callable[[Callable[[], object]], object] | None = None

    def set_host_run_sync_soon(
        self,
        run_sync_soon_threadsafe: Callable[[Callable[[], object]], object],
    ) -> None:
        self._host_run_sync_soon = run_sync_soon_threadsafe

    def wakeup_thread_and_signal_safe(self) -> None:
        # Safe at any bytecode boundary: a flag, and a call into the host
        self._pending = True
        assert self._host_run_sync_soon is not None, "set by start_guest_run"
        self._host_run_sync_soon(self._wake_from_host)

    def _wake_from_host(self) -> None:
        # A fresh host callback, so rescheduling is fine here. If several
        # wakeups were requested before this ran, the first call does the
        # work and the rest find nothing to do; the flag covers a wakeup
        # that arrived while the task was awake.
        task, self._waiting_task = self._waiting_task, None
        if task is not None:
            _core.reschedule(task)

    async def wait_woken(self) -> None:
        if not self._pending:
            self._waiting_task = _core.current_task()

            def abort(_: RaiseCancelT) -> Abort:
                self._waiting_task = None
                return _core.Abort.SUCCEEDED

            await _core.wait_task_rescheduled(abort)
        self._pending = False

    def wakeup_on_signals(self) -> None:
        # No signals (and no fds for signal.set_wakeup_fd to write to) here.
        pass

    def close(self) -> None:
        self._waiting_task = None
