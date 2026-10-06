from __future__ import annotations

from typing import TYPE_CHECKING

import attrs

from .. import _core

if TYPE_CHECKING:
    from ._run import Task
    from ._traps import Abort, RaiseCancelT


@attrs.define(eq=False)
class EmscriptenWakeup:
    """Replacement for `WakeupSocketpair` on Emscripten.

    The entry queue needs some way for `TrioToken.run_sync_soon` to wake up the
    task that runs the queued jobs. Normally that's a socketpair, so that it
    works from other threads and from signal handlers. On Emscripten there is
    no socketpair, but there are also no other threads, and everything runs on
    the host loop's thread, so we can just reschedule the waiting task
    directly. (`Runner.reschedule` then takes care of asking the host loop to
    run a guest tick, the same way it does when a task gets woken by host code.)

    Caveat: Pyodide can deliver `KeyboardInterrupt` via its "interrupt buffer",
    which runs Trio's SIGINT handler at an arbitrary bytecode boundary just
    like a real signal would. Rescheduling from there isn't atomic with
    respect to the run loop's state. That's a pre-existing limitation of
    `Runner.reschedule` being called from host callbacks, and interrupt
    buffers are rare, so we don't try to handle it here.

    """

    _pending: bool = False
    _waiting_task: Task | None = None

    def wakeup_thread_and_signal_safe(self) -> None:
        self._pending = True
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
        pass
