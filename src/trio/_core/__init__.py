"""
This namespace represents the core functionality that has to be built-in
and deal with private internal data structures. Things in this namespace
are publicly available in either trio, trio.lowlevel, or trio.testing.
"""

import sys
import typing as _t

from ._cancel_scope import CancelScope
from ._entry_queue import TrioToken
from ._exceptions import (
    BrokenResourceError,
    BusyResourceError,
    Cancelled,
    ClosedResourceError,
    EndOfChannel,
    RunFinishedError,
    TrioInternalError,
    WouldBlock,
)

# The public wrappers that gen_exports generates for the runner's and the
# instruments' methods come from their own modules, which sit above the
# modules they wrap; the wrappers for the platform I/O manager's methods are
# re-exported by _runner.py, which is what selects the platform's backend.
from ._generated_instrumentation import add_instrument, remove_instrument
from ._generated_runner import (
    current_clock,
    current_root_task,
    current_statistics,
    current_time,
    current_trio_token,
    reschedule,
    spawn_system_task,
    wait_all_tasks_blocked,
)
from ._ki import currently_ki_protected, disable_ki_protection, enable_ki_protection
from ._local import RunVar, RunVarToken
from ._mock_clock import MockClock

# Imports that always exist. The core is layered as _run -> _runner ->
# _traps -> _wake_token -> _cancel_scope -> _task -> _cancel_status ->
# _run_context, each importing only from the modules below it; see the
# module docstring of _task.py.
from ._run import Nursery, open_nursery, run, start_guest_run
from ._runner import (
    RunStatistics,
    in_trio_run,
    notify_closing,
    wait_readable,
    wait_writable,
)
from ._task import (
    TASK_STATUS_IGNORED,
    Task,
    TaskStatus,
    add_task_exit_callback,
    current_effective_deadline,
    current_task,
    in_trio_task,
    remove_task_exit_callback,
)
from ._thread_cache import start_thread_soon
from ._traps import (
    Abort,
    RaiseCancelT,
    cancel_shielded_checkpoint,
    checkpoint,
    checkpoint_if_cancelled,
    permanently_detach_coroutine_object,
    reattach_detached_coroutine_object,
    temporarily_detach_coroutine_object,
    wait_task_rescheduled,
)
from ._wake_token import WakeToken

# Windows imports
if sys.platform == "win32" or (
    not _t.TYPE_CHECKING and "sphinx.ext.autodoc" in sys.modules
):
    from ._runner import (
        current_iocp,
        monitor_completion_key,
        readinto_overlapped,
        register_with_iocp,
        wait_overlapped,
        write_overlapped,
    )
# Kqueue imports
if (
    sys.platform != "linux" and sys.platform != "win32" and sys.platform != "android"
) or (not _t.TYPE_CHECKING and "sphinx.ext.autodoc" in sys.modules):
    from ._runner import current_kqueue, monitor_kevent, wait_kevent

del sys  # It would be better to import sys as _sys, but mypy does not understand it
