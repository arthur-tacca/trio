"""The runner: the middle layer of Trio's core.

See the module docstring of ``_task.py`` for the layering. This module owns
the run loop (`unrolled_run`), the per-run objects around it (`Runner`), and
the platform I/O manager selection. It drives the `Scheduler` that
``_task.py`` defines, and knows about tasks and cancel scopes, but not
about nurseries: the init task that creates the system and main-task
nurseries is supplied by `_run.py`, and what a task belongs to is only ever
reached through the `TaskOwner` contract that ``_task.py`` defines.
"""

from __future__ import annotations

import enum
import random
import select
import sys
import warnings
from contextlib import suppress
from contextvars import copy_context
from time import perf_counter
from typing import TYPE_CHECKING, Any, Final, cast

import attrs
import outcome
from outcome import Error, Outcome, Value, capture
from sniffio import thread_local as sniffio_library
from sortedcontainers import SortedDict

from .._abc import Clock, Instrument
from ._asyncgens import AsyncGenerators
from ._entry_queue import EntryQueue, TrioToken
from ._exceptions import Cancelled, RunFinishedError, TrioInternalError
from ._instrumentation import Instruments
from ._ki import KIManager, enable_ki_protection
from ._mock_clock import MockClock
from ._run_context import GLOBAL_RUN_CONTEXT
from ._task import (
    CancelReason,
    CancelScope,
    Deadlines,
    Scheduler,
    Task,
    TaskOwner,
    _public,
)
from ._thread_cache import start_thread_soon
from ._traps import CancelShieldedCheckpoint, PermanentlyDetachCoroutineObject
from ._wake_token import WakeToken

if sys.version_info < (3, 11):
    from exceptiongroup import BaseExceptionGroup


if TYPE_CHECKING:
    import contextvars
    from collections.abc import Awaitable, Callable, Generator, Sequence

    from typing_extensions import Unpack

    from ._task import BaseExcT, PosArgT


# Passed as a sentinel
_NO_SEND: Final[Outcome[object]] = cast("Outcome[object]", object())


# When running under Hypothesis, we want examples to be reproducible and
# shrinkable.  We therefore register `_hypothesis_plugin_setup()` as a
# plugin, so that importing *Hypothesis* will make Trio's task
# scheduling loop deterministic.  We have a test for that, of course.
# Before Hypothesis supported entry-point plugins this integration was
# handled by pytest-trio, but we want it to work in e.g. unittest too.
_ALLOW_DETERMINISTIC_SCHEDULING: Final = False
_r = random.Random()


# no cover because we don't check the hypothesis plugin works with hypothesis
def _hypothesis_plugin_setup() -> None:  # pragma: no cover
    from hypothesis import register_random

    global _ALLOW_DETERMINISTIC_SCHEDULING
    _ALLOW_DETERMINISTIC_SCHEDULING = True  # type: ignore
    register_random(_r)

    # monkeypatch repr_callable to make repr's way better
    # requires importing hypothesis (in the test file or in conftest.py)
    try:
        from hypothesis.internal.reflection import get_pretty_function_description

        import trio.testing._raises_group

        def repr_callable(fun: Callable[[BaseExcT], bool]) -> str:
            # add quotes around the signature
            return repr(get_pretty_function_description(fun))

        trio.testing._raises_group.repr_callable = repr_callable
    except ImportError:
        pass


def _count_context_run_tb_frames() -> int:
    """Count implementation dependent traceback frames from Context.run()

    On CPython, Context.run() is implemented in C and doesn't show up in
    tracebacks. On PyPy, it is implemented in Python and adds 1 frame to
    tracebacks.

    Returns:
        int: Traceback frame count

    """

    def function_with_unique_name_xyzzy() -> NoReturn:
        try:
            1 / 0  # noqa: B018  # We need a ZeroDivisionError to fire
        except ZeroDivisionError:
            raise
        else:  # pragma: no cover
            raise TrioInternalError(
                "A ZeroDivisionError should have been raised, but it wasn't.",
            )

    ctx = copy_context()
    try:
        ctx.run(function_with_unique_name_xyzzy)
    except ZeroDivisionError as exc:
        tb = exc.__traceback__
        # Skip the frame where we caught it
        tb = tb.tb_next  # type: ignore[union-attr]
        count = 0
        while tb.tb_frame.f_code.co_name != "function_with_unique_name_xyzzy":  # type: ignore[union-attr]
            tb = tb.tb_next  # type: ignore[union-attr]
            count += 1
        return count
    else:  # pragma: no cover
        raise TrioInternalError(
            f"The purpose of {function_with_unique_name_xyzzy.__name__} is "
            "to raise a ZeroDivisionError, but it didn't.",
        )


CONTEXT_RUN_TB_FRAMES: Final = _count_context_run_tb_frames()


@attrs.frozen
class SystemClock(Clock):
    # Add a large random offset to our clock to ensure that if people
    # accidentally call time.perf_counter() directly or start comparing clocks
    # between different runs, then they'll notice the bug quickly:
    offset: float = attrs.Factory(lambda: _r.uniform(10000, 200000))

    def start_clock(self) -> None:
        pass

    # In cPython 3, on every platform except Windows, perf_counter is
    # exactly the same as time.monotonic; and on Windows, it uses
    # QueryPerformanceCounter instead of GetTickCount64.
    def current_time(self) -> float:
        return self.offset + perf_counter()

    def deadline_to_sleep_time(self, deadline: float) -> float:
        return deadline - self.current_time()


class IdlePrimedTypes(enum.Enum):
    WAITING_FOR_IDLE = 1
    AUTOJUMP_CLOCK = 2


################################################################
# The central Runner object
################################################################


@attrs.frozen
class RunStatistics:
    """An object containing run-loop-level debugging information.

    Currently, the following fields are defined:

    * ``tasks_living`` (int): The number of tasks that have been spawned
      and not yet exited.
    * ``tasks_runnable`` (int): The number of tasks that are currently
      queued on the run queue (as opposed to blocked waiting for something
      to happen).
    * ``seconds_to_next_deadline`` (float): The time until the next
      pending cancel scope deadline. May be negative if the deadline has
      expired but we haven't yet processed cancellations. May be
      :data:`~math.inf` if there are no pending deadlines.
    * ``run_sync_soon_queue_size`` (int): The number of
      unprocessed callbacks queued via
      :meth:`trio.lowlevel.TrioToken.run_sync_soon`.
    * ``io_statistics`` (object): Some statistics from Trio's I/O
      backend. This always has an attribute ``backend`` which is a string
      naming which operating-system-specific I/O backend is in use; the
      other attributes vary between backends.
    """

    tasks_living: int
    tasks_runnable: int
    seconds_to_next_deadline: float
    io_statistics: IOStatistics
    run_sync_soon_queue_size: int


# This holds all the state that gets trampolined back and forth between
# callbacks when we're running in guest mode.
#
# It has to be a separate object from Runner, and Runner *cannot* hold
# references to it (directly or indirectly)!
#
# The idea is that we want a chance to detect if our host loop quits and stops
# driving us forward. We detect that by unrolled_run_gen being garbage
# collected, and hitting its 'except GeneratorExit:' block. So this only
# happens if unrolled_run_gen is GCed.
#
# The Runner state is referenced from the global GLOBAL_RUN_CONTEXT. The only
# way it gets *un*referenced is by unrolled_run_gen completing, e.g. by being
# GCed. But if Runner has a direct or indirect reference to it, and the host
# loop has abandoned it, then this will never happen!
#
# So this object can reference Runner, but Runner can't reference it. The only
# references to it are the "in flight" callback chain on the host loop /
# worker thread.


@attrs.define(eq=False)
class GuestState:  # type: ignore[explicit-any]
    runner: Runner
    run_sync_soon_threadsafe: Callable[[Callable[[], object]], object]
    run_sync_soon_not_threadsafe: Callable[[Callable[[], object]], object]
    done_callback: Callable[[Outcome[Any]], object]  # type: ignore[explicit-any]
    unrolled_run_gen: Generator[float, EventResult, None]
    unrolled_run_next_send: Outcome[Any] = attrs.Factory(lambda: Value(None))  # type: ignore[explicit-any]

    def guest_tick(self) -> None:
        prev_library, sniffio_library.name = sniffio_library.name, "trio"
        try:
            timeout = self.unrolled_run_next_send.send(self.unrolled_run_gen)
        except StopIteration:
            assert self.runner.main_task_outcome is not None
            self.done_callback(self.runner.main_task_outcome)
            return
        except TrioInternalError as exc:
            self.done_callback(Error(exc))
            return
        finally:
            sniffio_library.name = prev_library

        # Optimization: try to skip going into the thread if we can avoid it
        events_outcome: Value[EventResult] | Error = capture(
            self.runner.io_manager.get_events,
            0,
        )
        if timeout <= 0 or isinstance(events_outcome, Error) or events_outcome.value:
            # No need to go into the thread
            self.unrolled_run_next_send = events_outcome
            self.runner.guest_tick_scheduled = True
            self.run_sync_soon_not_threadsafe(self.guest_tick)
        else:
            # Need to go into the thread and call get_events() there
            self.runner.guest_tick_scheduled = False

            def get_events() -> EventResult:
                return self.runner.io_manager.get_events(timeout)

            def deliver(events_outcome: Outcome[EventResult]) -> None:
                def in_main_thread() -> None:
                    self.unrolled_run_next_send = events_outcome
                    self.runner.guest_tick_scheduled = True
                    self.guest_tick()

                self.run_sync_soon_threadsafe(in_main_thread)

            start_thread_soon(get_events, deliver)


@attrs.define(eq=False)
class Runner:  # type: ignore[explicit-any]
    clock: Clock
    instruments: Instruments
    io_manager: TheIOManager
    ki_manager: KIManager
    strict_exception_groups: bool
    # The run queue, the living tasks and the deadlines, with spawning and
    # reaping; the loop in unrolled_run() drives it. Shares our clock and
    # instruments.
    scheduler: Scheduler

    # The root task, whose owner is the runner itself (see below).
    init_task: Task | None = None
    # Created by the init task (_run.init); the runner only ever uses it as a
    # place to spawn system tasks, through the TaskOwner contract.
    system_nursery: TaskOwner | None = None
    main_task: Task | None = None
    main_task_outcome: Outcome[object] | None = None
    # The cancel scope that init() wraps around the main task; a deferred
    # control-C is delivered by cancelling it.
    main_task_scope: CancelScope | None = None

    entry_queue: EntryQueue = attrs.Factory(EntryQueue)
    trio_token: TrioToken | None = None
    asyncgens: AsyncGenerators = attrs.Factory(AsyncGenerators)

    # Guest mode stuff
    guest_tick_scheduled: bool = False

    def wake(self) -> None:
        # The LoopWaker (see _task.py) for guest mode, which start_guest_run
        # installs on the scheduler's run queue and deadlines: make the run
        # loop tick again as soon as possible, unless a tick is already on
        # its way.
        if self.guest_tick_scheduled:
            return
        self.guest_tick_scheduled = True
        self.io_manager.force_wakeup()

    def close(self) -> None:
        self.io_manager.close()
        self.entry_queue.close()
        self.asyncgens.close()
        if "after_run" in self.instruments:
            self.instruments.call("after_run")
        # This is where KI protection gets disabled, so we do it last
        self.ki_manager.close()

    @_public
    def current_statistics(self) -> RunStatistics:
        """Returns ``RunStatistics``, which contains run-loop-level debugging information.

        Currently, the following fields are defined:

        * ``tasks_living`` (int): The number of tasks that have been spawned
          and not yet exited.
        * ``tasks_runnable`` (int): The number of tasks that are currently
          queued on the run queue (as opposed to blocked waiting for something
          to happen).
        * ``seconds_to_next_deadline`` (float): The time until the next
          pending cancel scope deadline. May be negative if the deadline has
          expired but we haven't yet processed cancellations. May be
          :data:`~math.inf` if there are no pending deadlines.
        * ``run_sync_soon_queue_size`` (int): The number of
          unprocessed callbacks queued via
          :meth:`trio.lowlevel.TrioToken.run_sync_soon`.
        * ``io_statistics`` (object): Some statistics from Trio's I/O
          backend. This always has an attribute ``backend`` which is a string
          naming which operating-system-specific I/O backend is in use; the
          other attributes vary between backends.

        """
        seconds_to_next_deadline = (
            self.scheduler.deadlines.next_deadline() - self.current_time()
        )
        return RunStatistics(
            tasks_living=len(self.scheduler.tasks),
            tasks_runnable=len(self.scheduler.run_queue),
            seconds_to_next_deadline=seconds_to_next_deadline,
            io_statistics=self.io_manager.statistics(),
            run_sync_soon_queue_size=self.entry_queue.size(),
        )

    @_public
    def current_time(self) -> float:
        """Returns the current time according to Trio's internal clock.

        Returns:
            float: The current time.

        Raises:
            RuntimeError: if not inside a call to :func:`trio.run`.

        """
        return self.clock.current_time()

    @_public
    def current_clock(self) -> Clock:
        """Returns the current :class:`~trio.abc.Clock`."""
        return self.clock

    @_public
    def current_root_task(self) -> Task | None:
        """Returns the current root :class:`Task`.

        This is the task that is the ultimate parent of all other tasks.

        """
        return self.init_task

    ################
    # The root task's owner
    ################

    # The runner is the TaskOwner (see _task.py) of the "<init>" task: it
    # spawns that task itself, in unrolled_run(), and nothing else may be
    # spawned beside it, so once that task exists the owner is closed.

    @property
    def _closed(self) -> bool:
        return self.init_task is not None

    def _child_spawned(self, task: Task) -> None:
        self.init_task = task

    def _child_finished(self, task: Task, outcome: Outcome[object]) -> None:
        # If the init task crashed, then something is very wrong and we
        # let the error propagate. (It'll eventually be wrapped in a
        # TrioInternalError.)
        outcome.unwrap()
        # the init task should be the last task to exit. If not, then
        # something is very wrong.
        if self.scheduler.tasks:  # pragma: no cover
            raise TrioInternalError

    ################
    # Core task handling primitives
    ################

    @_public
    def reschedule(
        self, task: Task, next_send: outcome.Outcome[object] = _NO_SEND
    ) -> None:
        """Reschedule the given task with the given
        :class:`outcome.Outcome`.

        See :func:`wait_task_rescheduled` for the gory details.

        There must be exactly one call to :func:`reschedule` for every call to
        :func:`wait_task_rescheduled`. (And when counting, keep in mind that
        returning :data:`Abort.SUCCEEDED` from an abort callback is equivalent
        to calling :func:`reschedule` once.)

        Args:
          task (trio.lowlevel.Task): the task to be rescheduled. Must be blocked
              in a call to :func:`wait_task_rescheduled` (or awaiting a
              :class:`WakeToken`).
          next_send (outcome.Outcome): the value (or error) to return (or
              raise) from :func:`wait_task_rescheduled`.

        """
        if next_send is _NO_SEND:
            next_send = Value(None)

        token = task._sleeping_on
        if token is None:
            raise RuntimeError(
                f"{task!r} is not blocked in wait_task_rescheduled "
                "or awaiting a WakeToken",
            )
        # The loop only ever binds a task to a WakeToken (_block_task_on),
        # though the task layer sees it as a CancelTarget.
        assert isinstance(token, WakeToken)
        token.reschedule(next_send)

    def _block_task_on(  # type: ignore[explicit-any]
        self,
        task: Task,
        token: WakeToken[Any, Any],
    ) -> None:
        # The task just yielded this token to the run loop: bind them
        # together, and see whether there's already a cancellation to deliver.
        task._cancel_points += 1
        token._task = task
        task._sleeping_on = token
        task._attempt_delivery_of_any_pending_cancel()

    ################
    # System tasks and init
    ################

    @_public
    def spawn_system_task(
        self,
        async_fn: Callable[[Unpack[PosArgT]], Awaitable[object]],
        *args: Unpack[PosArgT],
        name: object = None,
        context: contextvars.Context | None = None,
    ) -> Task:
        """Spawn a "system" task.

        System tasks have a few differences from regular tasks:

        * They don't need an explicit nursery; instead they go into the
          internal "system nursery".

        * If a system task raises an exception, then it's converted into a
          :exc:`~trio.TrioInternalError` and *all* tasks are cancelled. If you
          write a system task, you should be careful to make sure it doesn't
          crash.

        * System tasks are automatically cancelled when the main task exits.

        * By default, system tasks have :exc:`KeyboardInterrupt` protection
          *enabled*. If you want your task to be interruptible by control-C,
          then you need to use :func:`disable_ki_protection` explicitly (and
          come up with some plan for what to do with a
          :exc:`KeyboardInterrupt`, given that system tasks aren't allowed to
          raise exceptions).

        * System tasks do not inherit context variables from their creator.

        Towards the end of a call to :meth:`trio.run`, after the main
        task and all system tasks have exited, the system nursery
        becomes closed. At this point, new calls to
        :func:`spawn_system_task` will raise ``RuntimeError("Nursery
        is closed to new arrivals")`` instead of creating a system
        task. It's possible to encounter this state either in
        a ``finally`` block in an async generator, or in a callback
        passed to :meth:`TrioToken.run_sync_soon` at the right moment.

        Args:
          async_fn: An async callable.
          args: Positional arguments for ``async_fn``. If you want to pass
              keyword arguments, use :func:`functools.partial`.
          name: The name for this task. Only used for debugging/introspection
              (e.g. ``repr(task_obj)``). If this isn't a string,
              :func:`spawn_system_task` will try to make it one. A common use
              case is if you're wrapping a function before spawning a new
              task, you might pass the original function as the ``name=`` to
              make debugging easier.
          context: An optional ``contextvars.Context`` object with context variables
              to use for this task. You would normally get a copy of the current
              context with ``context = contextvars.copy_context()`` and then you would
              pass that ``context`` object here.

        Returns:
          Task: the newly spawned task

        """
        # The system nursery is created by the init task, which is the first
        # thing to run, so it exists by the time anything can call this.
        assert self.system_nursery is not None
        return self.scheduler.spawn(
            async_fn,
            args,
            self.system_nursery,
            name,
            system_task=True,
            context=context,
        )

    ################
    # Outside context problems
    ################

    @_public
    def current_trio_token(self) -> TrioToken:
        """Retrieve the :class:`TrioToken` for the current call to
        :func:`trio.run`.

        """
        if self.trio_token is None:
            self.trio_token = TrioToken._create(self.entry_queue)
        return self.trio_token

    ################
    # KI handling
    ################

    ki_pending: bool = False

    # deliver_ki is broke. Maybe move all the actual logic and state into
    # RunToken, and we'll only have one instance per runner? But then we can't
    # have a public constructor. Eh, but current_run_token() returning a
    # unique object per run feels pretty nice. Maybe let's just go for it. And
    # keep the class public so people can isinstance() it if they want.

    # This gets called from signal context
    def deliver_ki(self) -> None:
        """Called from the SIGINT handler when it's not safe to raise
        KeyboardInterrupt where the signal landed.

        The main task will be cancelled, and run() will raise
        KeyboardInterrupt once everything has shut down. A second control-C
        while that's in progress has nothing to add, so it's absorbed.
        """
        if self.ki_pending:
            return
        self.ki_pending = True
        # Make sure the run loop wakes up and gets to a safe point soon; the
        # actual cancellation happens there, not in the signal handler.
        with suppress(RunFinishedError):
            self.entry_queue.run_sync_soon(self._cancel_main_task_for_ki)

    def _cancel_main_task_for_ki(self) -> None:
        scope = self.main_task_scope
        if scope is None or scope.cancel_called or self.main_task_outcome is not None:
            # Either the main task doesn't exist yet (the run loop will try
            # again), we've already done this, or the main task has already
            # finished and we'll raise KeyboardInterrupt on the way out of
            # run().
            return
        scope._cancel(CancelReason(source="KeyboardInterrupt"))

    ################
    # Quiescing
    ################

    # sortedcontainers doesn't have types, and is reportedly very hard to type:
    # https://github.com/grantjenks/python-sortedcontainers/issues/68
    waiting_for_idle: Any = attrs.Factory(SortedDict)  # type: ignore[explicit-any]

    @_public
    async def wait_all_tasks_blocked(self, cushion: float = 0.0) -> None:
        """Block until there are no runnable tasks.

        This is useful in testing code when you want to give other tasks a
        chance to "settle down". The calling task is blocked, and doesn't wake
        up until all other tasks are also blocked for at least ``cushion``
        seconds. (Setting a non-zero ``cushion`` is intended to handle cases
        like two tasks talking to each other over a local socket, where we
        want to ignore the potential brief moment between a send and receive
        when all tasks are blocked.)

        Note that ``cushion`` is measured in *real* time, not the Trio clock
        time.

        If there are multiple tasks blocked in :func:`wait_all_tasks_blocked`,
        then the one with the shortest ``cushion`` is the one woken (and
        this task becoming unblocked resets the timers for the remaining
        tasks). If there are multiple tasks that have exactly the same
        ``cushion``, then all are woken.

        You should also consider :class:`trio.testing.Sequencer`, which
        provides a more explicit way to control execution ordering within a
        test, and will often produce more readable tests.

        Example:
          Here's an example of one way to test that Trio's locks are fair: we
          take the lock in the parent, start a child, wait for the child to be
          blocked waiting for the lock (!), and then check that we can't
          release and immediately re-acquire the lock::

             async def lock_taker(lock):
                 await lock.acquire()
                 lock.release()

             async def test_lock_fairness():
                 lock = trio.Lock()
                 await lock.acquire()
                 async with trio.open_nursery() as nursery:
                     nursery.start_soon(lock_taker, lock)
                     # child hasn't run yet, we have the lock
                     assert lock.locked()
                     assert lock._owner is trio.lowlevel.current_task()
                     await trio.testing.wait_all_tasks_blocked()
                     # now the child has run and is blocked on lock.acquire(), we
                     # still have the lock
                     assert lock.locked()
                     assert lock._owner is trio.lowlevel.current_task()
                     lock.release()
                     try:
                         # The child has a prior claim, so we can't have it
                         lock.acquire_nowait()
                     except trio.WouldBlock:
                         assert lock._owner is not trio.lowlevel.current_task()
                         print("PASS")
                     else:
                         print("FAIL")

        """

        def on_cancel(token: WakeToken[None]) -> None:
            del self.waiting_for_idle[key]
            token.reschedule_cancelled()

        token = WakeToken(on_cancel)
        key = (cushion, id(token))
        self.waiting_for_idle[key] = token
        await token


################################################################
# run
################################################################
#
# Trio's core task scheduler and coroutine runner is in 'unrolled_run'. It's
# called that because it has an unusual feature: it's actually a generator.
# Whenever it needs to fetch IO events from the OS, it yields, and waits for
# its caller to send the IO events back in. So the loop is "unrolled" into a
# sequence of generator send() calls.
#
# The reason for this unusual design is to support two different modes of
# operation, where the IO is handled differently.
#
# In normal mode using trio.run, the scheduler and IO run in the same thread:
#
# Main thread:
#
# +---------------------------+
# | Run tasks                 |
# | (unrolled_run)            |
# +---------------------------+
# | Block waiting for I/O     |
# | (io_manager.get_events)   |
# +---------------------------+
# | Run tasks                 |
# | (unrolled_run)            |
# +---------------------------+
# | Block waiting for I/O     |
# | (io_manager.get_events)   |
# +---------------------------+
# :
#
#
# In guest mode using trio.lowlevel.start_guest_run, the scheduler runs on the
# main thread as a host loop callback, but blocking for IO gets pushed into a
# worker thread:
#
# Main thread executing host loop:           Trio I/O thread:
#
# +---------------------------+
# | Run Trio tasks            |
# | (unrolled_run)            |
# +---------------------------+ --------------+
#                                             v
# +---------------------------+              +----------------------------+
# | Host loop does whatever   |              | Block waiting for Trio I/O |
# | it wants                  |              | (io_manager.get_events)    |
# +---------------------------+              +----------------------------+
#                                             |
# +---------------------------+ <-------------+
# | Run Trio tasks            |
# | (unrolled_run)            |
# +---------------------------+ --------------+
#                                             v
# +---------------------------+              +----------------------------+
# | Host loop does whatever   |              | Block waiting for Trio I/O |
# | it wants                  |              | (io_manager.get_events)    |
# +---------------------------+              +----------------------------+
# :                                            :
#
# Most of Trio's internals don't need to care about this difference. The main
# complication it creates is that in guest mode, we might need to wake up not
# just due to OS-reported IO events, but also because of code running on the
# host loop calling reschedule() or changing task deadlines. That's handled by
# the LoopWaker (see _task.py) that start_guest_run installs on the run queue
# and on the deadlines, which is Runner.wake.


def setup_runner(
    clock: Clock | None,
    instruments: Sequence[Instrument],
    restrict_keyboard_interrupt_to_checkpoints: bool,
    strict_exception_groups: bool,
) -> Runner:
    """Create a Runner object and install it as the GLOBAL_RUN_CONTEXT."""
    # It wouldn't be *hard* to support nested calls to run(), but I can't
    # think of a single good reason for it, so let's be conservative for
    # now:
    if in_trio_run():
        raise RuntimeError("Attempted to call run() from inside a run()")

    if clock is None:
        clock = SystemClock()
    instrument_group = Instruments(instruments)
    io_manager = TheIOManager()
    ki_manager = KIManager()

    scheduler = Scheduler(
        clock=clock,
        deadlines=Deadlines(),
        instruments=instrument_group,
        system_context=copy_context(),
    )
    runner = Runner(
        clock=clock,
        instruments=instrument_group,
        io_manager=io_manager,
        ki_manager=ki_manager,
        strict_exception_groups=strict_exception_groups,
        scheduler=scheduler,
    )
    runner.asyncgens.install_hooks(runner)

    # This is where KI protection gets enabled, so we want to do it early - in
    # particular before we start modifying global state like GLOBAL_RUN_CONTEXT
    ki_manager.install(runner.deliver_ki, restrict_keyboard_interrupt_to_checkpoints)

    GLOBAL_RUN_CONTEXT.runner = runner
    # The services the modules below us reach through the context (see
    # _run_context.py): the scheduler, and the run-local values of _local.py.
    GLOBAL_RUN_CONTEXT.scheduler = scheduler
    GLOBAL_RUN_CONTEXT.run_locals = {}
    return runner


# 24 hours is arbitrary, but it avoids issues like people setting timeouts of
# 10**20 and then getting integer overflows in the underlying system calls.
_MAX_TIMEOUT: Final = 24 * 60 * 60


# Weird quirk: this is written as a generator in order to support "guest
# mode", where our core event loop gets unrolled into a series of callbacks on
# the host loop. If you're doing a regular trio.run then this gets run
# straight through.
@enable_ki_protection
def unrolled_run(
    runner: Runner,
    init_fn: Callable[[Unpack[PosArgT]], Awaitable[object]],
    init_args: tuple[Unpack[PosArgT]],
    host_uses_signal_set_wakeup_fd: bool = False,
) -> Generator[float, EventResult, None]:
    # ``init_fn(*init_args)`` becomes the "<init>" task, the root of the task
    # tree; it's the caller's job (see _run.init) to set up the system and
    # main-task nurseries in there.
    __tracebackhide__ = True
    scheduler = runner.scheduler

    try:
        if not host_uses_signal_set_wakeup_fd:
            runner.entry_queue.wakeup.wakeup_on_signals()

        if "before_run" in runner.instruments:
            runner.instruments.call("before_run")
        runner.clock.start_clock()
        # The runner is the root task's owner, so this sets runner.init_task.
        scheduler.spawn(
            init_fn,
            init_args,
            runner,
            "<init>",
            system_task=True,
        )

        # You know how people talk about "event loops"? This 'while' loop right
        # here is our event loop:
        while scheduler.tasks:
            if scheduler.run_queue:
                timeout: float = 0
            else:
                deadline = scheduler.deadlines.next_deadline()
                timeout = runner.clock.deadline_to_sleep_time(deadline)
            timeout = min(max(0, timeout), _MAX_TIMEOUT)

            idle_primed = None
            if runner.waiting_for_idle:
                cushion, _ = runner.waiting_for_idle.keys()[0]
                if cushion < timeout:
                    timeout = cushion
                    idle_primed = IdlePrimedTypes.WAITING_FOR_IDLE
            # We use 'elif' here because if there are tasks in
            # wait_all_tasks_blocked, then those tasks will wake up without
            # jumping the clock, so we don't need to autojump.
            elif scheduler.clock_autojump_threshold < timeout:
                timeout = scheduler.clock_autojump_threshold
                idle_primed = IdlePrimedTypes.AUTOJUMP_CLOCK

            if "before_io_wait" in runner.instruments:
                runner.instruments.call("before_io_wait", timeout)

            # Driver will call io_manager.get_events(timeout) and pass it back
            # in through the yield
            events = yield timeout
            runner.io_manager.process_events(events)

            if "after_io_wait" in runner.instruments:
                runner.instruments.call("after_io_wait", timeout)

            # Process cancellations due to deadline expiry
            now = runner.clock.current_time()
            if scheduler.deadlines.expire(now):
                idle_primed = None

            # idle_primed != None means: if the IO wait hit the timeout, and
            # still nothing is happening, then we should start waking up
            # wait_all_tasks_blocked tasks or autojump the clock. But there
            # are some subtleties in defining "nothing is happening".
            #
            # 'not scheduler.run_queue' means that no tasks are currently
            # runnable. 'not events' means that the last IO wait call hit its
            # full timeout. These are very similar, and if idle_primed != None
            # and we're running in regular mode then they always go together.
            # But, in *guest* mode, they can happen independently, even when
            # idle_primed=True:
            #
            # - run_queue=empty and events=True: the host loop adjusted a
            #   deadline and that forced an IO wakeup before the timeout expired,
            #   even though no actual tasks were scheduled.
            #
            # - run_queue=nonempty and events=False: the IO wait hit its
            #   timeout, but then some code in the host thread rescheduled a task
            #   before we got here.
            #
            # So we need to check both.
            if idle_primed is not None and not scheduler.run_queue and not events:
                if idle_primed is IdlePrimedTypes.WAITING_FOR_IDLE:
                    while runner.waiting_for_idle:
                        key, token = runner.waiting_for_idle.peekitem(0)
                        if key[0] == cushion:
                            del runner.waiting_for_idle[key]
                            token.reschedule()
                        else:
                            break
                else:
                    assert idle_primed is IdlePrimedTypes.AUTOJUMP_CLOCK
                    assert isinstance(runner.clock, MockClock)
                    runner.clock._autojump()

            # A deferred control-C: cancel the main task (once it exists) here,
            # at a safe point, rather than from inside the signal handler.
            if runner.ki_pending:
                runner._cancel_main_task_for_ki()

            # Process all runnable tasks, but only the ones that are already
            # runnable now. Anything that becomes runnable during this cycle
            # needs to wait until the next pass. This avoids various
            # starvation issues by ensuring that there's never an unbounded
            # delay between successive checks for I/O.
            #
            # Also, we randomize the order of each batch to avoid assumptions
            # about scheduling order sneaking in. In the long run, I suspect
            # we'll either (a) use strict FIFO ordering and document that for
            # predictability/determinism, or (b) implement a more
            # sophisticated scheduler (e.g. some variant of fair queueing),
            # for better behavior under load. For now, this is the worst of
            # both worlds - but it keeps our options open. (If we do decide to
            # go all in on deterministic scheduling, then there are other
            # things that will probably need to change too, like the deadlines
            # tie-breaker and the non-deterministic ordering of
            # task._notify_queues.)
            batch = list(scheduler.run_queue)
            scheduler.run_queue.clear()
            if _ALLOW_DETERMINISTIC_SCHEDULING:
                # We're running under Hypothesis, and pytest-trio has patched
                # this in to make the scheduler deterministic and avoid flaky
                # tests. It's not worth the (small) performance cost in normal
                # operation, since we'll shuffle the list and _r is only
                # seeded for tests.
                batch.sort(key=lambda t: t._counter)
                _r.shuffle(batch)
            else:
                # 50% chance of reversing the batch, this way each task
                # can appear before/after any other task.
                if _r.random() < 0.5:
                    batch.reverse()
            while batch:
                task = batch.pop()
                GLOBAL_RUN_CONTEXT.task = task

                if "before_task_step" in runner.instruments:
                    runner.instruments.call("before_task_step", task)

                next_send_fn = task._next_send_fn
                next_send = task._next_send
                task._next_send_fn = task._next_send = None
                final_outcome: Outcome[object] | None = None

                assert next_send_fn is not None

                try:
                    # We used to unwrap the Outcome object here and send/throw
                    # its contents in directly, but it turns out that .throw()
                    # is buggy on CPython (all versions at time of writing):
                    #   https://bugs.python.org/issue29587
                    #   https://bugs.python.org/issue29590
                    #   https://bugs.python.org/issue40694
                    #   https://github.com/python/cpython/issues/108668
                    # So now we send in the Outcome object and unwrap it on the
                    # other side.
                    msg = task.context.run(next_send_fn, next_send)
                except StopIteration as stop_iteration:
                    final_outcome = Value(stop_iteration.value)
                except BaseException as task_exc:
                    # Store for later, removing uninteresting top frames: 1
                    # frame we always remove, because it's this function
                    # catching it, and then in addition we remove however many
                    # more Context.run adds.
                    tb = task_exc.__traceback__
                    for _ in range(1 + CONTEXT_RUN_TB_FRAMES):
                        if tb is not None:  # pragma: no branch
                            tb = tb.tb_next
                    final_outcome = Error(task_exc.with_traceback(tb))
                    # Remove local refs so that e.g. cancelled coroutine locals
                    # are not kept alive by this frame until another exception
                    # comes along.
                    del tb

                if final_outcome is not None:
                    # We can't call this directly inside the except: blocks
                    # above, because then the exceptions end up attaching
                    # themselves to other exceptions as __context__ in
                    # unwanted ways.
                    scheduler.task_exited(task, final_outcome)
                    # final_outcome may contain a traceback ref. It's not as
                    # crucial compared to the above, but this will allow more
                    # prompt release of resources in coroutine locals.
                    final_outcome = None
                else:
                    task._schedule_points += 1
                    if msg is CancelShieldedCheckpoint:
                        scheduler.enqueue(task, Value(None))
                    elif type(msg) is WakeToken:
                        runner._block_task_on(task, msg)
                    elif type(msg) is PermanentlyDetachCoroutineObject:
                        # Pretend the task just exited with the given outcome
                        scheduler.task_exited(task, msg.final_outcome)
                    else:
                        exc = TypeError(
                            f"trio.run received unrecognized yield message {msg!r}. "
                            "Are you trying to use a library written for some "
                            "other framework like asyncio? That won't work "
                            "without some kind of compatibility shim.",
                        )
                        # The foreign library probably doesn't adhere to our
                        # protocol of unwrapping whatever outcome gets sent in.
                        # Instead, we'll arrange to throw `exc` in directly,
                        # which works for at least asyncio and curio.
                        scheduler.enqueue(task, exc)
                        task._next_send_fn = task.coro.throw
                    # prevent long-lived reference
                    # TODO: develop test for this deletion
                    del msg

                if "after_task_step" in runner.instruments:
                    runner.instruments.call("after_task_step", task)
                del GLOBAL_RUN_CONTEXT.task
                # prevent long-lived references
                # TODO: develop test for this deletion
                del task, next_send, next_send_fn

    except GeneratorExit:
        # The run-loop generator has been garbage collected without finishing
        warnings.warn(
            RuntimeWarning(
                "Trio guest run got abandoned without properly finishing... "
                "weird stuff might happen",
            ),
            stacklevel=1,
        )
    except TrioInternalError:
        raise
    except BaseException as exc:
        raise TrioInternalError("internal error in Trio - please file a bug!") from exc
    finally:
        runner.close()
        GLOBAL_RUN_CONTEXT.__dict__.clear()

        # Have to do this after runner.close() has disabled KI protection,
        # because otherwise there's a race where ki_pending could get set
        # after we check it.
        if runner.ki_pending:
            ki = KeyboardInterrupt()
            if isinstance(runner.main_task_outcome, Error) and not _is_ki_cancellation(
                runner.main_task_outcome.error
            ):
                ki.__context__ = runner.main_task_outcome.error
            runner.main_task_outcome = Error(ki)


def _is_ki_cancellation(exc: BaseException) -> bool:
    # Is this just the Cancelled (or group of them) that we raised in the main
    # task on the user's behalf after a control-C? Chaining that onto the
    # KeyboardInterrupt would only be noise.
    if isinstance(exc, BaseExceptionGroup):
        return all(_is_ki_cancellation(e) for e in exc.exceptions)
    return isinstance(exc, Cancelled) and exc.source == "KeyboardInterrupt"


def in_trio_run() -> bool:
    """Check whether we are in a Trio run.
    This returns `True` if and only if :func:`~trio.current_time` will succeed.

    See also the discussion of differing ways of :ref:`detecting Trio <trio_contexts>`.
    """
    return hasattr(GLOBAL_RUN_CONTEXT, "runner")


# The platform's I/O backend: its manager, and the public wrappers that
# gen_exports generates for the manager's methods, which _core/__init__.py
# takes from here. (The wrappers for the runner's own methods are imported
# there from _generated_runner directly: they sit above this module, since
# they wrap it.) Under autodoc, every platform's wrappers are exported.
if "sphinx.ext.autodoc" in sys.modules:
    from ._generated_io_epoll import *
    from ._generated_io_kqueue import *
    from ._generated_io_windows import *

if sys.platform == "win32":
    from ._generated_io_windows import *
    from ._io_windows import (
        EventResult as EventResult,
        WindowsIOManager as TheIOManager,
        _WindowsStatistics as IOStatistics,
    )
elif (
    sys.platform == "linux"
    or sys.platform == "android"
    or (not TYPE_CHECKING and hasattr(select, "epoll"))
):
    from ._generated_io_epoll import *
    from ._io_epoll import (
        EpollIOManager as TheIOManager,
        EventResult as EventResult,
        _EpollStatistics as IOStatistics,
    )
elif TYPE_CHECKING or hasattr(select, "kqueue"):
    from ._generated_io_kqueue import *
    from ._io_kqueue import (
        EventResult as EventResult,
        KqueueIOManager as TheIOManager,
        _KqueueStatistics as IOStatistics,
    )
else:  # pragma: no cover
    _patchers = sorted({"eventlet", "gevent"}.intersection(sys.modules))
    if _patchers:
        raise NotImplementedError(
            "unsupported platform or primitives Trio depends on are monkey-patched out by "
            + ", ".join(_patchers),
        )

    raise NotImplementedError("unsupported platform")
