from __future__ import annotations

import time
from math import inf
from typing import TYPE_CHECKING, Any

from sortedcontainers import SortedDict

from .._abc import Clock
from .._util import final
from ._ki import enable_ki_protection
from ._run_context import current_task
from ._traps import Abort, wait_task_rescheduled

if TYPE_CHECKING:
    from collections.abc import Callable

    from ._run import Task
    from ._traps import RaiseCancelT

################################################################
# The glorious TestingClock
################################################################


# Prior art:
#   https://twistedmatrix.com/documents/current/api/twisted.internet.task.Clock.html
#   https://github.com/ztellman/manifold/issues/57
@final
class TestingClock(Clock):
    """A user-controllable clock suitable for writing tests.

    Args:
      rate (float): the initial :attr:`rate`.
      autojump_threshold (float): the initial :attr:`autojump_threshold`.

    .. attribute:: rate

       How many seconds of clock time pass per second of real time. Default is
       0.0, i.e. the clock only advances through manuals calls to :meth:`jump`
       or when the :attr:`autojump_threshold` is triggered. You can assign to
       this attribute to change it.

    .. attribute:: autojump_threshold

       The clock keeps an eye on the run loop, and if at any point it detects
       that all tasks have been blocked for this many real seconds (i.e.,
       according to the actual clock, not this clock), then the clock
       automatically jumps ahead to the run loop's next scheduled
       timeout. Default is :data:`math.inf`, i.e., to never autojump. You can
       assign to this attribute to change it.

       Basically the idea is that if you have code or tests that use sleeps
       and timeouts, you can use this to make it run much faster, totally
       automatically. (At least, as long as those sleeps/timeouts are
       happening inside Trio; if your test involves talking to external
       service and waiting for it to timeout then obviously we can't help you
       there.)

       You should set this to the smallest value that lets you reliably avoid
       "false alarms" where some I/O is in flight (e.g. between two halves of
       a socketpair) but the threshold gets triggered and time gets advanced
       anyway. This will depend on the details of your tests and test
       environment. If you aren't doing any I/O (like in our sleeping example
       above) then just set it to zero, and the clock will jump whenever all
       tasks are blocked.

       .. note:: If you use ``autojump_threshold`` and
          `wait_all_tasks_blocked` at the same time, then you might wonder how
          they interact, since they both cause things to happen after the run
          loop goes idle for some time. The answer is:
          `wait_all_tasks_blocked` takes priority. If there's a task blocked
          in `wait_all_tasks_blocked`, then the autojump feature treats that
          as active task and does *not* jump the clock.

    """

    # The name matches pytest's `Test*` collection pattern, but this is a
    # clock, not a test suite.
    __test__ = False

    def __init__(self, rate: float = 0.0, autojump_threshold: float = inf) -> None:
        # when the real clock said 'real_base', the virtual time was
        # 'virtual_base', and since then it's advanced at 'rate' virtual
        # seconds per real second.
        self._real_base = 0.0
        self._virtual_base = 0.0
        self._rate = 0.0

        # kept as an attribute so that our tests can monkeypatch it
        self._real_clock = time.perf_counter

        # Tasks parked in wait_all_tasks_blocked(), keyed by (cushion, id).
        # sortedcontainers doesn't have types, and is reportedly very hard
        # to type: https://github.com/grantjenks/python-sortedcontainers/issues/68
        self._idle_waiters: Any = SortedDict()  # type: ignore[explicit-any]
        self._idle_primed = False

        # use the property update logic to set initial values
        self.rate = rate
        self.autojump_threshold = autojump_threshold

    def __repr__(self) -> str:
        return f"<TestingClock, time={self.current_time():.7f}, rate={self._rate} @ {id(self):#x}>"

    @property
    def rate(self) -> float:
        return self._rate

    @rate.setter
    def rate(self, new_rate: float) -> None:
        if new_rate < 0:
            raise ValueError("rate must be >= 0")
        else:
            real = self._real_clock()
            virtual = self._real_to_virtual(real)
            self._virtual_base = virtual
            self._real_base = real
            self._rate = float(new_rate)

    @property
    def autojump_threshold(self) -> float:
        return self._autojump_threshold

    @autojump_threshold.setter
    def autojump_threshold(self, new_autojump_threshold: float) -> None:
        self._autojump_threshold = float(new_autojump_threshold)

    ################################################################
    # Idleness: wait_all_tasks_blocked, and autojumping
    ################################################################

    @enable_ki_protection
    async def wait_all_tasks_blocked(self, cushion: float = 0.0) -> None:
        """Block until there are no runnable tasks.

        See :func:`trio.testing.wait_all_tasks_blocked`, which is the way to
        call this.

        """
        task = current_task()
        key = (cushion, id(task))
        self._idle_waiters[key] = task

        def abort(_: RaiseCancelT) -> Abort:
            del self._idle_waiters[key]
            return Abort.SUCCEEDED

        return await wait_task_rescheduled(abort)  # type: ignore[no-any-return]

    def _idle_bound(self) -> float:
        # A pending wait_all_tasks_blocked waiter outranks autojumping: it
        # will wake without virtual time having to move, so we must not skip
        # past it. Both quantities live here, so we can simply choose.
        if self._idle_waiters:
            cushion: float = self._idle_waiters.keys()[0][0]
            return cushion
        return self._autojump_threshold

    def before_io_wait(
        self,
        *,
        relative_deadline: float,
        anything_runnable: bool,
    ) -> float:
        if anything_runnable or relative_deadline <= 0:
            # The loop is only polling (or a deadline is already due):
            # whatever ends that wait, it will not be an idle stretch.
            self._idle_primed = False
            return 0
        # The real seconds we would sleep if we were not watching for an
        # idle stretch. Our time is frozen at rate 0, so then no amount of
        # real waiting reaches the deadline.
        natural = relative_deadline / self._rate if self._rate > 0 else inf
        bound = self._idle_bound()
        # Both sides are real seconds, the unit the threshold and cushions
        # are quoted in; inf ("never autojump", the default) then simply
        # fails to be less than anything, itself included.
        self._idle_primed = bound < natural
        return bound if self._idle_primed else natural

    def after_io_wait(
        self,
        *,
        saw_events: bool,
        anything_runnable: bool,
        deadline_expired: bool,
        relative_deadline: float,
        reschedule: Callable[[Task], None],
    ) -> None:
        primed, self._idle_primed = self._idle_primed, False
        if not primed or saw_events or anything_runnable or deadline_expired:
            return

        # The run is idle. Waiters first, for the reason in _idle_bound.
        if self._idle_waiters:
            cushion = self._idle_waiters.keys()[0][0]
            while self._idle_waiters:
                key, task = self._idle_waiters.peekitem(0)
                if key[0] != cushion:
                    break
                del self._idle_waiters[key]
                reschedule(task)
            return

        if 0 < relative_deadline < inf:
            self.jump(relative_deadline)

    def _real_to_virtual(self, real: float) -> float:
        real_offset = real - self._real_base
        virtual_offset = self._rate * real_offset
        return self._virtual_base + virtual_offset

    def start_clock(self) -> None:
        # A clock object can be reused across sequential runs (its virtual
        # time deliberately carries over), but idle-waiter state from a
        # previous run -- possibly one that crashed or was interrupted while
        # tasks were parked -- must not leak into this one.
        self._idle_waiters.clear()
        self._idle_primed = False

    def current_time(self) -> float:
        return self._real_to_virtual(self._real_clock())

    def jump(self, seconds: float) -> None:
        """Manually advance the clock by the given number of seconds.

        Args:
          seconds (float): the number of seconds to jump the clock forward.

        Raises:
          ValueError: if you try to pass a negative value for ``seconds``.

        """
        if seconds < 0:
            raise ValueError("time can't go backwards")
        self._virtual_base += seconds
