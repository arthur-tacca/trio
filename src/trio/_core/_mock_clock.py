import time
from math import inf

from .._abc import Clock
from .._util import final

################################################################
# The glorious MockClock
################################################################


# Prior art:
#   https://twistedmatrix.com/documents/current/api/twisted.internet.task.Clock.html
#   https://github.com/ztellman/manifold/issues/57
@final
class MockClock(Clock):
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

    def __init__(self, rate: float = 0.0, autojump_threshold: float = inf) -> None:
        # when the real clock said 'real_base', the virtual time was
        # 'virtual_base', and since then it's advanced at 'rate' virtual
        # seconds per real second.
        self._real_base = 0.0
        self._virtual_base = 0.0
        self._rate = 0.0
        # The deadline stashed by a conversion that shortened its answer to
        # autojump_threshold so the run loop would report back; consumed
        # (and always cleared) by wait_has_ended once it does.
        self._jump_to: float | None = None

        # kept as an attribute so that our tests can monkeypatch it
        self._real_clock = time.perf_counter

        # use the property update logic to set initial values
        self.rate = rate
        self.autojump_threshold = autojump_threshold

    def __repr__(self) -> str:
        return f"<MockClock, time={self.current_time():.7f}, rate={self._rate} @ {id(self):#x}>"

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
        # The threshold is read fresh at every conversion, so a change takes
        # effect on the run loop's next pass.
        self._autojump_threshold = float(new_autojump_threshold)

    def _real_to_virtual(self, real: float) -> float:
        real_offset = real - self._real_base
        virtual_offset = self._rate * real_offset
        return self._virtual_base + virtual_offset

    def start_clock(self) -> None:
        # The stash is run-scoped: a run that tore down between priming a
        # jump and hearing the wait's outcome must not leak it into a new
        # run using the same clock.
        self._jump_to = None

    def current_time(self) -> float:
        return self._real_to_virtual(self._real_clock())

    def deadline_to_sleep_time(
        self,
        deadline: float,
        *,
        have_idle_waiters: bool,
    ) -> float:
        virtual_timeout = deadline - self.current_time()
        if virtual_timeout <= 0:
            return 0
        # The real seconds we would sleep if we were not watching for an
        # idle stretch. Our time is frozen at rate 0, so then no amount of
        # real waiting reaches the deadline.
        natural = virtual_timeout / self._rate if self._rate > 0 else inf
        if (
            # Both sides are real seconds, which is the unit the threshold
            # is quoted in; inf ("never autojump", the default) then simply
            # fails to be less than anything.
            self.autojump_threshold < natural
            # Tasks in wait_all_tasks_blocked wake after an idle stretch
            # without our time moving at all, so while any are pending we
            # decline to set up a jump that would skip past them.
            and not have_idle_waiters
        ):
            # Deliberately answer less than the truth: if the run then sits
            # idle for this whole shortened timeout, wait_has_ended jumps us
            # onto the deadline.
            self._jump_to = deadline
            return self.autojump_threshold
        return natural

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

    def wait_has_ended(
        self,
        *,
        saw_events: bool,
        anything_runnable: bool,
    ) -> None:
        # A jump needs two facts with different owners: ours, that the
        # conversion shortened its answer on purpose (the stash); and the
        # run loop's, that the wait produced nothing. An unprimed wait must
        # never jump, however idle - a natural sleep that reaches its
        # deadline is not an observation window.
        jump_to, self._jump_to = self._jump_to, None
        if jump_to is None or saw_events or anything_runnable:
            return
        jump = jump_to - self.current_time()
        if 0 < jump < inf:
            self.jump(jump)
