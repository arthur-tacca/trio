import time
from math import inf

import pytest

from trio import move_on_after, sleep
from trio.to_thread import run_sync as to_thread_run_sync

from ... import _core
from ..._abc import Clock
from .. import wait_all_tasks_blocked
from .._testing_clock import TestingClock
from .tutil import slow


def test_testing_clock() -> None:
    REAL_NOW = 123.0
    c = TestingClock()
    c._real_clock = lambda: REAL_NOW
    repr(c)  # smoke test
    assert c.rate == 0
    assert c.current_time() == 0
    c.jump(1.2)
    assert c.current_time() == 1.2
    with pytest.raises(ValueError, match=r"^time can't go backwards$"):
        c.jump(-1)
    assert c.current_time() == 1.2
    # relative deadlines, i.e. absolute 1.1 / 1.2 / 1.3 with now == 1.2
    assert c.before_io_wait(relative_deadline=-0.1, anything_runnable=False) == 0
    assert c.before_io_wait(relative_deadline=0, anything_runnable=False) == 0
    assert c.before_io_wait(relative_deadline=0.1, anything_runnable=False) == inf

    with pytest.raises(ValueError, match=r"^rate must be >= 0$"):
        c.rate = -1
    assert c.rate == 0

    c.rate = 2
    assert c.current_time() == 1.2
    REAL_NOW += 1
    assert c.current_time() == 3.2
    # absolute 3.1 / 3.2 / 4.2 with now == 3.2, at rate 2
    assert c.before_io_wait(relative_deadline=-0.1, anything_runnable=False) == 0
    assert c.before_io_wait(relative_deadline=0, anything_runnable=False) == 0
    assert c.before_io_wait(relative_deadline=1.0, anything_runnable=False) == 0.5

    c.rate = 0.5
    assert c.current_time() == 3.2
    # same, at rate 0.5
    assert c.before_io_wait(relative_deadline=-0.1, anything_runnable=False) == 0
    assert c.before_io_wait(relative_deadline=0, anything_runnable=False) == 0
    assert c.before_io_wait(relative_deadline=1.0, anything_runnable=False) == 2.0

    c.jump(0.8)
    assert c.current_time() == 4.0
    REAL_NOW += 1
    assert c.current_time() == 4.5

    c2 = TestingClock(rate=3)
    assert c2.rate == 3
    assert c2.current_time() < 10


async def test_testing_clock_autojump(testing_clock: TestingClock) -> None:
    assert testing_clock.autojump_threshold == inf

    testing_clock.autojump_threshold = 0
    assert testing_clock.autojump_threshold == 0

    real_start = time.perf_counter()

    virtual_start = _core.current_time()
    for i in range(10):
        print(f"sleeping {10 * i} seconds")
        await sleep(10 * i)
        print("woke up!")
        assert virtual_start + 10 * i == _core.current_time()
        virtual_start = _core.current_time()

    real_duration = time.perf_counter() - real_start
    print(f"Slept {10 * sum(range(10))} seconds in {real_duration} seconds")
    assert real_duration < 1

    testing_clock.autojump_threshold = 0.02
    t = _core.current_time()
    # this should wake up before the autojump threshold triggers, so time
    # shouldn't change
    await wait_all_tasks_blocked()
    assert t == _core.current_time()
    # this should too
    await wait_all_tasks_blocked(0.01)
    assert t == _core.current_time()

    # set up a situation where the autojump task is blocked for a long long
    # time, to make sure that cancel-and-adjust-threshold logic is working
    testing_clock.autojump_threshold = 10000
    await wait_all_tasks_blocked()
    testing_clock.autojump_threshold = 0
    # if the above line didn't take affect immediately, then this would be
    # bad:
    # ignore ASYNC116, not sleep_forever, trying to test a large but finite sleep
    await sleep(100000)  # noqa: ASYNC116


async def test_testing_clock_autojump_interference(testing_clock: TestingClock) -> None:
    testing_clock.autojump_threshold = 0.02

    testing_clock2 = TestingClock()
    # messing with the autojump threshold of a clock that isn't actually
    # installed in the run loop shouldn't do anything.
    testing_clock2.autojump_threshold = 0.01

    # if the autojump_threshold of 0.01 were in effect, then the next line
    # would block forever, as the autojump task kept waking up to try to
    # jump the clock.
    await wait_all_tasks_blocked(0.015)

    # but the 0.02 limit does apply
    # ignore ASYNC116, not sleep_forever, trying to test a large but finite sleep
    await sleep(100000)  # noqa: ASYNC116


def test_testing_clock_autojump_preset() -> None:
    # Check that we can set the autojump_threshold before the clock is
    # actually in use, and it gets picked up
    testing_clock = TestingClock(autojump_threshold=0.1)
    testing_clock.autojump_threshold = 0.01
    real_start = time.perf_counter()
    _core.run(sleep, 10000, clock=testing_clock)
    assert time.perf_counter() - real_start < 1


async def test_testing_clock_autojump_0_and_wait_all_tasks_blocked_0(
    testing_clock: TestingClock,
) -> None:
    # Checks that autojump_threshold=0 doesn't interfere with
    # calling wait_all_tasks_blocked with the default cushion=0.

    testing_clock.autojump_threshold = 0

    record = []

    async def sleeper() -> None:
        await sleep(100)
        record.append("yawn")

    async def waiter() -> None:
        await wait_all_tasks_blocked()
        record.append("waiter woke")
        await sleep(1000)
        record.append("waiter done")

    async with _core.open_nursery() as nursery:
        nursery.start_soon(sleeper)
        nursery.start_soon(waiter)

    assert record == ["waiter woke", "yawn", "waiter done"]


@slow
async def test_testing_clock_autojump_0_and_wait_all_tasks_blocked_nonzero(
    testing_clock: TestingClock,
) -> None:
    # Checks that autojump_threshold=0 doesn't interfere with
    # calling wait_all_tasks_blocked with a non-zero cushion.

    testing_clock.autojump_threshold = 0

    record = []

    async def sleeper() -> None:
        await sleep(100)
        record.append("yawn")

    async def waiter() -> None:
        await wait_all_tasks_blocked(1)
        record.append("waiter done")

    async with _core.open_nursery() as nursery:
        nursery.start_soon(sleeper)
        nursery.start_soon(waiter)

    assert record == ["waiter done", "yawn"]


def test_before_after_io_wait_direct() -> None:
    # The two hooks the run loop drives the clock through, exercised with no
    # run at all.
    def idle(c: TestingClock, relative_deadline: float) -> None:
        c.after_io_wait(
            saw_events=False,
            anything_runnable=False,
            deadline_expired=False,
            relative_deadline=relative_deadline,
            reschedule=lambda task: pytest.fail("nothing should be woken"),
        )

    def primed_answer(c: TestingClock, relative_deadline: float) -> float:
        return c.before_io_wait(
            relative_deadline=relative_deadline,
            anything_runnable=False,
        )

    # A running clock declines to shorten a sleep that is already shorter
    # than the threshold: the deadline arrives naturally first.
    REAL_NOW = 123.0
    r = TestingClock(autojump_threshold=1.0)
    r._real_clock = lambda: REAL_NOW
    r.rate = 1
    assert primed_answer(r, 0.5) == 0.5
    idle(r, 0.5)
    assert r.current_time() == 0  # not primed, so no jump

    c = TestingClock(autojump_threshold=1.0)
    assert primed_answer(c, 10.0) == 1.0  # shortened to the threshold
    idle(c, 10.0)
    assert c.current_time() == 10.0  # jumped onto the deadline 10s ahead

    assert primed_answer(c, inf) == 1.0
    idle(c, inf)  # nothing scheduled: nowhere to go
    assert c.current_time() == 10.0

    # tasks are runnable: the loop is only polling, so no watching either
    assert c.before_io_wait(relative_deadline=10.0, anything_runnable=True) == 0
    idle(c, 10.0)  # ...and an idle-looking report cannot jump
    assert c.current_time() == 10.0

    # a deadline due (or overdue): poll, and again no watching
    assert primed_answer(c, 0) == 0
    idle(c, 10.0)
    assert c.current_time() == 10.0
    assert primed_answer(c, -0.1) == 0
    idle(c, 10.0)
    assert c.current_time() == 10.0

    # primed, but the deadline moved behind us by wake time: time can't go
    # backwards, and due-right-now means nothing to skip
    assert primed_answer(c, 99.0) == 1.0
    idle(c, -0.1)
    assert c.current_time() == 10.0
    assert primed_answer(c, 99.0) == 1.0
    idle(c, 0)
    assert c.current_time() == 10.0

    # anything happening at all cancels the jump...
    assert primed_answer(c, 99.0) == 1.0
    c.after_io_wait(
        saw_events=True,
        anything_runnable=False,
        deadline_expired=False,
        relative_deadline=20.0,
        reschedule=lambda task: pytest.fail("nothing should be woken"),
    )
    assert c.current_time() == 10.0
    # ...and the priming does not leak into a later idle-looking report
    idle(c, 20.0)
    assert c.current_time() == 10.0

    # the default threshold is inf, meaning never watch and never jump: it
    # simply fails to be less than anything, itself included
    c2 = TestingClock()
    assert primed_answer(c2, 10.0) == inf
    idle(c2, 10.0)
    assert c2.current_time() == 0


def test_start_clock_resets_idle_state() -> None:
    # The primed flag is run-scoped: a run that tore down between the two
    # hooks must not leak it into a new run using the same clock.
    c = TestingClock(autojump_threshold=1.0)
    assert c.before_io_wait(relative_deadline=10.0, anything_runnable=False) == 1.0
    assert c._idle_primed
    c.start_clock()
    assert not c._idle_primed


def test_custom_clock_wrapping_testing_clock() -> None:
    # https://github.com/python-trio/trio/issues/3369 -- delegating to a
    # TestingClock through nothing but the abc.Clock surface must behave
    # identically to passing the TestingClock itself.
    class WrapperClock(Clock):
        def __init__(self) -> None:
            self._mock = TestingClock(autojump_threshold=0)

        def start_clock(self) -> None:
            self._mock.start_clock()

        def current_time(self) -> float:
            return self._mock.current_time()

        def before_io_wait(
            self,
            *,
            relative_deadline: float,
            anything_runnable: bool,
        ) -> float:
            return self._mock.before_io_wait(
                relative_deadline=relative_deadline,
                anything_runnable=anything_runnable,
            )

        def after_io_wait(self, **kwargs: object) -> None:
            self._mock.after_io_wait(**kwargs)  # type: ignore[arg-type]

        async def wait_all_tasks_blocked(self, cushion: float = 0.0) -> None:
            await self._mock.wait_all_tasks_blocked(cushion)

    record: list[str] = []

    async def sleeper() -> None:
        await sleep(100)
        record.append("yawn")

    async def waiter() -> None:
        await wait_all_tasks_blocked()
        record.append("waiter woke")

    async def main() -> None:
        assert _core.current_time() == 0
        await sleep(2)
        assert _core.current_time() == 2
        # wait_all_tasks_blocked still takes priority over the clock
        async with _core.open_nursery() as nursery:
            nursery.start_soon(sleeper)
            nursery.start_soon(waiter)

    _core.run(main, clock=WrapperClock())
    assert record == ["waiter woke", "yawn"]


def test_custom_clock_from_scratch() -> None:
    # The idle machinery is reachable through the public Clock interface
    # alone: a hand-rolled virtual clock sharing no code with TestingClock.
    class FrozenClock(Clock):
        def __init__(self) -> None:
            self._now = 0.0
            self._primed = False

        def current_time(self) -> float:
            return self._now

        def before_io_wait(
            self,
            *,
            relative_deadline: float,
            anything_runnable: bool,
        ) -> float:
            # ask to be told as soon as the run has nothing to do
            self._primed = not anything_runnable and relative_deadline > 0
            return 0.0

        def after_io_wait(self, **kwargs: object) -> None:
            if not self._primed:
                return
            self._primed = False
            if kwargs["saw_events"] or kwargs["anything_runnable"]:
                return
            if kwargs["deadline_expired"]:
                return
            relative_deadline = kwargs["relative_deadline"]
            assert isinstance(relative_deadline, float)
            if 0 < relative_deadline < inf:
                self._now += relative_deadline

    clock = FrozenClock()

    async def main() -> None:
        await sleep(10)
        await sleep(5)

    start = time.perf_counter()
    _core.run(main, clock=clock)
    assert clock.current_time() == 15
    # 15 virtual seconds in (much less than) 15 real ones
    assert time.perf_counter() - start < 10


def test_offset_clock_delegates_without_rebasing() -> None:
    # A clock that reports absolute wall-clock-style dates by adding an epoch
    # offset to an inner TestingClock. Because the run loop hands over
    # *relative* deadlines, every method but current_time() is a pure
    # forward -- there is no origin to translate, so there is no origin to
    # get wrong. (With absolute deadlines this clock had to subtract its
    # offset in two places, and forgetting either silently corrupted time.)
    EPOCH = 1_700_000_000.0

    class OffsetClock(Clock):
        def __init__(self) -> None:
            self._mock = TestingClock(autojump_threshold=0)

        def start_clock(self) -> None:
            self._mock.start_clock()

        def current_time(self) -> float:
            return EPOCH + self._mock.current_time()

        def before_io_wait(
            self,
            *,
            relative_deadline: float,
            anything_runnable: bool,
        ) -> float:
            return self._mock.before_io_wait(
                relative_deadline=relative_deadline,
                anything_runnable=anything_runnable,
            )

        def after_io_wait(self, **kwargs: object) -> None:
            # every argument is a boolean or a span of time -- nothing is in
            # any particular frame, so it all forwards untouched
            self._mock.after_io_wait(**kwargs)  # type: ignore[arg-type]

        async def wait_all_tasks_blocked(self, cushion: float = 0.0) -> None:
            await self._mock.wait_all_tasks_blocked(cushion)

    async def main() -> None:
        assert _core.current_time() == EPOCH
        await sleep(3600)
        assert _core.current_time() == EPOCH + 3600

    _core.run(main, clock=OffsetClock())


def test_wall_rate_clock_needs_only_current_time() -> None:
    # https://github.com/python-trio/trio/issues/3369 -- the motivating
    # example: a clock reporting datetime-style timestamps, running at wall
    # rate. Deadlines cross the interface as spans and the base class's
    # hooks default to wall rate, so current_time is the only method to
    # write -- and with no absolute instants to translate, there is no way
    # to get the epoch arithmetic wrong.
    EPOCH = 1_700_000_000.0

    class DateClock(Clock):
        def current_time(self) -> float:
            return EPOCH + time.perf_counter()

    async def main() -> None:
        start = _core.current_time()
        assert start >= EPOCH
        await sleep(0.05)
        assert _core.current_time() >= start + 0.05

    _core.run(main, clock=DateClock())


def test_autojump_ignores_in_flight_io() -> None:
    # The failure mode of open-loop designs (gh-3371): autojump armed, a
    # task blocked on real IO (here a worker thread's waker), and the IO
    # completes before the shortened wait's timeout. The clock must not
    # jump virtual time over the in-flight work just because it had primed
    # a jump when the wait began.
    clock = TestingClock(autojump_threshold=100)

    async def main() -> None:
        start = _core.current_time()
        with move_on_after(1000) as scope:
            await to_thread_run_sync(lambda: time.sleep(0.1))
        assert not scope.cancelled_caught
        assert _core.current_time() == start

    _core.run(main, clock=clock)
