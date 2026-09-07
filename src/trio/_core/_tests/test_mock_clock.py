import time
from math import inf

import pytest

from trio import sleep

from ... import _core
from ..._abc import Clock
from .. import wait_all_tasks_blocked
from .._mock_clock import MockClock
from .tutil import slow


def test_mock_clock() -> None:
    REAL_NOW = 123.0
    c = MockClock()
    c._real_clock = lambda: REAL_NOW
    repr(c)  # smoke test
    assert c.rate == 0
    assert c.current_time() == 0
    c.jump(1.2)
    assert c.current_time() == 1.2
    with pytest.raises(ValueError, match=r"^time can't go backwards$"):
        c.jump(-1)
    assert c.current_time() == 1.2
    assert c.deadline_to_sleep_time(1.1) == 0
    assert c.deadline_to_sleep_time(1.2) == 0
    assert c.deadline_to_sleep_time(1.3) > 999999

    with pytest.raises(ValueError, match=r"^rate must be >= 0$"):
        c.rate = -1
    assert c.rate == 0

    c.rate = 2
    assert c.current_time() == 1.2
    REAL_NOW += 1
    assert c.current_time() == 3.2
    assert c.deadline_to_sleep_time(3.1) == 0
    assert c.deadline_to_sleep_time(3.2) == 0
    assert c.deadline_to_sleep_time(4.2) == 0.5

    c.rate = 0.5
    assert c.current_time() == 3.2
    assert c.deadline_to_sleep_time(3.1) == 0
    assert c.deadline_to_sleep_time(3.2) == 0
    assert c.deadline_to_sleep_time(4.2) == 2.0

    c.jump(0.8)
    assert c.current_time() == 4.0
    REAL_NOW += 1
    assert c.current_time() == 4.5

    c2 = MockClock(rate=3)
    assert c2.rate == 3
    assert c2.current_time() < 10


async def test_mock_clock_autojump(mock_clock: MockClock) -> None:
    assert mock_clock.autojump_threshold == inf

    mock_clock.autojump_threshold = 0
    assert mock_clock.autojump_threshold == 0

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

    mock_clock.autojump_threshold = 0.02
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
    mock_clock.autojump_threshold = 10000
    await wait_all_tasks_blocked()
    mock_clock.autojump_threshold = 0
    # if the above line didn't take affect immediately, then this would be
    # bad:
    # ignore ASYNC116, not sleep_forever, trying to test a large but finite sleep
    await sleep(100000)  # noqa: ASYNC116


async def test_mock_clock_autojump_interference(mock_clock: MockClock) -> None:
    mock_clock.autojump_threshold = 0.02

    mock_clock2 = MockClock()
    # messing with the autojump threshold of a clock that isn't actually
    # installed in the run loop shouldn't do anything.
    mock_clock2.autojump_threshold = 0.01

    # if the autojump_threshold of 0.01 were in effect, then the next line
    # would block forever, as the autojump task kept waking up to try to
    # jump the clock.
    await wait_all_tasks_blocked(0.015)

    # but the 0.02 limit does apply
    # ignore ASYNC116, not sleep_forever, trying to test a large but finite sleep
    await sleep(100000)  # noqa: ASYNC116


def test_mock_clock_autojump_preset() -> None:
    # Check that we can set the autojump_threshold before the clock is
    # actually in use, and it gets picked up
    mock_clock = MockClock(autojump_threshold=0.1)
    mock_clock.autojump_threshold = 0.01
    real_start = time.perf_counter()
    _core.run(sleep, 10000, clock=mock_clock)
    assert time.perf_counter() - real_start < 1


async def test_mock_clock_autojump_0_and_wait_all_tasks_blocked_0(
    mock_clock: MockClock,
) -> None:
    # Checks that autojump_threshold=0 doesn't interfere with
    # calling wait_all_tasks_blocked with the default cushion=0.

    mock_clock.autojump_threshold = 0

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
async def test_mock_clock_autojump_0_and_wait_all_tasks_blocked_nonzero(
    mock_clock: MockClock,
) -> None:
    # Checks that autojump_threshold=0 doesn't interfere with
    # calling wait_all_tasks_blocked with a non-zero cushion.

    mock_clock.autojump_threshold = 0

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


def test_autojump_direct() -> None:
    # The public hook the run loop drives autojump through: check its
    # contract without a run.
    c = MockClock(autojump_threshold=1.0)
    c.autojump(10.0)  # jump exactly to the deadline
    assert c.current_time() == 10.0
    c.autojump(inf)  # nothing scheduled: nowhere to go
    assert c.current_time() == 10.0
    c.autojump(5.0)  # deadline in the past: time can't go backwards
    assert c.current_time() == 10.0


def test_custom_clock_wrapping_mock_clock() -> None:
    # https://github.com/python-trio/trio/issues/3369 -- delegating to a
    # MockClock through nothing but the abc.Clock surface must behave
    # identically to passing the MockClock itself.
    class WrapperClock(Clock):
        def __init__(self) -> None:
            self._mock = MockClock(autojump_threshold=0)

        def start_clock(self) -> None:
            self._mock.start_clock()

        def current_time(self) -> float:
            return self._mock.current_time()

        def deadline_to_sleep_time(self, deadline: float) -> float:
            return self._mock.deadline_to_sleep_time(deadline)

        def get_autojump_threshold(self) -> float:
            return self._mock.get_autojump_threshold()

        def autojump(self, next_deadline: float) -> None:
            self._mock.autojump(next_deadline)

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
    # alone: a hand-rolled virtual clock sharing no code with MockClock.
    class FrozenClock(Clock):
        def __init__(self) -> None:
            self._now = 0.0

        def start_clock(self) -> None:
            pass

        def current_time(self) -> float:
            return self._now

        def deadline_to_sleep_time(self, deadline: float) -> float:
            # frozen: no amount of real waiting reaches a future deadline
            return 0 if deadline <= self._now else 999999999

        def get_autojump_threshold(self) -> float:
            return 0.0

        def autojump(self, next_deadline: float) -> None:
            if next_deadline < inf:
                self._now = max(self._now, next_deadline)

    clock = FrozenClock()

    async def main() -> None:
        await sleep(10)
        await sleep(5)

    start = time.perf_counter()
    _core.run(main, clock=clock)
    assert clock.current_time() == 15
    # 15 virtual seconds in (much less than) 15 real ones
    assert time.perf_counter() - start < 10
