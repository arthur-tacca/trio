import time
from math import inf

import pytest

import trio
from trio import sleep

from ... import _core
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
    assert c.deadline_to_sleep_time(1.1, have_idle_waiters=False) == 0
    assert c.deadline_to_sleep_time(1.2, have_idle_waiters=False) == 0
    assert c.deadline_to_sleep_time(1.3, have_idle_waiters=False) == inf

    with pytest.raises(ValueError, match=r"^rate must be >= 0$"):
        c.rate = -1
    assert c.rate == 0

    c.rate = 2
    assert c.current_time() == 1.2
    REAL_NOW += 1
    assert c.current_time() == 3.2
    assert c.deadline_to_sleep_time(3.1, have_idle_waiters=False) == 0
    assert c.deadline_to_sleep_time(3.2, have_idle_waiters=False) == 0
    assert c.deadline_to_sleep_time(4.2, have_idle_waiters=False) == 0.5

    c.rate = 0.5
    assert c.current_time() == 3.2
    assert c.deadline_to_sleep_time(3.1, have_idle_waiters=False) == 0
    assert c.deadline_to_sleep_time(3.2, have_idle_waiters=False) == 0
    assert c.deadline_to_sleep_time(4.2, have_idle_waiters=False) == 2.0

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


def test_conversion_takes_the_runner_facts_as_parameters() -> None:
    # The conversion no longer inspects the runner: it can be called outside
    # a run even with autojumping armed (reaching for the runner's statistics
    # here would raise "must be called from async/task context").
    c = MockClock(autojump_threshold=1)
    assert c.deadline_to_sleep_time(10, have_idle_waiters=False) == 1
    assert c._jump_to == 10

    # Tasks in wait_all_tasks_blocked wake without the clock moving, so a
    # pending waiter declines the shortened sleep and nothing is stashed.
    c = MockClock(autojump_threshold=1)
    assert c.deadline_to_sleep_time(10, have_idle_waiters=True) == inf
    assert c._jump_to is None


def test_an_infinite_threshold_never_primes() -> None:
    # inf means "never autojump", and it is the default, so neither a frozen
    # clock nor a running one may set up a jump. Without the explicit test
    # for it the running case would prime a jump to infinity, and the frozen
    # case would be saved only by inf * 0 being nan.
    c = MockClock()  # frozen
    assert c.deadline_to_sleep_time(10, have_idle_waiters=False) == inf
    assert c._jump_to is None

    c = MockClock(rate=1)  # running, with nothing scheduled
    assert c.deadline_to_sleep_time(inf, have_idle_waiters=False) == inf
    assert c._jump_to is None


def test_wait_has_ended_jumps_only_after_a_wait_that_produced_nothing() -> None:
    def primed_clock() -> MockClock:
        c = MockClock(autojump_threshold=1)
        assert c.deadline_to_sleep_time(10, have_idle_waiters=False) == 1
        assert c._jump_to == 10
        return c

    # IO events arrived: the run was not idle, so no jump - and the stash
    # does not survive to poison a later, genuinely idle wait.
    c = primed_clock()
    c.wait_has_ended(saw_events=True, anything_runnable=False)
    assert c.current_time() == 0
    assert c._jump_to is None

    # A task became runnable (e.g. a guest-mode host rescheduled one), so
    # again the run was not idle: no jump.
    c = primed_clock()
    c.wait_has_ended(saw_events=False, anything_runnable=True)
    assert c.current_time() == 0
    assert c._jump_to is None

    # The wait ran its full shortened timeout and produced nothing: jump
    # exactly onto the stashed deadline.
    c = primed_clock()
    c.wait_has_ended(saw_events=False, anything_runnable=False)
    assert c.current_time() == 10
    assert c._jump_to is None

    # An unprimed wait never jumps, however idle: the conversion answered
    # the natural sleep, so this was not an observation window.
    c = MockClock(autojump_threshold=1)
    c.wait_has_ended(saw_events=False, anything_runnable=False)
    assert c.current_time() == 0


def test_start_clock_resets_the_stash() -> None:
    # The stash is run-scoped: a run that tore down between priming a jump
    # and hearing the wait's outcome must not leak it into a new run using
    # the same clock.
    c = MockClock(autojump_threshold=1)
    assert c.deadline_to_sleep_time(10, have_idle_waiters=False) == 1
    assert c._jump_to == 10
    c.start_clock()
    assert c._jump_to is None


def test_autojump_ignores_in_flight_io() -> None:
    # The failure mode of open-loop designs (gh-3371): autojump armed, a
    # task blocked on real IO (here a worker thread's waker), and the IO
    # completes before the shortened wait's deadline. The clock must not
    # jump virtual time over the in-flight work just because it had primed
    # a jump when the wait began.
    clock = MockClock(autojump_threshold=100)

    async def main() -> None:
        start = _core.current_time()
        with trio.move_on_after(1000) as scope:
            await trio.to_thread.run_sync(lambda: time.sleep(0.1))
        assert not scope.cancelled_caught
        assert _core.current_time() == start

    _core.run(main, clock=clock)
