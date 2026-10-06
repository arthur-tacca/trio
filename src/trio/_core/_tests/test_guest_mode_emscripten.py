# Tests for guest mode on Emscripten (e.g. Pyodide), where there are no worker
# threads, so Trio waits for its timeouts using host loop timers instead.
#
# These run on every platform, by pretending to be Emscripten: see
# enter_emscripten_mode.
from __future__ import annotations

import heapq
import sys
import time
from collections import deque
from collections.abc import Callable
from math import inf
from typing import TYPE_CHECKING, TypeAlias, TypeVar

import pytest

import trio
import trio.testing
from trio._core import _run
from trio._core._io_emscripten import EmscriptenIOManager
from trio._core._wakeup_emscripten import EmscriptenWakeup
from trio.abc import Instrument

from .tutil import gc_collect_harder, restore_unraisablehook

if TYPE_CHECKING:
    from collections.abc import Awaitable

    from outcome import Outcome

    from trio.abc import Clock

T = TypeVar("T")
InHost: TypeAlias = Callable[[Callable[[], object]], None]


def enter_emscripten_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make Trio behave as it does on Emscripten, even on other platforms."""
    if sys.platform == "emscripten":  # pragma: no cover
        return
    monkeypatch.setattr(sys, "platform", "emscripten")
    monkeypatch.setattr(_run, "TheIOManager", EmscriptenIOManager)


@pytest.fixture
def emscripten_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    enter_emscripten_mode(monkeypatch)


class FakeBrowserLoop:
    """A single-threaded host loop with setTimeout-style timers, like a
    browser's event loop.

    """

    def __init__(self) -> None:
        self.soon: deque[Callable[[], object]] = deque()
        self.timers: list[tuple[float, int, Callable[[], object]]] = []
        self.cancelled: set[int] = set()
        self.counter = 0
        self.soon_calls = 0
        self.timer_calls = 0

    def run_sync_soon(self, fn: Callable[[], object]) -> None:
        self.soon_calls += 1
        self.soon.append(fn)

    def run_sync_later(
        self,
        fn: Callable[[], object],
        delay: float,
    ) -> Callable[[], None]:
        self.timer_calls += 1
        self.counter += 1
        seq = self.counter
        heapq.heappush(self.timers, (time.perf_counter() + delay, seq, fn))

        def cancel() -> None:
            self.cancelled.add(seq)

        return cancel

    def pending_timers(self) -> int:
        return sum(1 for _, seq, _ in self.timers if seq not in self.cancelled)

    def run_until(self, done: Callable[[], bool]) -> None:
        while not done():
            if self.soon:
                self.soon.popleft()()
            elif self.timers:
                deadline, seq, fn = heapq.heappop(self.timers)
                if seq in self.cancelled:
                    self.cancelled.discard(seq)
                    continue
                delay = deadline - time.perf_counter()
                if delay > 5:  # pragma: no cover
                    pytest.fail(f"host loop would sleep for {delay} seconds")
                if delay > 0:
                    time.sleep(delay)
                fn()
            else:  # pragma: no cover
                pytest.fail("host loop has nothing to do, but Trio hasn't finished")


def emscripten_guest_run(
    trio_fn: Callable[[InHost], Awaitable[T]],
    *,
    host: FakeBrowserLoop | None = None,
    clock: Clock | None = None,
) -> T:
    if host is None:
        host = FakeBrowserLoop()
    result: Outcome[T] | None = None

    def done_callback(outcome: Outcome[T]) -> None:
        nonlocal result
        result = outcome

    trio.lowlevel.start_guest_run(
        trio_fn,
        host.run_sync_soon,
        run_sync_soon_threadsafe=host.run_sync_soon,
        run_sync_later=host.run_sync_later,
        done_callback=done_callback,
        clock=clock,
    )
    try:
        host.run_until(lambda: result is not None)
    except BaseException:
        # The host loop is dead, so drop its pending callbacks and timers.
        # Those are what keep an abandoned guest run alive, so this gives
        # Trio's state a chance to be GC'ed and warn about it.
        host.soon.clear()
        host.timers.clear()
        raise
    assert result is not None
    return result.unwrap()


@pytest.mark.usefixtures("emscripten_mode")
def test_emscripten_guest_trivial() -> None:
    async def trio_return(in_host: InHost) -> str:
        assert isinstance(
            trio.lowlevel.current_trio_token()._reentry_queue.wakeup,
            EmscriptenWakeup,
        )
        assert trio.lowlevel.current_statistics().io_statistics.backend == "emscripten"
        await trio.lowlevel.checkpoint()
        return "ok"

    assert emscripten_guest_run(trio_return) == "ok"

    async def trio_fail(in_host: InHost) -> None:
        raise KeyError("whoopsiedaisy")

    with pytest.raises(KeyError, match="whoopsiedaisy"):
        emscripten_guest_run(trio_fail)


@pytest.mark.usefixtures("emscripten_mode")
def test_emscripten_sleep_uses_host_timers() -> None:
    host = FakeBrowserLoop()

    async def trio_main(in_host: InHost) -> float:
        start = trio.current_time()
        for _ in range(3):
            await trio.sleep(0.02)
        return trio.current_time() - start

    assert emscripten_guest_run(trio_main, host=host) >= 0.06
    # Sleeping should be done with host timers, not by busy-looping through
    # run_sync_soon callbacks
    assert 3 <= host.timer_calls <= 10
    assert host.soon_calls <= 20
    assert host.pending_timers() == 0


@pytest.mark.usefixtures("emscripten_mode")
def test_emscripten_nurseries_and_cancellation() -> None:
    async def trio_main(in_host: InHost) -> list[str]:
        record: list[str] = []

        async def child(name: str, delay: float) -> None:
            await trio.sleep(delay)
            record.append(name)

        async with trio.open_nursery() as nursery:
            nursery.start_soon(child, "slow", 0.04)
            nursery.start_soon(child, "fast", 0.01)

        with trio.move_on_after(0.02) as cancel_scope:
            await trio.sleep_forever()
        assert cancel_scope.cancelled_caught
        record.append("timeout")

        async with trio.open_nursery() as nursery:
            nursery.start_soon(trio.sleep_forever)
            await trio.sleep(0.01)
            nursery.cancel_scope.cancel()
        record.append("cancelled")

        async def boom() -> None:
            await trio.sleep(0.01)
            raise ValueError("boom")

        with pytest.RaisesGroup(ValueError):
            async with trio.open_nursery() as nursery:
                nursery.start_soon(trio.sleep_forever)
                nursery.start_soon(boom)
        record.append("crashed")

        return record

    assert emscripten_guest_run(trio_main) == [
        "fast",
        "slow",
        "timeout",
        "cancelled",
        "crashed",
    ]


@pytest.mark.usefixtures("emscripten_mode")
def test_emscripten_host_can_directly_wake_trio_task() -> None:
    host = FakeBrowserLoop()

    async def trio_main(in_host: InHost) -> str:
        ev = trio.Event()
        # By the time this runs, Trio is idle and waiting on a (long) host
        # timer; waking the task has to cancel that timer and tick immediately
        in_host(ev.set)
        await ev.wait()
        return "ok"

    assert emscripten_guest_run(trio_main, host=host) == "ok"
    assert host.pending_timers() == 0


@pytest.mark.usefixtures("emscripten_mode")
def test_emscripten_host_altering_deadlines_wakes_trio_up() -> None:
    def set_deadline(cscope: trio.CancelScope, new_deadline: float) -> None:
        cscope.deadline = new_deadline

    async def trio_main(in_host: InHost) -> str:
        with trio.CancelScope() as cscope:
            in_host(lambda: set_deadline(cscope, -inf))
            await trio.sleep_forever()
        assert cscope.cancelled_caught

        with trio.CancelScope() as cscope:
            # also do a change that doesn't affect the next deadline, just to
            # exercise that path
            in_host(lambda: set_deadline(cscope, 1e6))
            in_host(lambda: set_deadline(cscope, -inf))
            await trio.sleep(999)
        assert cscope.cancelled_caught

        return "ok"

    assert emscripten_guest_run(trio_main) == "ok"


@pytest.mark.usefixtures("emscripten_mode")
def test_emscripten_run_sync_soon() -> None:
    async def trio_main(in_host: InHost) -> str:
        token = trio.lowlevel.current_trio_token()

        # From the host, while Trio is idle
        ev = trio.Event()
        in_host(lambda: token.run_sync_soon(ev.set))
        await ev.wait()

        # From inside Trio
        ev = trio.Event()
        token.run_sync_soon(ev.set)
        await ev.wait()

        # Several at once, from the host, delivered in order
        record: list[int] = []
        ev = trio.Event()

        def submit_several() -> None:
            for i in range(3):
                token.run_sync_soon(record.append, i)
            token.run_sync_soon(ev.set)

        in_host(submit_several)
        await ev.wait()
        assert record == [0, 1, 2]

        return "ok"

    assert emscripten_guest_run(trio_main) == "ok"


@pytest.mark.usefixtures("emscripten_mode")
def test_emscripten_autojump_clock() -> None:
    host = FakeBrowserLoop()
    clock = trio.testing.MockClock(autojump_threshold=0)

    async def trio_main(in_host: InHost) -> float:
        start = trio.current_time()
        async with trio.open_nursery() as nursery:
            nursery.start_soon(trio.sleep, 100)
            await trio.testing.wait_all_tasks_blocked()
        return trio.current_time() - start

    assert emscripten_guest_run(trio_main, host=host, clock=clock) == 100
    # Autojumping happens when the run loop goes idle with a zero timeout, so
    # no host timers are needed
    assert host.timer_calls == 0


@pytest.mark.usefixtures("emscripten_mode")
def test_emscripten_host_wakeup_doesnt_trigger_wait_all_tasks_blocked() -> None:
    # Same as test_host_wakeup_doesnt_trigger_wait_all_tasks_blocked in
    # test_guest_mode.py: a wakeup from the host that doesn't make any task
    # runnable must look like "woken early", not like "timeout expired", or
    # else wait_all_tasks_blocked would wrongly fire.
    def set_deadline(cscope: trio.CancelScope, new_deadline: float) -> None:
        cscope.deadline = new_deadline

    async def trio_main(in_host: InHost) -> str:
        async def sit_in_wait_all_tasks_blocked(watb_cscope: trio.CancelScope) -> None:
            with watb_cscope:
                await trio.testing.wait_all_tasks_blocked(cushion=9999)
                raise AssertionError(  # pragma: no cover
                    "wait_all_tasks_blocked should *not* return normally, "
                    "only by cancellation.",
                )
            assert watb_cscope.cancelled_caught

        async def get_woken_by_host_deadline(watb_cscope: trio.CancelScope) -> None:
            with trio.CancelScope() as cscope:

                class InstrumentHelper(Instrument):
                    def __init__(self) -> None:
                        self.primed = False

                    def before_io_wait(self, timeout: float) -> None:
                        if timeout == 9999:  # pragma: no branch
                            assert not self.primed
                            in_host(lambda: set_deadline(cscope, 1e9))
                            self.primed = True

                    def after_io_wait(self, timeout: float) -> None:
                        if self.primed:  # pragma: no branch
                            in_host(lambda: cscope.cancel())
                            trio.lowlevel.remove_instrument(self)

                trio.lowlevel.add_instrument(InstrumentHelper())
                await trio.sleep_forever()
            assert cscope.cancelled_caught
            watb_cscope.cancel()

        async with trio.open_nursery() as nursery:
            watb_cscope = trio.CancelScope()
            nursery.start_soon(sit_in_wait_all_tasks_blocked, watb_cscope)
            await trio.testing.wait_all_tasks_blocked()
            nursery.start_soon(get_woken_by_host_deadline, watb_cscope)

        return "ok"

    assert emscripten_guest_run(trio_main) == "ok"


@pytest.mark.usefixtures("emscripten_mode")
def test_emscripten_unsupported_apis() -> None:
    async def trio_main(in_host: InHost) -> None:
        with pytest.raises(NotImplementedError, match="not supported on Emscripten"):
            await trio.lowlevel.wait_readable(0)
        with pytest.raises(NotImplementedError, match="not supported on Emscripten"):
            await trio.lowlevel.wait_writable(0)
        # There can never be any waiters to notify, so this is a no-op
        trio.lowlevel.notify_closing(0)
        with pytest.raises(NotImplementedError, match="can't block"):
            _run.GLOBAL_RUN_CONTEXT.runner.io_manager.get_events(1)

    emscripten_guest_run(trio_main)

    # trio.run would have to block the browser's thread
    with pytest.raises(NotImplementedError, match="start_guest_run"):
        trio.run(trio.sleep, 0)


def test_emscripten_run_sync_later_validation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    host = FakeBrowserLoop()

    async def trio_main(in_host: InHost) -> None:
        raise AssertionError("should never run")  # pragma: no cover

    def done_callback(outcome: Outcome[None]) -> None:
        raise AssertionError("should never run")  # pragma: no cover

    if sys.platform != "emscripten":  # pragma: no branch
        with pytest.raises(TypeError, match="only supported on Emscripten"):
            trio.lowlevel.start_guest_run(
                trio_main,
                host.run_sync_soon,
                run_sync_soon_threadsafe=host.run_sync_soon,
                run_sync_later=host.run_sync_later,
                done_callback=done_callback,
            )

    enter_emscripten_mode(monkeypatch)
    with pytest.raises(TypeError, match="requires run_sync_later="):
        trio.lowlevel.start_guest_run(
            trio_main,
            host.run_sync_soon,
            run_sync_soon_threadsafe=host.run_sync_soon,
            done_callback=done_callback,
        )
    # Nothing got left behind
    assert not host.soon
    assert not trio.lowlevel.in_trio_run()


@restore_unraisablehook()
@pytest.mark.usefixtures("emscripten_mode")
def test_emscripten_guest_warns_if_abandoned() -> None:
    # Like test_guest_warns_if_abandoned in test_guest_mode.py. This also
    # checks that the IO manager's reference back to the GuestState is weak:
    # if the Runner kept the GuestState alive, the run would never be garbage
    # collected, and so never cleaned up.
    def do_abandoned_guest_run() -> None:
        async def abandoned_main(in_host: InHost) -> None:
            in_host(lambda: 1 / 0)
            while True:
                await trio.lowlevel.checkpoint()

        with pytest.raises(ZeroDivisionError):
            emscripten_guest_run(abandoned_main)

    with pytest.warns(  # noqa: PT031
        RuntimeWarning,
        match="Trio guest run got abandoned",
    ):
        do_abandoned_guest_run()
        gc_collect_harder()

    with pytest.raises(RuntimeError):
        trio.current_time()
