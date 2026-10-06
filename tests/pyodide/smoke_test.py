"""Smoke test for Trio's guest mode on Emscripten.

This runs *inside* Pyodide, driven by run_tests.mjs. It checks that
sleeping, nurseries and cancellation work on top of the JavaScript event loop,
and that the things that can't work on Emscripten fail in the expected way.
"""

from __future__ import annotations

import math
import sys
import time
from typing import TYPE_CHECKING

import js
import trio
from pyodide.ffi import create_once_callable, create_proxy

if TYPE_CHECKING:
    from collections.abc import Callable

    from outcome import Outcome

if sys.version_info < (3, 11):  # pragma: no cover
    from exceptiongroup import BaseExceptionGroup

stats = {"run_sync_soon": 0, "timers started": 0, "timers cancelled": 0}


def run_sync_soon(fn: Callable[[], object]) -> None:
    stats["run_sync_soon"] += 1
    js.setTimeout(create_once_callable(fn), 0)


def run_sync_later(fn: Callable[[], object], delay: float) -> Callable[[], None]:
    stats["timers started"] += 1

    def fire() -> None:
        try:
            fn()
        finally:
            proxy.destroy()

    proxy = create_proxy(fire)
    handle = js.setTimeout(proxy, delay * 1000)

    def cancel() -> None:
        stats["timers cancelled"] += 1
        js.clearTimeout(handle)
        proxy.destroy()

    return cancel


def from_js_later(fn: Callable[[], object], delay_ms: int) -> None:
    """Pretend to be some browser event (a click, a fetch completing, ...)."""
    js.setTimeout(create_once_callable(fn), delay_ms)


async def trio_main() -> dict[str, object]:
    results: dict[str, object] = {}

    start = trio.current_time()
    await trio.sleep(0.2)
    elapsed = trio.current_time() - start
    assert 0.19 <= elapsed <= 1, elapsed
    results["sleep(0.2) took"] = round(elapsed, 3)

    order: list[str] = []

    async def child(name: str, delay: float) -> None:
        await trio.sleep(delay)
        order.append(name)

    async with trio.open_nursery() as nursery:
        nursery.start_soon(child, "c", 0.15)
        nursery.start_soon(child, "a", 0.05)
        nursery.start_soon(child, "b", 0.10)
    assert order == ["a", "b", "c"], order
    results["nursery children finished in order"] = order

    start = trio.current_time()
    with trio.move_on_after(0.1) as cancel_scope:
        await trio.sleep_forever()
    elapsed = trio.current_time() - start
    assert cancel_scope.cancelled_caught
    assert 0.09 <= elapsed <= 1, elapsed
    results["move_on_after(0.1) took"] = round(elapsed, 3)

    token = trio.lowlevel.current_trio_token()
    with trio.CancelScope() as cancel_scope:
        from_js_later(lambda: token.run_sync_soon(cancel_scope.cancel), 50)
        await trio.sleep_forever()
    assert cancel_scope.cancelled_caught
    results["cancelled via run_sync_soon from a JS timer"] = True

    event = trio.Event()
    from_js_later(event.set, 50)
    await event.wait()
    results["woken by Event.set from a JS timer"] = True

    with trio.CancelScope() as cancel_scope:
        from_js_later(lambda: setattr(cancel_scope, "deadline", -math.inf), 50)
        await trio.sleep_forever()
    assert cancel_scope.cancelled_caught
    results["deadline changed from a JS timer"] = True

    async def boom() -> None:
        await trio.sleep(0.01)
        raise ValueError("boom")

    error: BaseException | None = None
    try:
        async with trio.open_nursery() as nursery:
            nursery.start_soon(trio.sleep_forever)
            nursery.start_soon(boom)
    except BaseException as exc:
        error = exc
    assert isinstance(error, BaseExceptionGroup), error
    assert len(error.exceptions) == 1
    assert isinstance(error.exceptions[0], ValueError)
    results["nursery exception"] = repr(error.exceptions[0])

    try:
        await trio.lowlevel.wait_readable(0)
    except NotImplementedError as exc:
        results["wait_readable"] = str(exc)
    else:  # pragma: no cover
        raise AssertionError("wait_readable didn't raise")

    try:
        await trio.to_thread.run_sync(lambda: 1)
    except RuntimeError as exc:
        results["to_thread.run_sync"] = str(exc)
    else:  # pragma: no cover
        raise AssertionError("to_thread.run_sync didn't raise")

    results["io backend"] = trio.lowlevel.current_statistics().io_statistics.backend
    assert results["io backend"] == "emscripten"
    return results


try:
    trio.run(trio.sleep, 0)
except NotImplementedError as exc:
    print("trio.run ->", exc)
else:  # pragma: no cover
    raise AssertionError("trio.run didn't raise")

started = time.perf_counter()


def done_callback(outcome: Outcome[dict[str, object]]) -> None:
    try:
        results = outcome.unwrap()
    except BaseException:
        import traceback

        traceback.print_exc()
        js.process.exitCode = 1
        return
    print("guest run finished after", round(time.perf_counter() - started, 3), "s")
    for key, value in results.items():
        print(f"  {key}: {value}")
    print("host stats:", stats)
    print("SMOKE TEST PASSED")


trio.lowlevel.start_guest_run(
    trio_main,
    run_sync_soon_threadsafe=run_sync_soon,
    run_sync_later=run_sync_later,
    done_callback=done_callback,
)
print("start_guest_run returned; the JS event loop now drives Trio")
