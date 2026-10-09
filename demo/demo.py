"""Runs inside Pyodide, in the page: starts Trio as a guest of the browser's
event loop and drives the demo."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import js
import trio
import trio.pyodide
from pyodide.ffi import create_once_callable, create_proxy

if TYPE_CHECKING:
    from collections.abc import Callable

    from outcome import Outcome

# --- host loop glue: how Trio asks the browser to call it back -------------


def run_sync_soon(fn: Callable[[], object]) -> None:
    js.setTimeout(create_once_callable(fn), 0)


def run_sync_later(fn: Callable[[], object], delay: float) -> Callable[[], None]:
    def fire() -> None:
        try:
            fn()
        finally:
            proxy.destroy()

    proxy = create_proxy(fire)
    handle = js.setTimeout(proxy, delay * 1000)

    def cancel() -> None:
        js.clearTimeout(handle)
        proxy.destroy()

    return cancel


# --- the demo ---------------------------------------------------------------


def element(id: str) -> Any:
    return js.document.getElementById(id)


def log(message: str) -> None:
    item = js.document.createElement("li")
    item.textContent = message
    element("log").appendChild(item)


async def clock() -> None:
    start = trio.current_time()
    while True:
        element("clock").textContent = f"{trio.current_time() - start:.1f}"
        await trio.sleep(0.1)


async def worker(name: str, delay: float) -> None:
    for step in range(1, 4):
        await trio.sleep(delay)
        log(f"{name} task: step {step}, at {delay * step:.1f} s")


async def slow_task(scope_holder: list[trio.CancelScope]) -> None:
    with trio.CancelScope() as scope:
        scope_holder.append(scope)
        element("cancel-btn").disabled = False
        element("cancel-status").textContent = "sleeping for an hour..."
        await trio.sleep(3600)
    element("cancel-status").textContent = (
        "cancelled by the button; Cancelled was raised at the await"
    )
    element("cancel-btn").disabled = True


async def fetch_hello() -> None:
    response = await trio.pyodide.fetch("hello.txt")
    element("fetch-out").textContent = (
        f"HTTP {response.status}: {await response.text()}"
    )


async def fetch_cancelled() -> None:
    output = element("fetch-out")
    # Stream the largest file to hand, the Trio wheel, whatever it is called today
    async with await trio.pyodide.fetch("wheels/index.json") as index:
        wheels = await index.json()
    assert isinstance(wheels, list)
    trio_wheel = next(name for name in wheels if name.startswith("trio-"))
    received = 0
    total = "?"
    with trio.move_on_after(0.3) as scope:
        async with await trio.pyodide.fetch(f"wheels/{trio_wheel}") as response:
            total = response.headers.get("content-length", "?")
            # Read slowly, in small pieces, so the timeout hits mid-stream
            while chunk := await response.body.receive_some(32 * 1024):
                received += len(chunk)
                output.textContent = f"streaming... {received} of {total} bytes"
                await trio.sleep(0.1)
    if scope.cancelled_caught:
        output.textContent = (
            f"timed out after 0.3 s with {received} of {total} bytes read; "
            "Cancelled was raised inside the read, and the request was aborted"
        )
    else:
        output.textContent = f"finished before the timeout: {received} bytes"


async def double(x: int) -> int:
    await trio.sleep(0.5)  # pretend to work
    return x * 2


async def main() -> None:
    async with trio.open_nursery() as nursery:
        nursery.start_soon(clock)
        for name, delay in [("fast", 0.3), ("medium", 0.6), ("slow", 1.0)]:
            nursery.start_soon(worker, name, delay)
        scope_holder: list[trio.CancelScope] = []
        nursery.start_soon(slow_task, scope_holder)

        # Expose things for the buttons. Synchronous Trio calls are fine from
        # JavaScript event handlers; async ones go through callable_from_js.
        js.cancelSlowTask = lambda: scope_holder[0].cancel()
        js.fetchHello = trio.pyodide.callable_from_js(nursery, fetch_hello)
        js.fetchCancelled = trio.pyodide.callable_from_js(nursery, fetch_cancelled)
        js.trioDouble = trio.pyodide.callable_from_js(nursery, double)
        for button_id in ("fetch-btn", "fetch-cancel-btn", "js-call-btn"):
            element(button_id).disabled = False
        element("status").textContent = "Trio is running"
        await trio.sleep_forever()


def done_callback(outcome: Outcome[None]) -> None:
    element("status").textContent = f"Trio finished: {outcome}"


trio.lowlevel.start_guest_run(
    main,
    run_sync_soon_threadsafe=run_sync_soon,
    run_sync_later=run_sync_later,
    done_callback=done_callback,
)
