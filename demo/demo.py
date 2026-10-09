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


class NurseryDemo:
    """The Start and Cancel buttons: five sleeping tasks in one nursery."""

    def __init__(self, spawner: trio.Nursery) -> None:
        # The page's long-lived nursery, which hosts each run of the demo
        self._spawner = spawner
        # The demo's own nursery, while a run is in progress
        self._nursery: trio.Nursery | None = None
        self._running = False
        self._start_time = 0.0

    def start(self) -> None:
        """Start a run, unless one is already in progress."""
        if self._running:
            return
        self._running = True
        self._spawner.start_soon(self._run)

    def cancel(self) -> None:
        """Cancel the running nursery, if there is one."""
        if self._nursery is None:
            return
        self._trace("cancel requested")
        self._nursery.cancel_scope.cancel()

    def _trace(self, message: str) -> None:
        elapsed = trio.current_time() - self._start_time
        item = js.document.createElement("li")
        item.textContent = f"{elapsed:4.1f} s  {message}"
        element("trace").appendChild(item)

    async def _run(self) -> None:
        element("trace").replaceChildren()
        self._start_time = trio.current_time()
        try:
            self._trace("opening the nursery")
            async with trio.open_nursery() as nursery:
                self._nursery = nursery
                for n in range(1, 6):
                    nursery.start_soon(self._sleeper, n)
                self._trace(
                    "all five tasks spawned; the nursery is now waiting for them"
                )
            if nursery.cancel_scope.cancelled_caught:
                self._trace("nursery finished: it was cancelled")
            else:
                self._trace("nursery finished: all tasks completed")
        finally:
            self._nursery = None
            self._running = False

    async def _sleeper(self, n: int) -> None:
        self._trace(f"task {n} started, sleeping for {n} s")
        try:
            await trio.sleep(n)
        except trio.Cancelled:
            self._trace(f"task {n} cancelled")
            raise
        self._trace(f"task {n} finished")


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
        demo = NurseryDemo(nursery)

        # Expose things for the buttons. Synchronous Trio calls are fine from
        # JavaScript event handlers; async ones go through callable_from_js.
        js.startNursery = demo.start
        js.cancelNursery = demo.cancel
        js.fetchHello = trio.pyodide.callable_from_js(nursery, fetch_hello)
        js.fetchCancelled = trio.pyodide.callable_from_js(nursery, fetch_cancelled)
        js.trioDouble = trio.pyodide.callable_from_js(nursery, double)
        for button_id in (
            "start-btn",
            "cancel-btn",
            "fetch-btn",
            "fetch-cancel-btn",
            "js-call-btn",
        ):
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
