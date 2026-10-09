"""Runs inside Pyodide, in the page: starts Trio as a guest of the browser's
event loop and drives the demo."""

import js
import trio
import trio.pyodide
from pyodide.ffi import create_once_callable, create_proxy

# --- host loop glue: how Trio asks the browser to call it back -------------


def run_sync_soon(fn):
    js.setTimeout(create_once_callable(fn), 0)


def run_sync_later(fn, delay):
    def fire():
        try:
            fn()
        finally:
            proxy.destroy()

    proxy = create_proxy(fire)
    handle = js.setTimeout(proxy, delay * 1000)

    def cancel():
        js.clearTimeout(handle)
        proxy.destroy()

    return cancel


# --- the demo ---------------------------------------------------------------


def element(id):
    return js.document.getElementById(id)


def log(message):
    item = js.document.createElement("li")
    item.textContent = message
    element("log").appendChild(item)


async def clock():
    start = trio.current_time()
    while True:
        element("clock").textContent = f"{trio.current_time() - start:.1f}"
        await trio.sleep(0.1)


async def worker(name, delay):
    for step in range(1, 4):
        await trio.sleep(delay)
        log(f"{name} task: step {step}, at {delay * step:.1f} s")


async def slow_task(scope_holder):
    with trio.CancelScope() as scope:
        scope_holder.append(scope)
        element("cancel-btn").disabled = False
        element("cancel-status").textContent = "sleeping for an hour…"
        await trio.sleep(3600)
    element("cancel-status").textContent = "cancelled by the button; Cancelled was raised at the await"
    element("cancel-btn").disabled = True


async def fetch_hello():
    response = await trio.pyodide.fetch("hello.txt")
    element("fetch-out").textContent = f"HTTP {response.status}: {await response.text()}"


async def fetch_cancelled():
    output = element("fetch-out")
    received = 0
    with trio.move_on_after(0.3) as scope:
        async with await trio.pyodide.fetch("wheels/trio-0.34.0+dev-py3-none-any.whl") as response:
            total = response.headers.get("content-length", "?")
            # Read slowly, in small pieces, so the timeout hits mid-stream
            while chunk := await response.body.receive_some(32 * 1024):
                received += len(chunk)
                output.textContent = f"streaming… {received} of {total} bytes"
                await trio.sleep(0.1)
    if scope.cancelled_caught:
        output.textContent = (
            f"timed out after 0.3 s with {received} of {total} bytes read; "
            "Cancelled was raised inside the read, and the request was aborted"
        )
    else:
        output.textContent = f"finished before the timeout: {received} bytes"


async def double(x):
    await trio.sleep(0.5)  # pretend to work
    return x * 2


async def main():
    async with trio.open_nursery() as nursery:
        nursery.start_soon(clock)
        for name, delay in [("fast", 0.3), ("medium", 0.6), ("slow", 1.0)]:
            nursery.start_soon(worker, name, delay)
        scope_holder = []
        nursery.start_soon(slow_task, scope_holder)

        # Expose things for the buttons. Synchronous Trio calls are fine from
        # JavaScript event handlers; async ones go through callable_from_js.
        js.cancelSlowTask = lambda: scope_holder[0].cancel()
        js.fetchHello = trio.pyodide.callable_from_js(nursery, fetch_hello)
        js.fetchCancelled = trio.pyodide.callable_from_js(nursery, fetch_cancelled)
        js.trioDouble = trio.pyodide.callable_from_js(nursery, double)
        for id in ("fetch-btn", "fetch-cancel-btn", "js-call-btn"):
            element(id).disabled = False
        element("status").textContent = "Trio is running"
        await trio.sleep_forever()


def done_callback(outcome):
    element("status").textContent = f"Trio finished: {outcome}"


trio.lowlevel.start_guest_run(
    main,
    run_sync_soon_threadsafe=run_sync_soon,
    run_sync_later=run_sync_later,
    done_callback=done_callback,
)
