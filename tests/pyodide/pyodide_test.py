"""Test for trio.pyodide: calling JavaScript from Trio and Trio from
JavaScript, and fetch.

This runs *inside* Pyodide, driven by run_tests.mjs, which also provides the
HTTP server at js.TEST_BASE_URL.
"""

from __future__ import annotations

import sys
import time
from typing import TYPE_CHECKING

import js
import trio
import trio.pyodide
from pyodide.code import run_js
from pyodide.ffi import JsException, create_once_callable, create_proxy

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from outcome import Outcome


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


async def expect_js_error(name: str, fn: Callable[[], Awaitable[object]]) -> str:
    error: JsException | None = None
    try:
        await fn()
    except JsException as exc:
        error = exc
    assert error is not None, f"expected a JS {name}"
    assert error.name == name, (error.name, error.message)
    return str(error.message)


async def expect_closed(response: trio.pyodide.Response) -> None:
    try:
        await response.body.receive_some()
    except trio.ClosedResourceError:
        return
    raise AssertionError("body should be closed")


async def expect_broken(fn: Callable[[], Awaitable[object]]) -> str:
    try:
        await fn()
    except trio.BrokenResourceError as exc:
        return str(exc)
    raise AssertionError("expected BrokenResourceError")


async def trio_main() -> dict[str, object]:
    base = js.TEST_BASE_URL
    results: dict[str, object] = {}

    def progress(label: str) -> None:
        # Printed as we go, so a hang shows where it happened
        print(" ...", label)

    # --- call and call_method ------------------------------------------------
    progress("call and call_method")
    assert await trio.pyodide.call(run_js("(a, b) => a + b"), 1, 2) == 3
    obj = await trio.pyodide.call(run_js("async (x) => ({ x })"), 5)
    assert obj.x == 5
    results["call: sync and async functions"] = True
    message = await expect_js_error(
        "RangeError",
        lambda: trio.pyodide.call(
            run_js("async () => { throw new RangeError('nope'); }")
        ),
    )
    results["call: rejected with an Error"] = f"RangeError: {message}"
    message = await expect_js_error(
        "TypeError",
        lambda: trio.pyodide.call(run_js("() => { throw new TypeError('sync'); }")),
    )
    results["call: synchronous throw"] = f"TypeError: {message}"
    rejected: trio.pyodide.JsPromiseRejected | None = None
    try:
        await trio.pyodide.call(run_js("async () => { throw 'a string'; }"))
    except trio.pyodide.JsPromiseRejected as exc:
        rejected = exc
    assert rejected is not None
    assert rejected.reason == "a string"
    results["call: rejected with a non-Error"] = repr(rejected.reason)
    headers = run_js("new Headers({ a: '1' })")
    assert await trio.pyodide.call_method(headers, "get", "a") == "1"
    results["call_method"] = True
    message = await expect_js_error(
        "TypeError",
        lambda: trio.pyodide.call_method(headers, "nonsense"),
    )
    results["call_method: not a method"] = f"TypeError: {message}"

    progress("wait_promise")
    assert await trio.pyodide.wait_promise(js.Promise.resolve("resolved")) == "resolved"
    message = await expect_js_error(
        "RangeError",
        lambda: trio.pyodide.wait_promise(
            run_js("Promise.reject(new RangeError('nope'))")
        ),
    )
    results["wait_promise: rejected with an Error"] = f"RangeError: {message}"

    # --- callable_from_js ----------------------------------------------------
    progress("callable_from_js")

    async def double(x: int) -> int:
        await trio.sleep(0.01)
        return x * 2

    async def boom() -> None:
        await trio.sleep(0.01)
        raise ValueError("boom")

    # The JS helpers below receive the Python function as a borrowed proxy and
    # call it synchronously; the promises they return come back to Python as
    # converted promises, which is what wait_promise is for.
    async with trio.open_nursery() as nursery:
        start_double = trio.pyodide.callable_from_js(nursery, double)
        promise = run_js("(start) => Promise.all([start(21), start(4)])")(start_double)
        values = await trio.pyodide.wait_promise(promise)
        assert values.to_py() == [42, 8], values
        results["callable_from_js: results"] = values.to_py()

        promise = run_js(
            "(start) => start().catch((e) => e.name + ': ' + e.message + ' | ' + e.stack.split('\\n')[0])",
        )(trio.pyodide.callable_from_js(nursery, boom))
        description = await trio.pyodide.wait_promise(promise)
        assert (
            description == "ValueError: boom | Traceback (most recent call last):"
        ), description
        results["callable_from_js: exception"] = description

        start_forever = trio.pyodide.callable_from_js(nursery, trio.sleep_forever)
        promise = run_js(
            """(start) => {
                const controller = new AbortController();
                const promise = start.callKwargs({ signal: controller.signal });
                setTimeout(() => controller.abort(), 20);
                return promise.catch((e) => e.name);
            }""",
        )(start_forever)
        assert await trio.pyodide.wait_promise(promise) == "AbortError"
        results["callable_from_js: aborted from JS"] = True

        promise = run_js("(start) => start().catch((e) => e.name)")(start_forever)
        await trio.sleep(0.01)
        nursery.cancel_scope.cancel()
    assert await trio.pyodide.wait_promise(promise) == trio.pyodide.CANCELLED_ERROR_NAME
    results["callable_from_js: cancelled by Trio"] = True
    promise = run_js("(start) => start(1).catch((e) => e.name)")(start_double)
    assert await trio.pyodide.wait_promise(promise) == "RuntimeError"
    results["callable_from_js: after the nursery closed"] = True

    # --- basic requests ------------------------------------------------------
    progress("basic requests")
    response = await trio.pyodide.fetch(base + "/hello")
    assert response.status == 200, response
    assert response.ok, response
    assert response.headers["content-type"] == "text/plain", response.headers
    assert await response.text() == "hello from node"
    results["GET /hello"] = repr(response)

    response = await trio.pyodide.fetch(
        base + "/echo",
        method="POST",
        headers={"X-Test": "yes"},
        body=b"ping",
    )
    assert await response.text() == "POST yes ping"
    results["POST /echo (bytes body)"] = True

    response = await trio.pyodide.fetch(base + "/echo", method="POST", body="text body")
    assert await response.text() == "POST undefined text body"
    results["POST /echo (str body)"] = True

    response = await trio.pyodide.fetch(base + "/json")
    assert await response.json() == {"answer": 42, "list": [1, 2, 3]}
    results["GET /json"] = True

    response = await trio.pyodide.fetch(base + "/missing")
    assert response.status == 404
    assert not response.ok
    assert await response.bytes() == b"nope"
    results["GET /missing"] = response.status

    try:
        await trio.pyodide.fetch(base + "/hello", signal=object())
    except TypeError as exc:
        results["fetch(signal=...)"] = f"TypeError: {exc}"

    # --- cancellation --------------------------------------------------------
    progress("cancellation")
    start = trio.current_time()
    with trio.move_on_after(0.2) as cancel_scope:
        await trio.pyodide.fetch(base + "/slow")
    elapsed = trio.current_time() - start
    assert cancel_scope.cancelled_caught
    assert 0.19 <= elapsed <= 1, elapsed
    results["cancelled while waiting for /slow"] = round(elapsed, 3)

    response = await trio.pyodide.fetch(base + "/stream")
    chunks = [chunk async for chunk in response.body]
    assert b"".join(chunks) == b"chunk0\nchunk1\nchunk2\nchunk3\nchunk4\n", chunks
    results["streamed /stream"] = len(chunks)

    response = await trio.pyodide.fetch(base + "/stream")
    received = b""
    with trio.move_on_after(0.1) as cancel_scope:
        async for chunk in response.body:
            received += chunk
    assert cancel_scope.cancelled_caught
    assert received.startswith(b"chunk0\n"), received
    results["cancelled while streaming"] = received
    # The request was aborted, so there's nothing more to read
    results["read after cancel"] = await expect_broken(response.body.receive_some)
    await expect_broken(response.text)

    # --- partial reads, closing ----------------------------------------------
    progress("partial reads, closing")
    async with await trio.pyodide.fetch(base + "/stream") as response:
        first = await response.body.receive_some(3)
        second = await response.body.receive_some(100)
        assert first == b"chu", first
        assert second == b"nk0\n", second
    await expect_closed(response)
    results["receive_some(max_bytes) and aclose"] = True

    response = await trio.pyodide.fetch(base + "/hello")
    await response.aclose()
    await expect_closed(response)
    results["aclose before reading"] = True

    # --- errors --------------------------------------------------------------
    progress("errors")
    message = await expect_js_error(
        "TypeError",
        lambda: trio.pyodide.fetch("http://127.0.0.1:1/"),
    )
    results["connection refused"] = f"TypeError: {message}"

    # --- concurrency ---------------------------------------------------------
    progress("concurrency")
    order = []

    async def one(path: str, name: str) -> None:
        response = await trio.pyodide.fetch(base + path)
        await response.text()
        order.append(name)

    async with trio.open_nursery() as nursery:
        nursery.start_soon(one, "/slow", "slow")
        nursery.start_soon(one, "/hello", "hello")
    assert order == ["hello", "slow"], order
    results["concurrent fetches"] = order
    return results


started = time.perf_counter()


def done_callback(outcome: Outcome[dict[str, object]]) -> None:
    try:
        results = outcome.unwrap()
    except BaseException:
        import traceback

        traceback.print_exc()
        js.process.exitCode = 1
        return
    finally:
        js.stopServer()
    print("guest run finished after", round(time.perf_counter() - started, 3), "s")
    for key, value in results.items():
        print(f"  {key}: {value}")
    print("PYODIDE TEST PASSED")


assert sys.platform == "emscripten"
trio.lowlevel.start_guest_run(
    trio_main,
    run_sync_soon_threadsafe=run_sync_soon,
    run_sync_later=run_sync_later,
    done_callback=done_callback,
)
