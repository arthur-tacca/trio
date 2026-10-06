# Trio and PEP 818's awaitable hooks

*A design note, not documentation of anything that exists yet.*

[PEP 818](https://peps.python.org/pep-0818/) proposes adding the core of
Pyodide's JavaScript foreign function interface to the standard library.
Its asyncio integration is hard-wired to asyncio at three points:

1. Step 7 of "Calling a JavaScript Function from Python": a promise
   returned by a JavaScript function is converted, eagerly, into an
   `asyncio.Future` created on `asyncio.get_event_loop()`.
2. `__await__` on a thenable `JSProxy` does the same conversion.
3. `pyawaitable_to_promise` runs a Python awaitable with
   `asyncio.ensure_future` and `add_done_callback`.

In the [discussion thread](https://discuss.python.org/t/pep-818-adding-the-core-of-the-pyodide-foreign-function-interface-to-python-2/109311),
hooks in the style of `sys.set_asyncgen_hooks` were floated as a way to let
other frameworks plug in. This note records what Trio would put in such
hooks. It is based on `trio.pyodide` (`src/trio/_pyodide.py`), which works
today by *working around* their absence; the last section says how.

## The hooks this note assumes

```python
jstypes.ffi.set_awaitable_hooks(
    js_to_py=...,  # (jsawaitable, done_callback) -> Python awaitable
    py_to_js=...,  # (pyawaitable) -> JavaScript promise
)
jstypes.ffi.get_awaitable_hooks()  # -> the current pair, like sys.get_asyncgen_hooks()
```

- `js_to_py` is called by step 7 and by `__await__`, with the *raw*
  thenable, before any future exists. `done_callback`, when given, is the
  FFI's function that releases the argument proxies of the call that
  produced the promise; it must run once the promise settles, whether or
  not anyone awaits.
- `py_to_js` is called by `pyawaitable_to_promise`, i.e. whenever
  JavaScript awaits a Python awaitable or Python hands one to JavaScript.
- The defaults are exactly the PEP's current asyncio implementations, so
  `await some_js_function()` from asyncio is unchanged.

The sketch below works whether step 7 stays eager or becomes lazy (a
`JSPromise` proxy whose `__await__` calls the hook); with lazy conversion,
`done_callback` would be attached by the FFI itself and the hook would
simply never receive one.

The code uses the PEP's names (`jstypes.ffi`, `jstypes.code.run_js`,
`jstypes.global_this`); in Pyodide today they are `pyodide.ffi`,
`pyodide.code.run_js` and `js`.

## `js_to_py`: a promise becomes a Trio awaitable around the raw promise

```python
import contextvars
import itertools
from contextlib import contextmanager

import trio
from jstypes import ffi
from jstypes.code import run_js

CANCELLED_ERROR_NAME = "TrioCancelled"

# Attach the handlers in JavaScript, so the promise itself never needs
# converting. Only the settled value or reason crosses into Python, as an
# ordinary argument, through the one permanent proxy of _settle.
_attach = run_js(
    """
    (settle) => (promise, token, done) => {
        Promise.resolve(promise).then(
            (value) => { settle(token, true, value); if (done) done(); },
            (reason) => { settle(token, false, reason); if (done) done(); },
        );
    }
    """,
)(ffi.create_proxy(lambda token, fulfilled, value: _settle(token, fulfilled, value)))

# The abort is done from JavaScript, because an Error object that has been
# through Python comes back wrapped in a PythonError.
_abort_with_cancellation = run_js(
    """
    (controller, name) => {
        const error = new Error("the Trio task waiting for this operation was cancelled");
        error.name = name;
        controller.abort(error);
    }
    """,
)

_pending: dict[int, "JsAwaitable"] = {}
_tokens = itertools.count()

# Which AbortController a cancelled await should abort. A context variable,
# so it scopes to the task and to a `with` block.
_abort_controller: contextvars.ContextVar[object] = contextvars.ContextVar(
    "trio_js_abort_controller", default=None
)


@contextmanager
def abort_on_cancel(controller):
    """Cancelling the task while it awaits a JavaScript promise inside this
    block aborts ``controller``, with an Error named "TrioCancelled"."""
    reset = _abort_controller.set(controller)
    try:
        yield
    finally:
        _abort_controller.reset(reset)


def _settle(token, fulfilled, value):
    # A microtask on the host loop, between guest ticks: the "host wakes a
    # Trio task" path that JavaScript callbacks already use.
    awaitable = _pending.pop(token)
    awaitable._outcome = (fulfilled, value)
    waiting, awaitable._waiting = awaitable._waiting, []
    for task in waiting:
        trio.lowlevel.reschedule(task)


class JsPromiseRejected(Exception):
    """A promise was rejected with something that isn't a JavaScript Error."""

    def __init__(self, reason):
        super().__init__(repr(reason))
        self.reason = reason


class JsAwaitable:
    """What the hook returns. It holds the *raw* promise, so handing it back
    to JavaScript gives JavaScript the same promise."""

    def __init__(self, promise, done_callback):
        self.promise = promise
        self._outcome = None
        self._waiting = []
        token = next(_tokens)
        _pending[token] = self
        _attach(promise, token, done_callback)  # eager, so done_callback always runs

    def __await__(self):
        if not trio.lowlevel.in_trio_task():
            # Awaited from asyncio, or from plain host code: defer to the hook
            # that was installed before ours. done_callback is already taken
            # care of, so don't pass it again.
            return _previous.js_to_py(self.promise, None).__await__()
        return self._wait(_abort_controller.get()).__await__()

    async def _wait(self, controller):
        raise_cancel = None
        if self._outcome is None:
            self._waiting.append(trio.lowlevel.current_task())

            def abort_fn(raise_cancel_):
                nonlocal raise_cancel
                raise_cancel = raise_cancel_
                if controller is not None:
                    _abort_with_cancellation(controller, CANCELLED_ERROR_NAME)
                # Cancellation surfaces when the promise settles, not before
                return trio.lowlevel.Abort.FAILED

            await trio.lowlevel.wait_task_rescheduled(abort_fn)
        else:
            await trio.lowlevel.checkpoint()
        fulfilled, value = self._outcome
        if fulfilled:
            return value
        if raise_cancel is not None and getattr(value, "name", None) in (
            CANCELLED_ERROR_NAME,
            "AbortError",
        ):
            raise_cancel()
        if isinstance(value, BaseException):
            raise value  # a JSException: the JavaScript Error itself
        raise JsPromiseRejected(value)


def trio_js_to_py(jsawaitable, done_callback=None):
    return JsAwaitable(jsawaitable, done_callback)
```

Four decisions in there:

- **Handlers attach eagerly, the framework choice happens lazily.**
  `done_callback` must run on settlement even if nobody awaits, so the
  handlers go on at hook time. But *who* is awaiting is only known inside
  `__await__`, so that is where Trio either takes the wait or defers to the
  previously installed hook. One installed pair of hooks then serves a page
  where asyncio code and a Trio guest run coexist, with nothing to install
  or restore per run.
- **A promise that settles before it is awaited is handled**, by storing
  the outcome and re-delivering it. That also gives Future-like multiple
  awaits.
- **Cancellation reads the controller from a context variable**, so plain
  `await js.fetch(url, signal=controller.signal)` inside
  `with abort_on_cancel(controller):` behaves exactly like today's
  `trio.pyodide.call(..., abort_controller=controller)`: the task keeps
  waiting until the promise settles, and the cancellation is delivered when
  the rejection caused by the abort arrives. If the operation ignores the
  signal and completes anyway, the task gets its value.
- **The FFI calls the function and binds `this` itself**, so there is no
  need for a `call_method`, and `call` shrinks to a convenience around
  `abort_on_cancel`.

## `py_to_js`: a coroutine JavaScript awaits becomes a task in a nursery

```python
import traceback
from contextlib import asynccontextmanager

from jstypes.global_this import Promise

_js_nursery: trio.Nursery | None = None


@asynccontextmanager
async def open_js_nursery():
    """While open, coroutines that JavaScript awaits run as tasks here."""
    global _js_nursery
    if _js_nursery is not None:
        raise RuntimeError("another open_js_nursery() is already open")
    async with trio.open_nursery() as nursery:
        _js_nursery = nursery
        try:
            yield nursery
        finally:
            _js_nursery = None


_make_error = run_js(
    """
    (name, message, stack) => {
        const error = new Error(message);
        error.name = name;
        if (stack != null) error.stack = stack;
        return error;
    }
    """,
)


def _promise_for(nursery, awaitable, signal=None):
    resolvers = []
    promise = Promise.new(lambda resolve, reject: resolvers.extend((resolve, reject)))
    resolve, reject = resolvers

    async def run():
        on_abort = None
        with trio.CancelScope() as scope:
            if signal is not None:
                if signal.aborted:
                    scope.cancel()
                else:
                    on_abort = ffi.create_proxy(lambda _event: scope.cancel())
                    signal.addEventListener("abort", on_abort)
            try:
                result = await awaitable
            except trio.Cancelled:
                if signal is not None and signal.aborted:
                    reject(signal.reason)
                else:
                    reject(_make_error(CANCELLED_ERROR_NAME, "the Trio task was cancelled", None))
                raise
            except BaseException as exc:
                reject(
                    _make_error(
                        type(exc).__name__,
                        str(exc),
                        "".join(traceback.format_exception(exc)),
                    )
                )
                if not isinstance(exc, Exception):
                    raise
                return
            finally:
                if on_abort is not None:
                    signal.removeEventListener("abort", on_abort)
                    on_abort.destroy()
            resolve(ffi.to_js(result))

    try:
        nursery.start_soon(run)
    except BaseException as exc:  # e.g. the nursery has closed
        reject(_make_error(type(exc).__name__, str(exc), None))
    return promise


def trio_py_to_js(pyawaitable):
    if _js_nursery is None:
        if trio.lowlevel.in_trio_task():
            raise RuntimeError(
                "JavaScript wants to await a Trio coroutine, but no "
                "trio.pyodide.open_js_nursery() is open to run it in"
            )
        # Nothing to do with Trio: let the previous hook (asyncio) run it.
        return _previous.py_to_js(pyawaitable)
    return _promise_for(_js_nursery, pyawaitable)


def callable_from_js(nursery, async_fn):
    """Still useful: it is the only way for JavaScript to pass an AbortSignal."""
    return lambda *args, signal=None: _promise_for(nursery, async_fn(*args), signal)
```

`_promise_for` is today's `callable_from_js` body: make the promise with
`Promise.new`, start a task in the nursery that awaits the awaitable,
resolve with `to_js(result)`, reject with an `Error` carrying the Python
traceback as `stack`, map an `AbortSignal` onto a cancel scope, and reject
with `"TrioCancelled"` when Trio cancels it. Exceptions go to the
JavaScript caller only, not to the nursery.

The nursery is a module-level value rather than a context variable because
JavaScript calls arrive in host context, outside any Trio task's context.
Trio cannot tell a coroutine's flavour before running it, hence the policy:
an open JS nursery claims it; otherwise it goes to the previous hook; and a
Trio task handing a coroutine to JavaScript with no nursery open gets a
clear error instead of asyncio's "Task got bad yield".

## Installation, and what gets simpler

```python
_previous = ffi.get_awaitable_hooks()
ffi.set_awaitable_hooks(js_to_py=trio_js_to_py, py_to_js=trio_py_to_js)
```

Once, at `import trio.pyodide`, chaining to whatever was there, because the
dispatch above already keeps asyncio working. (Installing and restoring per
run, the way Trio handles `sys.set_asyncgen_hooks`, would work too.)

With the hooks in place, `wait_promise`, `call_method` and the JavaScript
call helper all disappear from `trio.pyodide`, and `fetch` becomes

```python
controller = run_js("new AbortController()")
with abort_on_cancel(controller):
    response = await js.fetch(url, init)   # init.signal = controller.signal
    text = await response.text()
```

## Open questions

- There is no way for JavaScript to pass an `AbortSignal` through a plain
  `await` of a Python coroutine, so `callable_from_js` would stay for that.
- Flavour detection uses `trio.lowlevel.in_trio_task()`. A sniffio-style
  "current async library" query would let the hook defer to the right
  framework among several, rather than just to "the previous one".
- Whether `JsAwaitable` should also offer `then`, `catch` and `finally_`
  for symmetry with the PEP's `WebFuture`; delegating them to the raw
  promise is trivial.
- Lazy conversion in step 7 would be preferable to eager conversion with a
  hook: it keeps promise identity on the way back to JavaScript and avoids
  creating anything at all for promises that are never awaited.

## How `trio.pyodide` manages without the hooks today

Both workarounds rest on the same observation: the asyncio coupling happens
at the moment an awaitable crosses the boundary, so each arranges for the
awaitable never to cross.

- `trio.pyodide.call` and `call_method` do not call the JavaScript function
  from Python. They hand the function, its receiver and its arguments to a
  JavaScript helper, which calls it there, attaches the handlers there, and
  passes only the settled value to Python. The `this` of a method is lost
  when a method proxy is handed back to JavaScript, which is why
  `call_method` exists. A promise that *has* already crossed into Python is
  an `asyncio.Future` subclass; `wait_promise` waits on it through its
  `then` method, which runs through asyncio's callbacks and the
  `WebLoop`'s `setTimeout`, so it is the second choice.
- `trio.pyodide.callable_from_js` never gives JavaScript a coroutine. It
  gives it a synchronous Python function that creates a real JavaScript
  promise, starts the task with `nursery.start_soon`, and returns the
  promise, so Pyodide's `ensure_future` conversion never runs.
