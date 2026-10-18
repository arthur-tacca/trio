from __future__ import annotations

import pytest

import trio.lowlevel

from ... import _core
from ...testing import wait_all_tasks_blocked


async def test_task_exit_callbacks_run_on_exit() -> None:
    record: list[tuple[str, _core.Task]] = []

    def first(task: _core.Task) -> None:
        record.append(("first", task))

    def second(task: _core.Task) -> None:
        record.append(("second", task))

    go = trio.Event()

    async def child() -> None:
        task = _core.current_task()
        _core.add_task_exit_callback(task, first)
        _core.add_task_exit_callback(task, second)
        _core.add_task_exit_callback(task, first)  # duplicates are fine
        await go.wait()
        # the callbacks don't run until we actually exit
        assert record == []

    async with _core.open_nursery() as nursery:
        nursery.start_soon(child)
        await wait_all_tasks_blocked()
        assert record == []
        go.set()

    tasks = {task for _, task in record}
    assert len(tasks) == 1
    assert [name for name, _ in record] == ["first", "second", "first"]


async def test_task_exit_callbacks_run_before_the_nursery_sees_the_exit() -> None:
    # The callback runs while the task is still, as far as its nursery is
    # concerned, alive: a lock breaker relies on this to wake waiters before
    # the parent can observe the task's outcome.
    seen_in_callback: list[list[_core.Task]] = []

    async with _core.open_nursery() as nursery:

        def callback(task: _core.Task) -> None:
            seen_in_callback.append(list(nursery.child_tasks))

        async def child() -> None:
            _core.add_task_exit_callback(_core.current_task(), callback)

        nursery.start_soon(child)

    assert len(seen_in_callback) == 1
    assert len(seen_in_callback[0]) == 1


async def test_task_exit_callbacks_can_be_removed() -> None:
    record: list[str] = []

    def callback(task: _core.Task) -> None:
        record.append("ran")

    async def child() -> None:
        task = _core.current_task()
        _core.add_task_exit_callback(task, callback)
        _core.add_task_exit_callback(task, callback)
        _core.remove_task_exit_callback(task, callback)

    async with _core.open_nursery() as nursery:
        nursery.start_soon(child)
    assert record == ["ran"]

    # bound methods of the same object compare equal, so they can be
    # registered and removed by value, which is what the lot breaker relies on
    class Thing:
        def method(self, task: _core.Task) -> None:
            record.append("method")

    thing = Thing()
    task = _core.current_task()
    _core.add_task_exit_callback(task, thing.method)
    _core.remove_task_exit_callback(task, thing.method)
    assert not task._exit_callbacks


async def test_task_exit_callbacks_misuse() -> None:
    def callback(task: _core.Task) -> None:  # pragma: no cover
        raise AssertionError("should not run")

    task = _core.current_task()
    with pytest.raises(RuntimeError, match="not registered as an exit callback"):
        _core.remove_task_exit_callback(task, callback)

    # once a task has exited, it's too late to register anything
    exited: _core.Task | None = None

    async def child() -> None:
        nonlocal exited
        exited = _core.current_task()

    async with _core.open_nursery() as nursery:
        nursery.start_soon(child)
    assert exited is not None
    with pytest.raises(RuntimeError, match="has already exited"):
        _core.add_task_exit_callback(exited, callback)
    with pytest.raises(RuntimeError, match="not registered as an exit callback"):
        _core.remove_task_exit_callback(exited, callback)
