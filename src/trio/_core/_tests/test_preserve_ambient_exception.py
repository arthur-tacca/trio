from __future__ import annotations

import sys
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

import pytest

import trio

from ... import _core
from ...lowlevel import preserve_ambient_exception

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from types import TracebackType

    from typing_extensions import Self

if sys.version_info < (3, 11):
    from exceptiongroup import BaseExceptionGroup


def _cancelled() -> _core.Cancelled:
    return _core.Cancelled._create()


def test_suppresses_cancelled_when_handling_exception() -> None:
    original = ValueError("original")
    with pytest.raises(ValueError, match=r"^original$") as excinfo:
        try:
            raise original
        finally:
            with preserve_ambient_exception():
                raise _cancelled()
    assert excinfo.value is original
    assert original.__context__ is None


def test_cancelled_propagates_without_ambient_exception() -> None:
    cancelled = _cancelled()
    with pytest.raises(_core.Cancelled) as excinfo:
        with preserve_ambient_exception():
            raise cancelled
    assert excinfo.value is cancelled


def test_cancelled_propagates_after_exception_was_handled() -> None:
    # An exception that has already been caught is no longer "ambient".
    try:
        raise ValueError("handled")
    except ValueError:
        pass
    with pytest.raises(_core.Cancelled):
        with preserve_ambient_exception():
            raise _cancelled()


def test_other_exceptions_propagate() -> None:
    body_exc = KeyError("from body")

    # ... when there is no ambient exception
    with pytest.raises(KeyError) as excinfo:
        with preserve_ambient_exception():
            raise body_exc
    assert excinfo.value is body_exc

    # ... and when there is one, with the usual implicit chaining
    original = ValueError("original")
    with pytest.raises(KeyError) as excinfo:
        try:
            raise original
        finally:
            with preserve_ambient_exception():
                raise body_exc
    assert excinfo.value is body_exc
    assert body_exc.__context__ is original


def test_group_of_only_cancelled_is_suppressed() -> None:
    original = ValueError("original")
    with pytest.raises(ValueError, match=r"^original$") as excinfo:
        try:
            raise original
        finally:
            with preserve_ambient_exception():
                raise BaseExceptionGroup(
                    "outer",
                    [
                        _cancelled(),
                        BaseExceptionGroup("inner", [_cancelled(), _cancelled()]),
                    ],
                )
    assert excinfo.value is original


def test_group_containing_other_exceptions_propagates() -> None:
    # Regression test for a bug in an earlier version of this helper, which
    # used BaseExceptionGroup.subgroup() with a predicate. That predicate is
    # also applied to the group nodes themselves, which are never Cancelled,
    # so a group of only Cancelled exceptions was wrongly treated as
    # containing something else.
    group = BaseExceptionGroup("mixed", [_cancelled(), KeyError("from body")])
    with pytest.raises(BaseExceptionGroup) as excinfo:
        try:
            raise ValueError("original")
        finally:
            with preserve_ambient_exception():
                raise group
    assert excinfo.value is group

    # ... including when the non-Cancelled exception is nested
    nested = BaseExceptionGroup(
        "outer",
        [_cancelled(), BaseExceptionGroup("inner", [KeyError("from body")])],
    )
    with pytest.raises(BaseExceptionGroup) as excinfo:
        try:
            raise ValueError("original")
        finally:
            with preserve_ambient_exception():
                raise nested
    assert excinfo.value is nested


def test_group_without_ambient_exception_propagates() -> None:
    group = BaseExceptionGroup("only cancelled", [_cancelled()])
    with pytest.raises(BaseExceptionGroup) as excinfo:
        with preserve_ambient_exception():
            raise group
    assert excinfo.value is group


def test_explicit_ambient_exception() -> None:
    # Passing an exception explicitly works even when nothing is being
    # handled, e.g. when an __exit__ method is called by hand.
    with preserve_ambient_exception(ValueError("explicit")):
        raise _cancelled()

    # Passing None explicitly disables the suppression even when an
    # exception is being handled.
    with pytest.raises(_core.Cancelled):
        try:
            raise ValueError("original")
        finally:
            with preserve_ambient_exception(None):
                raise _cancelled()


def test_enter_returns_self_and_instance_is_reusable() -> None:
    cm = preserve_ambient_exception()
    with cm as entered:
        assert entered is cm

    # First use: an ambient exception is present, so Cancelled is suppressed.
    with pytest.raises(ValueError, match=r"^original$"):
        try:
            raise ValueError("original")
        finally:
            with cm:
                raise _cancelled()

    # Second use: no ambient exception, so the stale one from the first use
    # must not cause Cancelled to be suppressed.
    with pytest.raises(_core.Cancelled):
        with cm:
            raise _cancelled()


def test_does_not_hold_reference_to_exception_after_exit() -> None:
    cm = preserve_ambient_exception()
    try:
        raise ValueError("original")
    except ValueError:
        with cm:
            pass
    assert cm._captured is None


async def test_asynccontextmanager_usage() -> None:
    cleanup_finished = False

    @asynccontextmanager
    async def my_cm() -> AsyncIterator[None]:
        nonlocal cleanup_finished
        try:
            yield
        finally:
            with preserve_ambient_exception():
                await trio.lowlevel.checkpoint()
                cleanup_finished = True  # pragma: no cover

    with trio.CancelScope() as scope:
        with pytest.raises(ValueError, match=r"^original$"):
            async with my_cm():
                scope.cancel()
                raise ValueError("original")
        # The Cancelled was swallowed rather than reaching the scope...
        assert not scope.cancelled_caught
        # ... but the scope is still cancelled, so the next checkpoint
        # raises Cancelled again.
        await trio.lowlevel.checkpoint()
        raise AssertionError("unreachable")  # pragma: no cover
    assert scope.cancelled_caught
    assert not cleanup_finished


async def test_asynccontextmanager_usage_without_body_exception() -> None:
    @asynccontextmanager
    async def my_cm() -> AsyncIterator[None]:
        try:
            yield
        finally:
            with preserve_ambient_exception():
                await trio.lowlevel.checkpoint()

    # With no exception from the body, cancellation propagates as usual.
    with trio.CancelScope() as scope:
        async with my_cm():
            scope.cancel()
        raise AssertionError("unreachable")  # pragma: no cover
    assert scope.cancelled_caught


async def test_aexit_usage() -> None:
    class Resource:
        async def __aenter__(self) -> Self:
            return self

        async def __aexit__(
            self,
            exc_type: type[BaseException] | None,
            exc_value: BaseException | None,
            traceback: TracebackType | None,
        ) -> None:
            with preserve_ambient_exception(exc_value):
                await trio.lowlevel.checkpoint()

    with trio.CancelScope() as scope:
        with pytest.raises(ValueError, match=r"^original$"):
            async with Resource():
                scope.cancel()
                raise ValueError("original")
        assert not scope.cancelled_caught

    # Nested cancel scopes produce a plain Cancelled too, and an exception
    # group from a nursery inside the cleanup is handled as well.
    class NurseryResource:
        async def __aenter__(self) -> Self:
            return self

        async def __aexit__(
            self,
            exc_type: type[BaseException] | None,
            exc_value: BaseException | None,
            traceback: TracebackType | None,
        ) -> None:
            with preserve_ambient_exception(exc_value):
                async with trio.open_nursery(strict_exception_groups=True) as nursery:
                    nursery.start_soon(trio.sleep_forever)
                    nursery.start_soon(trio.sleep_forever)

    with trio.CancelScope() as scope:
        with pytest.raises(ValueError, match=r"^original$"):
            async with NurseryResource():
                scope.cancel()
                raise ValueError("original")
        assert not scope.cancelled_caught
