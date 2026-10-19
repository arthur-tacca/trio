"""Typing of WakeToken's constructor and reschedule() overloads."""

from __future__ import annotations

import outcome
from trio.lowlevel import WakeToken
from typing_extensions import assert_type


class Lot: ...


class Foo: ...


def abort_none(token: WakeToken[None, None]) -> None: ...


def abort_lot(token: WakeToken[None, Lot]) -> None: ...


def abort_opt(token: WakeToken[Foo | None]) -> None: ...


def abort_int(token: WakeToken[int]) -> None: ...


def check_construction() -> None:
    assert_type(WakeToken(abort_none), "WakeToken[None, None]")
    assert_type(WakeToken(abort_lot, Lot()), "WakeToken[None, Lot]")
    # Nothing pins the value type here, so it takes its default
    assert_type(WakeToken(WakeToken.reschedule_aborted), "WakeToken[None, None]")
    # data is required when its type isn't None, and checked when given
    WakeToken(abort_lot)  # type: ignore
    WakeToken(abort_lot, Foo())  # type: ignore


def check_reschedule() -> None:
    # The outcome may be left out only if None is a valid value for the token
    WakeToken(abort_none).reschedule()
    WakeToken(abort_opt).reschedule()
    WakeToken(abort_int).reschedule(outcome.Value(3))
    WakeToken(abort_int).reschedule()  # type: ignore
    # It's an outcome or nothing, never None itself
    WakeToken(abort_int).reschedule(None)  # type: ignore
    WakeToken(abort_none).reschedule(None)  # type: ignore
