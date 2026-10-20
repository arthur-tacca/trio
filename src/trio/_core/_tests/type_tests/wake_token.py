"""Typing of WakeToken's constructor and reschedule() overloads."""

from __future__ import annotations

import outcome
from trio.lowlevel import WakeToken
from typing_extensions import assert_type


class Lot: ...


class Foo: ...


def cancel_none(token: WakeToken[None, None]) -> None: ...


def cancel_lot(token: WakeToken[None, Lot]) -> None: ...


def cancel_opt(token: WakeToken[Foo | None]) -> None: ...


def cancel_int(token: WakeToken[int]) -> None: ...


def check_construction() -> None:
    assert_type(WakeToken(cancel_none), "WakeToken[None, None]")
    assert_type(WakeToken(cancel_lot, Lot()), "WakeToken[None, Lot]")
    # Nothing pins the value type here, so it takes its default
    assert_type(WakeToken(WakeToken.reschedule_cancelled), "WakeToken[None, None]")
    # data is required when its type isn't None, and checked when given
    WakeToken(cancel_lot)  # type: ignore
    WakeToken(cancel_lot, Foo())  # type: ignore


def check_reschedule() -> None:
    # The outcome may be left out only if None is a valid value for the token
    WakeToken(cancel_none).reschedule()
    WakeToken(cancel_opt).reschedule()
    WakeToken(cancel_int).reschedule(outcome.Value(3))
    WakeToken(cancel_int).reschedule()  # type: ignore
    # It's an outcome or nothing, never None itself
    WakeToken(cancel_int).reschedule(None)  # type: ignore
    WakeToken(cancel_none).reschedule(None)  # type: ignore
