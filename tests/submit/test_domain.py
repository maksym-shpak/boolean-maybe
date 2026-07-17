"""Unit tests for the domain layer: idempotency-key grammar/generation,
identifier generation, and the injectable clock/timestamp formatting.
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone

import pytest

from boolean_maybe.domain import clock as clock_mod
from boolean_maybe.domain import identifiers
from boolean_maybe.domain import idempotency_key


# -- Idempotency Key grammar -------------------------------------------------


@pytest.mark.parametrize(
    "value",
    ["a", "A" * 128, "job-a", "job_a.b~c-1", "0123456789"],
)
def test_accepted_key_values(value: str) -> None:
    assert idempotency_key.is_accepted_key(value)


@pytest.mark.parametrize(
    "value",
    [
        "",
        "A" * 129,
        "job a",  # whitespace
        "job/a",  # separator
        "job*a",  # reserved wildcard character
        "café",  # non-ASCII
    ],
)
def test_rejected_key_values(value: str) -> None:
    assert not idempotency_key.is_accepted_key(value)


def test_generated_key_matches_expected_format() -> None:
    key = idempotency_key.generate_key()
    assert re.fullmatch(r"job_[0-9a-f]{32}", key)
    assert idempotency_key.is_accepted_key(key)


def test_generated_key_is_not_derived_from_payload() -> None:
    first = idempotency_key.generate_key()
    second = idempotency_key.generate_key()
    assert first != second


# -- Identifiers --------------------------------------------------------------


def test_generated_id_matches_uuid4_canonical_form() -> None:
    value = identifiers.generate_id()
    assert re.fullmatch(
        r"[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}", value
    )


def test_generated_ids_are_independent() -> None:
    assert identifiers.generate_id() != identifiers.generate_id()


# -- Clock and timestamp formatting -------------------------------------------


def test_format_timestamp_has_six_fractional_digits_and_z_suffix() -> None:
    instant = datetime(2026, 7, 17, 12, 0, 0, 123456, tzinfo=timezone.utc)
    assert clock_mod.format_timestamp(instant) == "2026-07-17T12:00:00.123456Z"


def test_format_timestamp_converts_non_utc_to_utc() -> None:
    tz = timezone(timedelta(hours=2))
    instant = datetime(2026, 7, 17, 14, 0, 0, 0, tzinfo=tz)
    assert clock_mod.format_timestamp(instant) == "2026-07-17T12:00:00.000000Z"


def test_format_timestamp_rejects_naive_datetime() -> None:
    with pytest.raises(ValueError):
        clock_mod.format_timestamp(datetime(2026, 7, 17, 12, 0, 0))


def test_system_clock_returns_timezone_aware_utc_instant() -> None:
    instant = clock_mod.SystemClock().now()
    assert instant.tzinfo is not None
    assert instant.utcoffset() == timedelta(0)


def test_add_seconds_advances_instant() -> None:
    instant = datetime(2026, 7, 17, 12, 0, 0, tzinfo=timezone.utc)
    later = clock_mod.add_seconds(instant, 60)
    assert later == instant + timedelta(seconds=60)


class _FixedClock:
    def __init__(self, instant: datetime) -> None:
        self._instant = instant

    def now(self) -> datetime:
        return self._instant


def test_injected_clock_can_return_equal_instants_for_ordering_tests() -> None:
    fixed = _FixedClock(datetime(2026, 7, 17, 0, 0, 0, tzinfo=timezone.utc))
    assert fixed.now() == fixed.now()
