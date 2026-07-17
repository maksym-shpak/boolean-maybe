"""Unit tests for the domain layer: idempotency-key grammar/generation,
identifier generation, the injectable clock/timestamp formatting, and the
reliability-feature clock/randomness/sleeping abstractions.
"""

from __future__ import annotations

import asyncio
import re
import time
from datetime import datetime, timedelta, timezone

import pytest

from boolean_maybe.domain import clock as clock_mod
from boolean_maybe.domain import identifiers
from boolean_maybe.domain import idempotency_key
from boolean_maybe.domain import monotonic_clock as monotonic_clock_mod
from boolean_maybe.domain import random_source as random_source_mod
from boolean_maybe.domain import sleeper as sleeper_mod


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


def test_add_milliseconds_advances_instant() -> None:
    instant = datetime(2026, 7, 17, 12, 0, 0, tzinfo=timezone.utc)
    later = clock_mod.add_milliseconds(instant, 1500)
    assert later == instant + timedelta(milliseconds=1500)


def test_parse_timestamp_round_trips_format_timestamp() -> None:
    instant = datetime(2026, 7, 17, 12, 0, 0, 123456, tzinfo=timezone.utc)
    text = clock_mod.format_timestamp(instant)
    assert clock_mod.parse_timestamp(text) == instant


class _FixedClock:
    def __init__(self, instant: datetime) -> None:
        self._instant = instant

    def now(self) -> datetime:
        return self._instant


def test_injected_clock_can_return_equal_instants_for_ordering_tests() -> None:
    fixed = _FixedClock(datetime(2026, 7, 17, 0, 0, 0, tzinfo=timezone.utc))
    assert fixed.now() == fixed.now()


# -- Monotonic clock -----------------------------------------------------------


def test_system_monotonic_clock_advances() -> None:
    clock = monotonic_clock_mod.SystemMonotonicClock()
    first = clock.now()
    time.sleep(0.001)
    second = clock.now()
    assert second >= first


class _FixedMonotonicClock:
    def __init__(self, instant: float) -> None:
        self.instant = instant

    def now(self) -> float:
        return self.instant


def test_injected_monotonic_clock_is_independent_of_wall_clock() -> None:
    fixed = _FixedMonotonicClock(1000.0)
    assert fixed.now() == 1000.0
    fixed.instant += 30.0
    assert fixed.now() == 1030.0


# -- Random source ---------------------------------------------------------


def test_system_random_source_respects_inclusive_bounds() -> None:
    source = random_source_mod.SystemRandomSource()
    for _ in range(100):
        value = source.randint(0, 5)
        assert 0 <= value <= 5


def test_system_random_source_allows_degenerate_zero_width_range() -> None:
    source = random_source_mod.SystemRandomSource()
    assert source.randint(0, 0) == 0


class _SequenceRandomSource:
    def __init__(self, *values: int) -> None:
        self._values = iter(values)

    def randint(self, low: int, high: int) -> int:
        return next(self._values)


def test_injected_random_source_is_deterministic() -> None:
    source = _SequenceRandomSource(0, 500)
    assert source.randint(0, 500) == 0
    assert source.randint(0, 500) == 500


# -- Sleeper -----------------------------------------------------------------


def test_real_sleeper_sleeps_for_real_duration() -> None:
    # A generous requested duration and lower bound (rather than a tight
    # one) avoid flaking on coarse Windows timer resolution while still
    # failing a no-op `sleep()` implementation, which would return near `0`.
    async def _run() -> float:
        start = time.monotonic()
        await sleeper_mod.RealSleeper().sleep(0.2)
        return time.monotonic() - start

    elapsed = asyncio.run(_run())
    assert elapsed >= 0.1


def test_real_sleeper_is_cancellable() -> None:
    async def _run() -> None:
        task = asyncio.ensure_future(sleeper_mod.RealSleeper().sleep(10.0))
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(_run())


class _RecordingSleeper:
    def __init__(self) -> None:
        self.requested_seconds: list[float] = []

    async def sleep(self, seconds: float) -> None:
        self.requested_seconds.append(seconds)


def test_injected_sleeper_records_requested_duration_without_real_delay() -> None:
    async def _run() -> None:
        sleeper = _RecordingSleeper()
        await sleeper.sleep(30.0)
        assert sleeper.requested_seconds == [30.0]

    asyncio.run(_run())
