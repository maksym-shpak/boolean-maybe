"""Unit tests for `domain.retry_after.parse_retry_after`.

ADR-006 / `docs/specs/features/reliable-job-submission.md` accept exactly one
`Retry-After` value, in delta-seconds or IMF-fixdate form, converting a valid
future value no greater than one hour to a non-negative millisecond interval.
Everything else falls back to policy jitter with a sanitized diagnostic.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from boolean_maybe.domain import retry_after

NOW = datetime(2026, 7, 17, 12, 0, 0, tzinfo=timezone.utc)


def _imf_fixdate(instant: datetime) -> str:
    return instant.strftime("%a, %d %b %Y %H:%M:%S GMT")


# -- Delta-seconds form --------------------------------------------------------


@pytest.mark.parametrize(
    ("raw", "expected_ms"),
    [("0", 0), ("1", 1000), ("3600", 3_600_000)],
)
def test_accepts_valid_delta_seconds(raw: str, expected_ms: int) -> None:
    result = retry_after.parse_retry_after((raw,), now=NOW)
    assert result == retry_after.RetryAfterAccepted(expected_ms)


def test_rejects_delta_seconds_beyond_one_hour() -> None:
    result = retry_after.parse_retry_after(("3601",), now=NOW)
    assert isinstance(result, retry_after.RetryAfterRejected)
    assert result.diagnostic == "retry_after_too_large"


def test_rejects_negative_delta_seconds() -> None:
    result = retry_after.parse_retry_after(("-5",), now=NOW)
    assert result == retry_after.RetryAfterRejected("retry_after_negative")


@pytest.mark.parametrize("raw", ["1.5", "+5", "5s", "abc", ""])
def test_rejects_malformed_values(raw: str) -> None:
    result = retry_after.parse_retry_after((raw,), now=NOW)
    assert result == retry_after.RetryAfterRejected("retry_after_malformed")


# -- Missing / repeated ---------------------------------------------------------


def test_rejects_missing_value() -> None:
    result = retry_after.parse_retry_after((), now=NOW)
    assert result == retry_after.RetryAfterRejected("retry_after_missing")


def test_rejects_repeated_value() -> None:
    result = retry_after.parse_retry_after(("1", "2"), now=NOW)
    assert result == retry_after.RetryAfterRejected("retry_after_repeated")


# -- IMF-fixdate form -----------------------------------------------------------


def test_accepts_valid_future_imf_fixdate() -> None:
    future = NOW + timedelta(seconds=5)
    result = retry_after.parse_retry_after((_imf_fixdate(future),), now=NOW)
    assert result == retry_after.RetryAfterAccepted(5000)


def test_accepts_imf_fixdate_exactly_one_hour_in_future() -> None:
    future = NOW + timedelta(hours=1)
    result = retry_after.parse_retry_after((_imf_fixdate(future),), now=NOW)
    assert result == retry_after.RetryAfterAccepted(3_600_000)


def test_rejects_imf_fixdate_beyond_one_hour() -> None:
    future = NOW + timedelta(hours=1, seconds=1)
    result = retry_after.parse_retry_after((_imf_fixdate(future),), now=NOW)
    assert result == retry_after.RetryAfterRejected("retry_after_too_large")


def test_rejects_elapsed_imf_fixdate() -> None:
    past = NOW - timedelta(seconds=1)
    result = retry_after.parse_retry_after((_imf_fixdate(past),), now=NOW)
    assert result == retry_after.RetryAfterRejected("retry_after_elapsed")


def test_rejects_imf_fixdate_equal_to_now() -> None:
    result = retry_after.parse_retry_after((_imf_fixdate(NOW),), now=NOW)
    assert result == retry_after.RetryAfterRejected("retry_after_elapsed")


def test_rejects_non_gmt_timezone_label() -> None:
    future = NOW + timedelta(seconds=5)
    raw = future.strftime("%a, %d %b %Y %H:%M:%S") + " UTC"
    result = retry_after.parse_retry_after((raw,), now=NOW)
    assert result == retry_after.RetryAfterRejected("retry_after_malformed")


def test_rejects_obsolete_asctime_format() -> None:
    future = NOW + timedelta(seconds=5)
    raw = future.strftime("%a %b %d %H:%M:%S %Y")
    result = retry_after.parse_retry_after((raw,), now=NOW)
    assert result == retry_after.RetryAfterRejected("retry_after_malformed")


def test_imf_fixdate_never_shortens_the_server_instant_via_rounding() -> None:
    # `now` is 0.5s ahead of the top of a fresh second boundary; the true gap
    # to `future` is 4.5s, which must round *up* to 4500ms exactly, and never
    # down to a value that would let the client retry earlier than asked.
    now_with_fraction = NOW.replace(microsecond=500_000)
    future = NOW + timedelta(seconds=5)
    result = retry_after.parse_retry_after(
        (_imf_fixdate(future),), now=now_with_fraction
    )
    assert result == retry_after.RetryAfterAccepted(4500)


def test_imf_fixdate_rounds_up_sub_millisecond_remainder() -> None:
    # True gap is 4.999737s -- must round up to 5000ms, never down to 4999ms.
    now_with_fraction = NOW.replace(microsecond=263)
    future = NOW + timedelta(seconds=5)
    result = retry_after.parse_retry_after(
        (_imf_fixdate(future),), now=now_with_fraction
    )
    assert result == retry_after.RetryAfterAccepted(5000)
