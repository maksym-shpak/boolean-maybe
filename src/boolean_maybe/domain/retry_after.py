"""`Retry-After` parsing shared by submission and reconciliation `429` evidence.

ADR-006 accepts exactly one `Retry-After` value on a delivered `429`, in
either non-negative decimal delta-seconds or IMF-fixdate form with an
explicit UTC (`GMT`) interpretation. A valid future value no greater than
3,600,000 milliseconds becomes a non-negative interval; missing, repeated,
syntactically invalid, negative, elapsed, overflowing, or greater-than-one-
hour values are rejected in favor of policy jitter. The raw header text must
never cross this boundary into persistence -- only the parsed millisecond
value or a sanitized rejection diagnostic does.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from datetime import datetime, timezone

MAX_SERVER_DELAY_MS = 3_600_000

_NON_NEGATIVE_DELTA_SECONDS_RE = re.compile(r"^[0-9]+$")
_NEGATIVE_DELTA_SECONDS_RE = re.compile(r"^-[0-9]+$")
_IMF_FIXDATE_FORMAT = "%a, %d %b %Y %H:%M:%S GMT"


@dataclass(frozen=True)
class RetryAfterAccepted:
    server_delay_ms: int


@dataclass(frozen=True)
class RetryAfterRejected:
    diagnostic: str


RetryAfterResult = RetryAfterAccepted | RetryAfterRejected


def parse_retry_after(
    raw_values: tuple[str, ...], *, now: datetime
) -> RetryAfterResult:
    """Parse the `Retry-After` header value(s) observed on a delivered `429`.

    `raw_values` holds one string per occurrence of the header exactly as
    received; the caller must not pre-join repeated occurrences, since more
    than one occurrence is itself a rejection condition. `now` must be
    timezone-aware UTC.
    """

    if len(raw_values) == 0:
        return RetryAfterRejected("retry_after_missing")
    if len(raw_values) > 1:
        return RetryAfterRejected("retry_after_repeated")

    value = raw_values[0].strip()

    if _NEGATIVE_DELTA_SECONDS_RE.fullmatch(value):
        return RetryAfterRejected("retry_after_negative")

    if _NON_NEGATIVE_DELTA_SECONDS_RE.fullmatch(value):
        return _accept_delay_ms(int(value) * 1000)

    return _parse_imf_fixdate(value, now=now)


def _parse_imf_fixdate(value: str, *, now: datetime) -> RetryAfterResult:
    try:
        parsed = datetime.strptime(value, _IMF_FIXDATE_FORMAT).replace(
            tzinfo=timezone.utc
        )
    except ValueError:
        return RetryAfterRejected("retry_after_malformed")

    delta_seconds = (parsed - now).total_seconds()
    if delta_seconds <= 0:
        return RetryAfterRejected("retry_after_elapsed")

    # Round up so the accepted server instant is never shortened by the
    # sub-second precision difference between a date's second resolution
    # and `now`'s microsecond resolution.
    return _accept_delay_ms(math.ceil(delta_seconds * 1000))


def _accept_delay_ms(delay_ms: int) -> RetryAfterResult:
    if delay_ms > MAX_SERVER_DELAY_MS:
        return RetryAfterRejected("retry_after_too_large")
    return RetryAfterAccepted(delay_ms)
