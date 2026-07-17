"""Injectable UTC clock and RFC 3339 timestamp formatting.

Timestamps are UTC RFC 3339 strings with six fractional digits and the `Z`
suffix. The application uses one injectable clock so tests can control
`created_at`/`updated_at`/`started_at`/`completed_at` ordering, including the
boundary case where two events share the same instant.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Protocol


class Clock(Protocol):
    def now(self) -> datetime: ...


class SystemClock:
    """Returns the real current UTC instant."""

    def now(self) -> datetime:
        return datetime.now(timezone.utc)


def format_timestamp(instant: datetime) -> str:
    """Return `instant` as UTC RFC 3339 with six fractional digits and `Z`."""

    if instant.tzinfo is None:
        raise ValueError("instant must be timezone-aware")
    utc = instant.astimezone(timezone.utc)
    return utc.strftime("%Y-%m-%dT%H:%M:%S.") + f"{utc.microsecond:06d}Z"


def add_seconds(instant: datetime, seconds: float) -> datetime:
    return instant + timedelta(seconds=seconds)
