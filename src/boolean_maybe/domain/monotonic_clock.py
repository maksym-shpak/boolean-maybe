"""Injectable monotonic clock for invocation-local timing.

Durable eligibility (Job retry eligibility, the service rate-limit gate) uses
UTC wall-clock instants from `domain.clock.Clock` so it survives process and
host restart. The monotonic clock here is used only for invocation-local
bookkeeping that must never be affected by wall-clock adjustment: the
30-second invocation wait ceiling, the 10-second takeover quarantine, and the
requirement that no interval between successful lease renewals exceeds 20
seconds. It is never compared against durable state.
"""

from __future__ import annotations

import time
from typing import Protocol


class MonotonicClock(Protocol):
    def now(self) -> float: ...


class SystemMonotonicClock:
    """Returns the real monotonic clock reading in seconds."""

    def now(self) -> float:
        return time.monotonic()
