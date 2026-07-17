"""Injectable randomness for full-jitter retry/backoff scheduling.

ADR-006 requires full-jitter backoff (`policy_delay_ms(n) = uniform integer in
[0, cap_ms(n)]`) with an injected deterministic random source so tests can
cover both interval bounds. Randomness affects scheduling only; it never
influences evidence classification or identity.
"""

from __future__ import annotations

import random
from typing import Protocol


class RandomSource(Protocol):
    def randint(self, low: int, high: int) -> int:
        """Return a uniformly distributed integer in `[low, high]` inclusive."""
        ...


class SystemRandomSource:
    """Draws from a cryptographically secure random source."""

    def __init__(self) -> None:
        self._random = random.SystemRandom()

    def randint(self, low: int, high: int) -> int:
        return self._random.randint(low, high)
