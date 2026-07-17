"""Full-jitter exponential backoff scheduling (ADR-006).

Pure and stateless, so the persistence layer can compute the effective
retry delay atomically with the same `now` used for `completed_at` and gate
advancement, while the application workflow reuses the same formula for its
own bounded-wait scheduling.
"""

from __future__ import annotations

from .random_source import RandomSource

MAX_CAP_MS = 30_000
BASE_DELAY_MS = 500


def backoff_cap_ms(request_ordinal: int) -> int:
    """`min(30_000, 500 * 2 ** (n - 1))` for safe-retry request ordinal `n`."""

    return min(MAX_CAP_MS, BASE_DELAY_MS * (2 ** (request_ordinal - 1)))


def policy_delay_ms(request_ordinal: int, random_source: RandomSource) -> int:
    """Uniform integer in `[0, backoff_cap_ms(request_ordinal)]`, inclusive."""

    return random_source.randint(0, backoff_cap_ms(request_ordinal))


def effective_delay_ms(policy_ms: int, server_ms: int | None) -> int:
    """`max(policy_delay_ms, server_delay_ms)`, or just `policy_ms` when no

    valid server-required delay was accepted.
    """

    if server_ms is None:
        return policy_ms
    return max(policy_ms, server_ms)
