"""Bounds the 30-second invocation-local wait ceiling (ADR-006).

Tracks only the total duration this invocation has intentionally slept, not
elapsed wall-clock time: HTTP execution and SQLite busy-waiting are
separately bounded and must not themselves consume this budget, per the
approved specification ("One CLI invocation may intentionally sleep for at
most 30 seconds in total ... HTTP execution time and SQLite busy waiting
are separately bounded and are not counted as retry sleep"). Every sleep
still goes through the injected `Sleeper`, whose real implementation is
itself monotonic-clock-based (`asyncio.sleep`). `wait_while_renewing_lease`
additionally chunks a wait longer than the 20-second renewal cadence so an
actively held lease never goes that long without being renewed.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from ..domain.clock import Clock, parse_timestamp
from ..domain.sleeper import Sleeper

CEILING_SECONDS = 30.0
RENEWAL_CADENCE_SECONDS = 20.0


class WaitBudget:
    def __init__(
        self,
        sleeper: Sleeper,
        *,
        ceiling_seconds: float = CEILING_SECONDS,
    ) -> None:
        self._sleeper = sleeper
        self._ceiling_seconds = ceiling_seconds
        self._slept_seconds = 0.0

    def remaining_seconds(self) -> float:
        return max(0.0, self._ceiling_seconds - self._slept_seconds)

    async def sleep(self, seconds: float) -> None:
        await self._sleeper.sleep(seconds)
        self._slept_seconds += seconds


async def wait_if_it_fits(
    wait_budget: WaitBudget, clock: Clock, not_before: str
) -> bool:
    """Sleep out the delay to `not_before` if it fits the remaining

    invocation wait budget, and return `True` (the caller should
    reauthorize). Returns `False` (the caller should report a deferred,
    scheduled, or ambiguous result) otherwise. Shared by submission retry
    scheduling and automatic reconciliation.
    """

    delay_seconds = max(
        0.0, (parse_timestamp(not_before) - clock.now()).total_seconds()
    )
    if delay_seconds > wait_budget.remaining_seconds():
        return False
    await wait_budget.sleep(delay_seconds)
    return True


async def wait_while_renewing_lease(
    wait_budget: WaitBudget,
    clock: Clock,
    not_before: str,
    renew: Callable[[], Awaitable[bool]],
) -> bool | None:
    """Like `wait_if_it_fits`, but for a wait during which an active lease

    must be kept alive (reconciliation's own deferred-authorization wait):
    chunks any sleep longer than the renewal cadence into pieces, calling
    `renew` (which must return whether the lease is still owned) between
    chunks so no interval between successful renewals exceeds that cadence.

    Returns `True` if the full delay was slept out (the caller should
    reauthorize), `False` if the delay does not fit the remaining wait
    budget (report deferred/scheduled/ambiguous), or `None` if a renewal
    failed partway through (ownership lost -- the caller must stop
    immediately rather than treat the wait as satisfied).
    """

    delay_seconds = max(
        0.0, (parse_timestamp(not_before) - clock.now()).total_seconds()
    )
    if delay_seconds > wait_budget.remaining_seconds():
        return False

    remaining_delay = delay_seconds
    while remaining_delay > 0:
        chunk = min(remaining_delay, RENEWAL_CADENCE_SECONDS)
        await wait_budget.sleep(chunk)
        remaining_delay -= chunk
        if remaining_delay > 0 and not await renew():
            return None
    return True
