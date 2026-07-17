"""Unit tests for `application.waiting`: the invocation wait budget and the

plain / lease-renewing bounded-wait helpers.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from boolean_maybe.application import waiting
from boolean_maybe.domain import clock as clock_mod


class _FixedClock:
    def __init__(self, instant: datetime) -> None:
        self._instant = instant

    def now(self) -> datetime:
        return self._instant


class _FakeMonotonicClock:
    def __init__(self, start: float = 0.0) -> None:
        self.value = start

    def now(self) -> float:
        return self.value


class _FastSleeper:
    def __init__(self, monotonic_clock: _FakeMonotonicClock) -> None:
        self._monotonic_clock = monotonic_clock
        self.requested_seconds: list[float] = []

    async def sleep(self, seconds: float) -> None:
        self.requested_seconds.append(seconds)
        self._monotonic_clock.value += seconds


NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _not_before(seconds_from_now: float) -> str:
    return clock_mod.format_timestamp(clock_mod.add_seconds(NOW, seconds_from_now))


def test_wait_budget_remaining_seconds_decreases_only_by_actual_sleep_calls() -> None:
    monotonic_clock = _FakeMonotonicClock(100.0)
    budget = waiting.WaitBudget(_FastSleeper(monotonic_clock))
    assert budget.remaining_seconds() == 30.0
    asyncio.run(budget.sleep(10.0))
    assert budget.remaining_seconds() == 20.0


def test_wait_budget_remaining_seconds_ignores_elapsed_time_that_was_not_slept() -> (
    None
):
    # HTTP execution time and SQLite busy-waiting are separately bounded and
    # must not themselves consume this budget: advancing the monotonic clock
    # without going through `budget.sleep()` (e.g. while an HTTP call or a
    # SQLite transaction is in flight) must not reduce `remaining_seconds()`.
    monotonic_clock = _FakeMonotonicClock(100.0)
    budget = waiting.WaitBudget(_FastSleeper(monotonic_clock))
    assert budget.remaining_seconds() == 30.0
    monotonic_clock.value += 25.0  # simulated slow HTTP/SQLite work, not a sleep
    assert budget.remaining_seconds() == 30.0


def test_wait_if_it_fits_sleeps_and_returns_true_when_within_budget() -> None:
    monotonic_clock = _FakeMonotonicClock()
    sleeper = _FastSleeper(monotonic_clock)
    budget = waiting.WaitBudget(sleeper)

    result = asyncio.run(
        waiting.wait_if_it_fits(budget, _FixedClock(NOW), _not_before(5))
    )

    assert result is True
    assert sleeper.requested_seconds == [5.0]


def test_wait_if_it_fits_returns_false_when_delay_exceeds_budget() -> None:
    monotonic_clock = _FakeMonotonicClock()
    sleeper = _FastSleeper(monotonic_clock)
    budget = waiting.WaitBudget(sleeper)

    result = asyncio.run(
        waiting.wait_if_it_fits(budget, _FixedClock(NOW), _not_before(31))
    )

    assert result is False
    assert sleeper.requested_seconds == []


def test_wait_if_it_fits_treats_an_already_past_instant_as_zero_delay() -> None:
    monotonic_clock = _FakeMonotonicClock()
    sleeper = _FastSleeper(monotonic_clock)
    budget = waiting.WaitBudget(sleeper)

    result = asyncio.run(
        waiting.wait_if_it_fits(budget, _FixedClock(NOW), _not_before(-5))
    )

    assert result is True
    assert sleeper.requested_seconds == [0.0]


def test_wait_while_renewing_lease_chunks_long_delay_and_renews_between_chunks() -> (
    None
):
    monotonic_clock = _FakeMonotonicClock()
    sleeper = _FastSleeper(monotonic_clock)
    budget = waiting.WaitBudget(sleeper)
    renew_calls = 0

    async def renew() -> bool:
        nonlocal renew_calls
        renew_calls += 1
        return True

    # 25 seconds fits the 30-second budget but exceeds the 20-second
    # renewal cadence, so it must be split into a 20s chunk + a 5s chunk
    # with exactly one renewal in between.
    result = asyncio.run(
        waiting.wait_while_renewing_lease(
            budget, _FixedClock(NOW), _not_before(25), renew
        )
    )

    assert result is True
    assert sleeper.requested_seconds == [20.0, 5.0]
    assert renew_calls == 1


def test_wait_while_renewing_lease_does_not_renew_after_the_final_chunk() -> None:
    monotonic_clock = _FakeMonotonicClock()
    sleeper = _FastSleeper(monotonic_clock)
    budget = waiting.WaitBudget(sleeper)
    renew_calls = 0

    async def renew() -> bool:
        nonlocal renew_calls
        renew_calls += 1
        return True

    result = asyncio.run(
        waiting.wait_while_renewing_lease(
            budget, _FixedClock(NOW), _not_before(10), renew
        )
    )

    assert result is True
    assert sleeper.requested_seconds == [10.0]
    assert renew_calls == 0


def test_wait_while_renewing_lease_returns_none_when_renewal_fails() -> None:
    monotonic_clock = _FakeMonotonicClock()
    sleeper = _FastSleeper(monotonic_clock)
    budget = waiting.WaitBudget(sleeper)

    async def renew() -> bool:
        return False

    result = asyncio.run(
        waiting.wait_while_renewing_lease(
            budget, _FixedClock(NOW), _not_before(25), renew
        )
    )

    assert result is None
    # Only the first chunk was slept before the failed renewal stopped the wait.
    assert sleeper.requested_seconds == [20.0]


def test_wait_while_renewing_lease_returns_false_when_delay_exceeds_budget() -> None:
    monotonic_clock = _FakeMonotonicClock()
    sleeper = _FastSleeper(monotonic_clock)
    budget = waiting.WaitBudget(sleeper)

    async def renew() -> bool:
        raise AssertionError("must not be called when the delay is rejected up front")

    result = asyncio.run(
        waiting.wait_while_renewing_lease(
            budget, _FixedClock(NOW), _not_before(31), renew
        )
    )

    assert result is False
    assert sleeper.requested_seconds == []
