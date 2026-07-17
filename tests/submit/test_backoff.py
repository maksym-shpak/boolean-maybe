"""Unit tests for `domain.backoff`: full-jitter cap/delay formulas."""

from __future__ import annotations

from boolean_maybe.domain import backoff


class _FixedRandomSource:
    def __init__(self, value: int) -> None:
        self._value = value

    def randint(self, low: int, high: int) -> int:
        assert low <= self._value <= high
        return self._value


def test_backoff_cap_ms_doubles_per_ordinal() -> None:
    assert backoff.backoff_cap_ms(1) == 500
    assert backoff.backoff_cap_ms(2) == 1000
    assert backoff.backoff_cap_ms(3) == 2000


def test_backoff_cap_ms_is_bounded_at_thirty_seconds() -> None:
    assert backoff.backoff_cap_ms(10) == 30_000


def test_policy_delay_ms_at_zero_bound() -> None:
    assert backoff.policy_delay_ms(1, _FixedRandomSource(0)) == 0


def test_policy_delay_ms_at_cap_bound() -> None:
    assert backoff.policy_delay_ms(1, _FixedRandomSource(500)) == 500
    assert backoff.policy_delay_ms(2, _FixedRandomSource(1000)) == 1000


def test_effective_delay_ms_prefers_larger_of_policy_and_server() -> None:
    assert backoff.effective_delay_ms(200, 500) == 500
    assert backoff.effective_delay_ms(700, 500) == 700


def test_effective_delay_ms_uses_policy_when_no_server_delay() -> None:
    assert backoff.effective_delay_ms(200, None) == 200
