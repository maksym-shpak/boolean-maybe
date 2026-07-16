"""Idempotency-key grammar and reconciliation-path decoding tests."""

from __future__ import annotations

import pytest

from boolean_maybe.simulator import idempotency


@pytest.mark.parametrize(
    "value",
    [
        "job-a",
        "a",
        "A" * 128,
        "abc.def_ghi~jkl-123",
        "0123456789",
    ],
)
def test_accepted_keys(value: str) -> None:
    assert idempotency.is_accepted_key(value)


@pytest.mark.parametrize(
    "value",
    [
        "",
        "A" * 129,
        "job a",
        "job/a",
        "job%20a",
        "*",
        "café",
        " job-a",
        "job-a ",
        "job\ta",
    ],
)
def test_rejected_keys(value: str) -> None:
    assert not idempotency.is_accepted_key(value)


def test_wildcard_is_not_an_accepted_key() -> None:
    assert not idempotency.is_accepted_key("*")


# -- Reconciliation path decoding --------------------------------------------


def test_decode_plain_accepted_key() -> None:
    assert idempotency.decode_reconciliation_key("job-a") == "job-a"


def test_decode_percent_encoded_unreserved_characters() -> None:
    # Percent-encoding of already-unreserved characters is unnecessary but
    # must still decode to the identical accepted key.
    assert idempotency.decode_reconciliation_key("job%2Da") == "job-a"
    assert idempotency.decode_reconciliation_key("job%2ea") == "job.a"


def test_decode_is_exactly_one_pass_not_double_decoded() -> None:
    # "%2532" decodes once to the literal string "%32", which contains '%'
    # and is therefore not an accepted key: double-decoding must not occur.
    with pytest.raises(ValueError):
        idempotency.decode_reconciliation_key("%2532")


def test_decode_rejects_incomplete_percent_escape() -> None:
    with pytest.raises(ValueError):
        idempotency.decode_reconciliation_key("job%2")


def test_decode_rejects_invalid_hex_escape() -> None:
    with pytest.raises(ValueError):
        idempotency.decode_reconciliation_key("job%zz")


def test_decode_rejects_invalid_utf8() -> None:
    with pytest.raises(ValueError):
        idempotency.decode_reconciliation_key("%ff%fe")


def test_decode_rejects_encoded_separator() -> None:
    # %2F decodes to '/', which is outside the accepted key grammar.
    with pytest.raises(ValueError):
        idempotency.decode_reconciliation_key("job%2Fa")


def test_decode_rejects_literal_non_ascii() -> None:
    with pytest.raises(ValueError):
        idempotency.decode_reconciliation_key("café")


def test_decode_rejects_empty_segment() -> None:
    with pytest.raises(ValueError):
        idempotency.decode_reconciliation_key("")


def test_decode_rejects_wildcard() -> None:
    with pytest.raises(ValueError):
        idempotency.decode_reconciliation_key("*")
