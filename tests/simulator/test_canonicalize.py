"""RFC 8785/I-JSON conformance tests: canonicalization, digesting, rejection."""

from __future__ import annotations

import hashlib

import pytest

from boolean_maybe.simulator import canonicalize

INT_MAX = 2**53 - 1
INT_MIN = -(2**53) + 1


# -- RFC 8785 canonicalization conformance ----------------------------------


def test_member_order_and_whitespace_are_insignificant() -> None:
    left = canonicalize.parse_job_entry(b'{"b": 1, "a": 2}')
    right = canonicalize.parse_job_entry(b'{"a":2,"b":1}')

    assert canonicalize.canonicalize(left) == canonicalize.canonicalize(right)
    assert canonicalize.canonicalize(left) == b'{"a":2,"b":1}'


def test_object_keys_sort_by_utf16_code_unit_not_code_point() -> None:
    # RFC 8785 section 3.2.3 requires sorting object members by the UTF-16
    # encoding of their keys. U+1F600 requires a UTF-16 surrogate pair
    # starting with 0xD83D, which sorts *before* the single BMP code unit
    # U+FFFF (0xFFFF) even though U+1F600 is the larger Unicode code point.
    emoji_key = "\U0001f600"
    bmp_key = "￿"
    assert emoji_key.encode("utf-16-be")[0] < bmp_key.encode("utf-16-be")[0]

    job_entry = {bmp_key: 1, emoji_key: 2}
    canonical = canonicalize.canonicalize(job_entry)

    emoji_index = canonical.index(emoji_key.encode("utf-8"))
    bmp_index = canonical.index(bmp_key.encode("utf-8"))
    assert emoji_index < bmp_index


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (100, b"100"),
        (100.0, b"100"),
        (0.1, b"0.1"),
        (-0.0, b"0"),
        (1e-6, b"0.000001"),
        (1e-7, b"1e-7"),
    ],
)
def test_ecmascript_number_formatting(value: float, expected: bytes) -> None:
    assert canonicalize.canonicalize({"n": value}) == b'{"n":' + expected + b"}"


def test_array_order_is_significant() -> None:
    a = canonicalize.canonicalize({"items": [1, 2]})
    b = canonicalize.canonicalize({"items": [2, 1]})
    assert a != b


# -- Duplicate-member rejection ----------------------------------------------


def test_duplicate_root_member_is_rejected() -> None:
    with pytest.raises(canonicalize.JobEntryValidationError):
        canonicalize.parse_job_entry(b'{"a": 1, "a": 2}')


def test_duplicate_member_nested_below_root_is_rejected() -> None:
    with pytest.raises(canonicalize.JobEntryValidationError):
        canonicalize.parse_job_entry(b'{"a": {"x": 1, "x": 2}}')


def test_duplicate_member_inside_array_element_is_rejected() -> None:
    with pytest.raises(canonicalize.JobEntryValidationError):
        canonicalize.parse_job_entry(b'{"a": [{"x": 1, "x": 2}]}')


# -- I-JSON rejection ---------------------------------------------------------


def test_non_object_root_is_rejected() -> None:
    with pytest.raises(canonicalize.JobEntryValidationError):
        canonicalize.parse_job_entry(b"[1, 2, 3]")


@pytest.mark.parametrize("literal", [b"NaN", b"Infinity", b"-Infinity"])
def test_non_finite_constants_are_rejected(literal: bytes) -> None:
    with pytest.raises(canonicalize.JobEntryValidationError):
        canonicalize.parse_job_entry(b'{"n": ' + literal + b"}")


def test_integer_at_safe_boundary_is_accepted() -> None:
    job_entry = canonicalize.parse_job_entry(f'{{"n": {INT_MAX}}}'.encode())
    assert job_entry["n"] == INT_MAX


def test_integer_beyond_safe_boundary_is_rejected() -> None:
    with pytest.raises(canonicalize.JobEntryValidationError):
        canonicalize.parse_job_entry(f'{{"n": {INT_MAX + 1}}}'.encode())


def test_negative_integer_at_safe_boundary_is_accepted() -> None:
    job_entry = canonicalize.parse_job_entry(f'{{"n": {INT_MIN}}}'.encode())
    assert job_entry["n"] == INT_MIN


def test_negative_integer_beyond_safe_boundary_is_rejected() -> None:
    with pytest.raises(canonicalize.JobEntryValidationError):
        canonicalize.parse_job_entry(f'{{"n": {INT_MIN - 1}}}'.encode())


def test_integer_valued_float_beyond_safe_boundary_is_rejected() -> None:
    with pytest.raises(canonicalize.JobEntryValidationError):
        canonicalize.parse_job_entry(b'{"n": 1e20}')


def test_lone_surrogate_in_string_is_rejected() -> None:
    with pytest.raises(canonicalize.JobEntryValidationError):
        canonicalize.parse_job_entry('{"a": "\\ud800"}'.encode())


def test_lone_surrogate_in_key_is_rejected() -> None:
    with pytest.raises(canonicalize.JobEntryValidationError):
        canonicalize.parse_job_entry('{"\\ud800": 1}'.encode())


def test_malformed_json_is_rejected() -> None:
    with pytest.raises(canonicalize.JobEntryValidationError):
        canonicalize.parse_job_entry(b'{"a": }')


def test_malformed_utf8_is_rejected() -> None:
    with pytest.raises(canonicalize.JobEntryValidationError):
        canonicalize.parse_job_entry(b"\xff\xfe{}")


# -- Digest --------------------------------------------------------------


def test_payload_digest_format_and_value() -> None:
    canonical_bytes = b'{"a":1}'
    digest = canonicalize.payload_digest(canonical_bytes)

    assert digest == f"sha256:{hashlib.sha256(canonical_bytes).hexdigest()}"
    assert digest.startswith("sha256:")
    assert len(digest) == len("sha256:") + 64
    assert digest[len("sha256:") :] == digest[len("sha256:") :].lower()
