"""Smoke tests for the shared `boolean_maybe.canonical_json` module.

Full RFC 8785/I-JSON conformance coverage lives in
`tests/simulator/test_canonicalize.py`, exercised through the simulator's
compatibility re-export of this same module -- these tests only confirm the
shared module itself is directly usable and importable by application code
without going through the simulator package.
"""

from __future__ import annotations

import hashlib

import pytest

from boolean_maybe import canonical_json


def test_parse_and_canonicalize_round_trip() -> None:
    entry = canonical_json.parse_job_entry(b'{"b": 1, "a": 2}')
    canonical = canonical_json.canonicalize(entry)
    assert canonical == b'{"a":2,"b":1}'


def test_payload_digest_matches_sha256_of_canonical_bytes() -> None:
    canonical = b'{"a":1}'
    digest = canonical_json.payload_digest(canonical)
    assert digest == f"sha256:{hashlib.sha256(canonical).hexdigest()}"


def test_duplicate_member_is_rejected() -> None:
    with pytest.raises(canonical_json.JobEntryValidationError):
        canonical_json.parse_job_entry(b'{"a": 1, "a": 2}')


def test_non_object_root_is_rejected() -> None:
    with pytest.raises(canonical_json.JobEntryValidationError):
        canonical_json.parse_job_entry(b"[1, 2]")


def test_empty_object_is_accepted() -> None:
    entry = canonical_json.parse_job_entry(b"{}")
    assert canonical_json.canonicalize(entry) == b"{}"


def test_max_job_entry_bytes_is_one_mebibyte() -> None:
    assert canonical_json.MAX_JOB_ENTRY_BYTES == 1024 * 1024


def test_simulator_shim_reexports_the_same_functions() -> None:
    from boolean_maybe.simulator import canonicalize as simulator_canonicalize
    from boolean_maybe.simulator import strict_json as simulator_strict_json

    assert simulator_canonicalize.parse_job_entry is canonical_json.parse_job_entry
    assert simulator_canonicalize.canonicalize is canonical_json.canonicalize
    assert simulator_canonicalize.payload_digest is canonical_json.payload_digest
    assert simulator_strict_json.loads_strict is canonical_json.loads_strict
