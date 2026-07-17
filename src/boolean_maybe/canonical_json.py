"""Shared strict JSON parsing, RFC 8785 canonicalization, I-JSON validation, and digesting.

This is the one application-neutral implementation of these rules used by
both the CLI application (Job Entry validation) and the simulator (request
body and scenario-plan validation), so the two never risk maintaining
independently drifting copies of the same rules.

Python's `json` module silently keeps the last value for duplicate object
members and accepts the non-standard `NaN`/`Infinity`/`-Infinity` constants.
Both behaviors are unsafe for this contract, so parsing is routed through
`loads_strict` instead of calling `json.loads` directly. Before calling
`rfc8785.dumps()`, `parse_job_entry` also explicitly walks the parsed tree
for the remaining required I-JSON constraints: finite numbers, integers in
the interoperable exact range `[-(2**53)+1, (2**53)-1]`, and valid Unicode
scalar values. Any residual rejection by `rfc8785` itself is still treated
as a validation failure, never an internal error.
"""

from __future__ import annotations

import hashlib
import json
import math
from typing import Any

import rfc8785

_INT_MIN = -(2**53) + 1
_INT_MAX = 2**53 - 1

MAX_JOB_ENTRY_BYTES = 1024 * 1024


class DuplicateMemberError(ValueError):
    """A JSON object contained a repeated member name at some nesting level."""


class JobEntryValidationError(ValueError):
    """The input cannot be parsed or canonicalized as a Job Entry."""


def _reject_constant(token: str) -> float:
    raise ValueError(f"non-finite JSON constant {token!r} is not accepted")


def _object_pairs_hook(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    seen: set[str] = set()
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in seen:
            raise DuplicateMemberError(f"duplicate object member {key!r}")
        seen.add(key)
        result[key] = value
    return result


def loads_strict(text: str) -> Any:
    """Parse `text` rejecting duplicate members and non-finite constants."""

    return json.loads(
        text, object_pairs_hook=_object_pairs_hook, parse_constant=_reject_constant
    )


def parse_job_entry(raw: bytes) -> dict[str, Any]:
    """Parse and I-JSON-validate a Job Entry from raw UTF-8 JSON bytes."""

    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise JobEntryValidationError("input is not valid UTF-8") from exc

    try:
        parsed = loads_strict(text)
    except ValueError as exc:
        raise JobEntryValidationError("input is not valid JSON") from exc

    if not isinstance(parsed, dict):
        raise JobEntryValidationError("input root must be a JSON object")

    _validate_ijson(parsed)
    return parsed


def _validate_ijson(value: object) -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            _validate_string(key)
            _validate_ijson(item)
    elif isinstance(value, list):
        for item in value:
            _validate_ijson(item)
    elif isinstance(value, bool) or value is None:
        return
    elif isinstance(value, int):
        if value < _INT_MIN or value > _INT_MAX:
            raise JobEntryValidationError(
                "integer exceeds the interoperable safe range"
            )
    elif isinstance(value, float):
        if not math.isfinite(value):
            raise JobEntryValidationError("non-finite number is not accepted")
        if value == int(value) and not (_INT_MIN <= value <= _INT_MAX):
            raise JobEntryValidationError(
                "integer-valued number exceeds the interoperable safe range"
            )
    elif isinstance(value, str):
        _validate_string(value)
    else:  # pragma: no cover - json.loads cannot produce other types
        raise JobEntryValidationError("unsupported JSON value type")


def _validate_string(value: str) -> None:
    try:
        value.encode("utf-8", errors="strict")
    except UnicodeEncodeError as exc:
        raise JobEntryValidationError(
            "string contains an invalid Unicode scalar value"
        ) from exc


def canonicalize(job_entry: dict[str, Any]) -> bytes:
    """Return the RFC 8785 canonical UTF-8 bytes of a validated Job Entry."""

    try:
        return rfc8785.dumps(job_entry)
    except rfc8785.CanonicalizationError as exc:
        raise JobEntryValidationError(str(exc)) from exc


def payload_digest(canonical_bytes: bytes) -> str:
    """Return `sha256:<64 lowercase hex characters>` over canonical bytes."""

    return f"sha256:{hashlib.sha256(canonical_bytes).hexdigest()}"
