"""RFC 8785 Job Entry parsing, I-JSON validation, canonicalization, and digest.

Duplicate-member rejection happens during parsing (`strict_json`). Before
calling `rfc8785.dumps()`, this module explicitly walks the parsed tree for
the remaining required I-JSON constraints: finite numbers, integers in the
interoperable exact range `[-(2**53)+1, (2**53)-1]`, and valid Unicode
scalar values. Any residual rejection by `rfc8785` itself is still treated
as a validation failure, never an internal error.
"""

from __future__ import annotations

import hashlib
import math
from typing import Any

import rfc8785

from . import strict_json

_INT_MIN = -(2**53) + 1
_INT_MAX = 2**53 - 1


class JobEntryValidationError(ValueError):
    """The submitted body cannot be parsed or canonicalized as a Job Entry."""


def parse_job_entry(raw: bytes) -> dict[str, Any]:
    """Parse and I-JSON-validate a Job Entry from raw request-body bytes."""

    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise JobEntryValidationError("request body is not valid UTF-8") from exc

    try:
        parsed = strict_json.loads_strict(text)
    except ValueError as exc:
        raise JobEntryValidationError("request body is not valid JSON") from exc

    if not isinstance(parsed, dict):
        raise JobEntryValidationError("request body root must be a JSON object")

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
