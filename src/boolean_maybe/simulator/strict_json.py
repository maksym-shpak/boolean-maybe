"""Strict JSON parsing shared by Job Entry and scenario-plan validation.

Python's `json` module silently keeps the last value for duplicate object
members and accepts the non-standard `NaN`/`Infinity`/`-Infinity` constants.
Both behaviors are unsafe for this simulator's contract, so parsing is
routed through this module instead of calling `json.loads` directly.
"""

from __future__ import annotations

import json
from typing import Any


class DuplicateMemberError(ValueError):
    """A JSON object contained a repeated member name at some nesting level."""


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
