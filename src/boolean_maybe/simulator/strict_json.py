"""Compatibility re-export.

The canonical implementation now lives in `boolean_maybe.canonical_json`,
shared with the CLI application so both use exactly one implementation of
strict JSON parsing.
"""

from __future__ import annotations

from boolean_maybe.canonical_json import DuplicateMemberError, loads_strict

__all__ = ["DuplicateMemberError", "loads_strict"]
