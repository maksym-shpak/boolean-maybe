"""Compatibility re-export.

The canonical implementation now lives in `boolean_maybe.canonical_json`,
shared with the CLI application so both use exactly one implementation of
RFC 8785 canonicalization, I-JSON validation, and digesting.
"""

from __future__ import annotations

from boolean_maybe.canonical_json import (
    JobEntryValidationError,
    canonicalize,
    parse_job_entry,
    payload_digest,
)

__all__ = [
    "JobEntryValidationError",
    "canonicalize",
    "parse_job_entry",
    "payload_digest",
]
