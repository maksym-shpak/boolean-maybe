"""Idempotency Key grammar and generation.

Uses the same grammar as the simulator's `Idempotency-Key` contract: 1-128
ASCII characters drawn from `A-Z a-z 0-9 . _ ~ -`, compared exactly (never
trimmed, case-folded, decoded, or normalized). This module intentionally
duplicates that small grammar rather than importing the simulator package:
the CLI application and simulator remain independent runtime processes, and
this rule is a single-line regex, not the strict-JSON/canonicalization logic
that the specification requires to be shared.

A generated key uses a cryptographically secure random 128-bit value and is
never derived from the Job Entry.
"""

from __future__ import annotations

import re
import secrets

_KEY_RE = re.compile(r"^[A-Za-z0-9._~-]{1,128}$")
_GENERATED_PREFIX = "job_"


def is_accepted_key(value: str) -> bool:
    """Return whether `value` is an accepted idempotency key."""

    return bool(_KEY_RE.fullmatch(value))


def generate_key() -> str:
    """Return a new `job_<32 lowercase hex>` key from a secure random value."""

    return f"{_GENERATED_PREFIX}{secrets.token_hex(16)}"
