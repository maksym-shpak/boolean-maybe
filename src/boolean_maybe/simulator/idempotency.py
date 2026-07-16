"""Idempotency-key grammar and reconciliation-path decoding.

An accepted key is 1-128 ASCII characters drawn from `A-Z a-z 0-9 . _ ~ -`
and is compared exactly (never trimmed, case-folded, or normalized). `*` is
reserved for scenario-plan wildcard rules and is never an accepted key.
"""

from __future__ import annotations

import re

_KEY_RE = re.compile(r"^[A-Za-z0-9._~-]{1,128}$")
_HEX_PAIR_RE = re.compile(r"[0-9A-Fa-f]{2}")


def is_accepted_key(value: str) -> bool:
    """Return whether `value` is an accepted idempotency key."""

    return bool(_KEY_RE.fullmatch(value))


def decode_reconciliation_key(segment: str) -> str:
    """Strictly percent-decode one reconciliation path segment.

    Raises `ValueError` for incomplete or invalid percent escapes, invalid
    UTF-8, or a decoded value outside the accepted key grammar (including a
    decoded path separator). Decoding happens exactly once.
    """

    decoded_bytes = bytearray()
    index = 0
    length = len(segment)
    while index < length:
        char = segment[index]
        if char == "%":
            hex_pair = segment[index + 1 : index + 3]
            if len(hex_pair) != 2 or not _HEX_PAIR_RE.fullmatch(hex_pair):
                raise ValueError(
                    "invalid percent-escape in reconciliation path segment"
                )
            decoded_bytes.append(int(hex_pair, 16))
            index += 3
        else:
            code_point = ord(char)
            if code_point > 0x7F:
                raise ValueError("reconciliation path segment must be ASCII")
            decoded_bytes.append(code_point)
            index += 1

    try:
        decoded = decoded_bytes.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError("reconciliation path segment is not valid UTF-8") from exc

    if not is_accepted_key(decoded):
        raise ValueError(
            "decoded reconciliation key is not an accepted idempotency key"
        )

    return decoded
