"""In-memory processed records, per-key locks, and per-operation/per-key ordinals.

All state here is simulator-owned, starts empty, and is discarded on process
exit. Callers are expected to hold the lock returned by `lock_for(key)` for
the entire duration of ordinal assignment, scenario selection, state
inspection, conflict decision, and scenario state effect for that key.
"""

from __future__ import annotations

import hashlib
import threading
from dataclasses import dataclass


@dataclass(frozen=True)
class ProcessedRecord:
    idempotency_key: str
    canonical_bytes: bytes
    payload_digest: str
    remote_request_id: str


def default_remote_request_id(idempotency_key: str) -> str:
    digest = hashlib.sha256(idempotency_key.encode("utf-8")).hexdigest()
    return f"remote-{digest[:24]}"


class KeyCoordinator:
    """Owns per-key locks, processed records, and request ordinals."""

    def __init__(self) -> None:
        self._locks_guard = threading.Lock()
        self._locks: dict[str, threading.Lock] = {}
        self._records_guard = threading.Lock()
        self._records: dict[str, ProcessedRecord] = {}
        self._ordinals_guard = threading.Lock()
        self._ordinals: dict[tuple[str, str], int] = {}

    def lock_for(self, key: str) -> threading.Lock:
        with self._locks_guard:
            lock = self._locks.get(key)
            if lock is None:
                lock = threading.Lock()
                self._locks[key] = lock
            return lock

    def next_ordinal(self, operation: str, key: str) -> int:
        with self._ordinals_guard:
            ordinal = self._ordinals.get((operation, key), 0) + 1
            self._ordinals[(operation, key)] = ordinal
            return ordinal

    def get_record(self, key: str) -> ProcessedRecord | None:
        with self._records_guard:
            return self._records.get(key)

    def create_record(
        self,
        key: str,
        canonical_bytes: bytes,
        digest: str,
        *,
        duplicate_remote_id: bool,
    ) -> ProcessedRecord:
        remote_id = (
            "remote-duplicate"
            if duplicate_remote_id
            else default_remote_request_id(key)
        )
        record = ProcessedRecord(key, canonical_bytes, digest, remote_id)
        with self._records_guard:
            self._records[key] = record
        return record
