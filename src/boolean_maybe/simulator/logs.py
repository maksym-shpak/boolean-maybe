"""Minimal JSON Lines operational logging to stderr.

Logs must never contain raw idempotency keys, Job Entries, canonical
payloads, full payload digests, response bodies, scenario-plan content, or
raw exception text that may contain request data. An accepted key is
identified only by `key_fingerprint`. Unavailable optional fields are
omitted rather than emitted as `null`. Each event is written as one
complete JSON line, even from concurrent threads.
"""

from __future__ import annotations

import hashlib
import json
import sys
import threading
from datetime import datetime, timezone
from typing import TextIO


def key_fingerprint(idempotency_key: str) -> str:
    return hashlib.sha256(idempotency_key.encode("utf-8")).hexdigest()[:12]


def _timestamp() -> str:
    now = datetime.now(timezone.utc)
    return now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond // 1000:03d}Z"


class OperationalLogger:
    def __init__(self, stream: TextIO | None = None) -> None:
        self._stream = stream if stream is not None else sys.stderr
        self._lock = threading.Lock()

    def emit(
        self,
        event: str,
        *,
        level: str = "info",
        operation: str | None = None,
        idempotency_key: str | None = None,
        scenario: str | None = None,
        ordinal: int | None = None,
        status: int | None = None,
        processed: bool | None = None,
    ) -> None:
        record: dict[str, object] = {
            "timestamp": _timestamp(),
            "level": level,
            "event": event,
        }
        if operation is not None:
            record["operation"] = operation
        if idempotency_key is not None:
            record["key_fingerprint"] = key_fingerprint(idempotency_key)
        if scenario is not None:
            record["scenario"] = scenario
        if ordinal is not None:
            record["request_ordinal"] = ordinal
        if status is not None:
            record["status"] = status
        if processed is not None:
            record["processed"] = processed

        line = json.dumps(record, separators=(",", ":"))
        with self._lock:
            self._stream.write(line + "\n")
            self._stream.flush()
