"""Projects `attempt_history` from durable `submission_attempts` and

`attempt_observations` rows, without any HTTP.

Each item exposes core attempt fields plus a non-authoritative reconciliation
summary (`request_count`, `final_category`) aggregated from this attempt's
own `RECONCILIATION`-operation observations. `final_category` never replaces
the attempt's own `error_category`.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass

_RECONCILIATION_FINAL_CATEGORIES = frozenset(
    {
        "rate_limited",
        "server_uncertain",
        "transport_maybe_sent",
        "protocol_uncertain",
        "reconciliation_not_found",
        "reconciliation_inconclusive",
        "idempotency_conflict",
    }
)


@dataclass(frozen=True)
class ReconciliationSummary:
    request_count: int
    final_category: str | None


@dataclass(frozen=True)
class AttemptHistoryItem:
    attempt_id: str
    attempt_number: int
    state: str
    started_at: str
    completed_at: str | None
    http_status: int | None
    remote_request_id: str | None
    error_category: str | None
    retry_after_ms: int | None
    reconciliation: ReconciliationSummary


def read_attempt_history(
    conn: sqlite3.Connection, job_id: str
) -> list[AttemptHistoryItem]:
    attempt_rows = conn.execute(
        "SELECT attempt_id, attempt_number, state, started_at, completed_at, "
        "http_status, remote_request_id, error_category, retry_after_ms "
        "FROM submission_attempts WHERE job_id = ? ORDER BY attempt_number",
        (job_id,),
    ).fetchall()

    items: list[AttemptHistoryItem] = []
    for (
        attempt_id,
        attempt_number,
        state,
        started_at,
        completed_at,
        http_status,
        remote_request_id,
        error_category,
        retry_after_ms,
    ) in attempt_rows:
        (request_count,) = conn.execute(
            "SELECT COUNT(*) FROM attempt_observations "
            "WHERE attempt_id = ? AND operation = 'RECONCILIATION'",
            (attempt_id,),
        ).fetchone()

        final_category = (
            error_category
            if error_category in _RECONCILIATION_FINAL_CATEGORIES
            else None
        )
        if final_category is None and request_count > 0:
            last_row = conn.execute(
                "SELECT evidence_category FROM attempt_observations "
                "WHERE attempt_id = ? AND operation = 'RECONCILIATION' "
                "ORDER BY sequence_number DESC LIMIT 1",
                (attempt_id,),
            ).fetchone()
            if last_row is not None:
                final_category = last_row[0]

        items.append(
            AttemptHistoryItem(
                attempt_id=attempt_id,
                attempt_number=attempt_number,
                state=state,
                started_at=started_at,
                completed_at=completed_at,
                http_status=http_status,
                remote_request_id=remote_request_id,
                error_category=error_category,
                retry_after_ms=retry_after_ms,
                reconciliation=ReconciliationSummary(
                    request_count=request_count, final_category=final_category
                ),
            )
        )
    return items
