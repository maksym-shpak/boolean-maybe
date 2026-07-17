"""Durable service-wide rate-limit gate (ADR-006, migration version 2).

These are composable helpers, not self-contained transactions: ADR-006
requires the gate check to happen inside the same `BEGIN IMMEDIATE` as Job
eligibility and attempt allocation (submission) or lease/fencing
verification (reconciliation), so callers must already hold an open
transaction before calling either function.
"""

from __future__ import annotations

import sqlite3


def read_gate_not_before(conn: sqlite3.Connection) -> str | None:
    """Return the current gate instant, or `None` if no restriction exists."""

    row = conn.execute(
        "SELECT not_before FROM service_rate_limit_gate WHERE singleton_id = 1"
    ).fetchone()
    return row[0] if row is not None else None


def advance_gate(
    conn: sqlite3.Connection, *, candidate_not_before: str, now: str
) -> None:
    """Advance the gate to the later of its current value and `candidate_not_before`.

    A single upsert keeps "later of current and candidate" atomic without a
    separate read, so concurrent `429` observations from different processes
    can never regress each other's restriction.
    """

    conn.execute(
        "INSERT INTO service_rate_limit_gate (singleton_id, not_before, updated_at) "
        "VALUES (1, ?, ?) "
        "ON CONFLICT(singleton_id) DO UPDATE SET "
        "not_before = MAX(not_before, excluded.not_before), updated_at = excluded.updated_at",
        (candidate_not_before, now),
    )
