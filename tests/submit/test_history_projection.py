"""Unit tests for `application.history_projection.read_attempt_history`."""

from __future__ import annotations

from pathlib import Path

from boolean_maybe.application import history_projection
from boolean_maybe.persistence import connection as connection_mod

CANONICAL_A = b'{"a":1}'
NOW = "2026-01-01T00:00:00.000000Z"


def _open(tmp_path: Path):
    return connection_mod.open_connection(tmp_path / "db.sqlite3")


def _seed_job(conn, *, job_id: str, key: str, state: str) -> None:
    conn.execute("BEGIN IMMEDIATE")
    conn.execute(
        "INSERT INTO jobs (job_id, idempotency_key, payload_canonical, state, created_at, updated_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (job_id, key, CANONICAL_A, state, NOW, NOW),
    )
    conn.execute("COMMIT")


def _seed_completed_attempt(
    conn,
    *,
    attempt_id: str,
    job_id: str,
    attempt_number: int,
    state: str,
    http_status: int | None = None,
    remote_request_id: str | None = None,
    error_category: str | None = None,
    retry_after_ms: int | None = None,
) -> None:
    conn.execute("BEGIN IMMEDIATE")
    conn.execute(
        "INSERT INTO submission_attempts (attempt_id, job_id, attempt_number, state, "
        "started_at, completed_at, http_status, remote_request_id, error_category, "
        "retry_after_ms, fencing_generation) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)",
        (
            attempt_id,
            job_id,
            attempt_number,
            state,
            NOW,
            NOW,
            http_status,
            remote_request_id,
            error_category,
            retry_after_ms,
        ),
    )
    conn.execute("COMMIT")


def test_read_attempt_history_returns_empty_list_for_job_with_no_attempts(
    tmp_path: Path,
) -> None:
    conn = _open(tmp_path)
    try:
        _seed_job(conn, job_id="job-1", key="job-a", state="PENDING")
        assert history_projection.read_attempt_history(conn, "job-1") == []
    finally:
        conn.close()


def test_read_attempt_history_orders_by_attempt_number(tmp_path: Path) -> None:
    conn = _open(tmp_path)
    try:
        _seed_job(conn, job_id="job-1", key="job-a", state="FAILED_PERMANENT")
        _seed_completed_attempt(
            conn,
            attempt_id="attempt-2",
            job_id="job-1",
            attempt_number=2,
            state="PERMANENT_FAILURE",
            error_category="validation_rejected",
        )
        _seed_completed_attempt(
            conn,
            attempt_id="attempt-1",
            job_id="job-1",
            attempt_number=1,
            state="RETRYABLE_FAILURE",
            error_category="transport_not_sent",
            retry_after_ms=500,
        )
        history = history_projection.read_attempt_history(conn, "job-1")
        assert [item.attempt_number for item in history] == [1, 2]
        assert history[0].attempt_id == "attempt-1"
        assert history[0].retry_after_ms == 500
        assert history[1].attempt_id == "attempt-2"
    finally:
        conn.close()


def test_direct_success_attempt_has_zero_reconciliation_count_and_no_final_category(
    tmp_path: Path,
) -> None:
    conn = _open(tmp_path)
    try:
        _seed_job(conn, job_id="job-1", key="job-a", state="SUCCEEDED")
        _seed_completed_attempt(
            conn,
            attempt_id="attempt-1",
            job_id="job-1",
            attempt_number=1,
            state="SUCCEEDED",
            http_status=201,
            remote_request_id="remote-1",
        )
        history = history_projection.read_attempt_history(conn, "job-1")
        assert len(history) == 1
        assert history[0].reconciliation.request_count == 0
        assert history[0].reconciliation.final_category is None
    finally:
        conn.close()


def test_reconciled_success_has_positive_request_count_and_no_final_category(
    tmp_path: Path,
) -> None:
    conn = _open(tmp_path)
    try:
        _seed_job(conn, job_id="job-1", key="job-a", state="SUCCEEDED")
        _seed_completed_attempt(
            conn,
            attempt_id="attempt-1",
            job_id="job-1",
            attempt_number=1,
            state="SUCCEEDED",
            http_status=200,
            remote_request_id="remote-1",
        )
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "INSERT INTO attempt_observations (observation_id, attempt_id, sequence_id, "
            "sequence_number, operation, request_ordinal, observed_at, evidence_category, "
            "observed_fencing_generation, is_late) "
            "VALUES ('obs-1', 'attempt-1', 'seq-1', 1, 'RECONCILIATION', 1, ?, ?, 1, 0)",
            (NOW, None),
        )
        conn.execute("COMMIT")

        history = history_projection.read_attempt_history(conn, "job-1")
        assert history[0].reconciliation.request_count == 1
        assert history[0].reconciliation.final_category is None
        # The reconciliation summary never replaces the attempt's own category.
        assert history[0].error_category is None
    finally:
        conn.close()


def test_reconciliation_inconclusive_final_category_matches_attempt_error_category(
    tmp_path: Path,
) -> None:
    conn = _open(tmp_path)
    try:
        _seed_job(conn, job_id="job-1", key="job-a", state="AMBIGUOUS")
        _seed_completed_attempt(
            conn,
            attempt_id="attempt-1",
            job_id="job-1",
            attempt_number=1,
            state="AMBIGUOUS",
            error_category="reconciliation_inconclusive",
        )
        for ordinal in (1, 2, 3):
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                "INSERT INTO attempt_observations (observation_id, attempt_id, sequence_id, "
                "sequence_number, operation, request_ordinal, observed_at, evidence_category, "
                "observed_fencing_generation, is_late) "
                "VALUES (?, 'attempt-1', 'seq-1', ?, 'RECONCILIATION', ?, ?, 'server_uncertain', 1, 0)",
                (f"obs-{ordinal}", ordinal, ordinal, NOW),
            )
            conn.execute("COMMIT")

        history = history_projection.read_attempt_history(conn, "job-1")
        assert history[0].reconciliation.request_count == 3
        assert history[0].reconciliation.final_category == "reconciliation_inconclusive"
    finally:
        conn.close()


def test_late_submission_observations_are_excluded_from_reconciliation_count(
    tmp_path: Path,
) -> None:
    conn = _open(tmp_path)
    try:
        _seed_job(conn, job_id="job-1", key="job-a", state="SUCCEEDED")
        _seed_completed_attempt(
            conn,
            attempt_id="attempt-1",
            job_id="job-1",
            attempt_number=1,
            state="SUCCEEDED",
            http_status=201,
            remote_request_id="remote-1",
        )
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "INSERT INTO attempt_observations (observation_id, attempt_id, sequence_id, "
            "sequence_number, operation, request_ordinal, observed_at, evidence_category, "
            "observed_fencing_generation, is_late) "
            "VALUES ('obs-late', 'attempt-1', 'seq-late', 1, 'SUBMISSION', 1, ?, 'processed', 1, 1)",
            (NOW,),
        )
        conn.execute("COMMIT")

        history = history_projection.read_attempt_history(conn, "job-1")
        assert history[0].reconciliation.request_count == 0
    finally:
        conn.close()
