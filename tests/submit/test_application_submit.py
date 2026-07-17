"""Integration tests for the application submission workflow: end-to-end
success, stored replay, idempotency conflict, ineligible existing Jobs,
uncertain post-dispatch outcomes, corrupted evidence, and input validation.
"""

from __future__ import annotations

import asyncio
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from boolean_maybe import canonical_json
from boolean_maybe.application import submit as submit_workflow
from boolean_maybe.domain import clock as clock_mod
from boolean_maybe.external import client as external_client
from boolean_maybe.persistence import connection as connection_mod
from boolean_maybe.persistence import transactions as tx


def _request(
    database_path: Path,
    host: str,
    port: int,
    *,
    job_entry_raw: str = '{"a":1}',
    idempotency_key: str | None = None,
) -> submit_workflow.SubmitRequest:
    return submit_workflow.SubmitRequest(
        job_entry_raw=job_entry_raw,
        idempotency_key=idempotency_key,
        database_path=database_path,
        service_host=host,
        service_port=port,
    )


def _run(request: submit_workflow.SubmitRequest) -> submit_workflow.SubmitOutcome:
    return asyncio.run(submit_workflow.run_submit(request))


def test_duplicate_remote_request_id_keeps_jobs_distinct(
    make_live_simulator, tmp_path: Path
) -> None:
    running = make_live_simulator(
        {
            "version": 1,
            "rules": [
                {
                    "operation": "submission",
                    "idempotency_key": "*",
                    "scenario": "duplicate_remote_request_id",
                }
            ],
        }
    )
    database_path = tmp_path / "db.sqlite3"

    first = _run(
        _request(
            database_path,
            running.host,
            running.port,
            job_entry_raw='{"a":1}',
            idempotency_key="job-a",
        )
    )
    second = _run(
        _request(
            database_path,
            running.host,
            running.port,
            job_entry_raw='{"a":2}',
            idempotency_key="job-b",
        )
    )

    assert isinstance(first, submit_workflow.SubmitSuccess)
    assert isinstance(second, submit_workflow.SubmitSuccess)
    assert first.remote_request_id == second.remote_request_id == "remote-duplicate"
    assert first.job_id != second.job_id
    assert first.idempotency_key != second.idempotency_key


def test_429_then_success_retries_within_one_invocation_and_succeeds(
    make_live_simulator, tmp_path: Path
) -> None:
    running = make_live_simulator(
        {
            "version": 1,
            "rules": [
                {
                    "operation": "submission",
                    "idempotency_key": "job-a",
                    "scenario": "429_then_success",
                }
            ],
        }
    )
    database_path = tmp_path / "db.sqlite3"

    outcome = _run(
        _request(
            database_path,
            running.host,
            running.port,
            job_entry_raw='{"a":1}',
            idempotency_key="job-a",
        )
    )

    assert isinstance(outcome, submit_workflow.SubmitSuccess)
    assert outcome.outcome == "succeeded"
    assert outcome.submitted is True
    assert outcome.attempt_number == 2

    conn = connection_mod.open_connection(database_path)
    try:
        rows = conn.execute(
            "SELECT attempt_number, state FROM submission_attempts WHERE job_id = "
            "(SELECT job_id FROM jobs WHERE idempotency_key = 'job-a') "
            "ORDER BY attempt_number"
        ).fetchall()
        assert rows == [(1, "RETRYABLE_FAILURE"), (2, "SUCCEEDED")]
    finally:
        conn.close()


def test_processed_then_disconnect_reconciles_to_success_with_one_attempt(
    make_live_simulator, tmp_path: Path
) -> None:
    running = make_live_simulator(
        {
            "version": 1,
            "rules": [
                {
                    "operation": "submission",
                    "idempotency_key": "job-a",
                    "scenario": "processed_then_disconnect",
                }
            ],
        }
    )
    database_path = tmp_path / "db.sqlite3"

    outcome = _run(
        _request(
            database_path,
            running.host,
            running.port,
            job_entry_raw='{"a":1}',
            idempotency_key="job-a",
        )
    )

    assert isinstance(outcome, submit_workflow.SubmitSuccess)
    assert outcome.outcome == "succeeded"
    assert outcome.submitted is True
    assert outcome.attempt_number == 1

    conn = connection_mod.open_connection(database_path)
    try:
        rows = conn.execute(
            "SELECT attempt_number, state FROM submission_attempts WHERE job_id = "
            "(SELECT job_id FROM jobs WHERE idempotency_key = 'job-a')"
        ).fetchall()
        assert rows == [(1, "SUCCEEDED")]
    finally:
        conn.close()


def test_processed_then_500_reconciles_to_success_with_one_attempt(
    make_live_simulator, tmp_path: Path
) -> None:
    running = make_live_simulator(
        {
            "version": 1,
            "rules": [
                {
                    "operation": "submission",
                    "idempotency_key": "job-a",
                    "scenario": "processed_then_500",
                }
            ],
        }
    )
    database_path = tmp_path / "db.sqlite3"

    outcome = _run(
        _request(
            database_path,
            running.host,
            running.port,
            job_entry_raw='{"a":1}',
            idempotency_key="job-a",
        )
    )

    assert isinstance(outcome, submit_workflow.SubmitSuccess)
    assert outcome.outcome == "succeeded"

    conn = connection_mod.open_connection(database_path)
    try:
        rows = conn.execute(
            "SELECT attempt_number, state FROM submission_attempts WHERE job_id = "
            "(SELECT job_id FROM jobs WHERE idempotency_key = 'job-a')"
        ).fetchall()
        assert rows == [(1, "SUCCEEDED")]
    finally:
        conn.close()


def test_reconciliation_timeout_exhausts_to_ambiguous(
    make_live_simulator, tmp_path: Path
) -> None:
    # `reconciliation_timeout`'s per-key serialization holds its lock for
    # the simulator's real (long) internal delay regardless of how quickly
    # our own client gives up, so every one of the bounded 3 GETs times out
    # from the client's perspective within its own short deadline -- this
    # exercises genuine reconciliation-budget exhaustion, not a fast retry.
    running = make_live_simulator(
        {
            "version": 1,
            "rules": [
                {
                    "operation": "submission",
                    "idempotency_key": "job-a",
                    "scenario": "processed_then_disconnect",
                },
                {
                    "operation": "reconciliation",
                    "idempotency_key": "job-a",
                    "scenario": "reconciliation_timeout",
                },
            ],
        }
    )
    database_path = tmp_path / "db.sqlite3"

    outcome = asyncio.run(
        submit_workflow.run_submit(
            _request(
                database_path,
                running.host,
                running.port,
                job_entry_raw='{"a":1}',
                idempotency_key="job-a",
            ),
            http_timeout_seconds=1.0,
        )
    )

    assert isinstance(outcome, submit_workflow.SubmitError)
    assert outcome.outcome == "ambiguous"
    assert outcome.state == "AMBIGUOUS"

    conn = connection_mod.open_connection(database_path)
    try:
        rows = conn.execute(
            "SELECT attempt_number, state, error_category FROM submission_attempts "
            "WHERE job_id = (SELECT job_id FROM jobs WHERE idempotency_key = 'job-a')"
        ).fetchall()
        assert rows == [(1, "AMBIGUOUS", "reconciliation_inconclusive")]
    finally:
        conn.close()


class _FixedClock:
    def __init__(self, instant: datetime) -> None:
        self._instant = instant

    def now(self) -> datetime:
        return self._instant


class _FakeMonotonicClock:
    def __init__(self, start: float = 0.0) -> None:
        self.value = start

    def now(self) -> float:
        return self.value


class _FastSleeper:
    """Never actually sleeps; advances a paired fake monotonic clock so

    `WaitBudget.remaining_seconds()` reflects elapsed "time" without the
    real 10-second recovery quarantine slowing down the test suite.
    """

    def __init__(self, monotonic_clock: _FakeMonotonicClock) -> None:
        self._monotonic_clock = monotonic_clock
        self.requested_seconds: list[float] = []

    async def sleep(self, seconds: float) -> None:
        self.requested_seconds.append(seconds)
        self._monotonic_clock.value += seconds


def test_recovery_after_expired_lease_reconciles_matching_evidence_to_success(
    live_simulator, database_path: Path
) -> None:
    # Simulate a process that crashed after its pre-side-effect commit and
    # after its POST was actually processed remotely (so the simulator
    # already holds a matching record), leaving a `SUBMITTING`/`STARTED`
    # row with a lease that has since expired. A later invocation with the
    # same explicit key must claim it via fenced recovery, treat it as
    # `MAYBE_SENT`, and resolve it via reconciliation -- never resubmitting
    # and never creating a second attempt.
    canonical_bytes = canonical_json.canonicalize({"a": 1})
    digest = canonical_json.payload_digest(canonical_bytes)

    crashed_owner_outcome = external_client.submit_job(
        live_simulator.host, live_simulator.port, "job-a", canonical_bytes, digest
    )
    assert isinstance(crashed_owner_outcome, external_client.SubmitHttpSuccess)

    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    lease_expires_at = clock_mod.format_timestamp(start)  # already expired at `start`
    conn = connection_mod.open_connection(database_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "INSERT INTO jobs (job_id, idempotency_key, payload_canonical, state, created_at, updated_at) "
            "VALUES ('seed-job', 'job-a', ?, 'SUBMITTING', ?, ?)",
            (canonical_bytes, lease_expires_at, lease_expires_at),
        )
        conn.execute(
            "INSERT INTO submission_attempts (attempt_id, job_id, attempt_number, state, "
            "started_at, owner_token, fencing_generation, lease_expires_at) "
            "VALUES ('seed-attempt', 'seed-job', 1, 'STARTED', ?, 'crashed-owner', 1, ?)",
            (lease_expires_at, lease_expires_at),
        )
        conn.execute("COMMIT")
    finally:
        conn.close()

    fake_monotonic_clock = _FakeMonotonicClock()
    fake_sleeper = _FastSleeper(fake_monotonic_clock)

    outcome = asyncio.run(
        submit_workflow.run_submit(
            _request(
                database_path,
                live_simulator.host,
                live_simulator.port,
                job_entry_raw='{"a":1}',
                idempotency_key="job-a",
            ),
            clock=_FixedClock(start),
            sleeper=fake_sleeper,
        )
    )

    assert isinstance(outcome, submit_workflow.SubmitSuccess)
    assert outcome.outcome == "succeeded"
    assert outcome.submitted is False
    assert outcome.job_id == "seed-job"
    assert outcome.attempt_id == "seed-attempt"
    assert outcome.remote_request_id == crashed_owner_outcome.remote_request_id
    assert outcome.reconciliation_requests == 1

    # The quarantine wait actually happened (not skipped) and no attempt was
    # ever duplicated.
    assert 10.0 in fake_sleeper.requested_seconds
    conn = connection_mod.open_connection(database_path)
    try:
        rows = conn.execute(
            "SELECT attempt_id, attempt_number, state, fencing_generation, owner_token "
            "FROM submission_attempts WHERE job_id = 'seed-job'"
        ).fetchall()
        assert rows == [("seed-attempt", 1, "SUCCEEDED", 2, None)]
    finally:
        conn.close()


def test_recovery_with_unchanged_evidence_before_expiry_stays_job_in_progress(
    live_simulator, database_path: Path
) -> None:
    canonical_bytes = canonical_json.canonicalize({"a": 1})
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    lease_expires_at = clock_mod.format_timestamp(
        clock_mod.add_seconds(start, 60)
    )  # not yet expired at `start`
    conn = connection_mod.open_connection(database_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "INSERT INTO jobs (job_id, idempotency_key, payload_canonical, state, created_at, updated_at) "
            "VALUES ('seed-job', 'job-a', ?, 'SUBMITTING', ?, ?)",
            (
                canonical_bytes,
                clock_mod.format_timestamp(start),
                clock_mod.format_timestamp(start),
            ),
        )
        conn.execute(
            "INSERT INTO submission_attempts (attempt_id, job_id, attempt_number, state, "
            "started_at, owner_token, fencing_generation, lease_expires_at) "
            "VALUES ('seed-attempt', 'seed-job', 1, 'STARTED', ?, 'other-owner', 1, ?)",
            (clock_mod.format_timestamp(start), lease_expires_at),
        )
        conn.execute("COMMIT")
    finally:
        conn.close()

    outcome = asyncio.run(
        submit_workflow.run_submit(
            _request(
                database_path,
                live_simulator.host,
                live_simulator.port,
                job_entry_raw='{"a":1}',
                idempotency_key="job-a",
            ),
            clock=_FixedClock(start),
        )
    )

    assert isinstance(outcome, submit_workflow.SubmitError)
    assert outcome.outcome == "job_in_progress"
    assert outcome.state == "SUBMITTING"


def test_fresh_success_with_generated_key(live_simulator, database_path: Path) -> None:
    outcome = _run(_request(database_path, live_simulator.host, live_simulator.port))

    assert isinstance(outcome, submit_workflow.SubmitSuccess)
    assert outcome.outcome == "succeeded"
    assert outcome.submitted is True
    assert outcome.state == "SUCCEEDED"
    assert outcome.attempt_number == 1
    assert outcome.http_status == 201
    assert outcome.idempotency_key.startswith("job_")


def test_fresh_success_with_supplied_key(live_simulator, database_path: Path) -> None:
    outcome = _run(
        _request(
            database_path,
            live_simulator.host,
            live_simulator.port,
            idempotency_key="job-a",
        )
    )

    assert isinstance(outcome, submit_workflow.SubmitSuccess)
    assert outcome.idempotency_key == "job-a"


def test_replay_returns_already_completed_without_new_http(
    live_simulator, database_path: Path
) -> None:
    first = _run(
        _request(
            database_path,
            live_simulator.host,
            live_simulator.port,
            idempotency_key="job-a",
        )
    )
    assert isinstance(first, submit_workflow.SubmitSuccess)

    # Equivalent Job Entry with different member order/whitespace.
    second = _run(
        _request(
            database_path,
            live_simulator.host,
            live_simulator.port,
            job_entry_raw='{ "a" :    1 }',
            idempotency_key="job-a",
        )
    )

    assert isinstance(second, submit_workflow.SubmitSuccess)
    assert second.outcome == "already_completed"
    assert second.submitted is False
    assert second.job_id == first.job_id
    assert second.attempt_id == first.attempt_id
    assert second.remote_request_id == first.remote_request_id


def test_non_equivalent_reuse_is_idempotency_conflict(
    live_simulator, database_path: Path
) -> None:
    _run(
        _request(
            database_path,
            live_simulator.host,
            live_simulator.port,
            job_entry_raw='{"a":1}',
            idempotency_key="job-a",
        )
    )

    outcome = _run(
        _request(
            database_path,
            live_simulator.host,
            live_simulator.port,
            job_entry_raw='{"a":2}',
            idempotency_key="job-a",
        )
    )

    assert isinstance(outcome, submit_workflow.SubmitError)
    assert outcome.outcome == "idempotency_conflict"
    assert outcome.submitted is False
    assert outcome.state is None


def test_equivalent_pending_job_with_no_attempt_is_eligible_for_first_attempt(
    live_simulator, database_path: Path
) -> None:
    # Unlike the original single-Job vertical, a `PENDING` Job with no
    # attempt yet (e.g. left behind by a prior invocation the service gate
    # deferred) is now eligible for its first attempt rather than
    # permanently ineligible.
    canonical_bytes = canonical_json.canonicalize({"a": 1})
    now = "2026-01-01T00:00:00.000000Z"
    conn = connection_mod.open_connection(database_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "INSERT INTO jobs (job_id, idempotency_key, payload_canonical, state, created_at, updated_at) "
            "VALUES ('seed-job', 'job-a', ?, 'PENDING', ?, ?)",
            (canonical_bytes, now, now),
        )
        conn.execute("COMMIT")
    finally:
        conn.close()

    outcome = _run(
        _request(
            database_path,
            live_simulator.host,
            live_simulator.port,
            job_entry_raw='{"a":1}',
            idempotency_key="job-a",
        )
    )

    assert isinstance(outcome, submit_workflow.SubmitSuccess)
    assert outcome.outcome == "succeeded"
    assert outcome.job_id == "seed-job"


def test_equivalent_submitting_job_with_unexpired_lease_is_job_in_progress(
    live_simulator, database_path: Path
) -> None:
    canonical_bytes = canonical_json.canonicalize({"a": 1})
    now = "2026-01-01T00:00:00.000000Z"
    far_future = "2099-01-01T00:00:00.000000Z"
    conn = connection_mod.open_connection(database_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "INSERT INTO jobs (job_id, idempotency_key, payload_canonical, state, created_at, updated_at) "
            "VALUES ('seed-job', 'job-a', ?, 'SUBMITTING', ?, ?)",
            (canonical_bytes, now, now),
        )
        conn.execute(
            "INSERT INTO submission_attempts (attempt_id, job_id, attempt_number, state, "
            "started_at, owner_token, fencing_generation, lease_expires_at) "
            "VALUES ('seed-attempt', 'seed-job', 1, 'STARTED', ?, 'other-owner', 1, ?)",
            (now, far_future),
        )
        conn.execute("COMMIT")
    finally:
        conn.close()

    outcome = _run(
        _request(
            database_path,
            live_simulator.host,
            live_simulator.port,
            job_entry_raw='{"a":1}',
            idempotency_key="job-a",
        )
    )

    assert isinstance(outcome, submit_workflow.SubmitError)
    assert outcome.outcome == "job_in_progress"
    assert outcome.submitted is False
    assert outcome.state == "SUBMITTING"


def test_equivalent_retry_scheduled_job_with_far_future_eligibility_is_retry_scheduled(
    live_simulator, database_path: Path
) -> None:
    # No clock is injected here (the workflow uses the real system clock), so
    # `completed_at` must be anchored to the real current instant for the
    # 1-hour `retry_after_ms` to land in the future.
    canonical_bytes = canonical_json.canonicalize({"a": 1})
    now = clock_mod.format_timestamp(datetime.now(timezone.utc))
    conn = connection_mod.open_connection(database_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "INSERT INTO jobs (job_id, idempotency_key, payload_canonical, state, created_at, updated_at) "
            "VALUES ('seed-job', 'job-a', ?, 'RETRY_SCHEDULED', ?, ?)",
            (canonical_bytes, now, now),
        )
        conn.execute(
            "INSERT INTO submission_attempts (attempt_id, job_id, attempt_number, state, "
            "started_at, completed_at, http_status, error_category, retry_after_ms, "
            "fencing_generation) "
            "VALUES ('seed-attempt', 'seed-job', 1, 'RETRYABLE_FAILURE', ?, ?, 429, "
            "'rate_limited', 3600000, 1)",
            (now, now),
        )
        conn.execute("COMMIT")
    finally:
        conn.close()

    outcome = _run(
        _request(
            database_path,
            live_simulator.host,
            live_simulator.port,
            job_entry_raw='{"a":1}',
            idempotency_key="job-a",
        )
    )

    assert isinstance(outcome, submit_workflow.SubmitError)
    assert outcome.outcome == "retry_scheduled"
    assert outcome.submitted is False
    assert outcome.state == "RETRY_SCHEDULED"


def test_equivalent_failed_permanent_job_is_failed_permanent(
    live_simulator, database_path: Path
) -> None:
    canonical_bytes = canonical_json.canonicalize({"a": 1})
    now = "2026-01-01T00:00:00.000000Z"
    conn = connection_mod.open_connection(database_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "INSERT INTO jobs (job_id, idempotency_key, payload_canonical, state, created_at, updated_at) "
            "VALUES ('seed-job', 'job-a', ?, 'FAILED_PERMANENT', ?, ?)",
            (canonical_bytes, now, now),
        )
        conn.execute(
            "INSERT INTO submission_attempts (attempt_id, job_id, attempt_number, state, "
            "started_at, completed_at, error_category, fencing_generation) "
            "VALUES ('seed-attempt', 'seed-job', 1, 'PERMANENT_FAILURE', ?, ?, "
            "'validation_rejected', 1)",
            (now, now),
        )
        conn.execute("COMMIT")
    finally:
        conn.close()

    outcome = _run(
        _request(
            database_path,
            live_simulator.host,
            live_simulator.port,
            job_entry_raw='{"a":1}',
            idempotency_key="job-a",
        )
    )

    assert isinstance(outcome, submit_workflow.SubmitError)
    assert outcome.outcome == "failed_permanent"
    assert outcome.submitted is False
    assert outcome.state == "FAILED_PERMANENT"


def test_equivalent_failed_permanent_job_from_exhausted_retries_is_retry_exhausted(
    live_simulator, database_path: Path
) -> None:
    canonical_bytes = canonical_json.canonicalize({"a": 1})
    now = "2026-01-01T00:00:00.000000Z"
    conn = connection_mod.open_connection(database_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "INSERT INTO jobs (job_id, idempotency_key, payload_canonical, state, created_at, updated_at) "
            "VALUES ('seed-job', 'job-a', ?, 'FAILED_PERMANENT', ?, ?)",
            (canonical_bytes, now, now),
        )
        conn.execute(
            "INSERT INTO submission_attempts (attempt_id, job_id, attempt_number, state, "
            "started_at, completed_at, http_status, error_category, retry_after_ms, "
            "fencing_generation) "
            "VALUES ('seed-attempt', 'seed-job', 3, 'RETRYABLE_FAILURE', ?, ?, 429, "
            "'rate_limited', 2000, 1)",
            (now, now),
        )
        conn.execute("COMMIT")
    finally:
        conn.close()

    outcome = _run(
        _request(
            database_path,
            live_simulator.host,
            live_simulator.port,
            job_entry_raw='{"a":1}',
            idempotency_key="job-a",
        )
    )

    assert isinstance(outcome, submit_workflow.SubmitError)
    assert outcome.outcome == "retry_exhausted"
    assert outcome.submitted is False
    assert outcome.state == "FAILED_PERMANENT"


def test_equivalent_ambiguous_job_is_ambiguous(
    live_simulator, database_path: Path
) -> None:
    canonical_bytes = canonical_json.canonicalize({"a": 1})
    now = "2026-01-01T00:00:00.000000Z"
    conn = connection_mod.open_connection(database_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "INSERT INTO jobs (job_id, idempotency_key, payload_canonical, state, created_at, updated_at) "
            "VALUES ('seed-job', 'job-a', ?, 'AMBIGUOUS', ?, ?)",
            (canonical_bytes, now, now),
        )
        conn.execute(
            "INSERT INTO submission_attempts (attempt_id, job_id, attempt_number, state, "
            "started_at, completed_at, error_category, fencing_generation) "
            "VALUES ('seed-attempt', 'seed-job', 1, 'AMBIGUOUS', ?, ?, "
            "'reconciliation_inconclusive', 1)",
            (now, now),
        )
        conn.execute("COMMIT")
    finally:
        conn.close()

    outcome = _run(
        _request(
            database_path,
            live_simulator.host,
            live_simulator.port,
            job_entry_raw='{"a":1}',
            idempotency_key="job-a",
        )
    )

    assert isinstance(outcome, submit_workflow.SubmitError)
    assert outcome.outcome == "ambiguous"
    assert outcome.submitted is False
    assert outcome.state == "AMBIGUOUS"


def test_corrupted_succeeded_evidence_is_operational_failure_not_already_completed(
    live_simulator, database_path: Path
) -> None:
    canonical_bytes = canonical_json.canonicalize({"a": 1})
    now = "2026-01-01T00:00:00.000000Z"
    conn = connection_mod.open_connection(database_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "INSERT INTO jobs (job_id, idempotency_key, payload_canonical, state, created_at, updated_at) "
            "VALUES ('seed-job', 'job-a', ?, 'SUCCEEDED', ?, ?)",
            (canonical_bytes, now, now),
        )
        # Deliberately no successful attempt row: contradictory evidence.
        conn.execute("COMMIT")
    finally:
        conn.close()

    outcome = _run(
        _request(
            database_path,
            live_simulator.host,
            live_simulator.port,
            job_entry_raw='{"a":1}',
            idempotency_key="job-a",
        )
    )

    assert isinstance(outcome, submit_workflow.SubmitError)
    assert outcome.outcome == "local_persistence_failure"
    assert "traceback" not in outcome.message.lower()


def test_always_500_reconciles_to_ambiguous_via_404(
    make_live_simulator, database_path: Path
) -> None:
    # A delivered `500` is `MAYBE_SENT`-equivalent (`server_uncertain`):
    # rather than resubmitting, the workflow automatically reconciles by
    # idempotency key. Since `always_500` never processes the request, the
    # single reconciliation GET finds no record (`404`) and the Job/attempt
    # become terminally `AMBIGUOUS` -- proving no direct retry POST is sent
    # even though the simulator's own scenario name suggests "always fails".
    running = make_live_simulator(
        {
            "version": 1,
            "rules": [
                {
                    "operation": "submission",
                    "idempotency_key": "job-a",
                    "scenario": "always_500",
                }
            ],
        }
    )

    outcome = _run(
        _request(
            database_path,
            running.host,
            running.port,
            job_entry_raw='{"a":1}',
            idempotency_key="job-a",
        )
    )

    assert isinstance(outcome, submit_workflow.SubmitError)
    assert outcome.outcome == "ambiguous"
    assert outcome.submitted is True
    assert outcome.state == "AMBIGUOUS"

    conn = connection_mod.open_connection(database_path)
    try:
        job_state = conn.execute(
            "SELECT state FROM jobs WHERE idempotency_key = 'job-a'"
        ).fetchone()
        assert job_state == ("AMBIGUOUS",)
        attempt_row = conn.execute(
            "SELECT state, error_category FROM submission_attempts WHERE job_id = "
            "(SELECT job_id FROM jobs WHERE idempotency_key = 'job-a')"
        ).fetchone()
        assert attempt_row == ("AMBIGUOUS", "reconciliation_not_found")
    finally:
        conn.close()

    # `AMBIGUOUS` is terminal for automatic processing: a later invocation
    # must never retry and must report the same stored ambiguity.
    second = _run(
        _request(
            database_path,
            running.host,
            running.port,
            job_entry_raw='{"a":1}',
            idempotency_key="job-a",
        )
    )
    assert isinstance(second, submit_workflow.SubmitError)
    assert second.outcome == "ambiguous"
    assert second.state == "AMBIGUOUS"


class _JumpingClock:
    """Returns `start` for the pre-side-effect transaction's two `now()`
    reads (`created_at`/`started_at` and `lease_expires_at`), then jumps
    forward for every subsequent call (the pre-dispatch lease check)."""

    def __init__(self, start: datetime, jump_seconds: float) -> None:
        self._start = start
        self._jump = timedelta(seconds=jump_seconds)
        self._calls = 0

    def now(self) -> datetime:
        self._calls += 1
        return self._start if self._calls <= 2 else self._start + self._jump


def test_lease_expired_before_dispatch_sends_no_http_request(
    live_simulator, database_path: Path
) -> None:
    # The pre-side-effect commit computes `lease_expires_at` from the
    # clock's first two readings (both `start`, so the lease expires at
    # `start + 60s`); the pre-dispatch lease check then observes `start +
    # 61s`, already past that lease. No HTTP request may be sent.
    jumping_clock = _JumpingClock(
        datetime(2026, 1, 1, tzinfo=timezone.utc), jump_seconds=61
    )
    request = _request(
        database_path,
        live_simulator.host,
        live_simulator.port,
        idempotency_key="job-a",
    )

    outcome = asyncio.run(submit_workflow.run_submit(request, clock=jumping_clock))

    assert isinstance(outcome, submit_workflow.SubmitError)
    assert outcome.outcome == "ownership_lost"
    assert outcome.submitted is False
    assert outcome.state == "SUBMITTING"


class _ClockThatFailsOnThirdCall:
    """Returns `start` twice (the pre-side-effect commit), then raises."""

    def __init__(self, start: datetime) -> None:
        self._start = start
        self._calls = 0

    def now(self) -> datetime:
        self._calls += 1
        if self._calls <= 2:
            return self._start
        raise RuntimeError("simulated unexpected failure during lease verification")


def test_unexpected_failure_during_lease_check_proves_not_sent(
    live_simulator, database_path: Path
) -> None:
    # An unexpected (non-`PersistenceError`) failure while verifying the
    # lease happens strictly before `external_client.submit_job` is ever
    # called, so `submitted` must still be `False`, not the conservative
    # `True` used for failures that happen at or after actual HTTP dispatch.
    request = _request(
        database_path,
        live_simulator.host,
        live_simulator.port,
        idempotency_key="job-a",
    )

    outcome = asyncio.run(
        submit_workflow.run_submit(
            request,
            clock=_ClockThatFailsOnThirdCall(datetime(2026, 1, 1, tzinfo=timezone.utc)),
        )
    )

    assert isinstance(outcome, submit_workflow.SubmitError)
    assert outcome.outcome == "local_persistence_failure"
    assert outcome.submitted is False
    assert outcome.state == "SUBMITTING"

    conn = connection_mod.open_connection(database_path)
    try:
        row = conn.execute(
            "SELECT state FROM submission_attempts WHERE job_id = "
            "(SELECT job_id FROM jobs WHERE idempotency_key = 'job-a')"
        ).fetchone()
        assert row == ("STARTED",)
    finally:
        conn.close()


def test_failure_before_pre_side_effect_commit_persists_no_state_and_sends_no_http(
    live_simulator, database_path: Path
) -> None:
    # Simulates the process being interrupted partway through the
    # pre-side-effect transaction (after `BEGIN IMMEDIATE`, before the
    # `INSERT`/`COMMIT`): an unexpected failure while generating the local
    # `job_id` must leave no persisted Job or attempt behind, and must never
    # reach the HTTP dispatch step.
    def failing_id_generator() -> str:
        raise RuntimeError("simulated interruption before the pre-side-effect commit")

    request = _request(
        database_path,
        live_simulator.host,
        live_simulator.port,
        idempotency_key="job-a",
    )

    outcome = asyncio.run(
        submit_workflow.run_submit(request, id_generator=failing_id_generator)
    )

    assert isinstance(outcome, submit_workflow.SubmitError)
    assert outcome.outcome == "local_persistence_failure"
    assert outcome.submitted is False
    assert outcome.state is None

    conn = connection_mod.open_connection(database_path)
    try:
        row = conn.execute(
            "SELECT COUNT(*) FROM jobs WHERE idempotency_key = 'job-a'"
        ).fetchone()
        assert row == (0,)
    finally:
        conn.close()


# -- Input validation (exit-2 territory) --------------------------------------


def test_invalid_job_entry_raises_validation_error_before_any_database_access(
    live_simulator, tmp_path: Path
) -> None:
    database_path = tmp_path / "should-not-be-created" / "db.sqlite3"
    request = _request(
        database_path,
        live_simulator.host,
        live_simulator.port,
        job_entry_raw="not json",
    )

    with pytest.raises(submit_workflow.ValidationError):
        _run(request)

    assert not database_path.parent.exists()


def test_oversized_job_entry_raises_validation_error(
    live_simulator, database_path: Path
) -> None:
    huge = '{"pad":"' + ("x" * (canonical_json.MAX_JOB_ENTRY_BYTES)) + '"}'
    request = _request(
        database_path, live_simulator.host, live_simulator.port, job_entry_raw=huge
    )

    with pytest.raises(submit_workflow.ValidationError):
        _run(request)


def test_invalid_supplied_key_raises_validation_error(
    live_simulator, database_path: Path
) -> None:
    request = _request(
        database_path,
        live_simulator.host,
        live_simulator.port,
        idempotency_key="not a valid key!",
    )

    with pytest.raises(submit_workflow.ValidationError):
        _run(request)


# -- Late evidence: ownership lost after an HTTP response arrives -----------


def test_ownership_lost_after_http_response_records_late_evidence(
    live_simulator, database_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Simulate another process winning a fenced takeover of this exact
    # attempt strictly between this invocation's dispatch and its own
    # finalization attempt: the real POST still succeeds, but by the time
    # this invocation tries to finalize, its captured fencing generation no
    # longer matches. Per spec, this invocation may append the resulting
    # sanitized observation as late evidence but must not finalize the Job
    # itself, and must report `ownership_lost` rather than inventing success.
    real_submit_job = external_client.submit_job

    def _fake_submit_job(
        host: str,
        port: int,
        idempotency_key: str,
        canonical_bytes: bytes,
        expected_digest: str,
        *,
        timeout_seconds: float,
    ) -> external_client.SubmitHttpOutcome:
        result = real_submit_job(
            host,
            port,
            idempotency_key,
            canonical_bytes,
            expected_digest,
            timeout_seconds=timeout_seconds,
        )
        conn = connection_mod.open_connection(database_path)
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                "UPDATE submission_attempts SET fencing_generation = fencing_generation + 1 "
                "WHERE job_id = (SELECT job_id FROM jobs WHERE idempotency_key = ?)",
                (idempotency_key,),
            )
            conn.execute("COMMIT")
        finally:
            conn.close()
        return result

    monkeypatch.setattr(external_client, "submit_job", _fake_submit_job)

    outcome = asyncio.run(
        submit_workflow.run_submit(
            _request(
                database_path,
                live_simulator.host,
                live_simulator.port,
                job_entry_raw='{"a":1}',
                idempotency_key="job-a",
            )
        )
    )

    assert isinstance(outcome, submit_workflow.SubmitError)
    assert outcome.outcome == "ownership_lost"
    assert outcome.submitted is True

    conn = connection_mod.open_connection(database_path)
    try:
        attempt_row = conn.execute(
            "SELECT attempt_id, state FROM submission_attempts WHERE job_id = "
            "(SELECT job_id FROM jobs WHERE idempotency_key = 'job-a')"
        ).fetchone()
        attempt_id, state = attempt_row
        # Never finalized through the normal completion path.
        assert state == "STARTED"

        observations = conn.execute(
            "SELECT evidence_category, http_status, is_late FROM attempt_observations "
            "WHERE attempt_id = ?",
            (attempt_id,),
        ).fetchall()
        assert observations == [("processed", 201, 1)]
    finally:
        conn.close()


def test_current_owner_consumes_late_success_evidence_without_sending_a_get(
    live_simulator, database_path: Path
) -> None:
    # A stale owner already appended authoritative late evidence proving
    # success before losing its lease. A later invocation recovers the
    # expired attempt (fenced takeover); its reconciliation sequence must
    # consume that unconsumed late evidence and complete the Job without
    # ever issuing a GET, per "The current owner checks unconsumed late
    # observations before each reconciliation GET."
    canonical_bytes = canonical_json.canonicalize({"a": 1})

    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    lease_expires_at = clock_mod.format_timestamp(start)  # already expired at `start`

    conn = connection_mod.open_connection(database_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "INSERT INTO jobs (job_id, idempotency_key, payload_canonical, state, created_at, updated_at) "
            "VALUES ('seed-job', 'job-a', ?, 'SUBMITTING', ?, ?)",
            (canonical_bytes, lease_expires_at, lease_expires_at),
        )
        conn.execute(
            "INSERT INTO submission_attempts (attempt_id, job_id, attempt_number, state, "
            "started_at, owner_token, fencing_generation, lease_expires_at) "
            "VALUES ('seed-attempt', 'seed-job', 1, 'STARTED', ?, 'crashed-owner', 1, ?)",
            (lease_expires_at, lease_expires_at),
        )
        conn.execute("COMMIT")

        tx.append_late_observation(
            conn,
            attempt_id="seed-attempt",
            observed_fencing_generation=1,
            evidence_category="processed",
            operation="SUBMISSION",
            clock=_FixedClock(start),
            id_generator=lambda: "late-obs-1",
            http_status=201,
            remote_request_id="remote-late-1",
        )
    finally:
        conn.close()

    fake_monotonic_clock = _FakeMonotonicClock()
    fake_sleeper = _FastSleeper(fake_monotonic_clock)

    outcome = asyncio.run(
        submit_workflow.run_submit(
            _request(
                database_path,
                live_simulator.host,
                live_simulator.port,
                job_entry_raw='{"a":1}',
                idempotency_key="job-a",
            ),
            clock=_FixedClock(start),
            sleeper=fake_sleeper,
        )
    )

    assert isinstance(outcome, submit_workflow.SubmitSuccess)
    assert outcome.outcome == "succeeded"
    assert outcome.submitted is False
    assert outcome.job_id == "seed-job"
    assert outcome.remote_request_id == "remote-late-1"
    assert outcome.reconciliation_requests == 0

    conn = connection_mod.open_connection(database_path)
    try:
        row = conn.execute(
            "SELECT state FROM submission_attempts WHERE attempt_id = 'seed-attempt'"
        ).fetchone()
        assert row == ("SUCCEEDED",)
    finally:
        conn.close()


# -- Cancellation --------------------------------------------------------------


def test_cancellation_before_dispatch_completes_attempt_as_proven_not_sent(
    live_simulator, database_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Cancellation strictly before `connect()` begins (here: during the
    # pre-dispatch lease renewal, which always precedes any HTTP call) is
    # proven `NOT_SENT`: the owned attempt must be completed as a safe
    # `RETRYABLE_FAILURE` under normal budget rules -- durably preserving
    # eligibility and consuming lifetime budget -- rather than left stranded
    # `STARTED` with cancellation simply swallowing the evidence.
    real_renew_lease = tx.renew_lease
    started = threading.Event()
    release = threading.Event()

    def _blocking_renew_lease(*args, **kwargs):
        started.set()
        release.wait(timeout=5.0)
        return real_renew_lease(*args, **kwargs)

    monkeypatch.setattr(tx, "renew_lease", _blocking_renew_lease)

    async def _run() -> None:
        task = asyncio.ensure_future(
            submit_workflow.run_submit(
                _request(
                    database_path,
                    live_simulator.host,
                    live_simulator.port,
                    job_entry_raw='{"a":1}',
                    idempotency_key="job-a",
                )
            )
        )
        await asyncio.to_thread(started.wait, 5.0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        release.set()

    asyncio.run(_run())

    conn = connection_mod.open_connection(database_path)
    try:
        job_row = conn.execute(
            "SELECT state FROM jobs WHERE idempotency_key = 'job-a'"
        ).fetchone()
        assert job_row == ("RETRY_SCHEDULED",)
        attempt_row = conn.execute(
            "SELECT state, error_category FROM submission_attempts WHERE job_id = "
            "(SELECT job_id FROM jobs WHERE idempotency_key = 'job-a')"
        ).fetchone()
        assert attempt_row == ("RETRYABLE_FAILURE", "transport_not_sent")
    finally:
        conn.close()


class _HangingSleeper:
    """Never actually completes a sleep; signals `started` first so the test

    knows the workflow is genuinely inside the wait before cancelling it.
    """

    def __init__(self, started: asyncio.Event) -> None:
        self._started = started

    async def sleep(self, seconds: float) -> None:
        self._started.set()
        await asyncio.Event().wait()


def test_cancellation_during_retry_wait_creates_no_attempt_and_preserves_state(
    database_path: Path,
) -> None:
    # Cancellation during a retry wait must create no attempt or request and
    # must preserve the durable `RETRY_SCHEDULED` state exactly as it was.
    canonical_bytes = canonical_json.canonicalize({"a": 1})
    now = clock_mod.format_timestamp(datetime(2026, 1, 1, tzinfo=timezone.utc))

    conn = connection_mod.open_connection(database_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "INSERT INTO jobs (job_id, idempotency_key, payload_canonical, state, created_at, updated_at) "
            "VALUES ('seed-job', 'job-a', ?, 'RETRY_SCHEDULED', ?, ?)",
            (canonical_bytes, now, now),
        )
        conn.execute(
            "INSERT INTO submission_attempts (attempt_id, job_id, attempt_number, state, "
            "started_at, completed_at, http_status, error_category, retry_after_ms, "
            "fencing_generation) "
            "VALUES ('seed-attempt', 'seed-job', 1, 'RETRYABLE_FAILURE', ?, ?, 429, "
            "'rate_limited', 10000, 1)",
            (now, now),
        )
        conn.execute("COMMIT")
    finally:
        conn.close()

    async def _run() -> None:
        started = asyncio.Event()
        task = asyncio.ensure_future(
            submit_workflow.run_submit(
                _request(
                    database_path,
                    "127.0.0.1",
                    1,  # never dialed: cancellation happens during the wait
                    job_entry_raw='{"a":1}',
                    idempotency_key="job-a",
                ),
                clock=_FixedClock(datetime(2026, 1, 1, tzinfo=timezone.utc)),
                sleeper=_HangingSleeper(started),
            )
        )
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(_run())

    conn = connection_mod.open_connection(database_path)
    try:
        job_row = conn.execute(
            "SELECT state FROM jobs WHERE idempotency_key = 'job-a'"
        ).fetchone()
        assert job_row == ("RETRY_SCHEDULED",)
        attempt_rows = conn.execute(
            "SELECT attempt_number, state FROM submission_attempts WHERE job_id = 'seed-job'"
        ).fetchall()
        assert attempt_rows == [(1, "RETRYABLE_FAILURE")]
    finally:
        conn.close()


def test_cancellation_after_dispatch_leaves_attempt_recoverable_started(
    live_simulator, database_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Cancellation after dispatch may have begun (here: while awaiting the
    # submission HTTP call itself) must never claim a proven-`NOT_SENT`
    # result: the attempt stays recoverable `STARTED`/`SUBMITTING`, and no
    # reconciliation or new POST begins in this invocation.
    started = threading.Event()
    release = threading.Event()

    def _blocking_submit_job(*args, **kwargs):
        started.set()
        release.wait(timeout=5.0)
        return external_client.SubmitHttpTransportFailure(
            dispatch_may_have_begun=True, reason="test forced abandonment"
        )

    monkeypatch.setattr(external_client, "submit_job", _blocking_submit_job)

    async def _run() -> None:
        task = asyncio.ensure_future(
            submit_workflow.run_submit(
                _request(
                    database_path,
                    live_simulator.host,
                    live_simulator.port,
                    job_entry_raw='{"a":1}',
                    idempotency_key="job-a",
                )
            )
        )
        await asyncio.to_thread(started.wait, 5.0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        release.set()

    asyncio.run(_run())

    conn = connection_mod.open_connection(database_path)
    try:
        job_row = conn.execute(
            "SELECT state FROM jobs WHERE idempotency_key = 'job-a'"
        ).fetchone()
        assert job_row == ("SUBMITTING",)
        attempt_row = conn.execute(
            "SELECT state FROM submission_attempts WHERE job_id = "
            "(SELECT job_id FROM jobs WHERE idempotency_key = 'job-a')"
        ).fetchone()
        assert attempt_row == ("STARTED",)
    finally:
        conn.close()
