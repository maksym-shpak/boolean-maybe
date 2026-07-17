"""Integration tests for the application submission workflow: end-to-end
success, stored replay, idempotency conflict, ineligible existing Jobs,
uncertain post-dispatch outcomes, corrupted evidence, and input validation.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from boolean_maybe import canonical_json
from boolean_maybe.application import submit as submit_workflow
from boolean_maybe.persistence import connection as connection_mod


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


@pytest.mark.parametrize(
    "state",
    ["PENDING", "SUBMITTING", "RETRY_SCHEDULED", "FAILED_PERMANENT", "AMBIGUOUS"],
)
def test_equivalent_non_succeeded_job_is_job_not_eligible(
    live_simulator, database_path: Path, state: str
) -> None:
    canonical_bytes = canonical_json.canonicalize({"a": 1})
    now = "2026-01-01T00:00:00.000000Z"
    conn = connection_mod.open_connection(database_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "INSERT INTO jobs (job_id, idempotency_key, payload_canonical, state, created_at, updated_at) "
            "VALUES ('seed-job', 'job-a', ?, ?, ?, ?)",
            (canonical_bytes, state, now, now),
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
    assert outcome.outcome == "job_not_eligible"
    assert outcome.submitted is False
    assert outcome.state == state


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
    assert outcome.outcome == "submission_incomplete"
    assert "traceback" not in outcome.message.lower()


def test_always_500_leaves_job_submitting_and_reports_submission_incomplete(
    make_live_simulator, database_path: Path
) -> None:
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
    assert outcome.outcome == "submission_incomplete"
    assert outcome.submitted is True
    assert outcome.state == "SUBMITTING"

    conn = connection_mod.open_connection(database_path)
    try:
        job_state = conn.execute(
            "SELECT state FROM jobs WHERE idempotency_key = 'job-a'"
        ).fetchone()
        assert job_state == ("SUBMITTING",)
        attempt_state = conn.execute(
            "SELECT state FROM submission_attempts WHERE job_id = "
            "(SELECT job_id FROM jobs WHERE idempotency_key = 'job-a')"
        ).fetchone()
        assert attempt_state == ("STARTED",)
    finally:
        conn.close()

    # A later invocation must never treat the still-SUBMITTING Job as
    # eligible, and must send no further HTTP request.
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
    assert second.outcome == "job_not_eligible"
    assert second.state == "SUBMITTING"


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
    assert outcome.outcome == "submission_incomplete"
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
    assert outcome.outcome == "submission_incomplete"
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
    assert outcome.outcome == "submission_incomplete"
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
