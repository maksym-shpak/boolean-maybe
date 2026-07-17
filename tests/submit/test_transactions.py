"""Unit tests for the pre- and post-side-effect durable transactions:
resolution/creation, equivalence, existing-state branching, collision
handling, corruption detection, and lease/fencing verification.
"""

from __future__ import annotations

import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from boolean_maybe.persistence import connection as connection_mod
from boolean_maybe.persistence import transactions as tx
from boolean_maybe.persistence.errors import DatabaseCorruptionError, PersistenceError


class _FixedClock:
    def __init__(self, instant: datetime) -> None:
        self.instant = instant

    def now(self) -> datetime:
        return self.instant


def _clock(seconds_from_epoch_start: int = 0) -> _FixedClock:
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    return _FixedClock(base + timedelta(seconds=seconds_from_epoch_start))


def _open(tmp_path: Path):
    return connection_mod.open_connection(tmp_path / "db.sqlite3")


def _sequence(*values: str):
    it = iter(values)

    def generator() -> str:
        return next(it)

    return generator


def _counting_id_generator(prefix: str = "obs"):
    counter = [0]

    def generator() -> str:
        counter[0] += 1
        return f"{prefix}-{counter[0]}"

    return generator


CANONICAL_A = b'{"a":1}'
CANONICAL_B = b'{"b":2}'


# -- Fresh creation (supplied and generated keys) -----------------------------


def test_supplied_key_with_no_existing_job_creates_pending_then_submitting(
    tmp_path: Path,
) -> None:
    conn = _open(tmp_path)
    try:
        result = tx.run_pre_side_effect_transaction(
            conn,
            supplied_key="job-a",
            key_generator=_sequence(),
            canonical_bytes=CANONICAL_A,
            id_generator=_sequence("job-id-1", "attempt-id-1"),
            invocation_token="token-1",
            clock=_clock(),
        )
        assert isinstance(result, tx.ReadyToSubmit)
        assert result.job.idempotency_key == "job-a"
        assert result.job.state == "SUBMITTING"
        assert result.attempt.attempt_number == 1
        assert result.attempt.state == "STARTED"
        assert result.attempt.owner_token == "token-1"

        row = conn.execute(
            "SELECT state FROM jobs WHERE job_id = 'job-id-1'"
        ).fetchone()
        assert row == ("SUBMITTING",)
    finally:
        conn.close()


def test_generated_key_is_used_when_none_supplied(tmp_path: Path) -> None:
    conn = _open(tmp_path)
    try:
        result = tx.run_pre_side_effect_transaction(
            conn,
            supplied_key=None,
            key_generator=_sequence("generated-key-1"),
            canonical_bytes=CANONICAL_A,
            id_generator=_sequence("job-id-1", "attempt-id-1"),
            invocation_token="token-1",
            clock=_clock(),
        )
        assert isinstance(result, tx.ReadyToSubmit)
        assert result.job.idempotency_key == "generated-key-1"
    finally:
        conn.close()


def test_generated_key_collision_regenerates_without_reusing_winning_job(
    tmp_path: Path,
) -> None:
    conn = _open(tmp_path)
    try:
        first = tx.run_pre_side_effect_transaction(
            conn,
            supplied_key="taken-key",
            key_generator=_sequence(),
            canonical_bytes=CANONICAL_A,
            id_generator=_sequence("job-id-1", "attempt-id-1"),
            invocation_token="token-1",
            clock=_clock(),
        )
        assert isinstance(first, tx.ReadyToSubmit)

        second = tx.run_pre_side_effect_transaction(
            conn,
            supplied_key=None,
            key_generator=_sequence("taken-key", "fresh-key"),
            canonical_bytes=CANONICAL_B,
            id_generator=_sequence("job-id-2", "attempt-id-2"),
            invocation_token="token-2",
            clock=_clock(1),
        )
        assert isinstance(second, tx.ReadyToSubmit)
        assert second.job.idempotency_key == "fresh-key"
        assert second.job.job_id != first.job.job_id
    finally:
        conn.close()


def test_job_id_collision_at_insert_regenerates_job_id(tmp_path: Path) -> None:
    conn = _open(tmp_path)
    try:
        first = tx.run_pre_side_effect_transaction(
            conn,
            supplied_key="job-a",
            key_generator=_sequence(),
            canonical_bytes=CANONICAL_A,
            id_generator=_sequence("dup-job-id", "attempt-id-1"),
            invocation_token="token-1",
            clock=_clock(),
        )
        assert isinstance(first, tx.ReadyToSubmit)
        assert first.job.job_id == "dup-job-id"

        # Reuse the same job_id deliberately, then a fresh one.
        second = tx.run_pre_side_effect_transaction(
            conn,
            supplied_key="job-b",
            key_generator=_sequence(),
            canonical_bytes=CANONICAL_B,
            id_generator=_sequence("dup-job-id", "job-id-2", "attempt-id-2"),
            invocation_token="token-2",
            clock=_clock(1),
        )
        assert isinstance(second, tx.ReadyToSubmit)
        assert second.job.job_id == "job-id-2"
    finally:
        conn.close()


def test_attempt_id_collision_at_insert_regenerates_attempt_id(tmp_path: Path) -> None:
    conn = _open(tmp_path)
    try:
        first = tx.run_pre_side_effect_transaction(
            conn,
            supplied_key="job-a",
            key_generator=_sequence(),
            canonical_bytes=CANONICAL_A,
            id_generator=_sequence("job-id-1", "dup-attempt-id"),
            invocation_token="token-1",
            clock=_clock(),
        )
        assert isinstance(first, tx.ReadyToSubmit)
        assert first.attempt.attempt_id == "dup-attempt-id"

        second = tx.run_pre_side_effect_transaction(
            conn,
            supplied_key="job-b",
            key_generator=_sequence(),
            canonical_bytes=CANONICAL_B,
            id_generator=_sequence("job-id-2", "dup-attempt-id", "attempt-id-2"),
            invocation_token="token-2",
            clock=_clock(1),
        )
        assert isinstance(second, tx.ReadyToSubmit)
        assert second.attempt.attempt_id == "attempt-id-2"
    finally:
        conn.close()


# -- Existing-Job branching for a supplied key --------------------------------


def _seed_ready_to_submit(
    conn,
    key: str,
    canonical: bytes,
    *,
    job_id: str,
    attempt_id: str,
    token: str,
    at: int = 0,
) -> tx.ReadyToSubmit:
    result = tx.run_pre_side_effect_transaction(
        conn,
        supplied_key=key,
        key_generator=_sequence(),
        canonical_bytes=canonical,
        id_generator=_sequence(job_id, attempt_id),
        invocation_token=token,
        clock=_clock(at),
    )
    assert isinstance(result, tx.ReadyToSubmit)
    return result


def _seed_retry_scheduled(
    conn,
    *,
    job_id: str,
    key: str,
    canonical: bytes,
    attempt_id: str,
    completed_at: str,
    retry_after_ms: int,
    attempt_number: int = 1,
) -> None:
    conn.execute("BEGIN IMMEDIATE")
    conn.execute(
        "INSERT INTO jobs (job_id, idempotency_key, payload_canonical, state, created_at, updated_at) "
        "VALUES (?, ?, ?, 'RETRY_SCHEDULED', ?, ?)",
        (job_id, key, canonical, completed_at, completed_at),
    )
    conn.execute(
        "INSERT INTO submission_attempts (attempt_id, job_id, attempt_number, state, "
        "started_at, completed_at, http_status, error_category, retry_after_ms, "
        "fencing_generation) "
        "VALUES (?, ?, ?, 'RETRYABLE_FAILURE', ?, ?, 429, 'rate_limited', ?, 1)",
        (
            attempt_id,
            job_id,
            attempt_number,
            completed_at,
            completed_at,
            retry_after_ms,
        ),
    )
    conn.execute("COMMIT")


def _seed_terminal_job(
    conn,
    *,
    job_id: str,
    key: str,
    canonical: bytes,
    job_state: str,
    attempt_id: str,
    attempt_state: str,
    error_category: str,
) -> None:
    now = "2026-01-01T00:00:00.000000Z"
    conn.execute("BEGIN IMMEDIATE")
    conn.execute(
        "INSERT INTO jobs (job_id, idempotency_key, payload_canonical, state, created_at, updated_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (job_id, key, canonical, job_state, now, now),
    )
    conn.execute(
        "INSERT INTO submission_attempts (attempt_id, job_id, attempt_number, state, "
        "started_at, completed_at, error_category, fencing_generation) "
        "VALUES (?, ?, 1, ?, ?, ?, ?, 1)",
        (attempt_id, job_id, attempt_state, now, now, error_category),
    )
    conn.execute("COMMIT")


def test_equivalent_supplied_key_reuse_while_submitting_with_unexpired_lease_is_job_in_progress(
    tmp_path: Path,
) -> None:
    conn = _open(tmp_path)
    try:
        _seed_ready_to_submit(
            conn,
            "job-a",
            CANONICAL_A,
            job_id="job-id-1",
            attempt_id="attempt-id-1",
            token="t1",
        )

        result = tx.run_pre_side_effect_transaction(
            conn,
            supplied_key="job-a",
            key_generator=_sequence(),
            canonical_bytes=CANONICAL_A,
            id_generator=_sequence(),
            invocation_token="t2",
            clock=_clock(1),
        )
        assert isinstance(result, tx.JobInProgress)
        assert result.job.state == "SUBMITTING"
    finally:
        conn.close()


def test_equivalent_supplied_key_reuse_while_submitting_with_expired_lease_is_recovery_candidate(
    tmp_path: Path,
) -> None:
    # An expired lease is eligible for fenced recovery rather than being
    # silently treated as an invitation to start a duplicate attempt.
    conn = _open(tmp_path)
    try:
        ready = _seed_ready_to_submit(
            conn,
            "job-a",
            CANONICAL_A,
            job_id="job-id-1",
            attempt_id="attempt-id-1",
            token="t1",
        )

        result = tx.run_pre_side_effect_transaction(
            conn,
            supplied_key="job-a",
            key_generator=_sequence(),
            canonical_bytes=CANONICAL_A,
            id_generator=_sequence(),
            invocation_token="t2",
            clock=_clock(61),
        )
        assert isinstance(result, tx.RecoveryCandidate)
        assert result.attempt.attempt_id == ready.attempt.attempt_id
        assert result.attempt.owner_token == "t1"
        assert result.attempt.fencing_generation == 1
    finally:
        conn.close()


def test_equivalent_supplied_key_for_pending_job_is_ready_to_submit(
    tmp_path: Path,
) -> None:
    # A `PENDING` Job with no attempt yet (e.g. from a prior invocation
    # deferred by a closed gate) is eligible for its first attempt.
    conn = _open(tmp_path)
    try:
        now = "2026-01-01T00:00:00.000000Z"
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "INSERT INTO jobs (job_id, idempotency_key, payload_canonical, state, created_at, updated_at) "
            "VALUES ('job-x', 'job-a', ?, 'PENDING', ?, ?)",
            (CANONICAL_A, now, now),
        )
        conn.execute("COMMIT")

        result = tx.run_pre_side_effect_transaction(
            conn,
            supplied_key="job-a",
            key_generator=_sequence(),
            canonical_bytes=CANONICAL_A,
            id_generator=_sequence("attempt-id-1"),
            invocation_token="t1",
            clock=_clock(),
        )
        assert isinstance(result, tx.ReadyToSubmit)
        assert result.attempt.attempt_number == 1
    finally:
        conn.close()


def test_equivalent_supplied_key_for_eligible_retry_scheduled_job_is_ready_to_submit(
    tmp_path: Path,
) -> None:
    conn = _open(tmp_path)
    try:
        _seed_retry_scheduled(
            conn,
            job_id="job-x",
            key="job-a",
            canonical=CANONICAL_A,
            attempt_id="attempt-id-1",
            completed_at="2026-01-01T00:00:00.000000Z",
            retry_after_ms=1000,
        )

        result = tx.run_pre_side_effect_transaction(
            conn,
            supplied_key="job-a",
            key_generator=_sequence(),
            canonical_bytes=CANONICAL_A,
            id_generator=_sequence("attempt-id-2"),
            invocation_token="t1",
            # 2 seconds after completion: past the 1-second eligibility.
            clock=_clock(2),
        )
        assert isinstance(result, tx.ReadyToSubmit)
        assert result.attempt.attempt_number == 2
    finally:
        conn.close()


def test_equivalent_supplied_key_for_not_yet_eligible_retry_scheduled_job_waits(
    tmp_path: Path,
) -> None:
    conn = _open(tmp_path)
    try:
        _seed_retry_scheduled(
            conn,
            job_id="job-x",
            key="job-a",
            canonical=CANONICAL_A,
            attempt_id="attempt-id-1",
            completed_at="2026-01-01T00:00:00.000000Z",
            retry_after_ms=60_000,
        )

        result = tx.run_pre_side_effect_transaction(
            conn,
            supplied_key="job-a",
            key_generator=_sequence(),
            canonical_bytes=CANONICAL_A,
            id_generator=_sequence(),
            invocation_token="t1",
            # 1 second after completion: well before the 60-second eligibility.
            clock=_clock(1),
        )
        assert isinstance(result, tx.NotYetEligible)
        assert result.not_before == "2026-01-01T00:01:00.000000Z"
    finally:
        conn.close()


def test_forward_wall_clock_jump_past_eligibility_authorizes_the_retry(
    tmp_path: Path,
) -> None:
    # A forward wall-clock adjustment that moves `now` past the durable
    # `completed_at + retry_after_ms` eligibility must authorize the retry:
    # eligibility is recomputed fresh from wall-clock `now` on every
    # authorization attempt, never cached from an earlier read.
    conn = _open(tmp_path)
    try:
        _seed_retry_scheduled(
            conn,
            job_id="job-x",
            key="job-a",
            canonical=CANONICAL_A,
            attempt_id="attempt-id-1",
            completed_at="2026-01-01T00:00:00.000000Z",
            retry_after_ms=60_000,
        )

        # A forward jump to well past the 60-second eligibility (simulating
        # a wall-clock adjustment, not merely elapsed monotonic time).
        result = tx.run_pre_side_effect_transaction(
            conn,
            supplied_key="job-a",
            key_generator=_sequence(),
            canonical_bytes=CANONICAL_A,
            id_generator=_sequence("attempt-id-2"),
            invocation_token="t1",
            clock=_clock(61),
        )
        assert isinstance(result, tx.ReadyToSubmit)
        assert result.attempt.attempt_number == 2
    finally:
        conn.close()


def test_backward_wall_clock_jump_before_eligibility_still_defers(
    tmp_path: Path,
) -> None:
    # A backward wall-clock adjustment can only delay takeover/retry, never
    # bypass eligibility: reading `now` from well before the attempt even
    # completed must still report the same durable `not_before`, not an
    # earlier or corrupted value.
    conn = _open(tmp_path)
    try:
        _seed_retry_scheduled(
            conn,
            job_id="job-x",
            key="job-a",
            canonical=CANONICAL_A,
            attempt_id="attempt-id-1",
            completed_at="2026-01-01T00:00:00.000000Z",
            retry_after_ms=60_000,
        )

        result = tx.run_pre_side_effect_transaction(
            conn,
            supplied_key="job-a",
            key_generator=_sequence(),
            canonical_bytes=CANONICAL_A,
            id_generator=_sequence(),
            invocation_token="t1",
            clock=_clock(-3600),  # one hour before the attempt even completed
        )
        assert isinstance(result, tx.NotYetEligible)
        assert result.not_before == "2026-01-01T00:01:00.000000Z"
    finally:
        conn.close()


def test_equivalent_supplied_key_for_failed_permanent_job_is_terminal(
    tmp_path: Path,
) -> None:
    conn = _open(tmp_path)
    try:
        _seed_terminal_job(
            conn,
            job_id="job-x",
            key="job-a",
            canonical=CANONICAL_A,
            job_state="FAILED_PERMANENT",
            attempt_id="attempt-id-1",
            attempt_state="PERMANENT_FAILURE",
            error_category="validation_rejected",
        )

        result = tx.run_pre_side_effect_transaction(
            conn,
            supplied_key="job-a",
            key_generator=_sequence(),
            canonical_bytes=CANONICAL_A,
            id_generator=_sequence(),
            invocation_token="t1",
            clock=_clock(),
        )
        assert isinstance(result, tx.TerminalFailedPermanent)
        assert result.attempt.state == "PERMANENT_FAILURE"
    finally:
        conn.close()


def test_equivalent_supplied_key_for_ambiguous_job_is_terminal(tmp_path: Path) -> None:
    conn = _open(tmp_path)
    try:
        _seed_terminal_job(
            conn,
            job_id="job-x",
            key="job-a",
            canonical=CANONICAL_A,
            job_state="AMBIGUOUS",
            attempt_id="attempt-id-1",
            attempt_state="AMBIGUOUS",
            error_category="reconciliation_inconclusive",
        )

        result = tx.run_pre_side_effect_transaction(
            conn,
            supplied_key="job-a",
            key_generator=_sequence(),
            canonical_bytes=CANONICAL_A,
            id_generator=_sequence(),
            invocation_token="t1",
            clock=_clock(),
        )
        assert isinstance(result, tx.TerminalAmbiguous)
        assert result.attempt.state == "AMBIGUOUS"
    finally:
        conn.close()


def test_non_equivalent_supplied_key_reuse_is_idempotency_conflict(
    tmp_path: Path,
) -> None:
    conn = _open(tmp_path)
    try:
        _seed_ready_to_submit(
            conn,
            "job-a",
            CANONICAL_A,
            job_id="job-id-1",
            attempt_id="attempt-id-1",
            token="t1",
        )

        result = tx.run_pre_side_effect_transaction(
            conn,
            supplied_key="job-a",
            key_generator=_sequence(),
            canonical_bytes=CANONICAL_B,
            id_generator=_sequence(),
            invocation_token="t2",
            clock=_clock(1),
        )
        assert isinstance(result, tx.IdempotencyConflict)

        # The original Job must be unchanged.
        row = conn.execute(
            "SELECT payload_canonical, state FROM jobs WHERE job_id = 'job-id-1'"
        ).fetchone()
        assert row == (CANONICAL_A, "SUBMITTING")
    finally:
        conn.close()


def test_equivalent_succeeded_job_is_already_completed(tmp_path: Path) -> None:
    conn = _open(tmp_path)
    try:
        now = "2026-01-01T00:00:00.000000Z"
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "INSERT INTO jobs (job_id, idempotency_key, payload_canonical, state, created_at, updated_at) "
            "VALUES ('job-x', 'job-a', ?, 'SUCCEEDED', ?, ?)",
            (CANONICAL_A, now, now),
        )
        conn.execute(
            "INSERT INTO submission_attempts "
            "(attempt_id, job_id, attempt_number, state, started_at, completed_at, "
            "http_status, remote_request_id, fencing_generation) "
            "VALUES ('attempt-x', 'job-x', 1, 'SUCCEEDED', ?, ?, 201, 'remote-1', 1)",
            (now, now),
        )
        conn.execute("COMMIT")

        result = tx.run_pre_side_effect_transaction(
            conn,
            supplied_key="job-a",
            key_generator=_sequence(),
            canonical_bytes=CANONICAL_A,
            id_generator=_sequence(),
            invocation_token="t1",
            clock=_clock(),
        )
        assert isinstance(result, tx.AlreadyCompleted)
        assert result.job.job_id == "job-x"
        assert result.attempt.attempt_id == "attempt-x"
        assert result.attempt.http_status == 201
    finally:
        conn.close()


def test_succeeded_job_with_no_successful_attempt_is_database_corruption(
    tmp_path: Path,
) -> None:
    conn = _open(tmp_path)
    try:
        now = "2026-01-01T00:00:00.000000Z"
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "INSERT INTO jobs (job_id, idempotency_key, payload_canonical, state, created_at, updated_at) "
            "VALUES ('job-x', 'job-a', ?, 'SUCCEEDED', ?, ?)",
            (CANONICAL_A, now, now),
        )
        conn.execute("COMMIT")

        with pytest.raises(DatabaseCorruptionError):
            tx.run_pre_side_effect_transaction(
                conn,
                supplied_key="job-a",
                key_generator=_sequence(),
                canonical_bytes=CANONICAL_A,
                id_generator=_sequence(),
                invocation_token="t1",
                clock=_clock(),
            )
    finally:
        conn.close()


def test_succeeded_job_with_undecodable_payload_is_database_corruption(
    tmp_path: Path,
) -> None:
    conn = _open(tmp_path)
    try:
        now = "2026-01-01T00:00:00.000000Z"
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "INSERT INTO jobs (job_id, idempotency_key, payload_canonical, state, created_at, updated_at) "
            "VALUES ('job-x', 'job-a', X'ff00', 'SUCCEEDED', ?, ?)",
            (now, now),
        )
        conn.execute(
            "INSERT INTO submission_attempts "
            "(attempt_id, job_id, attempt_number, state, started_at, completed_at, "
            "http_status, remote_request_id, fencing_generation) "
            "VALUES ('attempt-x', 'job-x', 1, 'SUCCEEDED', ?, ?, 201, 'remote-1', 1)",
            (now, now),
        )
        conn.execute("COMMIT")

        with pytest.raises(DatabaseCorruptionError):
            tx.run_pre_side_effect_transaction(
                conn,
                supplied_key="job-a",
                key_generator=_sequence(),
                # Undecodable stored payload; the corrupt row is matched by
                # idempotency key alone (equivalence is a raw byte compare,
                # so use the same stored bytes here to reach that branch).
                canonical_bytes=b"\xff\x00",
                id_generator=_sequence(),
                invocation_token="t1",
                clock=_clock(),
            )
    finally:
        conn.close()


def test_stored_non_canonical_payload_is_database_corruption_not_conflict(
    tmp_path: Path,
) -> None:
    # Regression test: valid JSON that is merely *equivalent* to, but not
    # byte-identical with, its own canonical form must be detected as
    # corruption rather than silently compared byte-for-byte and reported
    # as an ordinary idempotency conflict.
    conn = _open(tmp_path)
    try:
        non_canonical = b'{"a": 1}'  # valid JSON; canonical form is b'{"a":1}'
        now = "2026-01-01T00:00:00.000000Z"
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "INSERT INTO jobs (job_id, idempotency_key, payload_canonical, state, created_at, updated_at) "
            "VALUES ('job-x', 'job-a', ?, 'PENDING', ?, ?)",
            (non_canonical, now, now),
        )
        conn.execute("COMMIT")

        with pytest.raises(DatabaseCorruptionError):
            tx.run_pre_side_effect_transaction(
                conn,
                supplied_key="job-a",
                key_generator=_sequence(),
                canonical_bytes=CANONICAL_A,
                id_generator=_sequence(),
                invocation_token="t1",
                clock=_clock(),
            )
    finally:
        conn.close()


def test_stored_successful_attempt_with_invalid_http_status_is_database_corruption(
    tmp_path: Path,
) -> None:
    conn = _open(tmp_path)
    try:
        now = "2026-01-01T00:00:00.000000Z"
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "INSERT INTO jobs (job_id, idempotency_key, payload_canonical, state, created_at, updated_at) "
            "VALUES ('job-x', 'job-a', ?, 'SUCCEEDED', ?, ?)",
            (CANONICAL_A, now, now),
        )
        conn.execute(
            "INSERT INTO submission_attempts "
            "(attempt_id, job_id, attempt_number, state, started_at, completed_at, "
            "http_status, remote_request_id, fencing_generation) "
            "VALUES ('attempt-x', 'job-x', 1, 'SUCCEEDED', ?, ?, 500, 'remote-1', 1)",
            (now, now),
        )
        conn.execute("COMMIT")

        with pytest.raises(DatabaseCorruptionError):
            tx.run_pre_side_effect_transaction(
                conn,
                supplied_key="job-a",
                key_generator=_sequence(),
                canonical_bytes=CANONICAL_A,
                id_generator=_sequence(),
                invocation_token="t1",
                clock=_clock(),
            )
    finally:
        conn.close()


def test_stored_empty_string_remote_request_id_is_not_corruption(
    tmp_path: Path,
) -> None:
    # Regression test: the external client accepts any string
    # `remote_request_id`, including "" (the specification requires "a
    # string", not a non-empty one). A fresh success stored with an empty
    # `remote_request_id` must remain replayable, not be rejected as
    # corrupted evidence.
    conn = _open(tmp_path)
    try:
        now = "2026-01-01T00:00:00.000000Z"
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "INSERT INTO jobs (job_id, idempotency_key, payload_canonical, state, created_at, updated_at) "
            "VALUES ('job-x', 'job-a', ?, 'SUCCEEDED', ?, ?)",
            (CANONICAL_A, now, now),
        )
        conn.execute(
            "INSERT INTO submission_attempts "
            "(attempt_id, job_id, attempt_number, state, started_at, completed_at, "
            "http_status, remote_request_id, fencing_generation) "
            "VALUES ('attempt-x', 'job-x', 1, 'SUCCEEDED', ?, ?, 201, '', 1)",
            (now, now),
        )
        conn.execute("COMMIT")

        result = tx.run_pre_side_effect_transaction(
            conn,
            supplied_key="job-a",
            key_generator=_sequence(),
            canonical_bytes=CANONICAL_A,
            id_generator=_sequence(),
            invocation_token="t1",
            clock=_clock(),
        )
        assert isinstance(result, tx.AlreadyCompleted)
        assert result.attempt.remote_request_id == ""
    finally:
        conn.close()


# -- Concurrency across real connections to the same file --------------------


def test_concurrent_same_supplied_key_creates_exactly_one_ready_to_submit(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "db.sqlite3"
    connection_mod.open_connection(db_path).close()

    outcomes: list[object] = []
    lock = threading.Lock()
    barrier = threading.Barrier(2)

    def worker(token: str, job_id: str, attempt_id: str, at: int) -> None:
        conn = connection_mod.open_connection(db_path)
        try:
            barrier.wait()
            result = tx.run_pre_side_effect_transaction(
                conn,
                supplied_key="race-key",
                key_generator=_sequence(),
                canonical_bytes=CANONICAL_A,
                id_generator=_sequence(job_id, attempt_id),
                invocation_token=token,
                clock=_clock(at),
            )
            with lock:
                outcomes.append(result)
        finally:
            conn.close()

    threads = [
        threading.Thread(target=worker, args=("t1", "job-1", "attempt-1", 0)),
        threading.Thread(target=worker, args=("t2", "job-2", "attempt-2", 1)),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)

    ready = [o for o in outcomes if isinstance(o, tx.ReadyToSubmit)]
    in_progress = [o for o in outcomes if isinstance(o, tx.JobInProgress)]
    assert len(ready) == 1
    assert len(in_progress) == 1
    assert in_progress[0].job.state == "SUBMITTING"


# -- Pre-dispatch lease renewal -----------------------------------------------


def test_renew_lease_succeeds_immediately_after_pre_side_effect_commit(
    tmp_path: Path,
) -> None:
    conn = _open(tmp_path)
    try:
        ready = _seed_ready_to_submit(
            conn,
            "job-a",
            CANONICAL_A,
            job_id="job-id-1",
            attempt_id="attempt-id-1",
            token="t1",
        )
        result = tx.renew_lease(
            conn,
            job_id=ready.job.job_id,
            attempt_id=ready.attempt.attempt_id,
            invocation_token="t1",
            fencing_generation=ready.attempt.fencing_generation,
            clock=_clock(1),
        )
        assert isinstance(result, tx.LeaseRenewed)

        # The lease is actually extended, not merely observed: a renewal
        # anchored at t=1s plus the 60-second lease duration must still be
        # valid at t=59s, well past the original t=0s expiry+59s boundary a
        # stale (unrenewed) lease would have failed.
        second = tx.renew_lease(
            conn,
            job_id=ready.job.job_id,
            attempt_id=ready.attempt.attempt_id,
            invocation_token="t1",
            fencing_generation=ready.attempt.fencing_generation,
            clock=_clock(59),
        )
        assert isinstance(second, tx.LeaseRenewed)
    finally:
        conn.close()


def test_renew_lease_fails_after_expiry(tmp_path: Path) -> None:
    conn = _open(tmp_path)
    try:
        ready = _seed_ready_to_submit(
            conn,
            "job-a",
            CANONICAL_A,
            job_id="job-id-1",
            attempt_id="attempt-id-1",
            token="t1",
        )
        result = tx.renew_lease(
            conn,
            job_id=ready.job.job_id,
            attempt_id=ready.attempt.attempt_id,
            invocation_token="t1",
            fencing_generation=ready.attempt.fencing_generation,
            clock=_clock(61),
        )
        assert isinstance(result, tx.LeaseLost)
    finally:
        conn.close()


def test_renew_lease_fails_for_a_different_invocation_token(
    tmp_path: Path,
) -> None:
    conn = _open(tmp_path)
    try:
        ready = _seed_ready_to_submit(
            conn,
            "job-a",
            CANONICAL_A,
            job_id="job-id-1",
            attempt_id="attempt-id-1",
            token="t1",
        )
        result = tx.renew_lease(
            conn,
            job_id=ready.job.job_id,
            attempt_id=ready.attempt.attempt_id,
            invocation_token="a-different-token",
            fencing_generation=ready.attempt.fencing_generation,
            clock=_clock(1),
        )
        assert isinstance(result, tx.LeaseLost)
    finally:
        conn.close()


def test_renew_lease_fails_for_a_stale_fencing_generation(tmp_path: Path) -> None:
    conn = _open(tmp_path)
    try:
        ready = _seed_ready_to_submit(
            conn,
            "job-a",
            CANONICAL_A,
            job_id="job-id-1",
            attempt_id="attempt-id-1",
            token="t1",
        )
        result = tx.renew_lease(
            conn,
            job_id=ready.job.job_id,
            attempt_id=ready.attempt.attempt_id,
            invocation_token="t1",
            fencing_generation=ready.attempt.fencing_generation + 1,
            clock=_clock(1),
        )
        assert isinstance(result, tx.LeaseLost)
    finally:
        conn.close()


# -- Post-side-effect submission finalization ---------------------------------


class _FixedRandomSource:
    def __init__(self, value: int = 0) -> None:
        self._value = value

    def randint(self, low: int, high: int) -> int:
        return max(low, min(high, self._value))


_SUCCESS_EVIDENCE = tx.SubmissionEvidence(
    disposition="succeeded",
    http_status=201,
    remote_request_id="remote-1",
    error_category=None,
    retry_after_values=(),
)


def test_finalize_submission_attempt_completes_attempt_and_job_on_success(
    tmp_path: Path,
) -> None:
    conn = _open(tmp_path)
    try:
        ready = _seed_ready_to_submit(
            conn,
            "job-a",
            CANONICAL_A,
            job_id="job-id-1",
            attempt_id="attempt-id-1",
            token="t1",
        )
        result = tx.finalize_submission_attempt(
            conn,
            job_id=ready.job.job_id,
            attempt_id=ready.attempt.attempt_id,
            attempt_number=ready.attempt.attempt_number,
            invocation_token="t1",
            fencing_generation=ready.attempt.fencing_generation,
            clock=_clock(5),
            random_source=_FixedRandomSource(),
            id_generator=_counting_id_generator(),
            evidence=_SUCCESS_EVIDENCE,
        )
        assert isinstance(result, tx.SubmissionFinalized)
        assert result.job.state == "SUCCEEDED"
        assert result.attempt.state == "SUCCEEDED"

        job_row = conn.execute(
            "SELECT state FROM jobs WHERE job_id = ?", (ready.job.job_id,)
        ).fetchone()
        assert job_row == ("SUCCEEDED",)

        attempt_row = conn.execute(
            "SELECT state, http_status, remote_request_id, owner_token, lease_expires_at "
            "FROM submission_attempts WHERE attempt_id = ?",
            (ready.attempt.attempt_id,),
        ).fetchone()
        assert attempt_row == ("SUCCEEDED", 201, "remote-1", None, None)
    finally:
        conn.close()


def test_finalize_submission_attempt_fails_when_invocation_token_does_not_match(
    tmp_path: Path,
) -> None:
    conn = _open(tmp_path)
    try:
        ready = _seed_ready_to_submit(
            conn,
            "job-a",
            CANONICAL_A,
            job_id="job-id-1",
            attempt_id="attempt-id-1",
            token="t1",
        )
        result = tx.finalize_submission_attempt(
            conn,
            job_id=ready.job.job_id,
            attempt_id=ready.attempt.attempt_id,
            attempt_number=ready.attempt.attempt_number,
            invocation_token="stale-token",
            fencing_generation=ready.attempt.fencing_generation,
            clock=_clock(5),
            random_source=_FixedRandomSource(),
            id_generator=_counting_id_generator(),
            evidence=_SUCCESS_EVIDENCE,
        )
        assert isinstance(result, tx.PostSideEffectFailure)

        job_row = conn.execute(
            "SELECT state FROM jobs WHERE job_id = ?", (ready.job.job_id,)
        ).fetchone()
        assert job_row == ("SUBMITTING",)
    finally:
        conn.close()


def test_finalize_submission_attempt_fails_when_lease_has_expired(
    tmp_path: Path,
) -> None:
    conn = _open(tmp_path)
    try:
        ready = _seed_ready_to_submit(
            conn,
            "job-a",
            CANONICAL_A,
            job_id="job-id-1",
            attempt_id="attempt-id-1",
            token="t1",
        )
        # 61 seconds later: the 60-second lease has expired.
        result = tx.finalize_submission_attempt(
            conn,
            job_id=ready.job.job_id,
            attempt_id=ready.attempt.attempt_id,
            attempt_number=ready.attempt.attempt_number,
            invocation_token="t1",
            fencing_generation=ready.attempt.fencing_generation,
            clock=_clock(61),
            random_source=_FixedRandomSource(),
            id_generator=_counting_id_generator(),
            evidence=_SUCCESS_EVIDENCE,
        )
        assert isinstance(result, tx.PostSideEffectFailure)

        job_row = conn.execute(
            "SELECT state FROM jobs WHERE job_id = ?", (ready.job.job_id,)
        ).fetchone()
        assert job_row == ("SUBMITTING",)
    finally:
        conn.close()


def test_finalize_submission_attempt_fails_for_a_stale_fencing_generation(
    tmp_path: Path,
) -> None:
    conn = _open(tmp_path)
    try:
        ready = _seed_ready_to_submit(
            conn,
            "job-a",
            CANONICAL_A,
            job_id="job-id-1",
            attempt_id="attempt-id-1",
            token="t1",
        )
        result = tx.finalize_submission_attempt(
            conn,
            job_id=ready.job.job_id,
            attempt_id=ready.attempt.attempt_id,
            attempt_number=ready.attempt.attempt_number,
            invocation_token="t1",
            fencing_generation=ready.attempt.fencing_generation + 1,
            clock=_clock(5),
            random_source=_FixedRandomSource(),
            id_generator=_counting_id_generator(),
            evidence=_SUCCESS_EVIDENCE,
        )
        assert isinstance(result, tx.PostSideEffectFailure)

        job_row = conn.execute(
            "SELECT state FROM jobs WHERE job_id = ?", (ready.job.job_id,)
        ).fetchone()
        assert job_row == ("SUBMITTING",)
    finally:
        conn.close()


def test_finalize_submission_attempt_permanent_failure_sets_terminal_state(
    tmp_path: Path,
) -> None:
    conn = _open(tmp_path)
    try:
        ready = _seed_ready_to_submit(
            conn,
            "job-a",
            CANONICAL_A,
            job_id="job-id-1",
            attempt_id="attempt-id-1",
            token="t1",
        )
        result = tx.finalize_submission_attempt(
            conn,
            job_id=ready.job.job_id,
            attempt_id=ready.attempt.attempt_id,
            attempt_number=ready.attempt.attempt_number,
            invocation_token="t1",
            fencing_generation=ready.attempt.fencing_generation,
            clock=_clock(5),
            random_source=_FixedRandomSource(),
            id_generator=_counting_id_generator(),
            evidence=tx.SubmissionEvidence(
                disposition="permanent_failure",
                http_status=400,
                remote_request_id=None,
                error_category="validation_rejected",
                retry_after_values=(),
            ),
        )
        assert isinstance(result, tx.SubmissionFinalized)
        assert result.job.state == "FAILED_PERMANENT"
        assert result.attempt.state == "PERMANENT_FAILURE"
        assert result.attempt.error_category == "validation_rejected"
    finally:
        conn.close()


def test_finalize_submission_attempt_retryable_failure_schedules_retry(
    tmp_path: Path,
) -> None:
    conn = _open(tmp_path)
    try:
        ready = _seed_ready_to_submit(
            conn,
            "job-a",
            CANONICAL_A,
            job_id="job-id-1",
            attempt_id="attempt-id-1",
            token="t1",
        )
        result = tx.finalize_submission_attempt(
            conn,
            job_id=ready.job.job_id,
            attempt_id=ready.attempt.attempt_id,
            attempt_number=ready.attempt.attempt_number,
            invocation_token="t1",
            fencing_generation=ready.attempt.fencing_generation,
            clock=_clock(5),
            random_source=_FixedRandomSource(0),
            id_generator=_counting_id_generator(),
            evidence=tx.SubmissionEvidence(
                disposition="retryable_failure",
                http_status=None,
                remote_request_id=None,
                error_category="transport_not_sent",
                retry_after_values=(),
            ),
        )
        assert isinstance(result, tx.SubmissionRetryScheduled)
        assert result.job.state == "RETRY_SCHEDULED"
        assert result.attempt.state == "RETRYABLE_FAILURE"
        assert result.attempt.retry_after_ms == 0
    finally:
        conn.close()


def test_finalize_submission_attempt_retryable_failure_exhausts_budget_on_third_attempt(
    tmp_path: Path,
) -> None:
    conn = _open(tmp_path)
    try:
        _seed_retry_scheduled(
            conn,
            job_id="job-x",
            key="job-a",
            canonical=CANONICAL_A,
            attempt_id="attempt-id-2",
            completed_at="2026-01-01T00:00:00.000000Z",
            retry_after_ms=0,
            attempt_number=2,
        )
        ready = tx.run_pre_side_effect_transaction(
            conn,
            supplied_key="job-a",
            key_generator=_sequence(),
            canonical_bytes=CANONICAL_A,
            id_generator=_sequence("attempt-id-3"),
            invocation_token="t1",
            clock=_clock(1),
        )
        assert isinstance(ready, tx.ReadyToSubmit)
        assert ready.attempt.attempt_number == 3

        result = tx.finalize_submission_attempt(
            conn,
            job_id=ready.job.job_id,
            attempt_id=ready.attempt.attempt_id,
            attempt_number=ready.attempt.attempt_number,
            invocation_token="t1",
            fencing_generation=ready.attempt.fencing_generation,
            clock=_clock(2),
            random_source=_FixedRandomSource(0),
            id_generator=_counting_id_generator(),
            evidence=tx.SubmissionEvidence(
                disposition="retryable_failure",
                http_status=429,
                remote_request_id=None,
                error_category="rate_limited",
                retry_after_values=(),
            ),
        )
        assert isinstance(result, tx.SubmissionFinalized)
        assert result.job.state == "FAILED_PERMANENT"
        assert result.attempt.state == "RETRYABLE_FAILURE"
    finally:
        conn.close()


def test_finalize_submission_attempt_retained_leaves_attempt_and_job_unchanged(
    tmp_path: Path,
) -> None:
    conn = _open(tmp_path)
    try:
        ready = _seed_ready_to_submit(
            conn,
            "job-a",
            CANONICAL_A,
            job_id="job-id-1",
            attempt_id="attempt-id-1",
            token="t1",
        )
        result = tx.finalize_submission_attempt(
            conn,
            job_id=ready.job.job_id,
            attempt_id=ready.attempt.attempt_id,
            attempt_number=ready.attempt.attempt_number,
            invocation_token="t1",
            fencing_generation=ready.attempt.fencing_generation,
            clock=_clock(5),
            random_source=_FixedRandomSource(),
            id_generator=_counting_id_generator(),
            evidence=tx.SubmissionEvidence(
                disposition="retained",
                http_status=500,
                remote_request_id=None,
                error_category="server_uncertain",
                retry_after_values=(),
            ),
        )
        assert isinstance(result, tx.SubmissionRetained)

        job_row = conn.execute(
            "SELECT state FROM jobs WHERE job_id = ?", (ready.job.job_id,)
        ).fetchone()
        assert job_row == ("SUBMITTING",)
        attempt_row = conn.execute(
            "SELECT state FROM submission_attempts WHERE attempt_id = ?",
            (ready.attempt.attempt_id,),
        ).fetchone()
        assert attempt_row == ("STARTED",)
    finally:
        conn.close()


def test_finalize_submission_attempt_rate_limited_advances_gate(
    tmp_path: Path,
) -> None:
    conn = _open(tmp_path)
    try:
        ready = _seed_ready_to_submit(
            conn,
            "job-a",
            CANONICAL_A,
            job_id="job-id-1",
            attempt_id="attempt-id-1",
            token="t1",
        )
        tx.finalize_submission_attempt(
            conn,
            job_id=ready.job.job_id,
            attempt_id=ready.attempt.attempt_id,
            attempt_number=ready.attempt.attempt_number,
            invocation_token="t1",
            fencing_generation=ready.attempt.fencing_generation,
            clock=_clock(0),
            random_source=_FixedRandomSource(0),
            id_generator=_counting_id_generator(),
            evidence=tx.SubmissionEvidence(
                disposition="retryable_failure",
                http_status=429,
                remote_request_id=None,
                error_category="rate_limited",
                retry_after_values=("1",),
            ),
        )
        gate_row = conn.execute(
            "SELECT not_before FROM service_rate_limit_gate WHERE singleton_id = 1"
        ).fetchone()
        assert gate_row == ("2026-01-01T00:00:01.000000Z",)
    finally:
        conn.close()


def test_shared_gate_blocks_an_unrelated_jobs_first_attempt(tmp_path: Path) -> None:
    # Job A's `429` closes the one service-wide gate; an unrelated Job B's
    # *first* attempt must be deferred by that same gate, not just Job A's
    # own retries.
    conn = _open(tmp_path)
    try:
        ready_a = _seed_ready_to_submit(
            conn,
            "job-a",
            CANONICAL_A,
            job_id="job-id-a",
            attempt_id="attempt-id-a1",
            token="t1",
        )
        tx.finalize_submission_attempt(
            conn,
            job_id=ready_a.job.job_id,
            attempt_id=ready_a.attempt.attempt_id,
            attempt_number=ready_a.attempt.attempt_number,
            invocation_token="t1",
            fencing_generation=ready_a.attempt.fencing_generation,
            clock=_clock(0),
            random_source=_FixedRandomSource(0),
            id_generator=_counting_id_generator(),
            evidence=tx.SubmissionEvidence(
                disposition="retryable_failure",
                http_status=429,
                remote_request_id=None,
                error_category="rate_limited",
                retry_after_values=("3600",),
            ),
        )

        result_b = tx.run_pre_side_effect_transaction(
            conn,
            supplied_key="job-b",
            key_generator=_sequence(),
            canonical_bytes=CANONICAL_B,
            id_generator=_sequence("job-id-b"),
            invocation_token="t2",
            clock=_clock(1),
        )
        assert isinstance(result_b, tx.NotYetEligible)
        assert result_b.not_before == "2026-01-01T01:00:00.000000Z"
    finally:
        conn.close()


def test_forward_wall_clock_jump_past_gate_authorizes_an_unrelated_jobs_attempt(
    tmp_path: Path,
) -> None:
    # The gate check is a fresh wall-clock comparison on every authorization
    # attempt, never a cached decision: a forward wall-clock adjustment past
    # the gate's `not_before` must authorize an unrelated Job's first
    # attempt, exactly as genuinely waiting out the delay would.
    conn = _open(tmp_path)
    try:
        ready_a = _seed_ready_to_submit(
            conn,
            "job-a",
            CANONICAL_A,
            job_id="job-id-a",
            attempt_id="attempt-id-a1",
            token="t1",
        )
        tx.finalize_submission_attempt(
            conn,
            job_id=ready_a.job.job_id,
            attempt_id=ready_a.attempt.attempt_id,
            attempt_number=ready_a.attempt.attempt_number,
            invocation_token="t1",
            fencing_generation=ready_a.attempt.fencing_generation,
            clock=_clock(0),
            random_source=_FixedRandomSource(0),
            id_generator=_counting_id_generator(),
            evidence=tx.SubmissionEvidence(
                disposition="retryable_failure",
                http_status=429,
                remote_request_id=None,
                error_category="rate_limited",
                retry_after_values=("3600",),
            ),
        )

        # A forward jump to well past the gate's `not_before` (`00:01:00` +
        # 3600s), simulating a wall-clock adjustment rather than a genuine
        # 3600-second wait.
        result_b = tx.run_pre_side_effect_transaction(
            conn,
            supplied_key="job-b",
            key_generator=_sequence(),
            canonical_bytes=CANONICAL_B,
            id_generator=_sequence("job-id-b", "attempt-id-b1"),
            invocation_token="t2",
            clock=_clock(3601),
        )
        assert isinstance(result_b, tx.ReadyToSubmit)
    finally:
        conn.close()


# -- Reconciliation authorization and finalization ----------------------------


def _seed_retained_attempt(
    conn, key: str, canonical: bytes, *, job_id: str, attempt_id: str, token: str
) -> tuple[tx.JobRow, tx.AttemptRow]:
    """Seed a Job/attempt left `SUBMITTING`/`STARTED` by a `retained`

    (`5xx`/`MAYBE_SENT`/protocol-uncertain) submission classification --
    exactly the state automatic reconciliation begins from.
    """

    ready = _seed_ready_to_submit(
        conn, key, canonical, job_id=job_id, attempt_id=attempt_id, token=token
    )
    result = tx.finalize_submission_attempt(
        conn,
        job_id=ready.job.job_id,
        attempt_id=ready.attempt.attempt_id,
        attempt_number=ready.attempt.attempt_number,
        invocation_token=token,
        fencing_generation=ready.attempt.fencing_generation,
        clock=_clock(1),
        random_source=_FixedRandomSource(0),
        id_generator=_counting_id_generator(),
        evidence=tx.SubmissionEvidence(
            disposition="retained",
            http_status=500,
            remote_request_id=None,
            error_category="server_uncertain",
            retry_after_values=(),
        ),
    )
    assert isinstance(result, tx.SubmissionRetained)
    return result.job, result.attempt


def test_authorize_reconciliation_request_succeeds_and_renews_lease(
    tmp_path: Path,
) -> None:
    conn = _open(tmp_path)
    try:
        job, attempt = _seed_retained_attempt(
            conn,
            "job-a",
            CANONICAL_A,
            job_id="job-id-1",
            attempt_id="attempt-id-1",
            token="t1",
        )
        result = tx.authorize_reconciliation_request(
            conn,
            job_id=job.job_id,
            attempt_id=attempt.attempt_id,
            invocation_token="t1",
            fencing_generation=attempt.fencing_generation,
            clock=_clock(2),
        )
        assert isinstance(result, tx.ReconciliationAuthorized)

        lease_row = conn.execute(
            "SELECT lease_expires_at FROM submission_attempts WHERE attempt_id = ?",
            (attempt.attempt_id,),
        ).fetchone()
        assert lease_row == ("2026-01-01T00:01:02.000000Z",)
    finally:
        conn.close()


def test_authorize_reconciliation_request_deferred_by_gate(tmp_path: Path) -> None:
    conn = _open(tmp_path)
    try:
        job, attempt = _seed_retained_attempt(
            conn,
            "job-a",
            CANONICAL_A,
            job_id="job-id-1",
            attempt_id="attempt-id-1",
            token="t1",
        )
        tx.advance_gate_for_reconciliation_rate_limit(
            conn, delay_ms=60_000, clock=_clock(2)
        )

        result = tx.authorize_reconciliation_request(
            conn,
            job_id=job.job_id,
            attempt_id=attempt.attempt_id,
            invocation_token="t1",
            fencing_generation=attempt.fencing_generation,
            clock=_clock(3),
        )
        assert isinstance(result, tx.ReconciliationDeferred)
        assert result.not_before == "2026-01-01T00:01:02.000000Z"
    finally:
        conn.close()


def test_authorize_reconciliation_request_deferred_by_own_not_before(
    tmp_path: Path,
) -> None:
    conn = _open(tmp_path)
    try:
        job, attempt = _seed_retained_attempt(
            conn,
            "job-a",
            CANONICAL_A,
            job_id="job-id-1",
            attempt_id="attempt-id-1",
            token="t1",
        )
        result = tx.authorize_reconciliation_request(
            conn,
            job_id=job.job_id,
            attempt_id=attempt.attempt_id,
            invocation_token="t1",
            fencing_generation=attempt.fencing_generation,
            clock=_clock(2),
            not_before="2026-01-01T00:05:00.000000Z",
        )
        assert isinstance(result, tx.ReconciliationDeferred)
        assert result.not_before == "2026-01-01T00:05:00.000000Z"
    finally:
        conn.close()


def test_authorize_reconciliation_request_ownership_lost_for_wrong_token(
    tmp_path: Path,
) -> None:
    conn = _open(tmp_path)
    try:
        job, attempt = _seed_retained_attempt(
            conn,
            "job-a",
            CANONICAL_A,
            job_id="job-id-1",
            attempt_id="attempt-id-1",
            token="t1",
        )
        result = tx.authorize_reconciliation_request(
            conn,
            job_id=job.job_id,
            attempt_id=attempt.attempt_id,
            invocation_token="stale-token",
            fencing_generation=attempt.fencing_generation,
            clock=_clock(2),
        )
        assert isinstance(result, tx.ReconciliationOwnershipLost)
    finally:
        conn.close()


def test_record_reconciliation_result_matched_completes_success(
    tmp_path: Path,
) -> None:
    conn = _open(tmp_path)
    try:
        job, attempt = _seed_retained_attempt(
            conn,
            "job-a",
            CANONICAL_A,
            job_id="job-id-1",
            attempt_id="attempt-id-1",
            token="t1",
        )
        result = tx.record_reconciliation_result(
            conn,
            job_id=job.job_id,
            attempt_id=attempt.attempt_id,
            invocation_token="t1",
            fencing_generation=attempt.fencing_generation,
            clock=_clock(3),
            disposition="matched",
            http_status=200,
            remote_request_id="remote-1",
        )
        assert isinstance(result, tx.ReconciliationFinalized)
        assert result.job.state == "SUCCEEDED"
        assert result.attempt.state == "SUCCEEDED"
        assert result.attempt.http_status == 200
        assert result.attempt.remote_request_id == "remote-1"

        job_row = conn.execute(
            "SELECT state FROM jobs WHERE job_id = ?", (job.job_id,)
        ).fetchone()
        assert job_row == ("SUCCEEDED",)
    finally:
        conn.close()


def test_record_reconciliation_result_conflict_completes_permanent_failure(
    tmp_path: Path,
) -> None:
    conn = _open(tmp_path)
    try:
        job, attempt = _seed_retained_attempt(
            conn,
            "job-a",
            CANONICAL_A,
            job_id="job-id-1",
            attempt_id="attempt-id-1",
            token="t1",
        )
        result = tx.record_reconciliation_result(
            conn,
            job_id=job.job_id,
            attempt_id=attempt.attempt_id,
            invocation_token="t1",
            fencing_generation=attempt.fencing_generation,
            clock=_clock(3),
            disposition="conflict",
        )
        assert isinstance(result, tx.ReconciliationFinalized)
        assert result.job.state == "FAILED_PERMANENT"
        assert result.attempt.state == "PERMANENT_FAILURE"
        assert result.attempt.error_category == "idempotency_conflict"
    finally:
        conn.close()


def test_record_reconciliation_result_not_found_completes_ambiguous(
    tmp_path: Path,
) -> None:
    conn = _open(tmp_path)
    try:
        job, attempt = _seed_retained_attempt(
            conn,
            "job-a",
            CANONICAL_A,
            job_id="job-id-1",
            attempt_id="attempt-id-1",
            token="t1",
        )
        result = tx.record_reconciliation_result(
            conn,
            job_id=job.job_id,
            attempt_id=attempt.attempt_id,
            invocation_token="t1",
            fencing_generation=attempt.fencing_generation,
            clock=_clock(3),
            disposition="not_found",
        )
        assert isinstance(result, tx.ReconciliationFinalized)
        assert result.job.state == "AMBIGUOUS"
        assert result.attempt.state == "AMBIGUOUS"
        assert result.attempt.error_category == "reconciliation_not_found"
    finally:
        conn.close()


def test_record_reconciliation_result_inconclusive_completes_ambiguous(
    tmp_path: Path,
) -> None:
    conn = _open(tmp_path)
    try:
        job, attempt = _seed_retained_attempt(
            conn,
            "job-a",
            CANONICAL_A,
            job_id="job-id-1",
            attempt_id="attempt-id-1",
            token="t1",
        )
        result = tx.record_reconciliation_result(
            conn,
            job_id=job.job_id,
            attempt_id=attempt.attempt_id,
            invocation_token="t1",
            fencing_generation=attempt.fencing_generation,
            clock=_clock(3),
            disposition="inconclusive",
        )
        assert isinstance(result, tx.ReconciliationFinalized)
        assert result.job.state == "AMBIGUOUS"
        assert result.attempt.state == "AMBIGUOUS"
        assert result.attempt.error_category == "reconciliation_inconclusive"
    finally:
        conn.close()


def test_record_reconciliation_result_fails_for_stale_fencing(tmp_path: Path) -> None:
    conn = _open(tmp_path)
    try:
        job, attempt = _seed_retained_attempt(
            conn,
            "job-a",
            CANONICAL_A,
            job_id="job-id-1",
            attempt_id="attempt-id-1",
            token="t1",
        )
        result = tx.record_reconciliation_result(
            conn,
            job_id=job.job_id,
            attempt_id=attempt.attempt_id,
            invocation_token="t1",
            fencing_generation=attempt.fencing_generation + 1,
            clock=_clock(3),
            disposition="matched",
            http_status=200,
            remote_request_id="remote-1",
        )
        assert isinstance(result, tx.PostSideEffectFailure)

        job_row = conn.execute(
            "SELECT state FROM jobs WHERE job_id = ?", (job.job_id,)
        ).fetchone()
        assert job_row == ("SUBMITTING",)
    finally:
        conn.close()


def test_advance_gate_for_reconciliation_rate_limit(tmp_path: Path) -> None:
    conn = _open(tmp_path)
    try:
        tx.advance_gate_for_reconciliation_rate_limit(
            conn, delay_ms=1000, clock=_clock(0)
        )
        gate_row = conn.execute(
            "SELECT not_before FROM service_rate_limit_gate WHERE singleton_id = 1"
        ).fetchone()
        assert gate_row == ("2026-01-01T00:00:01.000000Z",)

        # A later, smaller candidate must not regress the gate.
        tx.advance_gate_for_reconciliation_rate_limit(conn, delay_ms=1, clock=_clock(0))
        gate_row = conn.execute(
            "SELECT not_before FROM service_rate_limit_gate WHERE singleton_id = 1"
        ).fetchone()
        assert gate_row == ("2026-01-01T00:00:01.000000Z",)
    finally:
        conn.close()


# -- Fenced recovery claim ------------------------------------------------------


def test_claim_expired_attempt_succeeds_and_advances_fencing(tmp_path: Path) -> None:
    conn = _open(tmp_path)
    try:
        ready = _seed_ready_to_submit(
            conn,
            "job-a",
            CANONICAL_A,
            job_id="job-id-1",
            attempt_id="attempt-id-1",
            token="t1",
        )
        result = tx.claim_expired_attempt(
            conn,
            job_id=ready.job.job_id,
            attempt_id=ready.attempt.attempt_id,
            captured_owner_token=ready.attempt.owner_token,
            captured_fencing_generation=ready.attempt.fencing_generation,
            captured_lease_expires_at=ready.attempt.lease_expires_at,
            new_invocation_token="t2",
            clock=_clock(61),
        )
        assert isinstance(result, tx.ClaimSucceeded)
        assert result.attempt.owner_token == "t2"
        assert result.attempt.fencing_generation == 2
        assert result.attempt.lease_expires_at == "2026-01-01T00:02:01.000000Z"

        row = conn.execute(
            "SELECT owner_token, fencing_generation FROM submission_attempts WHERE attempt_id = ?",
            (ready.attempt.attempt_id,),
        ).fetchone()
        assert row == ("t2", 2)
    finally:
        conn.close()


def test_claim_expired_attempt_rejects_when_lease_was_renewed_during_quarantine(
    tmp_path: Path,
) -> None:
    conn = _open(tmp_path)
    try:
        ready = _seed_ready_to_submit(
            conn,
            "job-a",
            CANONICAL_A,
            job_id="job-id-1",
            attempt_id="attempt-id-1",
            token="t1",
        )
        captured_lease_expires_at = ready.attempt.lease_expires_at

        # The original owner renews (or another process already claimed it)
        # during the quarantine window: the captured evidence is now stale.
        renewal = tx.renew_lease(
            conn,
            job_id=ready.job.job_id,
            attempt_id=ready.attempt.attempt_id,
            invocation_token="t1",
            fencing_generation=ready.attempt.fencing_generation,
            clock=_clock(1),
        )
        assert isinstance(renewal, tx.LeaseRenewed)

        result = tx.claim_expired_attempt(
            conn,
            job_id=ready.job.job_id,
            attempt_id=ready.attempt.attempt_id,
            captured_owner_token=ready.attempt.owner_token,
            captured_fencing_generation=ready.attempt.fencing_generation,
            captured_lease_expires_at=captured_lease_expires_at,
            new_invocation_token="t2",
            clock=_clock(61),
        )
        assert isinstance(result, tx.ClaimRejected)
    finally:
        conn.close()


def test_claim_expired_attempt_rejects_when_no_longer_expired(tmp_path: Path) -> None:
    conn = _open(tmp_path)
    try:
        ready = _seed_ready_to_submit(
            conn,
            "job-a",
            CANONICAL_A,
            job_id="job-id-1",
            attempt_id="attempt-id-1",
            token="t1",
        )
        # Re-check happens before the lease's actual 60s expiry.
        result = tx.claim_expired_attempt(
            conn,
            job_id=ready.job.job_id,
            attempt_id=ready.attempt.attempt_id,
            captured_owner_token=ready.attempt.owner_token,
            captured_fencing_generation=ready.attempt.fencing_generation,
            captured_lease_expires_at=ready.attempt.lease_expires_at,
            new_invocation_token="t2",
            clock=_clock(1),
        )
        assert isinstance(result, tx.ClaimRejected)
    finally:
        conn.close()


def test_claim_expired_attempt_rejects_for_a_completed_attempt(tmp_path: Path) -> None:
    conn = _open(tmp_path)
    try:
        ready = _seed_ready_to_submit(
            conn,
            "job-a",
            CANONICAL_A,
            job_id="job-id-1",
            attempt_id="attempt-id-1",
            token="t1",
        )
        captured_owner_token = ready.attempt.owner_token
        captured_fencing_generation = ready.attempt.fencing_generation
        captured_lease_expires_at = ready.attempt.lease_expires_at

        finalize_result = tx.finalize_submission_attempt(
            conn,
            job_id=ready.job.job_id,
            attempt_id=ready.attempt.attempt_id,
            attempt_number=ready.attempt.attempt_number,
            invocation_token="t1",
            fencing_generation=ready.attempt.fencing_generation,
            clock=_clock(1),
            random_source=_FixedRandomSource(),
            id_generator=_counting_id_generator(),
            evidence=_SUCCESS_EVIDENCE,
        )
        assert isinstance(finalize_result, tx.SubmissionFinalized)

        result = tx.claim_expired_attempt(
            conn,
            job_id=ready.job.job_id,
            attempt_id=ready.attempt.attempt_id,
            captured_owner_token=captured_owner_token,
            captured_fencing_generation=captured_fencing_generation,
            captured_lease_expires_at=captured_lease_expires_at,
            new_invocation_token="t2",
            clock=_clock(61),
        )
        assert isinstance(result, tx.ClaimRejected)
    finally:
        conn.close()


# -- Late evidence ---------------------------------------------------------


def test_append_late_observation_succeeds_regardless_of_ownership(
    tmp_path: Path,
) -> None:
    conn = _open(tmp_path)
    try:
        ready = _seed_ready_to_submit(
            conn,
            "job-a",
            CANONICAL_A,
            job_id="job-id-1",
            attempt_id="attempt-id-1",
            token="t1",
        )
        # "stale-token" never owned this attempt, yet the append must still
        # succeed: by definition a stale owner cannot pass an ownership check.
        observation_id = tx.append_late_observation(
            conn,
            attempt_id=ready.attempt.attempt_id,
            observed_fencing_generation=ready.attempt.fencing_generation,
            evidence_category="processed",
            operation="SUBMISSION",
            clock=_clock(5),
            id_generator=_sequence("seq-1", "obs-1"),
            http_status=201,
            remote_request_id="remote-1",
        )
        assert observation_id == "obs-1"

        row = conn.execute(
            "SELECT is_late, consumed_at, evidence_category FROM attempt_observations "
            "WHERE observation_id = ?",
            (observation_id,),
        ).fetchone()
        assert row == (1, None, "processed")

        # Attempt/Job state must be completely untouched by the append.
        job_row = conn.execute(
            "SELECT state FROM jobs WHERE job_id = ?", (ready.job.job_id,)
        ).fetchone()
        assert job_row == ("SUBMITTING",)
    finally:
        conn.close()


def test_append_late_observation_allocates_increasing_sequence_numbers(
    tmp_path: Path,
) -> None:
    conn = _open(tmp_path)
    try:
        ready = _seed_ready_to_submit(
            conn,
            "job-a",
            CANONICAL_A,
            job_id="job-id-1",
            attempt_id="attempt-id-1",
            token="t1",
        )
        tx.append_late_observation(
            conn,
            attempt_id=ready.attempt.attempt_id,
            observed_fencing_generation=1,
            evidence_category="server_uncertain",
            operation="SUBMISSION",
            clock=_clock(5),
            id_generator=_sequence("seq-1", "obs-1"),
        )
        tx.append_late_observation(
            conn,
            attempt_id=ready.attempt.attempt_id,
            observed_fencing_generation=1,
            evidence_category="server_uncertain",
            operation="SUBMISSION",
            clock=_clock(6),
            id_generator=_sequence("seq-2", "obs-2"),
        )
        rows = conn.execute(
            "SELECT observation_id, sequence_number FROM attempt_observations "
            "WHERE attempt_id = ? ORDER BY sequence_number",
            (ready.attempt.attempt_id,),
        ).fetchall()
        assert rows == [("obs-1", 1), ("obs-2", 2)]
    finally:
        conn.close()


def test_consume_late_observations_ignores_non_actionable_evidence(
    tmp_path: Path,
) -> None:
    conn = _open(tmp_path)
    try:
        ready = _seed_ready_to_submit(
            conn,
            "job-a",
            CANONICAL_A,
            job_id="job-id-1",
            attempt_id="attempt-id-1",
            token="t1",
        )
        tx.append_late_observation(
            conn,
            attempt_id=ready.attempt.attempt_id,
            observed_fencing_generation=1,
            evidence_category="server_uncertain",
            operation="SUBMISSION",
            clock=_clock(5),
            id_generator=_sequence("seq-1", "obs-1"),
        )
        result = tx.consume_late_observations(
            conn,
            job_id=ready.job.job_id,
            attempt_id=ready.attempt.attempt_id,
            invocation_token="t1",
            fencing_generation=ready.attempt.fencing_generation,
            clock=_clock(6),
            random_source=_FixedRandomSource(),
        )
        assert isinstance(result, tx.NoActionableLateEvidence)

        job_row = conn.execute(
            "SELECT state FROM jobs WHERE job_id = ?", (ready.job.job_id,)
        ).fetchone()
        assert job_row == ("SUBMITTING",)
    finally:
        conn.close()


def test_consume_late_observations_completes_success(tmp_path: Path) -> None:
    conn = _open(tmp_path)
    try:
        ready = _seed_ready_to_submit(
            conn,
            "job-a",
            CANONICAL_A,
            job_id="job-id-1",
            attempt_id="attempt-id-1",
            token="t1",
        )
        tx.append_late_observation(
            conn,
            attempt_id=ready.attempt.attempt_id,
            observed_fencing_generation=1,
            evidence_category="processed",
            operation="SUBMISSION",
            clock=_clock(5),
            id_generator=_sequence("seq-1", "obs-1"),
            http_status=201,
            remote_request_id="remote-1",
        )
        result = tx.consume_late_observations(
            conn,
            job_id=ready.job.job_id,
            attempt_id=ready.attempt.attempt_id,
            invocation_token="t1",
            fencing_generation=ready.attempt.fencing_generation,
            clock=_clock(6),
            random_source=_FixedRandomSource(),
        )
        assert isinstance(result, tx.LateEvidenceConsumed)
        assert result.job.state == "SUCCEEDED"
        assert result.attempt.state == "SUCCEEDED"
        assert result.attempt.remote_request_id == "remote-1"

        consumed_row = conn.execute(
            "SELECT consumed_at FROM attempt_observations WHERE observation_id = 'obs-1'"
        ).fetchone()
        assert consumed_row == ("2026-01-01T00:00:06.000000Z",)
    finally:
        conn.close()


def test_consume_late_observations_completes_permanent_failure(tmp_path: Path) -> None:
    conn = _open(tmp_path)
    try:
        ready = _seed_ready_to_submit(
            conn,
            "job-a",
            CANONICAL_A,
            job_id="job-id-1",
            attempt_id="attempt-id-1",
            token="t1",
        )
        tx.append_late_observation(
            conn,
            attempt_id=ready.attempt.attempt_id,
            observed_fencing_generation=1,
            evidence_category="idempotency_conflict",
            operation="SUBMISSION",
            clock=_clock(5),
            id_generator=_sequence("seq-1", "obs-1"),
            http_status=409,
        )
        result = tx.consume_late_observations(
            conn,
            job_id=ready.job.job_id,
            attempt_id=ready.attempt.attempt_id,
            invocation_token="t1",
            fencing_generation=ready.attempt.fencing_generation,
            clock=_clock(6),
            random_source=_FixedRandomSource(),
        )
        assert isinstance(result, tx.LateEvidenceConsumed)
        assert result.job.state == "FAILED_PERMANENT"
        assert result.attempt.state == "PERMANENT_FAILURE"
        assert result.attempt.error_category == "idempotency_conflict"
        assert result.attempt.http_status == 409
    finally:
        conn.close()


def test_consume_late_observations_never_reclassifies_a_completed_attempt(
    tmp_path: Path,
) -> None:
    conn = _open(tmp_path)
    try:
        ready = _seed_ready_to_submit(
            conn,
            "job-a",
            CANONICAL_A,
            job_id="job-id-1",
            attempt_id="attempt-id-1",
            token="t1",
        )
        finalize_result = tx.finalize_submission_attempt(
            conn,
            job_id=ready.job.job_id,
            attempt_id=ready.attempt.attempt_id,
            attempt_number=ready.attempt.attempt_number,
            invocation_token="t1",
            fencing_generation=ready.attempt.fencing_generation,
            clock=_clock(1),
            random_source=_FixedRandomSource(),
            id_generator=_counting_id_generator(),
            evidence=_SUCCESS_EVIDENCE,
        )
        assert isinstance(finalize_result, tx.SubmissionFinalized)

        # A late observation arriving after completion stays append-only:
        # consumption is impossible since ownership no longer matches an
        # active `STARTED` attempt.
        tx.append_late_observation(
            conn,
            attempt_id=ready.attempt.attempt_id,
            observed_fencing_generation=1,
            evidence_category="idempotency_conflict",
            operation="SUBMISSION",
            clock=_clock(5),
            id_generator=_sequence("seq-1", "obs-1"),
            http_status=409,
        )
        result = tx.consume_late_observations(
            conn,
            job_id=ready.job.job_id,
            attempt_id=ready.attempt.attempt_id,
            invocation_token="t1",
            fencing_generation=ready.attempt.fencing_generation,
            clock=_clock(6),
            random_source=_FixedRandomSource(),
        )
        assert isinstance(result, tx.PostSideEffectFailure)

        job_row = conn.execute(
            "SELECT state FROM jobs WHERE job_id = ?", (ready.job.job_id,)
        ).fetchone()
        assert job_row == ("SUCCEEDED",)
    finally:
        conn.close()


def test_pre_side_effect_wraps_lock_contention_as_persistence_error(
    tmp_path: Path,
) -> None:
    import sqlite3

    db_path = tmp_path / "db.sqlite3"
    connection_mod.open_connection(db_path).close()

    # Held for the entire test (never released): the busy_timeout retry
    # window is a soft, imprecise deadline in SQLite, so racing it against a
    # timed release is flaky. Holding the lock unconditionally makes the
    # pre-side-effect transaction's failure deterministic.
    blocker = sqlite3.connect(
        str(db_path), autocommit=True, timeout=0, check_same_thread=False
    )
    blocker.execute("BEGIN EXCLUSIVE")

    conn = sqlite3.connect(str(db_path), autocommit=True, timeout=0)
    conn.execute("PRAGMA busy_timeout = 5000")
    try:
        with pytest.raises(PersistenceError):
            tx.run_pre_side_effect_transaction(
                conn,
                supplied_key="job-a",
                key_generator=_sequence(),
                canonical_bytes=CANONICAL_A,
                id_generator=_sequence(),
                invocation_token="t1",
                clock=_clock(),
            )
    finally:
        conn.close()
        blocker.execute("COMMIT")
        blocker.close()
