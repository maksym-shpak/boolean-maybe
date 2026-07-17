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


def test_equivalent_supplied_key_reuse_while_submitting_is_job_not_eligible(
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
        assert isinstance(result, tx.JobNotEligible)
        assert result.state == "SUBMITTING"
    finally:
        conn.close()


@pytest.mark.parametrize(
    "state", ["PENDING", "RETRY_SCHEDULED", "FAILED_PERMANENT", "AMBIGUOUS"]
)
def test_equivalent_supplied_key_for_every_non_succeeded_state_is_job_not_eligible(
    tmp_path: Path, state: str
) -> None:
    conn = _open(tmp_path)
    try:
        now = "2026-01-01T00:00:00.000000Z"
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "INSERT INTO jobs (job_id, idempotency_key, payload_canonical, state, created_at, updated_at) "
            "VALUES ('job-x', 'job-a', ?, ?, ?, ?)",
            (CANONICAL_A, state, now, now),
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
        assert isinstance(result, tx.JobNotEligible)
        assert result.state == state
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
    not_eligible = [o for o in outcomes if isinstance(o, tx.JobNotEligible)]
    assert len(ready) == 1
    assert len(not_eligible) == 1
    assert not_eligible[0].state == "SUBMITTING"


# -- Pre-dispatch lease verification ------------------------------------------


def test_lease_still_valid_immediately_after_pre_side_effect_commit(
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
        assert (
            tx.verify_lease_still_valid(
                conn,
                job_id=ready.job.job_id,
                attempt_id=ready.attempt.attempt_id,
                invocation_token="t1",
                clock=_clock(1),
            )
            is True
        )
    finally:
        conn.close()


def test_lease_no_longer_valid_after_expiry(tmp_path: Path) -> None:
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
        assert (
            tx.verify_lease_still_valid(
                conn,
                job_id=ready.job.job_id,
                attempt_id=ready.attempt.attempt_id,
                invocation_token="t1",
                clock=_clock(61),
            )
            is False
        )
    finally:
        conn.close()


def test_lease_no_longer_valid_for_a_different_invocation_token(
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
        assert (
            tx.verify_lease_still_valid(
                conn,
                job_id=ready.job.job_id,
                attempt_id=ready.attempt.attempt_id,
                invocation_token="a-different-token",
                clock=_clock(1),
            )
            is False
        )
    finally:
        conn.close()


# -- Post-side-effect success transaction -------------------------------------


def test_post_side_effect_success_completes_attempt_and_job(tmp_path: Path) -> None:
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
        result = tx.run_post_side_effect_success(
            conn,
            job_id=ready.job.job_id,
            attempt_id=ready.attempt.attempt_id,
            invocation_token="t1",
            clock=_clock(5),
            http_status=201,
            remote_request_id="remote-1",
        )
        assert isinstance(result, tx.PostSideEffectSuccess)

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


def test_post_side_effect_fails_when_invocation_token_does_not_match(
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
        result = tx.run_post_side_effect_success(
            conn,
            job_id=ready.job.job_id,
            attempt_id=ready.attempt.attempt_id,
            invocation_token="stale-token",
            clock=_clock(5),
            http_status=201,
            remote_request_id="remote-1",
        )
        assert isinstance(result, tx.PostSideEffectFailure)

        job_row = conn.execute(
            "SELECT state FROM jobs WHERE job_id = ?", (ready.job.job_id,)
        ).fetchone()
        assert job_row == ("SUBMITTING",)
    finally:
        conn.close()


def test_post_side_effect_fails_when_lease_has_expired(tmp_path: Path) -> None:
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
        result = tx.run_post_side_effect_success(
            conn,
            job_id=ready.job.job_id,
            attempt_id=ready.attempt.attempt_id,
            invocation_token="t1",
            clock=_clock(61),
            http_status=201,
            remote_request_id="remote-1",
        )
        assert isinstance(result, tx.PostSideEffectFailure)

        job_row = conn.execute(
            "SELECT state FROM jobs WHERE job_id = ?", (ready.job.job_id,)
        ).fetchone()
        assert job_row == ("SUBMITTING",)
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
