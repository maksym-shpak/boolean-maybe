"""Pre- and post-side-effect durable transactions for the submit workflow.

These functions are synchronous (called by the application workflow through
`asyncio.to_thread()`) and each opens and commits or rolls back exactly one
`BEGIN IMMEDIATE` transaction. No connection, transaction, or cursor is held
open across HTTP: the workflow calls the pre-side-effect transaction, then
performs HTTP on its own, then calls the post-side-effect transaction.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, replace
from typing import Callable

from ..domain.clock import Clock, add_seconds, format_timestamp
from .. import canonical_json
from .errors import DatabaseCorruptionError, PersistenceError

LEASE_SECONDS = 60
_FENCING_GENERATION = 1


@dataclass(frozen=True)
class JobRow:
    job_id: str
    idempotency_key: str
    payload_canonical: bytes
    state: str
    created_at: str
    updated_at: str


@dataclass(frozen=True)
class AttemptRow:
    attempt_id: str
    job_id: str
    attempt_number: int
    state: str
    started_at: str
    completed_at: str | None
    http_status: int | None
    remote_request_id: str | None
    owner_token: str | None
    fencing_generation: int
    lease_expires_at: str | None


@dataclass(frozen=True)
class ReadyToSubmit:
    job: JobRow
    attempt: AttemptRow
    invocation_token: str


@dataclass(frozen=True)
class AlreadyCompleted:
    job: JobRow
    attempt: AttemptRow


@dataclass(frozen=True)
class JobNotEligible:
    state: str


@dataclass(frozen=True)
class IdempotencyConflict:
    pass


PreSideEffectResult = (
    ReadyToSubmit | AlreadyCompleted | JobNotEligible | IdempotencyConflict
)


@dataclass(frozen=True)
class PostSideEffectSuccess:
    completed_at: str


@dataclass(frozen=True)
class PostSideEffectFailure:
    reason: str


PostSideEffectResult = PostSideEffectSuccess | PostSideEffectFailure


def run_pre_side_effect_transaction(
    conn: sqlite3.Connection,
    *,
    supplied_key: str | None,
    key_generator: Callable[[], str],
    canonical_bytes: bytes,
    id_generator: Callable[[], str],
    invocation_token: str,
    clock: Clock,
    lease_seconds: int = LEASE_SECONDS,
) -> PreSideEffectResult:
    while True:
        key = supplied_key if supplied_key is not None else key_generator()
        _begin_immediate(conn)
        try:
            existing = _find_job_by_key(conn, key)

            if existing is None:
                now = format_timestamp(clock.now())
                job_id = id_generator()
                try:
                    job_id = _insert_job(
                        conn, job_id, key, canonical_bytes, now, id_generator
                    )
                except sqlite3.IntegrityError:
                    conn.execute("ROLLBACK")
                    continue

                job = JobRow(job_id, key, canonical_bytes, "PENDING", now, now)
                attempt = _start_attempt(
                    conn,
                    job,
                    id_generator,
                    clock,
                    lease_seconds,
                    invocation_token,
                    now,
                )
                job = replace(job, state="SUBMITTING", updated_at=now)
                conn.execute("COMMIT")
                return ReadyToSubmit(
                    job=job, attempt=attempt, invocation_token=invocation_token
                )

            if supplied_key is None:
                # A generated key collided with an existing persisted key.
                # Never reuse the winning Job; generate a fresh key instead.
                conn.execute("ROLLBACK")
                continue

            # An existing row is about to authorize a decision (conflict,
            # replay, or ineligibility) purely from its stored bytes; verify
            # those bytes are themselves an uncorrupted canonical Job Entry
            # *before* trusting a raw-byte equivalence comparison against
            # them, regardless of which branch below is ultimately taken.
            try:
                _verify_payload_canonical(existing.payload_canonical)
            except DatabaseCorruptionError:
                conn.execute("ROLLBACK")
                raise

            if existing.payload_canonical != canonical_bytes:
                conn.execute("COMMIT")
                return IdempotencyConflict()

            if existing.state == "SUCCEEDED":
                attempt = _find_successful_attempt(conn, existing.job_id)
                if attempt is None:
                    conn.execute("ROLLBACK")
                    raise DatabaseCorruptionError(
                        "SUCCEEDED Job has no usable successful attempt"
                    )
                try:
                    _verify_attempt_success_evidence(attempt)
                except DatabaseCorruptionError:
                    conn.execute("ROLLBACK")
                    raise
                conn.execute("COMMIT")
                return AlreadyCompleted(job=existing, attempt=attempt)

            conn.execute("COMMIT")
            return JobNotEligible(state=existing.state)
        except sqlite3.OperationalError as exc:
            conn.execute("ROLLBACK")
            raise PersistenceError(
                f"pre-side-effect transaction failed: {exc.__class__.__name__}"
            ) from exc


def verify_lease_still_valid(
    conn: sqlite3.Connection,
    *,
    job_id: str,
    attempt_id: str,
    invocation_token: str,
    clock: Clock,
) -> bool:
    """Re-read Job/attempt state immediately before HTTP dispatch begins.

    The pre-side-effect commit authorizes a request, but time may pass
    before the workflow actually dispatches it (scheduling delay, suspend/
    resume). This is a read-only check -- it never mutates state -- so the
    caller must not initiate HTTP when it returns `False`.
    """

    try:
        job = _select_job(conn, job_id)
        attempt = _select_attempt(conn, attempt_id)
    except sqlite3.OperationalError as exc:
        raise PersistenceError(
            f"lease verification failed: {exc.__class__.__name__}"
        ) from exc

    now = format_timestamp(clock.now())
    return (
        job is not None
        and attempt is not None
        and job.state == "SUBMITTING"
        and attempt.state == "STARTED"
        and attempt.owner_token == invocation_token
        and attempt.fencing_generation == _FENCING_GENERATION
        and attempt.lease_expires_at is not None
        and attempt.lease_expires_at > now
    )


def run_post_side_effect_success(
    conn: sqlite3.Connection,
    *,
    job_id: str,
    attempt_id: str,
    invocation_token: str,
    clock: Clock,
    http_status: int,
    remote_request_id: str,
) -> PostSideEffectResult:
    _begin_immediate(conn)
    try:
        job = _select_job(conn, job_id)
        attempt = _select_attempt(conn, attempt_id)
        now = format_timestamp(clock.now())

        if (
            job is None
            or attempt is None
            or job.state != "SUBMITTING"
            or attempt.state != "STARTED"
            or attempt.owner_token != invocation_token
            or attempt.fencing_generation != _FENCING_GENERATION
            or attempt.lease_expires_at is None
            or attempt.lease_expires_at <= now
        ):
            conn.execute("ROLLBACK")
            return PostSideEffectFailure(
                reason="lease, fencing, or state no longer matches this invocation"
            )

        conn.execute(
            "UPDATE submission_attempts SET state = 'SUCCEEDED', completed_at = ?, "
            "http_status = ?, remote_request_id = ?, owner_token = NULL, "
            "lease_expires_at = NULL WHERE attempt_id = ?",
            (now, http_status, remote_request_id, attempt_id),
        )
        conn.execute(
            "UPDATE jobs SET state = 'SUCCEEDED', updated_at = ? WHERE job_id = ?",
            (now, job_id),
        )
        conn.execute("COMMIT")
        return PostSideEffectSuccess(completed_at=now)
    except sqlite3.OperationalError as exc:
        conn.execute("ROLLBACK")
        raise PersistenceError(
            f"post-side-effect transaction failed: {exc.__class__.__name__}"
        ) from exc


def _begin_immediate(conn: sqlite3.Connection) -> None:
    try:
        conn.execute("BEGIN IMMEDIATE")
    except sqlite3.OperationalError as exc:
        raise PersistenceError(
            f"could not begin transaction: {exc.__class__.__name__}"
        ) from exc


def _insert_job(
    conn: sqlite3.Connection,
    job_id: str,
    key: str,
    canonical_bytes: bytes,
    now: str,
    id_generator: Callable[[], str],
) -> str:
    while True:
        try:
            conn.execute(
                "INSERT INTO jobs "
                "(job_id, idempotency_key, payload_canonical, state, created_at, updated_at) "
                "VALUES (?, ?, ?, 'PENDING', ?, ?)",
                (job_id, key, canonical_bytes, now, now),
            )
            return job_id
        except sqlite3.IntegrityError as exc:
            if "jobs.job_id" in str(exc):
                job_id = id_generator()
                continue
            raise


def _start_attempt(
    conn: sqlite3.Connection,
    job: JobRow,
    id_generator: Callable[[], str],
    clock: Clock,
    lease_seconds: int,
    invocation_token: str,
    now: str,
) -> AttemptRow:
    attempt_id = id_generator()
    lease_expires_at = format_timestamp(add_seconds(clock.now(), lease_seconds))
    while True:
        try:
            conn.execute(
                "INSERT INTO submission_attempts "
                "(attempt_id, job_id, attempt_number, state, started_at, "
                "owner_token, fencing_generation, lease_expires_at) "
                "VALUES (?, ?, 1, 'STARTED', ?, ?, ?, ?)",
                (
                    attempt_id,
                    job.job_id,
                    now,
                    invocation_token,
                    _FENCING_GENERATION,
                    lease_expires_at,
                ),
            )
            break
        except sqlite3.IntegrityError as exc:
            if "attempt_id" in str(exc):
                attempt_id = id_generator()
                continue
            raise

    conn.execute(
        "UPDATE jobs SET state = 'SUBMITTING', updated_at = ? WHERE job_id = ?",
        (now, job.job_id),
    )
    return AttemptRow(
        attempt_id=attempt_id,
        job_id=job.job_id,
        attempt_number=1,
        state="STARTED",
        started_at=now,
        completed_at=None,
        http_status=None,
        remote_request_id=None,
        owner_token=invocation_token,
        fencing_generation=_FENCING_GENERATION,
        lease_expires_at=lease_expires_at,
    )


def _find_job_by_key(conn: sqlite3.Connection, key: str) -> JobRow | None:
    row = conn.execute(
        "SELECT job_id, idempotency_key, payload_canonical, state, created_at, updated_at "
        "FROM jobs WHERE idempotency_key = ?",
        (key,),
    ).fetchone()
    return JobRow(*row) if row is not None else None


def _select_job(conn: sqlite3.Connection, job_id: str) -> JobRow | None:
    row = conn.execute(
        "SELECT job_id, idempotency_key, payload_canonical, state, created_at, updated_at "
        "FROM jobs WHERE job_id = ?",
        (job_id,),
    ).fetchone()
    return JobRow(*row) if row is not None else None


def _select_attempt(conn: sqlite3.Connection, attempt_id: str) -> AttemptRow | None:
    row = conn.execute(
        "SELECT attempt_id, job_id, attempt_number, state, started_at, completed_at, "
        "http_status, remote_request_id, owner_token, fencing_generation, lease_expires_at "
        "FROM submission_attempts WHERE attempt_id = ?",
        (attempt_id,),
    ).fetchone()
    return AttemptRow(*row) if row is not None else None


def _find_successful_attempt(
    conn: sqlite3.Connection, job_id: str
) -> AttemptRow | None:
    rows = conn.execute(
        "SELECT attempt_id, job_id, attempt_number, state, started_at, completed_at, "
        "http_status, remote_request_id, owner_token, fencing_generation, lease_expires_at "
        "FROM submission_attempts WHERE job_id = ? AND state = 'SUCCEEDED'",
        (job_id,),
    ).fetchall()
    if len(rows) != 1:
        return None
    return AttemptRow(*rows[0])


def _verify_payload_canonical(payload_canonical: bytes) -> None:
    """Verify persisted bytes are themselves an uncorrupted canonical Job Entry.

    Decodability and I-JSON validity alone are insufficient: valid JSON that
    is merely *equivalent* to, but not byte-identical with, its own
    canonical form indicates the stored bytes were never actually produced
    by this application's canonicalization step, and a raw-byte equivalence
    comparison against such bytes cannot be trusted.
    """

    try:
        job_entry = canonical_json.parse_job_entry(payload_canonical)
        recanonicalized = canonical_json.canonicalize(job_entry)
    except canonical_json.JobEntryValidationError as exc:
        raise DatabaseCorruptionError(
            "persisted payload_canonical is not a valid canonicalizable Job Entry"
        ) from exc
    if recanonicalized != payload_canonical:
        raise DatabaseCorruptionError(
            "persisted payload_canonical is not in canonical form"
        )


def _verify_attempt_success_evidence(attempt: AttemptRow) -> None:
    if attempt.http_status not in (200, 201):
        raise DatabaseCorruptionError(
            "persisted successful attempt has an invalid http_status"
        )
    # Matches the external client's own success contract (`isinstance(...,
    # str)`, not a non-empty-string check): the specification requires "a
    # string remote_request_id", not a non-empty one, so a fresh success
    # this client accepted must never be rejected as corrupted on replay.
    if attempt.remote_request_id is None:
        raise DatabaseCorruptionError(
            "persisted successful attempt is missing a remote_request_id"
        )
