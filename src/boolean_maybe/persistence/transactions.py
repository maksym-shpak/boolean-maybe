"""Pre- and post-side-effect durable transactions for the submit workflow.

These functions are synchronous (called by the application workflow through
`asyncio.to_thread()`) and each opens and commits or rolls back exactly one
`BEGIN IMMEDIATE` transaction. No connection, transaction, or cursor is held
open across HTTP: the workflow calls the pre-side-effect transaction, then
performs HTTP on its own, then calls the post-side-effect transaction.

`docs/specs/features/reliable-job-submission.md` extends the original
single-attempt vertical with a durable retry budget, the service-wide
rate-limit gate, the full submission classification matrix, bounded
automatic reconciliation, fenced recovery of an interrupted attempt, and a
late-evidence path for a stale (fenced-out) owner's observation.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, replace
from typing import Callable, Literal

from .. import canonical_json
from ..domain import backoff
from ..domain import retry_after as retry_after_mod
from ..domain.clock import Clock, add_milliseconds, format_timestamp, parse_timestamp
from ..domain.random_source import RandomSource
from . import rate_limit_gate
from .errors import DatabaseCorruptionError, PersistenceError

LEASE_SECONDS = 60
INITIAL_FENCING_GENERATION = 1
MAX_SUBMISSION_ATTEMPTS = 3


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
    error_category: str | None
    retry_after_ms: int | None
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
class IdempotencyConflict:
    pass


@dataclass(frozen=True)
class NotYetEligible:
    """`PENDING` or `RETRY_SCHEDULED`, but Job eligibility or the service gate

    has not yet arrived. `not_before` is the later of the two, letting the
    workflow decide whether to wait (if it fits the remaining invocation
    budget) or report a deferred/scheduled result.
    """

    job: JobRow
    not_before: str


@dataclass(frozen=True)
class JobInProgress:
    """`SUBMITTING` with an unexpired lease: another invocation owns the

    active attempt. No HTTP is authorized.
    """

    job: JobRow


@dataclass(frozen=True)
class RecoveryCandidate:
    """`SUBMITTING` with an expired lease: eligible for fenced recovery.

    `attempt` carries the exact owner token, fencing generation, and lease
    expiry the caller must capture before the quarantine wait and recheck
    unchanged afterward, per ADR-004/ADR-006. This is a read-only read; it
    does not itself claim the attempt.
    """

    job: JobRow
    attempt: AttemptRow


@dataclass(frozen=True)
class TerminalFailedPermanent:
    job: JobRow
    attempt: AttemptRow


@dataclass(frozen=True)
class TerminalAmbiguous:
    job: JobRow
    attempt: AttemptRow


PreSideEffectResult = (
    ReadyToSubmit
    | AlreadyCompleted
    | IdempotencyConflict
    | NotYetEligible
    | JobInProgress
    | RecoveryCandidate
    | TerminalFailedPermanent
    | TerminalAmbiguous
)


@dataclass(frozen=True)
class PostSideEffectFailure:
    reason: str


@dataclass(frozen=True)
class LeaseRenewed:
    lease_expires_at: str


@dataclass(frozen=True)
class LeaseLost:
    pass


LeaseRenewalResult = LeaseRenewed | LeaseLost


@dataclass(frozen=True)
class SubmissionEvidence:
    """Classified submission evidence, produced by

    `application.classification.classify_submission_outcome` from an
    `external.client.SubmitHttpOutcome`. Keeps this persistence module free
    of any HTTP-specific import.
    """

    disposition: Literal[
        "succeeded", "permanent_failure", "retryable_failure", "retained"
    ]
    http_status: int | None
    remote_request_id: str | None
    error_category: str | None
    retry_after_values: tuple[str, ...]


@dataclass(frozen=True)
class SubmissionFinalized:
    """A terminal classification: `SUCCEEDED`, `PERMANENT_FAILURE`, or a

    budget-exhausting `RETRYABLE_FAILURE` (Job -> `FAILED_PERMANENT`).
    """

    job: JobRow
    attempt: AttemptRow


@dataclass(frozen=True)
class SubmissionRetryScheduled:
    """A `RETRYABLE_FAILURE` with budget remaining: Job -> `RETRY_SCHEDULED`."""

    job: JobRow
    attempt: AttemptRow


@dataclass(frozen=True)
class SubmissionRetained:
    """`5xx` / `MAYBE_SENT` transport / protocol-uncertain: the attempt

    remains `STARTED` and the Job remains `SUBMITTING`, pending automatic
    reconciliation added by a later stage.
    """

    job: JobRow
    attempt: AttemptRow


SubmissionRecordResult = (
    SubmissionFinalized
    | SubmissionRetryScheduled
    | SubmissionRetained
    | PostSideEffectFailure
)


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
                return _authorize_or_defer(
                    conn,
                    job=job,
                    attempt_number=1,
                    now=now,
                    id_generator=id_generator,
                    invocation_token=invocation_token,
                    clock=clock,
                    lease_seconds=lease_seconds,
                )

            if supplied_key is None:
                # A generated key collided with an existing persisted key.
                # Never reuse the winning Job; generate a fresh key instead.
                conn.execute("ROLLBACK")
                continue

            # An existing row is about to authorize a decision purely from
            # its stored bytes; verify those bytes are themselves an
            # uncorrupted canonical Job Entry *before* trusting a raw-byte
            # equivalence comparison against them, regardless of branch.
            try:
                _verify_payload_canonical(existing.payload_canonical)
            except DatabaseCorruptionError:
                conn.execute("ROLLBACK")
                raise

            if existing.payload_canonical != canonical_bytes:
                conn.execute("COMMIT")
                return IdempotencyConflict()

            return _route_existing_job(
                conn,
                job=existing,
                id_generator=id_generator,
                invocation_token=invocation_token,
                clock=clock,
                lease_seconds=lease_seconds,
            )
        except sqlite3.OperationalError as exc:
            conn.execute("ROLLBACK")
            raise PersistenceError(
                f"pre-side-effect transaction failed: {exc.__class__.__name__}"
            ) from exc


def _route_existing_job(
    conn: sqlite3.Connection,
    *,
    job: JobRow,
    id_generator: Callable[[], str],
    invocation_token: str,
    clock: Clock,
    lease_seconds: int,
) -> PreSideEffectResult:
    if job.state == "SUCCEEDED":
        attempt = _find_successful_attempt(conn, job.job_id)
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
        return AlreadyCompleted(job=job, attempt=attempt)

    if job.state == "PENDING":
        now = format_timestamp(clock.now())
        return _authorize_or_defer(
            conn,
            job=job,
            attempt_number=1,
            now=now,
            id_generator=id_generator,
            invocation_token=invocation_token,
            clock=clock,
            lease_seconds=lease_seconds,
        )

    if job.state == "RETRY_SCHEDULED":
        latest = _select_latest_attempt(conn, job.job_id)
        if (
            latest is None
            or latest.state != "RETRYABLE_FAILURE"
            or latest.completed_at is None
            or latest.retry_after_ms is None
        ):
            conn.execute("ROLLBACK")
            raise DatabaseCorruptionError(
                "RETRY_SCHEDULED Job is missing retry eligibility evidence"
            )
        now = format_timestamp(clock.now())
        job_eligible_at = format_timestamp(
            add_milliseconds(
                parse_timestamp(latest.completed_at), latest.retry_after_ms
            )
        )
        return _authorize_or_defer(
            conn,
            job=job,
            attempt_number=latest.attempt_number + 1,
            now=now,
            id_generator=id_generator,
            invocation_token=invocation_token,
            clock=clock,
            lease_seconds=lease_seconds,
            job_eligible_at=job_eligible_at,
        )

    if job.state == "SUBMITTING":
        latest = _select_latest_attempt(conn, job.job_id)
        if latest is None or latest.state != "STARTED":
            conn.execute("ROLLBACK")
            raise DatabaseCorruptionError(
                "SUBMITTING Job has no usable STARTED attempt evidence"
            )
        now = format_timestamp(clock.now())
        if latest.lease_expires_at is not None and latest.lease_expires_at > now:
            conn.execute("COMMIT")
            return JobInProgress(job=job)
        conn.execute("COMMIT")
        return RecoveryCandidate(job=job, attempt=latest)

    if job.state == "FAILED_PERMANENT":
        latest = _select_latest_attempt(conn, job.job_id)
        if latest is None or latest.state not in (
            "PERMANENT_FAILURE",
            "RETRYABLE_FAILURE",
        ):
            conn.execute("ROLLBACK")
            raise DatabaseCorruptionError(
                "FAILED_PERMANENT Job has no usable terminal attempt evidence"
            )
        conn.execute("COMMIT")
        return TerminalFailedPermanent(job=job, attempt=latest)

    if job.state == "AMBIGUOUS":
        latest = _select_latest_attempt(conn, job.job_id)
        if latest is None or latest.state != "AMBIGUOUS":
            conn.execute("ROLLBACK")
            raise DatabaseCorruptionError(
                "AMBIGUOUS Job has no usable terminal attempt evidence"
            )
        conn.execute("COMMIT")
        return TerminalAmbiguous(job=job, attempt=latest)

    conn.execute("ROLLBACK")
    raise DatabaseCorruptionError(f"Job has an unrecognized state: {job.state!r}")


def _authorize_or_defer(
    conn: sqlite3.Connection,
    *,
    job: JobRow,
    attempt_number: int,
    now: str,
    id_generator: Callable[[], str],
    invocation_token: str,
    clock: Clock,
    lease_seconds: int,
    job_eligible_at: str | None = None,
) -> ReadyToSubmit | NotYetEligible:
    """Authorize the next attempt now, or report the instant it becomes due.

    Computes `max(job_eligible_at, service gate not_before)` inside this
    transaction, before allocating an attempt, per ADR-006. `now` is
    computed once by the caller (rather than read again here) so the total
    number of `clock.now()` calls per authorization stays predictable for
    callers that inject a call-counting fake clock.
    """

    gate_not_before = rate_limit_gate.read_gate_not_before(conn)
    effective_not_before = _later_of(job_eligible_at, gate_not_before)

    if effective_not_before is not None and effective_not_before > now:
        conn.execute("COMMIT")
        return NotYetEligible(job=job, not_before=effective_not_before)

    attempt = _start_attempt(
        conn,
        job,
        attempt_number,
        id_generator,
        clock,
        lease_seconds,
        invocation_token,
        now,
    )
    job = replace(job, state="SUBMITTING", updated_at=now)
    conn.execute("COMMIT")
    return ReadyToSubmit(job=job, attempt=attempt, invocation_token=invocation_token)


def _later_of(a: str | None, b: str | None) -> str | None:
    if a is None:
        return b
    if b is None:
        return a
    return max(a, b)


def renew_lease(
    conn: sqlite3.Connection,
    *,
    job_id: str,
    attempt_id: str,
    invocation_token: str,
    fencing_generation: int,
    clock: Clock,
    lease_seconds: int = LEASE_SECONDS,
) -> LeaseRenewalResult:
    """Verify current ownership and extend the lease to a fresh window.

    Used both immediately before every HTTP dispatch (so authorization and
    renewal happen together, with dispatch following commit immediately)
    and periodically while the owner waits, so no interval between
    successful renewals exceeds the caller's cadence requirement. Renewal
    changes no domain state; the caller must not initiate HTTP, or must stop
    waiting and reroute, when this returns `LeaseLost`.
    """

    _begin_immediate(conn)
    try:
        job = _select_job(conn, job_id)
        attempt = _select_attempt(conn, attempt_id)
        now_instant = clock.now()
        now = format_timestamp(now_instant)

        if not _owns_active_attempt(
            job, attempt, invocation_token, fencing_generation, now
        ):
            conn.execute("ROLLBACK")
            return LeaseLost()

        lease_expires_at = format_timestamp(
            add_milliseconds(now_instant, lease_seconds * 1000)
        )
        conn.execute(
            "UPDATE submission_attempts SET lease_expires_at = ? WHERE attempt_id = ?",
            (lease_expires_at, attempt_id),
        )
        conn.execute("COMMIT")
        return LeaseRenewed(lease_expires_at=lease_expires_at)
    except sqlite3.OperationalError as exc:
        conn.execute("ROLLBACK")
        raise PersistenceError(
            f"lease renewal failed: {exc.__class__.__name__}"
        ) from exc


@dataclass(frozen=True)
class ClaimSucceeded:
    job: JobRow
    attempt: AttemptRow


@dataclass(frozen=True)
class ClaimRejected:
    pass


ClaimResult = ClaimSucceeded | ClaimRejected


def claim_expired_attempt(
    conn: sqlite3.Connection,
    *,
    job_id: str,
    attempt_id: str,
    captured_owner_token: str | None,
    captured_fencing_generation: int,
    captured_lease_expires_at: str | None,
    new_invocation_token: str,
    clock: Clock,
    lease_seconds: int = LEASE_SECONDS,
) -> ClaimResult:
    """Claim an attempt whose lease was captured as expired, after the

    caller's quarantine wait. Requires the exact captured owner token,
    fencing generation, and lease expiry to remain unchanged and still
    expired under the current wall clock; any change (renewal, a different
    claim, a finalized attempt) rejects the claim so the caller must not
    reuse the stale evidence. On success, advances fencing by exactly one,
    replaces the owner token, and issues a fresh lease.
    """

    _begin_immediate(conn)
    try:
        job = _select_job(conn, job_id)
        attempt = _select_attempt(conn, attempt_id)
        now_instant = clock.now()
        now = format_timestamp(now_instant)

        if (
            job is None
            or attempt is None
            or job.state != "SUBMITTING"
            or attempt.state != "STARTED"
            or attempt.owner_token != captured_owner_token
            or attempt.fencing_generation != captured_fencing_generation
            or attempt.lease_expires_at != captured_lease_expires_at
            or attempt.lease_expires_at is None
            or attempt.lease_expires_at > now
        ):
            conn.execute("ROLLBACK")
            return ClaimRejected()

        new_fencing_generation = attempt.fencing_generation + 1
        new_lease_expires_at = format_timestamp(
            add_milliseconds(now_instant, lease_seconds * 1000)
        )
        conn.execute(
            "UPDATE submission_attempts SET owner_token = ?, fencing_generation = ?, "
            "lease_expires_at = ? WHERE attempt_id = ?",
            (
                new_invocation_token,
                new_fencing_generation,
                new_lease_expires_at,
                attempt_id,
            ),
        )
        conn.execute("COMMIT")
        return ClaimSucceeded(
            job=job,
            attempt=replace(
                attempt,
                owner_token=new_invocation_token,
                fencing_generation=new_fencing_generation,
                lease_expires_at=new_lease_expires_at,
            ),
        )
    except sqlite3.OperationalError as exc:
        conn.execute("ROLLBACK")
        raise PersistenceError(
            f"attempt claim failed: {exc.__class__.__name__}"
        ) from exc


def _insert_submission_observation(
    conn: sqlite3.Connection,
    *,
    attempt_id: str,
    observed_at: str,
    observed_fencing_generation: int,
    id_generator: Callable[[], str],
    dispatch_certainty: Literal["NOT_SENT", "MAYBE_SENT"] | None,
    evidence_category: str | None,
    http_status: int | None,
    remote_request_id: str | None,
    retry_after_ms: int | None = None,
    retry_after_diagnostic: str | None = None,
) -> None:
    """Insert the one durable observation row for a completed submission

    POST, inside the caller's already-open transaction so the observation
    and the attempt/Job state transition it explains commit atomically.
    """

    (sequence_number,) = conn.execute(
        "SELECT COALESCE(MAX(sequence_number), 0) + 1 FROM attempt_observations "
        "WHERE attempt_id = ?",
        (attempt_id,),
    ).fetchone()
    sequence_id = id_generator()
    observation_id = id_generator()
    while True:
        try:
            conn.execute(
                "INSERT INTO attempt_observations ("
                "observation_id, attempt_id, sequence_id, sequence_number, operation, "
                "request_ordinal, observed_at, dispatch_certainty, evidence_category, "
                "http_status, remote_request_id, retry_after_ms, retry_after_diagnostic, "
                "observed_fencing_generation, is_late"
                ") VALUES (?, ?, ?, ?, 'SUBMISSION', 1, ?, ?, ?, ?, ?, ?, ?, ?, 0)",
                (
                    observation_id,
                    attempt_id,
                    sequence_id,
                    sequence_number,
                    observed_at,
                    dispatch_certainty,
                    evidence_category,
                    http_status,
                    remote_request_id,
                    retry_after_ms,
                    retry_after_diagnostic,
                    observed_fencing_generation,
                ),
            )
            return
        except sqlite3.IntegrityError as exc:
            if "observation_id" in str(exc) or "sequence_id" in str(exc):
                observation_id = id_generator()
                sequence_id = id_generator()
                continue
            raise


def finalize_submission_attempt(
    conn: sqlite3.Connection,
    *,
    job_id: str,
    attempt_id: str,
    attempt_number: int,
    invocation_token: str,
    fencing_generation: int,
    clock: Clock,
    random_source: RandomSource,
    id_generator: Callable[[], str],
    evidence: SubmissionEvidence,
) -> SubmissionRecordResult:
    """Atomically record one submission observation and apply the ADR-006

    classification matrix. A `retained` disposition (`5xx`/`MAYBE_SENT`/
    protocol-uncertain) intentionally leaves the attempt `STARTED` and the
    Job `SUBMITTING`; a later stage routes it into automatic reconciliation.
    """

    _begin_immediate(conn)
    try:
        job = _select_job(conn, job_id)
        attempt = _select_attempt(conn, attempt_id)
        now_instant = clock.now()
        now = format_timestamp(now_instant)

        if not _owns_active_attempt(
            job, attempt, invocation_token, fencing_generation, now
        ):
            conn.execute("ROLLBACK")
            return PostSideEffectFailure(
                reason="lease, fencing, or state no longer matches this invocation"
            )
        assert job is not None and attempt is not None

        dispatch_certainty: Literal["NOT_SENT", "MAYBE_SENT"] | None = (
            "NOT_SENT"
            if evidence.error_category == "transport_not_sent"
            else "MAYBE_SENT"
            if evidence.disposition == "retained"
            else None
        )

        if evidence.disposition == "succeeded":
            _insert_submission_observation(
                conn,
                attempt_id=attempt_id,
                observed_at=now,
                observed_fencing_generation=fencing_generation,
                id_generator=id_generator,
                dispatch_certainty=dispatch_certainty,
                evidence_category=None,
                http_status=evidence.http_status,
                remote_request_id=evidence.remote_request_id,
            )
            conn.execute(
                "UPDATE submission_attempts SET state = 'SUCCEEDED', completed_at = ?, "
                "http_status = ?, remote_request_id = ?, owner_token = NULL, "
                "lease_expires_at = NULL WHERE attempt_id = ?",
                (now, evidence.http_status, evidence.remote_request_id, attempt_id),
            )
            conn.execute(
                "UPDATE jobs SET state = 'SUCCEEDED', updated_at = ? WHERE job_id = ?",
                (now, job_id),
            )
            conn.execute("COMMIT")
            return SubmissionFinalized(
                job=replace(job, state="SUCCEEDED", updated_at=now),
                attempt=replace(
                    attempt,
                    state="SUCCEEDED",
                    completed_at=now,
                    http_status=evidence.http_status,
                    remote_request_id=evidence.remote_request_id,
                    owner_token=None,
                    lease_expires_at=None,
                ),
            )

        if evidence.disposition == "permanent_failure":
            _insert_submission_observation(
                conn,
                attempt_id=attempt_id,
                observed_at=now,
                observed_fencing_generation=fencing_generation,
                id_generator=id_generator,
                dispatch_certainty=dispatch_certainty,
                evidence_category=evidence.error_category,
                http_status=evidence.http_status,
                remote_request_id=None,
            )
            conn.execute(
                "UPDATE submission_attempts SET state = 'PERMANENT_FAILURE', completed_at = ?, "
                "http_status = ?, error_category = ?, owner_token = NULL, "
                "lease_expires_at = NULL WHERE attempt_id = ?",
                (now, evidence.http_status, evidence.error_category, attempt_id),
            )
            conn.execute(
                "UPDATE jobs SET state = 'FAILED_PERMANENT', updated_at = ? WHERE job_id = ?",
                (now, job_id),
            )
            conn.execute("COMMIT")
            return SubmissionFinalized(
                job=replace(job, state="FAILED_PERMANENT", updated_at=now),
                attempt=replace(
                    attempt,
                    state="PERMANENT_FAILURE",
                    completed_at=now,
                    http_status=evidence.http_status,
                    error_category=evidence.error_category,
                    owner_token=None,
                    lease_expires_at=None,
                ),
            )

        if evidence.disposition == "retryable_failure":
            parsed = retry_after_mod.parse_retry_after(
                evidence.retry_after_values, now=now_instant
            )
            server_ms = (
                parsed.server_delay_ms
                if isinstance(parsed, retry_after_mod.RetryAfterAccepted)
                else None
            )
            retry_after_diagnostic = (
                parsed.diagnostic
                if isinstance(parsed, retry_after_mod.RetryAfterRejected)
                else None
            )
            policy_ms = backoff.policy_delay_ms(attempt_number, random_source)
            delay_ms = backoff.effective_delay_ms(policy_ms, server_ms)
            budget_remains = attempt_number < MAX_SUBMISSION_ATTEMPTS

            if evidence.error_category == "rate_limited":
                gate_not_before = format_timestamp(
                    add_milliseconds(now_instant, delay_ms)
                )
                rate_limit_gate.advance_gate(
                    conn, candidate_not_before=gate_not_before, now=now
                )

            job_state = "RETRY_SCHEDULED" if budget_remains else "FAILED_PERMANENT"
            _insert_submission_observation(
                conn,
                attempt_id=attempt_id,
                observed_at=now,
                observed_fencing_generation=fencing_generation,
                id_generator=id_generator,
                dispatch_certainty=dispatch_certainty,
                evidence_category=evidence.error_category,
                http_status=evidence.http_status,
                remote_request_id=None,
                retry_after_ms=delay_ms,
                retry_after_diagnostic=retry_after_diagnostic,
            )
            conn.execute(
                "UPDATE submission_attempts SET state = 'RETRYABLE_FAILURE', completed_at = ?, "
                "http_status = ?, error_category = ?, retry_after_ms = ?, "
                "owner_token = NULL, lease_expires_at = NULL WHERE attempt_id = ?",
                (
                    now,
                    evidence.http_status,
                    evidence.error_category,
                    delay_ms,
                    attempt_id,
                ),
            )
            conn.execute(
                "UPDATE jobs SET state = ?, updated_at = ? WHERE job_id = ?",
                (job_state, now, job_id),
            )
            conn.execute("COMMIT")

            finalized_attempt = replace(
                attempt,
                state="RETRYABLE_FAILURE",
                completed_at=now,
                http_status=evidence.http_status,
                error_category=evidence.error_category,
                retry_after_ms=delay_ms,
                owner_token=None,
                lease_expires_at=None,
            )
            finalized_job = replace(job, state=job_state, updated_at=now)
            if budget_remains:
                return SubmissionRetryScheduled(
                    job=finalized_job, attempt=finalized_attempt
                )
            return SubmissionFinalized(job=finalized_job, attempt=finalized_attempt)

        # "retained": 5xx / MAYBE_SENT transport / protocol-uncertain.
        _insert_submission_observation(
            conn,
            attempt_id=attempt_id,
            observed_at=now,
            observed_fencing_generation=fencing_generation,
            id_generator=id_generator,
            dispatch_certainty=dispatch_certainty,
            evidence_category=evidence.error_category,
            http_status=evidence.http_status,
            remote_request_id=None,
        )
        conn.execute("COMMIT")
        return SubmissionRetained(job=job, attempt=attempt)
    except sqlite3.OperationalError as exc:
        conn.execute("ROLLBACK")
        raise PersistenceError(
            f"submission finalization failed: {exc.__class__.__name__}"
        ) from exc


@dataclass(frozen=True)
class ReconciliationAuthorized:
    pass


@dataclass(frozen=True)
class ReconciliationDeferred:
    not_before: str


@dataclass(frozen=True)
class ReconciliationOwnershipLost:
    pass


ReconciliationAuthorizationResult = (
    ReconciliationAuthorized | ReconciliationDeferred | ReconciliationOwnershipLost
)


def authorize_reconciliation_request(
    conn: sqlite3.Connection,
    *,
    job_id: str,
    attempt_id: str,
    invocation_token: str,
    fencing_generation: int,
    clock: Clock,
    lease_seconds: int = LEASE_SECONDS,
    not_before: str | None = None,
) -> ReconciliationAuthorizationResult:
    """Authorize one reconciliation GET immediately before dispatch.

    Verifies current lease/fencing ownership and `max(not_before, service
    gate not_before)` inside one transaction, renewing the lease on success
    so the GET can follow immediately after commit, per ADR-006. `not_before`
    is the caller's own backoff/`Retry-After` eligibility for the next
    request in the active bounded sequence (`None` for the first request).
    """

    _begin_immediate(conn)
    try:
        job = _select_job(conn, job_id)
        attempt = _select_attempt(conn, attempt_id)
        now_instant = clock.now()
        now = format_timestamp(now_instant)

        if not _owns_active_attempt(
            job, attempt, invocation_token, fencing_generation, now
        ):
            conn.execute("ROLLBACK")
            return ReconciliationOwnershipLost()

        gate_not_before = rate_limit_gate.read_gate_not_before(conn)
        effective_not_before = _later_of(not_before, gate_not_before)
        if effective_not_before is not None and effective_not_before > now:
            conn.execute("COMMIT")
            return ReconciliationDeferred(not_before=effective_not_before)

        lease_expires_at = format_timestamp(
            add_milliseconds(now_instant, lease_seconds * 1000)
        )
        conn.execute(
            "UPDATE submission_attempts SET lease_expires_at = ? WHERE attempt_id = ?",
            (lease_expires_at, attempt_id),
        )
        conn.execute("COMMIT")
        return ReconciliationAuthorized()
    except sqlite3.OperationalError as exc:
        conn.execute("ROLLBACK")
        raise PersistenceError(
            f"reconciliation authorization failed: {exc.__class__.__name__}"
        ) from exc


def advance_gate_for_reconciliation_rate_limit(
    conn: sqlite3.Connection, *, delay_ms: int, clock: Clock
) -> None:
    """Advance the shared gate for a delivered reconciliation `429`.

    A standalone short transaction: the gate is persistence-coordination
    metadata shared by every Job and is not scoped to this attempt's
    ownership.
    """

    _begin_immediate(conn)
    try:
        now_instant = clock.now()
        now = format_timestamp(now_instant)
        gate_not_before = format_timestamp(add_milliseconds(now_instant, delay_ms))
        rate_limit_gate.advance_gate(
            conn, candidate_not_before=gate_not_before, now=now
        )
        conn.execute("COMMIT")
    except sqlite3.OperationalError as exc:
        conn.execute("ROLLBACK")
        raise PersistenceError(
            f"gate advance failed: {exc.__class__.__name__}"
        ) from exc


@dataclass(frozen=True)
class ReconciliationFinalized:
    job: JobRow
    attempt: AttemptRow


ReconciliationRecordResult = ReconciliationFinalized | PostSideEffectFailure


def record_reconciliation_result(
    conn: sqlite3.Connection,
    *,
    job_id: str,
    attempt_id: str,
    invocation_token: str,
    fencing_generation: int,
    clock: Clock,
    disposition: Literal["matched", "conflict", "not_found", "inconclusive"],
    http_status: int | None = None,
    remote_request_id: str | None = None,
) -> ReconciliationRecordResult:
    """Atomically resolve the active `STARTED` attempt from reconciliation

    evidence. Never creates a new SubmissionAttempt and never authorizes
    another POST; `disposition="inconclusive"` (budget exhaustion or an
    unfit wait ceiling) completes the attempt as `AMBIGUOUS` without a
    backing HTTP observation, per the approved specification.
    """

    _begin_immediate(conn)
    try:
        job = _select_job(conn, job_id)
        attempt = _select_attempt(conn, attempt_id)
        now = format_timestamp(clock.now())

        if not _owns_active_attempt(
            job, attempt, invocation_token, fencing_generation, now
        ):
            conn.execute("ROLLBACK")
            return PostSideEffectFailure(
                reason="lease, fencing, or state no longer matches this invocation"
            )
        assert job is not None and attempt is not None

        if disposition == "matched":
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
            return ReconciliationFinalized(
                job=replace(job, state="SUCCEEDED", updated_at=now),
                attempt=replace(
                    attempt,
                    state="SUCCEEDED",
                    completed_at=now,
                    http_status=http_status,
                    remote_request_id=remote_request_id,
                    owner_token=None,
                    lease_expires_at=None,
                ),
            )

        if disposition == "conflict":
            conn.execute(
                "UPDATE submission_attempts SET state = 'PERMANENT_FAILURE', completed_at = ?, "
                "error_category = 'idempotency_conflict', owner_token = NULL, "
                "lease_expires_at = NULL WHERE attempt_id = ?",
                (now, attempt_id),
            )
            conn.execute(
                "UPDATE jobs SET state = 'FAILED_PERMANENT', updated_at = ? WHERE job_id = ?",
                (now, job_id),
            )
            conn.execute("COMMIT")
            return ReconciliationFinalized(
                job=replace(job, state="FAILED_PERMANENT", updated_at=now),
                attempt=replace(
                    attempt,
                    state="PERMANENT_FAILURE",
                    completed_at=now,
                    error_category="idempotency_conflict",
                    owner_token=None,
                    lease_expires_at=None,
                ),
            )

        error_category = (
            "reconciliation_not_found"
            if disposition == "not_found"
            else "reconciliation_inconclusive"
        )
        conn.execute(
            "UPDATE submission_attempts SET state = 'AMBIGUOUS', completed_at = ?, "
            "error_category = ?, owner_token = NULL, lease_expires_at = NULL "
            "WHERE attempt_id = ?",
            (now, error_category, attempt_id),
        )
        conn.execute(
            "UPDATE jobs SET state = 'AMBIGUOUS', updated_at = ? WHERE job_id = ?",
            (now, job_id),
        )
        conn.execute("COMMIT")
        return ReconciliationFinalized(
            job=replace(job, state="AMBIGUOUS", updated_at=now),
            attempt=replace(
                attempt,
                state="AMBIGUOUS",
                completed_at=now,
                error_category=error_category,
                owner_token=None,
                lease_expires_at=None,
            ),
        )
    except sqlite3.OperationalError as exc:
        conn.execute("ROLLBACK")
        raise PersistenceError(
            f"reconciliation finalization failed: {exc.__class__.__name__}"
        ) from exc


def append_observation(
    conn: sqlite3.Connection,
    *,
    attempt_id: str,
    sequence_id: str,
    request_ordinal: int,
    operation: Literal["SUBMISSION", "RECONCILIATION"],
    observed_fencing_generation: int,
    evidence_category: str | None,
    clock: Clock,
    id_generator: Callable[[], str],
    dispatch_certainty: Literal["NOT_SENT", "MAYBE_SENT"] | None = None,
    http_status: int | None = None,
    remote_request_id: str | None = None,
    retry_after_ms: int | None = None,
    retry_after_diagnostic: str | None = None,
) -> str:
    """Append one sanitized, non-late HTTP observation for the current

    owner's active attempt. `sequence_id` groups every observation in one
    submission observation or one bounded reconciliation sequence;
    `request_ordinal` is the one-based request number within that sequence.
    Standalone (own transaction): the caller is expected to have already
    verified ownership via the authorization step that preceded dispatch.
    """

    _begin_immediate(conn)
    try:
        now = format_timestamp(clock.now())
        (sequence_number,) = conn.execute(
            "SELECT COALESCE(MAX(sequence_number), 0) + 1 FROM attempt_observations "
            "WHERE attempt_id = ?",
            (attempt_id,),
        ).fetchone()
        observation_id = id_generator()
        while True:
            try:
                conn.execute(
                    "INSERT INTO attempt_observations ("
                    "observation_id, attempt_id, sequence_id, sequence_number, operation, "
                    "request_ordinal, observed_at, dispatch_certainty, evidence_category, "
                    "http_status, remote_request_id, retry_after_ms, retry_after_diagnostic, "
                    "observed_fencing_generation, is_late"
                    ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)",
                    (
                        observation_id,
                        attempt_id,
                        sequence_id,
                        sequence_number,
                        operation,
                        request_ordinal,
                        now,
                        dispatch_certainty,
                        evidence_category,
                        http_status,
                        remote_request_id,
                        retry_after_ms,
                        retry_after_diagnostic,
                        observed_fencing_generation,
                    ),
                )
                break
            except sqlite3.IntegrityError as exc:
                if "observation_id" in str(exc):
                    observation_id = id_generator()
                    continue
                raise
        conn.execute("COMMIT")
        return observation_id
    except sqlite3.OperationalError as exc:
        conn.execute("ROLLBACK")
        raise PersistenceError(
            f"observation append failed: {exc.__class__.__name__}"
        ) from exc


def append_late_observation(
    conn: sqlite3.Connection,
    *,
    attempt_id: str,
    observed_fencing_generation: int,
    evidence_category: str,
    operation: Literal["SUBMISSION", "RECONCILIATION"],
    clock: Clock,
    id_generator: Callable[[], str],
    http_status: int | None = None,
    remote_request_id: str | None = None,
    retry_after_ms: int | None = None,
    retry_after_diagnostic: str | None = None,
) -> str:
    """Unconditionally append a sanitized observation from a stale

    (fenced-out) owner. No ownership check is performed -- by definition
    the caller may no longer hold it -- and this never updates attempt or
    Job state. The current owner later decides whether to act on it via
    `consume_late_observations`. `operation` must match whichever request
    (submission POST or reconciliation GET) actually produced this evidence.
    """

    _begin_immediate(conn)
    try:
        now = format_timestamp(clock.now())
        (sequence_number,) = conn.execute(
            "SELECT COALESCE(MAX(sequence_number), 0) + 1 FROM attempt_observations "
            "WHERE attempt_id = ?",
            (attempt_id,),
        ).fetchone()
        sequence_id = id_generator()
        observation_id = id_generator()
        while True:
            try:
                conn.execute(
                    "INSERT INTO attempt_observations ("
                    "observation_id, attempt_id, sequence_id, sequence_number, operation, "
                    "request_ordinal, observed_at, evidence_category, http_status, "
                    "remote_request_id, retry_after_ms, retry_after_diagnostic, "
                    "observed_fencing_generation, is_late"
                    ") VALUES (?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, 1)",
                    (
                        observation_id,
                        attempt_id,
                        sequence_id,
                        sequence_number,
                        operation,
                        now,
                        evidence_category,
                        http_status,
                        remote_request_id,
                        retry_after_ms,
                        retry_after_diagnostic,
                        observed_fencing_generation,
                    ),
                )
                break
            except sqlite3.IntegrityError as exc:
                if "observation_id" in str(exc) or "sequence_id" in str(exc):
                    observation_id = id_generator()
                    sequence_id = id_generator()
                    continue
                raise
        conn.execute("COMMIT")
        return observation_id
    except sqlite3.OperationalError as exc:
        conn.execute("ROLLBACK")
        raise PersistenceError(
            f"late observation append failed: {exc.__class__.__name__}"
        ) from exc


@dataclass(frozen=True)
class LateEvidenceConsumed:
    job: JobRow
    attempt: AttemptRow


@dataclass(frozen=True)
class NoActionableLateEvidence:
    pass


LateEvidenceConsumptionResult = (
    LateEvidenceConsumed | NoActionableLateEvidence | PostSideEffectFailure
)

_LATE_EVIDENCE_SUCCESS_CATEGORY = "processed"
_LATE_EVIDENCE_PERMANENT_HTTP_STATUS = {
    "validation_rejected": 400,
    "idempotency_conflict": 409,
}
_LATE_EVIDENCE_RATE_LIMITED_CATEGORY = "rate_limited"


def _is_actionable_late_evidence(
    evidence_category: str | None,
    http_status: int | None,
    remote_request_id: str | None,
) -> bool:
    """Require the HTTP status (and, for success, a remote request ID) to be

    internally consistent with the claimed category before ever acting on
    late evidence -- a category alone is not enough to authorize a terminal
    transition or a durable retry schedule from a stale owner's report.
    """

    if evidence_category == _LATE_EVIDENCE_SUCCESS_CATEGORY:
        return remote_request_id is not None and http_status in (200, 201)
    if evidence_category in _LATE_EVIDENCE_PERMANENT_HTTP_STATUS:
        return http_status == _LATE_EVIDENCE_PERMANENT_HTTP_STATUS[evidence_category]
    if evidence_category == _LATE_EVIDENCE_RATE_LIMITED_CATEGORY:
        return http_status == 429
    return False


def consume_late_observations(
    conn: sqlite3.Connection,
    *,
    job_id: str,
    attempt_id: str,
    invocation_token: str,
    fencing_generation: int,
    clock: Clock,
    random_source: RandomSource,
) -> LateEvidenceConsumptionResult:
    """Let the current owner act on unconsumed late evidence from a stale

    owner, atomically with the resulting transition. Authoritative matching
    success or authoritative permanent rejection/conflict complete the
    attempt and Job; an authoritative late `429` completes the attempt as
    `RETRYABLE_FAILURE` under the same lifetime budget rules and advances
    the durable gate, exactly as a live `429` observation would. Every other
    category remains diagnostic and is left unconsumed here. A completed
    attempt can never be reclassified: the ownership check alone already
    prevents that, since a completed attempt is no longer `STARTED`/
    `SUBMITTING`.
    """

    _begin_immediate(conn)
    try:
        job = _select_job(conn, job_id)
        attempt = _select_attempt(conn, attempt_id)
        now_instant = clock.now()
        now = format_timestamp(now_instant)

        if not _owns_active_attempt(
            job, attempt, invocation_token, fencing_generation, now
        ):
            conn.execute("ROLLBACK")
            return PostSideEffectFailure(
                reason="lease, fencing, or state no longer matches this invocation"
            )
        assert job is not None and attempt is not None

        rows = conn.execute(
            "SELECT observation_id, evidence_category, http_status, "
            "remote_request_id, retry_after_ms FROM attempt_observations "
            "WHERE attempt_id = ? AND is_late = 1 AND consumed_at IS NULL "
            "ORDER BY sequence_number",
            (attempt_id,),
        ).fetchall()

        actionable = next(
            (
                row
                for row in rows
                if _is_actionable_late_evidence(row[1], row[2], row[3])
            ),
            None,
        )
        if actionable is None:
            conn.execute("COMMIT")
            return NoActionableLateEvidence()

        (
            observation_id,
            evidence_category,
            http_status,
            remote_request_id,
            server_retry_after_ms,
        ) = actionable
        conn.execute(
            "UPDATE attempt_observations SET consumed_at = ? WHERE observation_id = ?",
            (now, observation_id),
        )

        if evidence_category == _LATE_EVIDENCE_SUCCESS_CATEGORY:
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
            return LateEvidenceConsumed(
                job=replace(job, state="SUCCEEDED", updated_at=now),
                attempt=replace(
                    attempt,
                    state="SUCCEEDED",
                    completed_at=now,
                    http_status=http_status,
                    remote_request_id=remote_request_id,
                    owner_token=None,
                    lease_expires_at=None,
                ),
            )

        if evidence_category == _LATE_EVIDENCE_RATE_LIMITED_CATEGORY:
            budget_remains = attempt.attempt_number < MAX_SUBMISSION_ATTEMPTS
            policy_ms = backoff.policy_delay_ms(attempt.attempt_number, random_source)
            delay_ms = backoff.effective_delay_ms(policy_ms, server_retry_after_ms)
            gate_not_before = format_timestamp(add_milliseconds(now_instant, delay_ms))
            rate_limit_gate.advance_gate(
                conn, candidate_not_before=gate_not_before, now=now
            )
            job_state = "RETRY_SCHEDULED" if budget_remains else "FAILED_PERMANENT"
            conn.execute(
                "UPDATE submission_attempts SET state = 'RETRYABLE_FAILURE', "
                "completed_at = ?, http_status = ?, error_category = 'rate_limited', "
                "retry_after_ms = ?, owner_token = NULL, lease_expires_at = NULL "
                "WHERE attempt_id = ?",
                (now, http_status, delay_ms, attempt_id),
            )
            conn.execute(
                "UPDATE jobs SET state = ?, updated_at = ? WHERE job_id = ?",
                (job_state, now, job_id),
            )
            conn.execute("COMMIT")
            return LateEvidenceConsumed(
                job=replace(job, state=job_state, updated_at=now),
                attempt=replace(
                    attempt,
                    state="RETRYABLE_FAILURE",
                    completed_at=now,
                    http_status=http_status,
                    error_category="rate_limited",
                    retry_after_ms=delay_ms,
                    owner_token=None,
                    lease_expires_at=None,
                ),
            )

        conn.execute(
            "UPDATE submission_attempts SET state = 'PERMANENT_FAILURE', completed_at = ?, "
            "http_status = ?, error_category = ?, owner_token = NULL, lease_expires_at = NULL "
            "WHERE attempt_id = ?",
            (now, http_status, evidence_category, attempt_id),
        )
        conn.execute(
            "UPDATE jobs SET state = 'FAILED_PERMANENT', updated_at = ? WHERE job_id = ?",
            (now, job_id),
        )
        conn.execute("COMMIT")
        return LateEvidenceConsumed(
            job=replace(job, state="FAILED_PERMANENT", updated_at=now),
            attempt=replace(
                attempt,
                state="PERMANENT_FAILURE",
                completed_at=now,
                http_status=http_status,
                error_category=evidence_category,
                owner_token=None,
                lease_expires_at=None,
            ),
        )
    except sqlite3.OperationalError as exc:
        conn.execute("ROLLBACK")
        raise PersistenceError(
            f"late evidence consumption failed: {exc.__class__.__name__}"
        ) from exc


def _owns_active_attempt(
    job: JobRow | None,
    attempt: AttemptRow | None,
    invocation_token: str,
    fencing_generation: int,
    now: str,
) -> bool:
    """Whether `invocation_token` still holds the unexpired, unfenced lease

    on an active `SUBMITTING`/`STARTED` attempt, at the exact
    `fencing_generation` it was authorized under. Callers pass the fencing
    generation they last observed rather than a fixed constant, so a future
    takeover that advances the stored generation is correctly detected as a
    loss of ownership for every earlier invocation.
    """

    return (
        job is not None
        and attempt is not None
        and job.state == "SUBMITTING"
        and attempt.state == "STARTED"
        and attempt.owner_token == invocation_token
        and attempt.fencing_generation == fencing_generation
        and attempt.lease_expires_at is not None
        and attempt.lease_expires_at > now
    )


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
    attempt_number: int,
    id_generator: Callable[[], str],
    clock: Clock,
    lease_seconds: int,
    invocation_token: str,
    now: str,
) -> AttemptRow:
    attempt_id = id_generator()
    lease_expires_at = format_timestamp(
        add_milliseconds(clock.now(), lease_seconds * 1000)
    )
    while True:
        try:
            conn.execute(
                "INSERT INTO submission_attempts "
                "(attempt_id, job_id, attempt_number, state, started_at, "
                "owner_token, fencing_generation, lease_expires_at) "
                "VALUES (?, ?, ?, 'STARTED', ?, ?, ?, ?)",
                (
                    attempt_id,
                    job.job_id,
                    attempt_number,
                    now,
                    invocation_token,
                    INITIAL_FENCING_GENERATION,
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
        attempt_number=attempt_number,
        state="STARTED",
        started_at=now,
        completed_at=None,
        http_status=None,
        remote_request_id=None,
        error_category=None,
        retry_after_ms=None,
        owner_token=invocation_token,
        fencing_generation=INITIAL_FENCING_GENERATION,
        lease_expires_at=lease_expires_at,
    )


_ATTEMPT_COLUMNS = (
    "attempt_id, job_id, attempt_number, state, started_at, completed_at, "
    "http_status, remote_request_id, error_category, retry_after_ms, "
    "owner_token, fencing_generation, lease_expires_at"
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
        f"SELECT {_ATTEMPT_COLUMNS} FROM submission_attempts WHERE attempt_id = ?",
        (attempt_id,),
    ).fetchone()
    return AttemptRow(*row) if row is not None else None


def _select_latest_attempt(conn: sqlite3.Connection, job_id: str) -> AttemptRow | None:
    row = conn.execute(
        f"SELECT {_ATTEMPT_COLUMNS} FROM submission_attempts WHERE job_id = ? "
        "ORDER BY attempt_number DESC LIMIT 1",
        (job_id,),
    ).fetchone()
    return AttemptRow(*row) if row is not None else None


def _find_successful_attempt(
    conn: sqlite3.Connection, job_id: str
) -> AttemptRow | None:
    rows = conn.execute(
        f"SELECT {_ATTEMPT_COLUMNS} FROM submission_attempts "
        "WHERE job_id = ? AND state = 'SUCCEEDED'",
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
