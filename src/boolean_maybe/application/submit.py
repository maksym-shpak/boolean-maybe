"""The Job submission workflow: the one asynchronous entry point the CLI

adapter calls through `asyncio.run()`.

Coordinates: Job Entry validation/canonicalization, idempotency-key
resolution, a loop over the durable pre-side-effect authorization (handling
existing terminal/active/deferred Jobs), at most one HTTP request per
authorized attempt, and the durable post-side-effect classification.
Persistence and HTTP are both synchronous adapters isolated from the event
loop via `asyncio.to_thread()`, called strictly sequentially (never
overlapping), so this feature creates no unbounded task set.

`docs/specs/features/reliable-job-submission.md` extends the original
single-attempt vertical (`docs/specs/features/submit-single-job.md`) with a
durable retry budget, a service-wide rate-limit gate, bounded in-process
waiting for eligibility that arrives soon, automatic reconciliation after an
uncertain (`5xx`/`MAYBE_SENT`/protocol-uncertain) observation, and fenced
recovery of an interrupted attempt after its lease expires. Every result
also carries `attempt_history` (a durable projection, attached once at the
end of the invocation) and `reconciliation_requests`.
"""

from __future__ import annotations

import asyncio
import sqlite3
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Callable, Literal

from .. import canonical_json
from ..domain import retry_after as retry_after_mod
from ..domain.clock import (
    Clock,
    SystemClock,
    add_milliseconds,
    format_timestamp,
    parse_timestamp,
)
from ..domain.idempotency_key import generate_key, is_accepted_key
from ..domain.identifiers import generate_id
from ..domain.random_source import RandomSource, SystemRandomSource
from ..domain.sleeper import RealSleeper, Sleeper
from ..external import client as external_client
from ..persistence import connection as persistence_connection
from ..persistence import transactions
from ..persistence.errors import PersistenceError
from . import classification, history_projection, reconciliation_sequence, recovery
from .waiting import WaitBudget, wait_if_it_fits

SuccessOutcome = Literal["succeeded", "already_completed"]
ErrorOutcome = Literal[
    "idempotency_conflict",
    "job_in_progress",
    "failed_permanent",
    "retry_exhausted",
    "ambiguous",
    "submission_deferred",
    "retry_scheduled",
    "ownership_lost",
    "local_persistence_failure",
]


class ValidationError(ValueError):
    """The Job Entry or supplied Idempotency Key failed validation.

    The CLI adapter maps this to exit code `2`; it is raised before any
    database is opened, any Job is created, or any HTTP request begins.
    """


@dataclass(frozen=True)
class SubmitRequest:
    job_entry_raw: str
    idempotency_key: str | None
    database_path: Path
    service_host: str
    service_port: int


@dataclass(frozen=True)
class SubmitSuccess:
    outcome: SuccessOutcome
    submitted: bool
    job_id: str
    idempotency_key: str
    state: str
    attempt_id: str
    attempt_number: int
    http_status: int
    remote_request_id: str
    payload_digest: str
    reconciliation_requests: int = 0
    attempt_history: list[history_projection.AttemptHistoryItem] | None = None


@dataclass(frozen=True)
class SubmitError:
    outcome: ErrorOutcome
    submitted: bool
    idempotency_key: str | None
    state: str | None
    message: str
    job_id: str | None = None
    reconciliation_requests: int = 0
    attempt_history: list[history_projection.AttemptHistoryItem] | None = None


SubmitOutcome = SubmitSuccess | SubmitError


@dataclass(frozen=True)
class _RetryEligibleAfterDispatch:
    """A `RETRYABLE_FAILURE` was just recorded with budget remaining; the

    caller decides whether its eligibility fits the remaining wait budget.
    """

    job: transactions.JobRow
    attempt: transactions.AttemptRow


async def run_submit(
    request: SubmitRequest,
    *,
    clock: Clock | None = None,
    sleeper: Sleeper | None = None,
    random_source: RandomSource | None = None,
    id_generator: Callable[[], str] = generate_id,
    key_generator: Callable[[], str] = generate_key,
    lease_seconds: int = transactions.LEASE_SECONDS,
    http_timeout_seconds: float = external_client.DEFAULT_TIMEOUT_SECONDS,
) -> SubmitOutcome:
    if clock is None:
        clock = SystemClock()
    if sleeper is None:
        sleeper = RealSleeper()
    if random_source is None:
        random_source = SystemRandomSource()
    canonical_bytes, digest = _validate_job_entry(request.job_entry_raw)

    supplied_key = request.idempotency_key
    if supplied_key is not None and not is_accepted_key(supplied_key):
        raise ValidationError("Idempotency Key does not match the accepted grammar")

    try:
        conn = await asyncio.to_thread(
            persistence_connection.open_connection, request.database_path
        )
    except PersistenceError:
        return SubmitError(
            outcome="local_persistence_failure",
            submitted=False,
            idempotency_key=supplied_key,
            state=None,
            message="local persistence initialization failed",
        )
    except Exception:
        # No Job can exist yet: nothing has been authorized or dispatched.
        return SubmitError(
            outcome="local_persistence_failure",
            submitted=False,
            idempotency_key=supplied_key,
            state=None,
            message="an unexpected internal error occurred before any Job could be authorized",
        )

    try:
        outcome = await _run_with_connection(
            conn,
            request=request,
            supplied_key=supplied_key,
            canonical_bytes=canonical_bytes,
            digest=digest,
            clock=clock,
            wait_budget=WaitBudget(sleeper),
            random_source=random_source,
            id_generator=id_generator,
            key_generator=key_generator,
            lease_seconds=lease_seconds,
            http_timeout_seconds=http_timeout_seconds,
        )
        if outcome.job_id is not None:
            # Best-effort: `attempt_history` is an additive projection, not
            # the terminal result. A failure here must not discard the
            # already-determined outcome (its real `submitted`, `state`,
            # `job_id`, and `idempotency_key`) in favor of a generic
            # internal-failure fallback that would misreport them.
            try:
                history = await asyncio.to_thread(
                    history_projection.read_attempt_history, conn, outcome.job_id
                )
                outcome = replace(outcome, attempt_history=history)
            except Exception:
                pass
        return outcome
    finally:
        await asyncio.to_thread(conn.close)


def _validate_job_entry(job_entry_raw: str) -> tuple[bytes, str]:
    raw_bytes = job_entry_raw.encode("utf-8")
    if len(raw_bytes) > canonical_json.MAX_JOB_ENTRY_BYTES:
        raise ValidationError("Job Entry exceeds the 1 MiB size limit")

    try:
        job_entry = canonical_json.parse_job_entry(raw_bytes)
        canonical_bytes = canonical_json.canonicalize(job_entry)
    except canonical_json.JobEntryValidationError as exc:
        raise ValidationError(str(exc)) from exc

    return canonical_bytes, canonical_json.payload_digest(canonical_bytes)


async def _run_with_connection(
    conn: sqlite3.Connection,
    *,
    request: SubmitRequest,
    supplied_key: str | None,
    canonical_bytes: bytes,
    digest: str,
    clock: Clock,
    wait_budget: WaitBudget,
    random_source: RandomSource,
    id_generator: Callable[[], str],
    key_generator: Callable[[], str],
    lease_seconds: int,
    http_timeout_seconds: float,
) -> SubmitOutcome:
    submitted_this_invocation = False
    # Once a Job is resolved or created (whether from a user-supplied key or
    # a freshly generated one), every subsequent reauthorization within this
    # same invocation's loop must reuse that exact key. Otherwise a
    # generated key -- resolved fresh by `run_pre_side_effect_transaction`
    # only when `supplied_key is None` -- would be regenerated on every
    # loop iteration, creating a new Job each time instead of retrying the
    # one already authorized.
    current_key = supplied_key
    # Tracks the Job once any branch below has read one, purely so a later
    # persistence failure in the *same* invocation's loop can still report
    # `job_id` instead of omitting it unnecessarily.
    known_job_id: str | None = None

    try:
        invocation_token = id_generator()
    except Exception:
        # No Job can exist yet: nothing has been authorized or dispatched.
        return SubmitError(
            outcome="local_persistence_failure",
            submitted=False,
            idempotency_key=supplied_key,
            state=None,
            message="an unexpected internal error occurred before any Job could be authorized",
        )

    while True:
        try:
            pre_result = await asyncio.to_thread(
                transactions.run_pre_side_effect_transaction,
                conn,
                supplied_key=current_key,
                key_generator=key_generator,
                canonical_bytes=canonical_bytes,
                id_generator=id_generator,
                invocation_token=invocation_token,
                clock=clock,
                lease_seconds=lease_seconds,
            )
        except PersistenceError:
            return SubmitError(
                outcome="local_persistence_failure",
                submitted=submitted_this_invocation,
                idempotency_key=current_key,
                state=None,
                job_id=known_job_id,
                message="local persistence operation failed",
            )
        except Exception:
            # No Job can be confirmed SUBMITTING yet: this transaction either
            # never committed or its outcome could not be determined here.
            return SubmitError(
                outcome="local_persistence_failure",
                submitted=submitted_this_invocation,
                idempotency_key=current_key,
                state=None,
                job_id=known_job_id,
                message="an unexpected internal error occurred before any Job could be authorized",
            )

        if isinstance(pre_result, transactions.IdempotencyConflict):
            assert supplied_key is not None
            return SubmitError(
                outcome="idempotency_conflict",
                submitted=False,
                idempotency_key=supplied_key,
                state=None,
                message="the Idempotency Key is already bound to a different Job Entry",
            )

        if isinstance(pre_result, transactions.AlreadyCompleted):
            job = pre_result.job
            attempt = pre_result.attempt
            # Type narrowing only: `transactions._verify_attempt_success_evidence`
            # already raised `DatabaseCorruptionError` (converted to
            # `local_persistence_failure` above) for any stored attempt whose
            # `http_status`/`remote_request_id` are not valid success evidence,
            # so these fields are guaranteed non-null by the time this is reached.
            assert attempt.http_status is not None
            assert attempt.remote_request_id is not None
            return SubmitSuccess(
                outcome="already_completed",
                submitted=False,
                job_id=job.job_id,
                idempotency_key=job.idempotency_key,
                state=job.state,
                attempt_id=attempt.attempt_id,
                attempt_number=attempt.attempt_number,
                http_status=attempt.http_status,
                remote_request_id=attempt.remote_request_id,
                payload_digest=canonical_json.payload_digest(job.payload_canonical),
            )

        if isinstance(pre_result, transactions.JobInProgress):
            job = pre_result.job
            return SubmitError(
                outcome="job_in_progress",
                submitted=submitted_this_invocation,
                idempotency_key=job.idempotency_key,
                state=job.state,
                job_id=job.job_id,
                message="another invocation currently owns the active attempt for this Job",
            )

        if isinstance(pre_result, transactions.RecoveryCandidate):
            current_key = pre_result.job.idempotency_key
            known_job_id = pre_result.job.job_id
            recovery_result = await recovery.recover_and_reconcile(
                conn,
                candidate=pre_result,
                invocation_token=invocation_token,
                clock=clock,
                wait_budget=wait_budget,
                random_source=random_source,
                id_generator=id_generator,
                lease_seconds=lease_seconds,
                service_host=request.service_host,
                service_port=request.service_port,
                expected_digest=digest,
                http_timeout_seconds=http_timeout_seconds,
            )
            if isinstance(recovery_result, recovery.RecoveryRerouted):
                # Captured evidence changed, or the lease was no longer
                # expired on recheck: reauthorize from current state rather
                # than reuse this stale evidence.
                continue
            if isinstance(recovery_result, recovery.RecoveryPersistenceFailed):
                return SubmitError(
                    outcome="local_persistence_failure",
                    submitted=submitted_this_invocation,
                    idempotency_key=current_key,
                    state="SUBMITTING",
                    job_id=known_job_id,
                    message="local recovery processing failed",
                )
            # Recovery itself never dispatches a POST; only the reconciliation
            # GETs run under this invocation.
            return _render_reconciliation_outcome(
                recovery_result, digest, submitted=submitted_this_invocation
            )

        if isinstance(pre_result, transactions.TerminalFailedPermanent):
            job = pre_result.job
            outcome_name = _failed_permanent_outcome(pre_result.attempt)
            return SubmitError(
                outcome=outcome_name,
                submitted=submitted_this_invocation,
                idempotency_key=job.idempotency_key,
                state=job.state,
                job_id=job.job_id,
                message="the Job has reached a permanent terminal state",
            )

        if isinstance(pre_result, transactions.TerminalAmbiguous):
            job = pre_result.job
            return SubmitError(
                outcome="ambiguous",
                submitted=submitted_this_invocation,
                idempotency_key=job.idempotency_key,
                state=job.state,
                job_id=job.job_id,
                message="remote processing for this Job cannot be determined",
            )

        if isinstance(pre_result, transactions.NotYetEligible):
            job = pre_result.job
            current_key = job.idempotency_key
            known_job_id = job.job_id
            outcome_name = (
                "submission_deferred" if job.state == "PENDING" else "retry_scheduled"
            )
            if await wait_if_it_fits(wait_budget, clock, pre_result.not_before):
                continue
            return SubmitError(
                outcome=outcome_name,
                submitted=submitted_this_invocation,
                idempotency_key=job.idempotency_key,
                state=job.state,
                job_id=job.job_id,
                message="the required delay does not fit the remaining invocation wait budget",
            )

        assert isinstance(pre_result, transactions.ReadyToSubmit)
        job = pre_result.job
        attempt = pre_result.attempt
        known_job_id = job.job_id

        # `_dispatch_and_finalize` internally stages its own exception handling
        # (lease check vs. HTTP dispatch vs. finalization) so `submitted`
        # reflects what actually happened. This outer catch is a last-resort
        # fallback for a bug in that staging itself; since the pre-side-effect
        # transaction has already committed by this point (the Job is durably
        # `SUBMITTING`), it defaults to the conservative `submitted=True`.
        try:
            step_result, this_attempt_submitted = await _dispatch_and_finalize(
                conn,
                request=request,
                job=job,
                attempt=attempt,
                canonical_bytes=canonical_bytes,
                digest=digest,
                clock=clock,
                wait_budget=wait_budget,
                invocation_token=invocation_token,
                http_timeout_seconds=http_timeout_seconds,
                random_source=random_source,
                id_generator=id_generator,
            )
        except Exception:
            return SubmitError(
                outcome="local_persistence_failure",
                submitted=True,
                idempotency_key=job.idempotency_key,
                state="SUBMITTING",
                job_id=job.job_id,
                message="an unexpected internal error occurred after the Job was authorized for submission",
            )

        submitted_this_invocation = submitted_this_invocation or this_attempt_submitted

        if isinstance(step_result, _RetryEligibleAfterDispatch):
            current_key = step_result.job.idempotency_key
            known_job_id = step_result.job.job_id
            scheduled_attempt = step_result.attempt
            assert scheduled_attempt.completed_at is not None
            assert scheduled_attempt.retry_after_ms is not None
            not_before = format_timestamp(
                add_milliseconds(
                    parse_timestamp(scheduled_attempt.completed_at),
                    scheduled_attempt.retry_after_ms,
                )
            )
            if await wait_if_it_fits(wait_budget, clock, not_before):
                continue
            return SubmitError(
                outcome="retry_scheduled",
                submitted=submitted_this_invocation,
                idempotency_key=step_result.job.idempotency_key,
                state="RETRY_SCHEDULED",
                job_id=step_result.job.job_id,
                message="the required delay does not fit the remaining invocation wait budget",
            )

        return replace(step_result, submitted=submitted_this_invocation)


def _failed_permanent_outcome(
    attempt: transactions.AttemptRow,
) -> Literal["retry_exhausted", "failed_permanent"]:
    if attempt.state == "RETRYABLE_FAILURE":
        return "retry_exhausted"
    return "failed_permanent"


async def _dispatch_and_finalize(
    conn: sqlite3.Connection,
    *,
    request: SubmitRequest,
    job: transactions.JobRow,
    attempt: transactions.AttemptRow,
    canonical_bytes: bytes,
    digest: str,
    clock: Clock,
    wait_budget: WaitBudget,
    invocation_token: str,
    http_timeout_seconds: float,
    random_source: RandomSource,
    id_generator: Callable[[], str],
) -> tuple[SubmitOutcome | _RetryEligibleAfterDispatch, bool]:
    """Dispatch this one authorized attempt and classify its observation.

    Returns the step result alongside whether *this specific attempt's*
    dispatch may have begun (proven `False` only for a lease lost before
    dispatch or an adapter-proven `NOT_SENT` transport failure). The caller
    ORs this into the invocation-wide `submitted` flag, since an earlier
    attempt in the same invocation may already have dispatched even if this
    one did not.
    """

    try:
        lease_renewal = await asyncio.to_thread(
            transactions.renew_lease,
            conn,
            job_id=job.job_id,
            attempt_id=attempt.attempt_id,
            invocation_token=invocation_token,
            fencing_generation=attempt.fencing_generation,
            clock=clock,
        )
    except asyncio.CancelledError:
        # This step runs strictly before `external_client.submit_job` is
        # ever called, so cancellation here is proven `NOT_SENT` per the
        # approved specification: complete the owned attempt as a safe
        # retryable failure under normal budget rules (durably recording
        # eligibility and consuming lifetime budget) before letting
        # cancellation propagate, rather than abandoning it silently.
        try:
            await asyncio.to_thread(
                transactions.finalize_submission_attempt,
                conn,
                job_id=job.job_id,
                attempt_id=attempt.attempt_id,
                attempt_number=attempt.attempt_number,
                invocation_token=invocation_token,
                fencing_generation=attempt.fencing_generation,
                clock=clock,
                random_source=random_source,
                id_generator=id_generator,
                evidence=transactions.SubmissionEvidence(
                    disposition="retryable_failure",
                    http_status=None,
                    remote_request_id=None,
                    error_category="transport_not_sent",
                    retry_after_values=(),
                ),
            )
        except Exception:
            pass
        raise
    except Exception:
        # Nothing before this point can have dispatched HTTP: the lease
        # check runs strictly before `external_client.submit_job` is ever
        # called, so any failure here -- expected `PersistenceError` or an
        # unanticipated bug -- still proves the request was not sent.
        return (
            SubmitError(
                outcome="local_persistence_failure",
                submitted=False,
                idempotency_key=job.idempotency_key,
                state="SUBMITTING",
                job_id=job.job_id,
                message="local lease verification failed before dispatch",
            ),
            False,
        )

    if isinstance(lease_renewal, transactions.LeaseLost):
        # Proven NOT_SENT: no HTTP request is made when the lease can no
        # longer be confirmed unexpired immediately before dispatch. This is
        # a proven ownership/fencing loss (per the spec's "If renewal,
        # authorization, or finalization loses ownership ... returns
        # `ownership_lost`"), not a generic local persistence failure.
        return (
            SubmitError(
                outcome="ownership_lost",
                submitted=False,
                idempotency_key=job.idempotency_key,
                state="SUBMITTING",
                job_id=job.job_id,
                message="the invocation lease expired before dispatch could begin",
            ),
            False,
        )

    try:
        http_outcome = await asyncio.to_thread(
            external_client.submit_job,
            request.service_host,
            request.service_port,
            job.idempotency_key,
            canonical_bytes,
            digest,
            timeout_seconds=http_timeout_seconds,
        )
    except Exception:
        # The HTTP adapter itself is expected to catch and classify every
        # transport failure as a `SubmitHttpTransportFailure` (see
        # `external/client.py`); this only guards against a bug escaping
        # that contract. By this point dispatch may genuinely have begun,
        # so this is the first point in this helper where `submitted=True`
        # reflects real uncertainty rather than a proven negative.
        return (
            SubmitError(
                outcome="local_persistence_failure",
                submitted=True,
                idempotency_key=job.idempotency_key,
                state="SUBMITTING",
                job_id=job.job_id,
                message="the external submission did not complete successfully",
            ),
            True,
        )

    evidence = classification.classify_submission_outcome(http_outcome)
    # Only a proven-`NOT_SENT` transport failure disproves dispatch for this
    # specific attempt; every other classification (success, a delivered
    # 4xx/5xx, or `MAYBE_SENT`/protocol-uncertain) implies a request was
    # actually transmitted.
    this_attempt_submitted = evidence.error_category != "transport_not_sent"

    try:
        record_result = await asyncio.to_thread(
            transactions.finalize_submission_attempt,
            conn,
            job_id=job.job_id,
            attempt_id=attempt.attempt_id,
            attempt_number=attempt.attempt_number,
            invocation_token=invocation_token,
            fencing_generation=attempt.fencing_generation,
            clock=clock,
            random_source=random_source,
            id_generator=id_generator,
            evidence=evidence,
        )
    except Exception:
        return (
            SubmitError(
                outcome="local_persistence_failure",
                submitted=this_attempt_submitted,
                idempotency_key=job.idempotency_key,
                state="SUBMITTING",
                job_id=job.job_id,
                message="local finalization failed after a remote response",
            ),
            this_attempt_submitted,
        )

    if isinstance(record_result, transactions.PostSideEffectFailure):
        # A stale owner may append its resulting sanitized observation
        # (tagged `is_late`) but may not finalize the Job through the
        # normal completion path; the current owner may later consume it.
        # Best-effort: the primary outcome below already reflects ownership
        # loss regardless of whether this append itself succeeds.
        late_evidence_category = (
            "processed"
            if evidence.disposition == "succeeded"
            else evidence.error_category
        ) or "protocol_uncertain"
        late_retry_after_ms: int | None = None
        late_retry_after_diagnostic: str | None = None
        if late_evidence_category == "rate_limited":
            parsed = retry_after_mod.parse_retry_after(
                evidence.retry_after_values, now=clock.now()
            )
            if isinstance(parsed, retry_after_mod.RetryAfterAccepted):
                late_retry_after_ms = parsed.server_delay_ms
            else:
                late_retry_after_diagnostic = parsed.diagnostic
        try:
            await asyncio.to_thread(
                transactions.append_late_observation,
                conn,
                attempt_id=attempt.attempt_id,
                observed_fencing_generation=attempt.fencing_generation,
                evidence_category=late_evidence_category,
                operation="SUBMISSION",
                clock=clock,
                id_generator=id_generator,
                http_status=evidence.http_status,
                remote_request_id=evidence.remote_request_id,
                retry_after_ms=late_retry_after_ms,
                retry_after_diagnostic=late_retry_after_diagnostic,
            )
        except Exception:
            pass
        return (
            SubmitError(
                outcome="ownership_lost",
                submitted=this_attempt_submitted,
                idempotency_key=job.idempotency_key,
                state="SUBMITTING",
                job_id=job.job_id,
                message="this invocation lost lease or fencing ownership before finalization committed",
            ),
            this_attempt_submitted,
        )

    if isinstance(record_result, transactions.SubmissionRetained):
        # 5xx / MAYBE_SENT transport / protocol-uncertain: reconcile the
        # same active attempt by idempotency key rather than resubmitting.
        reconciliation_outcome = (
            await reconciliation_sequence.run_reconciliation_sequence(
                conn,
                service_host=request.service_host,
                service_port=request.service_port,
                job=record_result.job,
                attempt=record_result.attempt,
                expected_digest=digest,
                invocation_token=invocation_token,
                fencing_generation=attempt.fencing_generation,
                clock=clock,
                wait_budget=wait_budget,
                random_source=random_source,
                id_generator=id_generator,
                http_timeout_seconds=http_timeout_seconds,
            )
        )
        return (
            _render_reconciliation_outcome(
                reconciliation_outcome, digest, submitted=this_attempt_submitted
            ),
            this_attempt_submitted,
        )

    if isinstance(record_result, transactions.SubmissionRetryScheduled):
        return (
            _RetryEligibleAfterDispatch(
                job=record_result.job, attempt=record_result.attempt
            ),
            this_attempt_submitted,
        )

    assert isinstance(record_result, transactions.SubmissionFinalized)
    finalized_job = record_result.job
    finalized_attempt = record_result.attempt

    if finalized_attempt.state == "SUCCEEDED":
        assert finalized_attempt.http_status is not None
        assert finalized_attempt.remote_request_id is not None
        return (
            SubmitSuccess(
                outcome="succeeded",
                submitted=True,
                job_id=finalized_job.job_id,
                idempotency_key=finalized_job.idempotency_key,
                state="SUCCEEDED",
                attempt_id=finalized_attempt.attempt_id,
                attempt_number=finalized_attempt.attempt_number,
                http_status=finalized_attempt.http_status,
                remote_request_id=finalized_attempt.remote_request_id,
                payload_digest=digest,
            ),
            this_attempt_submitted,
        )

    if finalized_attempt.state == "PERMANENT_FAILURE":
        return (
            SubmitError(
                outcome="failed_permanent",
                submitted=this_attempt_submitted,
                idempotency_key=finalized_job.idempotency_key,
                state="FAILED_PERMANENT",
                job_id=finalized_job.job_id,
                message="the external service definitively rejected this submission",
            ),
            this_attempt_submitted,
        )

    assert finalized_attempt.state == "RETRYABLE_FAILURE"
    return (
        SubmitError(
            outcome="retry_exhausted",
            submitted=this_attempt_submitted,
            idempotency_key=finalized_job.idempotency_key,
            state="FAILED_PERMANENT",
            job_id=finalized_job.job_id,
            message="the lifetime submission attempt budget is exhausted",
        ),
        this_attempt_submitted,
    )


def _render_reconciliation_outcome(
    outcome: reconciliation_sequence.ReconciliationOutcome,
    digest: str,
    *,
    submitted: bool,
) -> SubmitOutcome:
    """Render a bounded reconciliation sequence's result.

    `submitted` reflects whether *this invocation* began or may have begun
    a submission POST: `True` when reconciliation followed this
    invocation's own dispatch, `False` for a recovery-only invocation that
    never sent one (only reconciliation GETs), per the approved
    specification. `reconciliation_requests` is always this invocation's
    own GET count, regardless of outcome.
    """

    job = outcome.job
    attempt = outcome.attempt
    reconciliation_requests = outcome.request_count

    if outcome.persistence_failed:
        return SubmitError(
            outcome="local_persistence_failure",
            submitted=submitted,
            idempotency_key=job.idempotency_key,
            state="SUBMITTING",
            job_id=job.job_id,
            reconciliation_requests=reconciliation_requests,
            message="local reconciliation processing failed",
        )

    if outcome.ownership_lost:
        return SubmitError(
            outcome="ownership_lost",
            submitted=submitted,
            idempotency_key=job.idempotency_key,
            state=job.state,
            job_id=job.job_id,
            reconciliation_requests=reconciliation_requests,
            message="this invocation lost lease or fencing ownership during reconciliation",
        )

    if attempt.state == "SUCCEEDED":
        assert attempt.http_status is not None
        assert attempt.remote_request_id is not None
        return SubmitSuccess(
            outcome="succeeded",
            submitted=submitted,
            job_id=job.job_id,
            idempotency_key=job.idempotency_key,
            state="SUCCEEDED",
            attempt_id=attempt.attempt_id,
            attempt_number=attempt.attempt_number,
            http_status=attempt.http_status,
            remote_request_id=attempt.remote_request_id,
            payload_digest=digest,
            reconciliation_requests=reconciliation_requests,
        )

    if attempt.state == "PERMANENT_FAILURE":
        return SubmitError(
            outcome="failed_permanent",
            submitted=submitted,
            idempotency_key=job.idempotency_key,
            state="FAILED_PERMANENT",
            job_id=job.job_id,
            reconciliation_requests=reconciliation_requests,
            message="reconciliation or late evidence proved a permanent rejection or conflict",
        )

    if attempt.state == "RETRYABLE_FAILURE":
        # A late `429` observation was consumed under normal safe-retry
        # budget rules while this invocation was reconciling; report the
        # same outcome a live `429` would produce rather than crashing on
        # an attempt state this renderer did not previously expect.
        return SubmitError(
            outcome="retry_exhausted"
            if job.state == "FAILED_PERMANENT"
            else "retry_scheduled",
            submitted=submitted,
            idempotency_key=job.idempotency_key,
            state=job.state,
            job_id=job.job_id,
            reconciliation_requests=reconciliation_requests,
            message="late rate-limit evidence was consumed under the normal retry budget",
        )

    assert attempt.state == "AMBIGUOUS"
    return SubmitError(
        outcome="ambiguous",
        submitted=submitted,
        idempotency_key=job.idempotency_key,
        state="AMBIGUOUS",
        job_id=job.job_id,
        reconciliation_requests=reconciliation_requests,
        message="remote processing for this Job cannot be determined",
    )
