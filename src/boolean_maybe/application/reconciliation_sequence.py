"""Bounded automatic reconciliation after an uncertain submission observation

(`5xx` / `MAYBE_SENT` transport / protocol-uncertain), per ADR-006.

Issues at most three GETs by idempotency key against the same active
`STARTED` attempt, never creating another SubmissionAttempt and never
sending another POST. Matching authoritative evidence completes success;
authoritative conflict completes permanent failure; `404`, exhaustion, or an
unfit wait ceiling completes the attempt and Job as `AMBIGUOUS`.
"""

from __future__ import annotations

import asyncio
import sqlite3
from dataclasses import dataclass
from typing import Callable, Literal

from ..domain import backoff
from ..domain import retry_after as retry_after_mod
from ..domain.clock import Clock, add_milliseconds, format_timestamp
from ..domain.random_source import RandomSource
from ..external import reconcile as external_reconcile
from ..persistence import transactions
from ..persistence.errors import PersistenceError
from . import classification
from .waiting import WaitBudget, wait_while_renewing_lease

MAX_RECONCILIATION_REQUESTS = 3


@dataclass(frozen=True)
class ReconciliationOutcome:
    job: transactions.JobRow
    attempt: transactions.AttemptRow
    request_count: int
    ownership_lost: bool = False
    persistence_failed: bool = False


async def run_reconciliation_sequence(
    conn: sqlite3.Connection,
    *,
    service_host: str,
    service_port: int,
    job: transactions.JobRow,
    attempt: transactions.AttemptRow,
    expected_digest: str,
    invocation_token: str,
    fencing_generation: int,
    clock: Clock,
    wait_budget: WaitBudget,
    random_source: RandomSource,
    id_generator: Callable[[], str],
    http_timeout_seconds: float,
) -> ReconciliationOutcome:
    request_count = 0
    not_before: str | None = None
    sequence_id = id_generator()

    while True:
        consumed = await _try_consume_late_evidence(
            conn,
            job=job,
            attempt=attempt,
            invocation_token=invocation_token,
            fencing_generation=fencing_generation,
            clock=clock,
            random_source=random_source,
            request_count=request_count,
        )
        if consumed is not None:
            return consumed

        try:
            auth = await asyncio.to_thread(
                transactions.authorize_reconciliation_request,
                conn,
                job_id=job.job_id,
                attempt_id=attempt.attempt_id,
                invocation_token=invocation_token,
                fencing_generation=fencing_generation,
                clock=clock,
                not_before=not_before,
            )
        except Exception:
            return ReconciliationOutcome(
                job, attempt, request_count, persistence_failed=True
            )

        if isinstance(auth, transactions.ReconciliationOwnershipLost):
            return ReconciliationOutcome(
                job, attempt, request_count, ownership_lost=True
            )

        if isinstance(auth, transactions.ReconciliationDeferred):

            async def _renew() -> bool:
                try:
                    renewal = await asyncio.to_thread(
                        transactions.renew_lease,
                        conn,
                        job_id=job.job_id,
                        attempt_id=attempt.attempt_id,
                        invocation_token=invocation_token,
                        fencing_generation=fencing_generation,
                        clock=clock,
                    )
                except Exception:
                    return False
                return isinstance(renewal, transactions.LeaseRenewed)

            wait_result = await wait_while_renewing_lease(
                wait_budget, clock, auth.not_before, _renew
            )
            if wait_result is None:
                return ReconciliationOutcome(
                    job, attempt, request_count, ownership_lost=True
                )
            if wait_result:
                continue
            return await _finalize(
                conn,
                job=job,
                attempt=attempt,
                invocation_token=invocation_token,
                fencing_generation=fencing_generation,
                clock=clock,
                id_generator=id_generator,
                random_source=random_source,
                disposition="inconclusive",
                request_count=request_count,
            )

        assert isinstance(auth, transactions.ReconciliationAuthorized)

        try:
            http_outcome = await asyncio.to_thread(
                external_reconcile.reconcile_job,
                service_host,
                service_port,
                job.idempotency_key,
                expected_digest,
                timeout_seconds=http_timeout_seconds,
            )
        except Exception:
            # The adapter itself is expected to catch and classify every
            # transport failure; this only guards against a bug escaping
            # that contract, defaulting to the same conservative diagnostic
            # a genuine transport failure would produce.
            http_outcome = external_reconcile.ReconcileTransportFailure(
                reason="an unexpected internal error occurred during reconciliation"
            )

        request_count += 1
        evidence = classification.classify_reconciliation_outcome(http_outcome)
        observed_http_status = getattr(http_outcome, "http_status", None)
        observed_remote_request_id = (
            http_outcome.remote_request_id
            if isinstance(http_outcome, external_reconcile.ReconcileMatch)
            else None
        )

        # Parse `Retry-After` before recording the observation (not after),
        # so a delivered `429`'s parsed server delay/diagnostic is captured
        # in the same durable observation row rather than lost.
        now_instant = clock.now()
        retry_after_ms: int | None = None
        retry_after_diagnostic: str | None = None
        server_ms: int | None = None
        if evidence.error_category == "rate_limited":
            parsed = retry_after_mod.parse_retry_after(
                evidence.retry_after_values, now=now_instant
            )
            if isinstance(parsed, retry_after_mod.RetryAfterAccepted):
                server_ms = parsed.server_delay_ms
                retry_after_ms = server_ms
            else:
                retry_after_diagnostic = parsed.diagnostic

        try:
            await asyncio.to_thread(
                transactions.append_observation,
                conn,
                attempt_id=attempt.attempt_id,
                sequence_id=sequence_id,
                request_ordinal=request_count,
                operation="RECONCILIATION",
                observed_fencing_generation=fencing_generation,
                evidence_category=evidence.error_category,
                clock=clock,
                id_generator=id_generator,
                http_status=observed_http_status,
                remote_request_id=observed_remote_request_id,
                retry_after_ms=retry_after_ms,
                retry_after_diagnostic=retry_after_diagnostic,
            )
        except Exception:
            return ReconciliationOutcome(
                job, attempt, request_count, persistence_failed=True
            )

        if evidence.disposition != "retryable":
            http_status = observed_http_status
            remote_request_id = observed_remote_request_id
            return await _finalize(
                conn,
                job=job,
                attempt=attempt,
                invocation_token=invocation_token,
                fencing_generation=fencing_generation,
                clock=clock,
                id_generator=id_generator,
                random_source=random_source,
                disposition=evidence.disposition,
                request_count=request_count,
                http_status=http_status,
                remote_request_id=remote_request_id,
            )

        # "retryable": 429 / 5xx / transport / protocol-uncertain. Advance
        # the shared gate on a delivered 429, then decide whether another
        # GET remains within both the request budget and the wait ceiling.
        policy_ms = backoff.policy_delay_ms(request_count, random_source)
        delay_ms = backoff.effective_delay_ms(policy_ms, server_ms)

        if evidence.error_category == "rate_limited":
            try:
                await asyncio.to_thread(
                    transactions.advance_gate_for_reconciliation_rate_limit,
                    conn,
                    delay_ms=delay_ms,
                    clock=clock,
                )
            except Exception:
                return ReconciliationOutcome(
                    job, attempt, request_count, persistence_failed=True
                )

        if request_count >= MAX_RECONCILIATION_REQUESTS:
            return await _finalize(
                conn,
                job=job,
                attempt=attempt,
                invocation_token=invocation_token,
                fencing_generation=fencing_generation,
                clock=clock,
                id_generator=id_generator,
                random_source=random_source,
                disposition="inconclusive",
                request_count=request_count,
            )

        not_before = format_timestamp(add_milliseconds(clock.now(), delay_ms))


_LATE_EVIDENCE_CATEGORY_BY_DISPOSITION: dict[str, str] = {
    "matched": "processed",
    "conflict": "idempotency_conflict",
    "not_found": "reconciliation_not_found",
    "inconclusive": "reconciliation_inconclusive",
}


async def _try_consume_late_evidence(
    conn: sqlite3.Connection,
    *,
    job: transactions.JobRow,
    attempt: transactions.AttemptRow,
    invocation_token: str,
    fencing_generation: int,
    clock: Clock,
    random_source: RandomSource,
    request_count: int,
) -> ReconciliationOutcome | None:
    """Let the current owner act on unconsumed late evidence from a stale

    owner before issuing the next GET or finalizing terminal `AMBIGUOUS`,
    per the approved specification. `None` means nothing was actionable, so
    the caller proceeds with its normal next step.
    """

    try:
        result = await asyncio.to_thread(
            transactions.consume_late_observations,
            conn,
            job_id=job.job_id,
            attempt_id=attempt.attempt_id,
            invocation_token=invocation_token,
            fencing_generation=fencing_generation,
            clock=clock,
            random_source=random_source,
        )
    except Exception:
        return ReconciliationOutcome(
            job, attempt, request_count, persistence_failed=True
        )

    if isinstance(result, transactions.NoActionableLateEvidence):
        return None
    if isinstance(result, transactions.PostSideEffectFailure):
        return ReconciliationOutcome(job, attempt, request_count, ownership_lost=True)
    assert isinstance(result, transactions.LateEvidenceConsumed)
    return ReconciliationOutcome(result.job, result.attempt, request_count)


async def _finalize(
    conn: sqlite3.Connection,
    *,
    job: transactions.JobRow,
    attempt: transactions.AttemptRow,
    invocation_token: str,
    fencing_generation: int,
    clock: Clock,
    id_generator: Callable[[], str],
    random_source: RandomSource,
    disposition: Literal["matched", "conflict", "not_found", "inconclusive"],
    request_count: int,
    http_status: int | None = None,
    remote_request_id: str | None = None,
) -> ReconciliationOutcome:
    if disposition == "inconclusive":
        consumed = await _try_consume_late_evidence(
            conn,
            job=job,
            attempt=attempt,
            invocation_token=invocation_token,
            fencing_generation=fencing_generation,
            clock=clock,
            random_source=random_source,
            request_count=request_count,
        )
        if consumed is not None:
            return consumed

    try:
        record = await asyncio.to_thread(
            transactions.record_reconciliation_result,
            conn,
            job_id=job.job_id,
            attempt_id=attempt.attempt_id,
            invocation_token=invocation_token,
            fencing_generation=fencing_generation,
            clock=clock,
            disposition=disposition,
            http_status=http_status,
            remote_request_id=remote_request_id,
        )
    except PersistenceError:
        return ReconciliationOutcome(
            job, attempt, request_count, persistence_failed=True
        )
    except Exception:
        return ReconciliationOutcome(
            job, attempt, request_count, persistence_failed=True
        )

    if isinstance(record, transactions.PostSideEffectFailure):
        # A stale owner may append its resulting sanitized observation but
        # may not finalize the Job through the normal completion path; the
        # current owner may later consume it. Best-effort: the primary
        # outcome below already reflects ownership loss regardless of
        # whether this append itself succeeds.
        try:
            await asyncio.to_thread(
                transactions.append_late_observation,
                conn,
                attempt_id=attempt.attempt_id,
                observed_fencing_generation=fencing_generation,
                evidence_category=_LATE_EVIDENCE_CATEGORY_BY_DISPOSITION[disposition],
                operation="RECONCILIATION",
                clock=clock,
                id_generator=id_generator,
                http_status=http_status,
                remote_request_id=remote_request_id,
            )
        except Exception:
            pass
        return ReconciliationOutcome(job, attempt, request_count, ownership_lost=True)

    return ReconciliationOutcome(record.job, record.attempt, request_count)
