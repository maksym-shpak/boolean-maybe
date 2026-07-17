"""Fenced recovery of an interrupted `SUBMITTING` Job / `STARTED` attempt

(ADR-004, ADR-006).

Reached only for an equivalent existing `SUBMITTING` Job selected with an
explicit (user-supplied) idempotency key whose lease has already expired.
Captures the exact owner token, fencing generation, and lease expiry, waits
out a fixed 10-second monotonic quarantine without holding a transaction,
then requires those exact values to remain unchanged and still expired
before claiming the attempt and advancing its fencing generation by exactly
one. A successfully claimed attempt is always treated as `MAYBE_SENT`,
regardless of its actual crash boundary, and is reconciled -- never
resubmitted, never given a replacement attempt. The 10-second quarantine
counts toward (not in addition to) the invocation's 30-second wait ceiling,
leaving up to 20 seconds for the reconciliation sequence that follows.
"""

from __future__ import annotations

import asyncio
import sqlite3
from dataclasses import dataclass
from typing import Callable

from ..domain.clock import Clock
from ..domain.random_source import RandomSource
from ..persistence import transactions
from . import reconciliation_sequence
from .waiting import WaitBudget

QUARANTINE_SECONDS = 10.0


@dataclass(frozen=True)
class RecoveryRerouted:
    """The claim was rejected: captured evidence changed, or the lease was

    no longer expired when rechecked. The caller must reauthorize from
    current state rather than reuse this stale evidence.
    """


@dataclass(frozen=True)
class RecoveryPersistenceFailed:
    pass


RecoveryResult = (
    reconciliation_sequence.ReconciliationOutcome
    | RecoveryRerouted
    | RecoveryPersistenceFailed
)


async def recover_and_reconcile(
    conn: sqlite3.Connection,
    *,
    candidate: transactions.RecoveryCandidate,
    invocation_token: str,
    clock: Clock,
    wait_budget: WaitBudget,
    random_source: RandomSource,
    id_generator: Callable[[], str],
    lease_seconds: int,
    service_host: str,
    service_port: int,
    expected_digest: str,
    http_timeout_seconds: float,
) -> RecoveryResult:
    captured_owner_token = candidate.attempt.owner_token
    captured_fencing_generation = candidate.attempt.fencing_generation
    captured_lease_expires_at = candidate.attempt.lease_expires_at

    # A fixed wait, not a "does it fit" check: the quarantine is mandatory
    # before any claim may be attempted, per the approved specification.
    await wait_budget.sleep(QUARANTINE_SECONDS)

    try:
        claim = await asyncio.to_thread(
            transactions.claim_expired_attempt,
            conn,
            job_id=candidate.job.job_id,
            attempt_id=candidate.attempt.attempt_id,
            captured_owner_token=captured_owner_token,
            captured_fencing_generation=captured_fencing_generation,
            captured_lease_expires_at=captured_lease_expires_at,
            new_invocation_token=invocation_token,
            clock=clock,
            lease_seconds=lease_seconds,
        )
    except Exception:
        return RecoveryPersistenceFailed()

    if isinstance(claim, transactions.ClaimRejected):
        return RecoveryRerouted()

    assert isinstance(claim, transactions.ClaimSucceeded)
    # The claimed attempt is always MAYBE_SENT regardless of its actual
    # crash boundary; recovery never resubmits and never creates another
    # attempt -- only the same bounded reconciliation sequence applies.
    return await reconciliation_sequence.run_reconciliation_sequence(
        conn,
        service_host=service_host,
        service_port=service_port,
        job=claim.job,
        attempt=claim.attempt,
        expected_digest=expected_digest,
        invocation_token=invocation_token,
        fencing_generation=claim.attempt.fencing_generation,
        clock=clock,
        wait_budget=wait_budget,
        random_source=random_source,
        id_generator=id_generator,
        http_timeout_seconds=http_timeout_seconds,
    )
