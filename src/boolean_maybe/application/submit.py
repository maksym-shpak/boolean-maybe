"""The single-Job submission workflow: the one asynchronous entry point the
CLI adapter calls through `asyncio.run()`.

Coordinates: Job Entry validation/canonicalization, idempotency-key
resolution, the durable pre-side-effect transaction, at most one HTTP
request, and the durable post-side-effect transaction. Persistence and HTTP
are both synchronous adapters isolated from the event loop via
`asyncio.to_thread()`, called strictly sequentially (never overlapping), so
this feature creates no unbounded task set.

This feature deliberately implements only the fully-completed
`PENDING -> SUBMITTING -> SUCCEEDED` path. Any non-success HTTP or transport
observation, or a failed/uncertain post-side-effect commit, leaves the
committed `SUBMITTING` Job and `STARTED` attempt unchanged for a later
recovery feature; see `docs/specs/features/submit-single-job.md`.
"""

from __future__ import annotations

import asyncio
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Literal

from .. import canonical_json
from ..domain.clock import Clock, SystemClock
from ..domain.idempotency_key import generate_key, is_accepted_key
from ..domain.identifiers import generate_id
from ..external import client as external_client
from ..persistence import connection as persistence_connection
from ..persistence import transactions
from ..persistence.errors import PersistenceError

SuccessOutcome = Literal["succeeded", "already_completed"]
ErrorOutcome = Literal[
    "idempotency_conflict", "job_not_eligible", "submission_incomplete"
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


@dataclass(frozen=True)
class SubmitError:
    outcome: ErrorOutcome
    submitted: bool
    idempotency_key: str | None
    state: str | None
    message: str


SubmitOutcome = SubmitSuccess | SubmitError


async def run_submit(
    request: SubmitRequest,
    *,
    clock: Clock | None = None,
    id_generator: Callable[[], str] = generate_id,
    key_generator: Callable[[], str] = generate_key,
    lease_seconds: int = transactions.LEASE_SECONDS,
    http_timeout_seconds: float = external_client.DEFAULT_TIMEOUT_SECONDS,
) -> SubmitOutcome:
    if clock is None:
        clock = SystemClock()
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
            outcome="submission_incomplete",
            submitted=False,
            idempotency_key=supplied_key,
            state=None,
            message="local persistence initialization failed",
        )
    except Exception:
        # No Job can exist yet: nothing has been authorized or dispatched.
        return SubmitError(
            outcome="submission_incomplete",
            submitted=False,
            idempotency_key=supplied_key,
            state=None,
            message="an unexpected internal error occurred before any Job could be authorized",
        )

    try:
        return await _run_with_connection(
            conn,
            request=request,
            supplied_key=supplied_key,
            canonical_bytes=canonical_bytes,
            digest=digest,
            clock=clock,
            id_generator=id_generator,
            key_generator=key_generator,
            lease_seconds=lease_seconds,
            http_timeout_seconds=http_timeout_seconds,
        )
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
    id_generator: Callable[[], str],
    key_generator: Callable[[], str],
    lease_seconds: int,
    http_timeout_seconds: float,
) -> SubmitOutcome:
    try:
        invocation_token = id_generator()
        pre_result = await asyncio.to_thread(
            transactions.run_pre_side_effect_transaction,
            conn,
            supplied_key=supplied_key,
            key_generator=key_generator,
            canonical_bytes=canonical_bytes,
            id_generator=id_generator,
            invocation_token=invocation_token,
            clock=clock,
            lease_seconds=lease_seconds,
        )
    except PersistenceError:
        return SubmitError(
            outcome="submission_incomplete",
            submitted=False,
            idempotency_key=supplied_key,
            state=None,
            message="local persistence operation failed",
        )
    except Exception:
        # No Job can be confirmed SUBMITTING yet: this transaction either
        # never committed or its outcome could not be determined here.
        return SubmitError(
            outcome="submission_incomplete",
            submitted=False,
            idempotency_key=supplied_key,
            state=None,
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

    if isinstance(pre_result, transactions.JobNotEligible):
        assert supplied_key is not None
        return SubmitError(
            outcome="job_not_eligible",
            submitted=False,
            idempotency_key=supplied_key,
            state=pre_result.state,
            message="an equivalent Job already exists and is not eligible for a new submission",
        )

    if isinstance(pre_result, transactions.AlreadyCompleted):
        job = pre_result.job
        attempt = pre_result.attempt
        # Type narrowing only: `transactions._verify_attempt_success_evidence`
        # already raised `DatabaseCorruptionError` (converted to
        # `submission_incomplete` above) for any stored attempt whose
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

    assert isinstance(pre_result, transactions.ReadyToSubmit)
    job = pre_result.job
    attempt = pre_result.attempt

    # `_dispatch_and_finalize` internally stages its own exception handling
    # (lease check vs. HTTP dispatch vs. finalization) so `submitted`
    # reflects what actually happened. This outer catch is a last-resort
    # fallback for a bug in that staging itself; since the pre-side-effect
    # transaction has already committed by this point (the Job is durably
    # `SUBMITTING`), it defaults to the conservative `submitted=True`.
    try:
        return await _dispatch_and_finalize(
            conn,
            request=request,
            job=job,
            attempt=attempt,
            canonical_bytes=canonical_bytes,
            digest=digest,
            clock=clock,
            invocation_token=invocation_token,
            http_timeout_seconds=http_timeout_seconds,
        )
    except Exception:
        return SubmitError(
            outcome="submission_incomplete",
            submitted=True,
            idempotency_key=job.idempotency_key,
            state="SUBMITTING",
            message="an unexpected internal error occurred after the Job was authorized for submission",
        )


async def _dispatch_and_finalize(
    conn: sqlite3.Connection,
    *,
    request: SubmitRequest,
    job: transactions.JobRow,
    attempt: transactions.AttemptRow,
    canonical_bytes: bytes,
    digest: str,
    clock: Clock,
    invocation_token: str,
    http_timeout_seconds: float,
) -> SubmitOutcome:
    try:
        lease_still_valid = await asyncio.to_thread(
            transactions.verify_lease_still_valid,
            conn,
            job_id=job.job_id,
            attempt_id=attempt.attempt_id,
            invocation_token=invocation_token,
            clock=clock,
        )
    except Exception:
        # Nothing before this point can have dispatched HTTP: the lease
        # check runs strictly before `external_client.submit_job` is ever
        # called, so any failure here -- expected `PersistenceError` or an
        # unanticipated bug -- still proves the request was not sent.
        return SubmitError(
            outcome="submission_incomplete",
            submitted=False,
            idempotency_key=job.idempotency_key,
            state="SUBMITTING",
            message="local lease verification failed before dispatch",
        )

    if not lease_still_valid:
        # Proven NOT_SENT: no HTTP request is made when the lease can no
        # longer be confirmed unexpired immediately before dispatch.
        return SubmitError(
            outcome="submission_incomplete",
            submitted=False,
            idempotency_key=job.idempotency_key,
            state="SUBMITTING",
            message="the invocation lease expired before dispatch could begin",
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
        # transport failure as a `SubmitHttpFailure` (see `external/client.py`);
        # this only guards against a bug escaping that contract. By this
        # point dispatch may genuinely have begun, so this is the first
        # point in this helper where `submitted=True` reflects real
        # uncertainty rather than a proven negative.
        return SubmitError(
            outcome="submission_incomplete",
            submitted=True,
            idempotency_key=job.idempotency_key,
            state="SUBMITTING",
            message="the external submission did not complete successfully",
        )

    if isinstance(http_outcome, external_client.SubmitHttpFailure):
        return SubmitError(
            outcome="submission_incomplete",
            submitted=http_outcome.dispatch_may_have_begun,
            idempotency_key=job.idempotency_key,
            state="SUBMITTING",
            message="the external submission did not complete successfully",
        )

    try:
        post_result = await asyncio.to_thread(
            transactions.run_post_side_effect_success,
            conn,
            job_id=job.job_id,
            attempt_id=attempt.attempt_id,
            invocation_token=invocation_token,
            clock=clock,
            http_status=http_outcome.http_status,
            remote_request_id=http_outcome.remote_request_id,
        )
    except Exception:
        # HTTP already succeeded by this point, so `submitted=True` here is
        # certain, not merely a conservative default.
        return SubmitError(
            outcome="submission_incomplete",
            submitted=True,
            idempotency_key=job.idempotency_key,
            state="SUBMITTING",
            message="local finalization failed after a successful remote response",
        )

    if isinstance(post_result, transactions.PostSideEffectFailure):
        return SubmitError(
            outcome="submission_incomplete",
            submitted=True,
            idempotency_key=job.idempotency_key,
            state="SUBMITTING",
            message="local finalization could not verify invocation ownership",
        )

    return SubmitSuccess(
        outcome="succeeded",
        submitted=True,
        job_id=job.job_id,
        idempotency_key=job.idempotency_key,
        state="SUCCEEDED",
        attempt_id=attempt.attempt_id,
        attempt_number=attempt.attempt_number,
        http_status=http_outcome.http_status,
        remote_request_id=http_outcome.remote_request_id,
        payload_digest=digest,
    )
