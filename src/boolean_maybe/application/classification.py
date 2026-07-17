"""Maps `external.client.SubmitHttpOutcome`/`external.reconcile.ReconcileHttpOutcome`

into `transactions.SubmissionEvidence`/`ReconciliationEvidence`.

Keeps the HTTP-specific adapter types out of the persistence layer: this is
the one seam where "what actually happened on the wire" becomes "what the
durable state machine does about it" (ADR-006's classification matrix).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from ..external import client as external_client
from ..external import reconcile as external_reconcile
from ..persistence.transactions import SubmissionEvidence

ReconciliationDisposition = Literal["matched", "conflict", "not_found", "retryable"]


@dataclass(frozen=True)
class ReconciliationEvidence:
    disposition: ReconciliationDisposition
    error_category: str | None
    retry_after_values: tuple[str, ...]


def classify_submission_outcome(
    outcome: external_client.SubmitHttpOutcome,
) -> SubmissionEvidence:
    if isinstance(outcome, external_client.SubmitHttpSuccess):
        return SubmissionEvidence(
            disposition="succeeded",
            http_status=outcome.http_status,
            remote_request_id=outcome.remote_request_id,
            error_category=None,
            retry_after_values=(),
        )
    if isinstance(outcome, external_client.SubmitHttpValidationRejected):
        return SubmissionEvidence(
            disposition="permanent_failure",
            http_status=400,
            remote_request_id=None,
            error_category="validation_rejected",
            retry_after_values=(),
        )
    if isinstance(outcome, external_client.SubmitHttpIdempotencyConflict):
        return SubmissionEvidence(
            disposition="permanent_failure",
            http_status=409,
            remote_request_id=None,
            error_category="idempotency_conflict",
            retry_after_values=(),
        )
    if isinstance(outcome, external_client.SubmitHttpRateLimited):
        return SubmissionEvidence(
            disposition="retryable_failure",
            http_status=429,
            remote_request_id=None,
            error_category="rate_limited",
            retry_after_values=outcome.retry_after_values,
        )
    if isinstance(outcome, external_client.SubmitHttpTransportFailure):
        if not outcome.dispatch_may_have_begun:
            return SubmissionEvidence(
                disposition="retryable_failure",
                http_status=None,
                remote_request_id=None,
                error_category="transport_not_sent",
                retry_after_values=(),
            )
        return SubmissionEvidence(
            disposition="retained",
            http_status=None,
            remote_request_id=None,
            error_category="transport_maybe_sent",
            retry_after_values=(),
        )
    if isinstance(outcome, external_client.SubmitHttpServerError):
        return SubmissionEvidence(
            disposition="retained",
            http_status=outcome.http_status,
            remote_request_id=None,
            error_category="server_uncertain",
            retry_after_values=(),
        )
    if isinstance(outcome, external_client.SubmitHttpProtocolUncertain):
        return SubmissionEvidence(
            disposition="retained",
            http_status=None,
            remote_request_id=None,
            error_category="protocol_uncertain",
            retry_after_values=(),
        )
    raise AssertionError(f"unclassified SubmitHttpOutcome: {outcome!r}")


def classify_reconciliation_outcome(
    outcome: external_reconcile.ReconcileHttpOutcome,
) -> ReconciliationEvidence:
    if isinstance(outcome, external_reconcile.ReconcileMatch):
        return ReconciliationEvidence(
            disposition="matched", error_category=None, retry_after_values=()
        )
    if isinstance(outcome, external_reconcile.ReconcileConflict):
        return ReconciliationEvidence(
            disposition="conflict",
            error_category="idempotency_conflict",
            retry_after_values=(),
        )
    if isinstance(outcome, external_reconcile.ReconcileNotFound):
        return ReconciliationEvidence(
            disposition="not_found",
            error_category="reconciliation_not_found",
            retry_after_values=(),
        )
    if isinstance(outcome, external_reconcile.ReconcileRateLimited):
        return ReconciliationEvidence(
            disposition="retryable",
            error_category="rate_limited",
            retry_after_values=outcome.retry_after_values,
        )
    if isinstance(outcome, external_reconcile.ReconcileServerError):
        return ReconciliationEvidence(
            disposition="retryable",
            error_category="server_uncertain",
            retry_after_values=(),
        )
    if isinstance(outcome, external_reconcile.ReconcileTransportFailure):
        return ReconciliationEvidence(
            disposition="retryable",
            error_category="transport_maybe_sent",
            retry_after_values=(),
        )
    if isinstance(outcome, external_reconcile.ReconcileProtocolUncertain):
        return ReconciliationEvidence(
            disposition="retryable",
            error_category="protocol_uncertain",
            retry_after_values=(),
        )
    raise AssertionError(f"unclassified ReconcileHttpOutcome: {outcome!r}")
