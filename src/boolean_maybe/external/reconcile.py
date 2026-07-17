"""Standard-library HTTP client for `GET /jobs/by-idempotency-key/{key}`
reconciliation requests.

Reconciliation only observes remote state and never initiates processing
(`docs/specs/features/reliable-job-submission.md`), so unlike
`external.client.submit_job` this adapter has no `NOT_SENT`/`MAYBE_SENT`
dispatch-certainty concept: a GET is idempotent and always safely retryable
within its own bounded budget, so every transport failure -- whether before
or after the connection formed -- is reported identically as
`ReconcileTransportFailure`.
"""

from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import quote

from . import _transport

DEFAULT_TIMEOUT_SECONDS = 10.0

_EXPECTED_PROCESSED_FIELDS = frozenset(
    {"idempotency_key", "status", "payload_digest", "remote_request_id"}
)
_EXPECTED_NOT_FOUND_FIELDS = frozenset({"idempotency_key", "status"})


@dataclass(frozen=True)
class ReconcileMatch:
    """Authoritative `200 processed` with matching key and payload digest."""

    http_status: int
    remote_request_id: str


@dataclass(frozen=True)
class ReconcileConflict:
    """Authoritative `200 processed` proving the key is bound to a different

    payload -- definitive evidence of a permanent conflict, not success.
    """

    http_status: int = 200


@dataclass(frozen=True)
class ReconcileNotFound:
    """Delivered valid `404 not_found`: a point-in-time negative observation,

    never by itself proof that no earlier processing occurred.
    """

    http_status: int = 404


@dataclass(frozen=True)
class ReconcileRateLimited:
    """Delivered valid `429`. `retry_after_values` is the raw, unparsed header

    text; parsing is deferred to `domain.retry_after.parse_retry_after`.
    """

    retry_after_values: tuple[str, ...]
    http_status: int = 429


@dataclass(frozen=True)
class ReconcileServerError:
    """Delivered `5xx`: ambiguous, never retried automatically from this GET."""

    http_status: int


@dataclass(frozen=True)
class ReconcileProtocolUncertain:
    """A redirect, malformed, oversized, wrong-Content-Type, or otherwise

    unexpected complete response.
    """

    reason: str


@dataclass(frozen=True)
class ReconcileTransportFailure:
    """Timeout, disconnect, or any other transport-level failure."""

    reason: str


ReconcileHttpOutcome = (
    ReconcileMatch
    | ReconcileConflict
    | ReconcileNotFound
    | ReconcileRateLimited
    | ReconcileServerError
    | ReconcileProtocolUncertain
    | ReconcileTransportFailure
)


def reconcile_job(
    host: str,
    port: int,
    idempotency_key: str,
    expected_digest: str,
    *,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
) -> ReconcileHttpOutcome:
    """Send exactly one reconciliation GET and classify the observation.

    The path component is percent-encoded even though the accepted key
    alphabet is already URL-unreserved, per the approved specification.
    """

    path = f"/jobs/by-idempotency-key/{quote(idempotency_key, safe='')}"
    outcome = _transport.send_request(
        host,
        port,
        "GET",
        path,
        body=None,
        headers={},
        timeout_seconds=timeout_seconds,
    )
    if isinstance(
        outcome, (_transport.TransportConnectFailed, _transport.TransportFailed)
    ):
        return ReconcileTransportFailure(reason=outcome.reason)
    return _classify_response(outcome, idempotency_key, expected_digest)


def _classify_response(
    response: _transport.TransportResponse,
    expected_key: str,
    expected_digest: str,
) -> ReconcileHttpOutcome:
    status = response.status
    if status == 200:
        return _classify_processed(response, expected_key, expected_digest)
    if status == 404:
        return _classify_not_found(response, expected_key)
    if status == 429:
        return _classify_rate_limited(response)
    if 500 <= status <= 599:
        return ReconcileServerError(http_status=status)
    return ReconcileProtocolUncertain(reason=f"unexpected HTTP status {status}")


def _classify_processed(
    response: _transport.TransportResponse,
    expected_key: str,
    expected_digest: str,
) -> ReconcileHttpOutcome:
    if response.body is None:
        return ReconcileProtocolUncertain(
            reason="response body exceeded the 64 KiB limit"
        )
    if not _transport.has_json_content_type(response.content_type):
        return ReconcileProtocolUncertain(
            reason="response Content-Type is not application/json"
        )

    parsed = _transport.parse_json_object(response.body)
    if parsed is None or set(parsed.keys()) != _EXPECTED_PROCESSED_FIELDS:
        return ReconcileProtocolUncertain(
            reason="response body does not have the exact expected field set"
        )
    if parsed.get("status") != "processed":
        return ReconcileProtocolUncertain(reason="response status is not 'processed'")
    if (
        not isinstance(parsed.get("idempotency_key"), str)
        or parsed.get("idempotency_key") != expected_key
    ):
        return ReconcileProtocolUncertain(
            reason="response idempotency_key does not match"
        )
    remote_request_id = parsed.get("remote_request_id")
    if not isinstance(remote_request_id, str):
        return ReconcileProtocolUncertain(
            reason="response remote_request_id is not a string"
        )
    payload_digest = parsed.get("payload_digest")
    if not isinstance(payload_digest, str):
        return ReconcileProtocolUncertain(
            reason="response payload_digest is not a string"
        )

    if payload_digest == expected_digest:
        return ReconcileMatch(http_status=200, remote_request_id=remote_request_id)
    return ReconcileConflict()


def _classify_not_found(
    response: _transport.TransportResponse,
    expected_key: str,
) -> ReconcileHttpOutcome:
    if response.body is None:
        return ReconcileProtocolUncertain(
            reason="response body exceeded the 64 KiB limit"
        )
    if not _transport.has_json_content_type(response.content_type):
        return ReconcileProtocolUncertain(
            reason="response Content-Type is not application/json"
        )

    parsed = _transport.parse_json_object(response.body)
    if parsed is None or set(parsed.keys()) != _EXPECTED_NOT_FOUND_FIELDS:
        return ReconcileProtocolUncertain(
            reason="response body does not have the exact expected field set"
        )
    if parsed.get("status") != "not_found":
        return ReconcileProtocolUncertain(reason="response status is not 'not_found'")
    if (
        not isinstance(parsed.get("idempotency_key"), str)
        or parsed.get("idempotency_key") != expected_key
    ):
        return ReconcileProtocolUncertain(
            reason="response idempotency_key does not match"
        )
    return ReconcileNotFound()


def _classify_rate_limited(
    response: _transport.TransportResponse,
) -> ReconcileHttpOutcome:
    error = _transport.parse_error_envelope(
        response.body, expected_keys={"code", "message"}
    )
    if error is None:
        return ReconcileProtocolUncertain(
            reason="429 response body is not a well-formed error envelope"
        )
    if error.get("code") != "rate_limited":
        return ReconcileProtocolUncertain(
            reason="429 response code is not rate_limited"
        )
    return ReconcileRateLimited(retry_after_values=response.retry_after_values)
