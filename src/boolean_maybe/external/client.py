"""Standard-library HTTP client for the single `POST /jobs` submission request.

Uses `_transport.send_request` (called by the application workflow through
`asyncio.to_thread()`, isolated from the event-loop thread) to enforce one
true ten-second deadline across connect, request transmission, and receipt
of the complete response; the 64 KiB response-body cap; and the exact
success-evidence contract (Content-Type, exact field set, and exact field
types). This client classifies only public HTTP evidence; it never reads
simulator scenario configuration, and it never follows redirects.

`docs/specs/features/reliable-job-submission.md` requires a definitive
classification matrix -- `200`/`201` success, delivered `400`/`409`/`429`,
delivered `5xx`, and everything else -- rather than one generic failure.
Only a connect-time failure proves `NOT_SENT`; every other outcome,
including a received-but-invalid response, is `MAYBE_SENT` by construction
(a response could only have arrived if the request was transmitted).
"""

from __future__ import annotations

from dataclasses import dataclass

from . import _transport

DEFAULT_TIMEOUT_SECONDS = 10.0

_EXPECTED_SUCCESS_FIELDS = frozenset(
    {"idempotency_key", "status", "payload_digest", "remote_request_id", "replayed"}
)


@dataclass(frozen=True)
class SubmitHttpSuccess:
    http_status: int
    remote_request_id: str


@dataclass(frozen=True)
class SubmitHttpValidationRejected:
    """Delivered valid `400`: this request definitely was not processed."""


@dataclass(frozen=True)
class SubmitHttpIdempotencyConflict:
    """Delivered authoritative `409`: the key is bound to a different payload."""


@dataclass(frozen=True)
class SubmitHttpRateLimited:
    """Delivered valid `429`. `retry_after_values` is the raw, unparsed header

    text (zero or more occurrences, exactly as received) -- parsing requires
    an observation-time clock the transport layer does not own, so it is
    deferred to `domain.retry_after.parse_retry_after`. The raw text must
    never be persisted.
    """

    retry_after_values: tuple[str, ...]


@dataclass(frozen=True)
class SubmitHttpServerError:
    """Delivered `5xx`: ambiguous: the request may have been processed."""

    http_status: int


@dataclass(frozen=True)
class SubmitHttpProtocolUncertain:
    """A redirect, malformed, oversized, wrong-Content-Type, or otherwise

    unexpected complete response. Ambiguous, like `SubmitHttpServerError`.
    """

    reason: str


@dataclass(frozen=True)
class SubmitHttpTransportFailure:
    # True unless the adapter can prove no request bytes were ever sent
    # (any failure while establishing the connection, before any bytes are
    # written). Missing or uncertain evidence defaults to True, per the
    # conservative dispatch-certainty default this feature's `submitted`
    # field relies on.
    dispatch_may_have_begun: bool
    reason: str


SubmitHttpOutcome = (
    SubmitHttpSuccess
    | SubmitHttpValidationRejected
    | SubmitHttpIdempotencyConflict
    | SubmitHttpRateLimited
    | SubmitHttpServerError
    | SubmitHttpProtocolUncertain
    | SubmitHttpTransportFailure
)


def submit_job(
    host: str,
    port: int,
    idempotency_key: str,
    canonical_bytes: bytes,
    expected_digest: str,
    *,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
) -> SubmitHttpOutcome:
    """Send exactly one `POST /jobs` request and classify the observation."""

    outcome = _transport.send_request(
        host,
        port,
        "POST",
        "/jobs",
        body=canonical_bytes,
        headers={
            "Content-Type": "application/json",
            "Idempotency-Key": idempotency_key,
        },
        timeout_seconds=timeout_seconds,
    )
    if isinstance(outcome, _transport.TransportConnectFailed):
        return SubmitHttpTransportFailure(
            dispatch_may_have_begun=False, reason=outcome.reason
        )
    if isinstance(outcome, _transport.TransportFailed):
        return SubmitHttpTransportFailure(
            dispatch_may_have_begun=True, reason=outcome.reason
        )
    return _classify_response(outcome, idempotency_key, expected_digest)


def _classify_response(
    response: _transport.TransportResponse,
    expected_key: str,
    expected_digest: str,
) -> SubmitHttpOutcome:
    status = response.status
    if status in (200, 201):
        return _classify_success(response, expected_key, expected_digest)
    if status == 400:
        return _classify_validation_rejected(response)
    if status == 409:
        return _classify_idempotency_conflict(response, expected_digest)
    if status == 429:
        return _classify_rate_limited(response)
    if 500 <= status <= 599:
        return SubmitHttpServerError(http_status=status)
    return SubmitHttpProtocolUncertain(reason=f"unexpected HTTP status {status}")


def _classify_success(
    response: _transport.TransportResponse,
    expected_key: str,
    expected_digest: str,
) -> SubmitHttpOutcome:
    if response.body is None:
        return SubmitHttpProtocolUncertain(
            reason="response body exceeded the 64 KiB limit"
        )
    if not _transport.has_json_content_type(response.content_type):
        return SubmitHttpProtocolUncertain(
            reason="response Content-Type is not application/json"
        )

    parsed = _transport.parse_json_object(response.body)
    if parsed is None:
        return SubmitHttpProtocolUncertain(reason="response body is not a JSON object")
    if set(parsed.keys()) != _EXPECTED_SUCCESS_FIELDS:
        return SubmitHttpProtocolUncertain(
            reason="response body does not have the exact expected field set"
        )

    replayed = parsed.get("replayed")
    if not isinstance(replayed, bool):
        return SubmitHttpProtocolUncertain(reason="response replayed is not a boolean")
    status = response.status
    if status == 201 and replayed is not False:
        return SubmitHttpProtocolUncertain(
            reason="201 response did not report replayed=false"
        )
    if status == 200 and replayed is not True:
        return SubmitHttpProtocolUncertain(
            reason="200 response did not report replayed=true"
        )
    if parsed.get("status") != "processed":
        return SubmitHttpProtocolUncertain(reason="response status is not 'processed'")
    if (
        not isinstance(parsed.get("idempotency_key"), str)
        or parsed.get("idempotency_key") != expected_key
    ):
        return SubmitHttpProtocolUncertain(
            reason="response idempotency_key does not match"
        )
    if (
        not isinstance(parsed.get("payload_digest"), str)
        or parsed.get("payload_digest") != expected_digest
    ):
        return SubmitHttpProtocolUncertain(
            reason="response payload_digest does not match"
        )
    remote_request_id = parsed.get("remote_request_id")
    if not isinstance(remote_request_id, str):
        return SubmitHttpProtocolUncertain(
            reason="response remote_request_id is not a string"
        )

    return SubmitHttpSuccess(http_status=status, remote_request_id=remote_request_id)


def _classify_validation_rejected(
    response: _transport.TransportResponse,
) -> SubmitHttpOutcome:
    error = _transport.parse_error_envelope(
        response.body, expected_keys={"code", "message"}
    )
    if error is None:
        return SubmitHttpProtocolUncertain(
            reason="400 response body is not a well-formed error envelope"
        )
    return SubmitHttpValidationRejected()


def _classify_idempotency_conflict(
    response: _transport.TransportResponse,
    expected_digest: str,
) -> SubmitHttpOutcome:
    error = _transport.parse_error_envelope(
        response.body,
        expected_keys={
            "code",
            "message",
            "stored_payload_digest",
            "submitted_payload_digest",
        },
    )
    if error is None:
        return SubmitHttpProtocolUncertain(
            reason="409 response body is not a well-formed error envelope"
        )
    if error.get("code") != "idempotency_conflict":
        return SubmitHttpProtocolUncertain(
            reason="409 response code is not idempotency_conflict"
        )
    submitted_digest = error.get("submitted_payload_digest")
    if not isinstance(submitted_digest, str) or submitted_digest != expected_digest:
        return SubmitHttpProtocolUncertain(
            reason="409 response submitted_payload_digest does not match"
        )
    if not isinstance(error.get("stored_payload_digest"), str):
        return SubmitHttpProtocolUncertain(
            reason="409 response stored_payload_digest is not a string"
        )
    return SubmitHttpIdempotencyConflict()


def _classify_rate_limited(
    response: _transport.TransportResponse,
) -> SubmitHttpOutcome:
    error = _transport.parse_error_envelope(
        response.body, expected_keys={"code", "message"}
    )
    if error is None:
        return SubmitHttpProtocolUncertain(
            reason="429 response body is not a well-formed error envelope"
        )
    if error.get("code") != "rate_limited":
        return SubmitHttpProtocolUncertain(
            reason="429 response code is not rate_limited"
        )
    return SubmitHttpRateLimited(retry_after_values=response.retry_after_values)
