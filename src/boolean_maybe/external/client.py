"""Standard-library HTTP client for the single `POST /jobs` submission request.

Uses `http.client` (called by the application workflow through
`asyncio.to_thread()`, isolated from the event-loop thread). Enforces one
true ten-second deadline across connect, request transmission, and receipt
of the complete response; the 64 KiB response-body cap; and the exact
success-evidence contract (Content-Type, exact field set, and exact field
types). This client classifies only public HTTP evidence; it never reads
simulator scenario configuration, and it never follows redirects
(`http.client` never follows them automatically).
"""

from __future__ import annotations

import http.client
import re
import socket
import time
from dataclasses import dataclass

from .. import canonical_json

MAX_RESPONSE_BYTES = 64 * 1024
DEFAULT_TIMEOUT_SECONDS = 10.0

_CONTENT_TYPE_RE = re.compile(
    r"^application/json(?:\s*;\s*charset=utf-8)?$", re.IGNORECASE
)
_EXPECTED_SUCCESS_FIELDS = frozenset(
    {"idempotency_key", "status", "payload_digest", "remote_request_id", "replayed"}
)


@dataclass(frozen=True)
class SubmitHttpSuccess:
    http_status: int
    remote_request_id: str


@dataclass(frozen=True)
class SubmitHttpFailure:
    # True unless the adapter can prove no request bytes were ever sent
    # (any failure while establishing the connection, before any bytes are
    # written). Missing or uncertain evidence defaults to True, per the
    # conservative dispatch-certainty default this feature's `submitted`
    # field relies on.
    dispatch_may_have_begun: bool
    reason: str


SubmitHttpOutcome = SubmitHttpSuccess | SubmitHttpFailure


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

    deadline = time.monotonic() + timeout_seconds
    conn = http.client.HTTPConnection(host, port, timeout=timeout_seconds)
    try:
        try:
            conn.connect()
        except OSError as exc:
            # Nothing can have been dispatched: the connection never formed.
            return SubmitHttpFailure(
                dispatch_may_have_begun=False,
                reason=f"connect failed: {exc.__class__.__name__}",
            )

        # Capture the socket object directly: for a `Connection: close`
        # response, `HTTPConnection.getresponse()` sets `conn.sock = None`
        # as soon as it finishes reading headers (before this function gets
        # control back), even though the response body is still read from
        # this same underlying socket via `response.fp`. Applying the
        # shrinking deadline through `conn.sock` would silently stop
        # updating the timeout at that point; this captured reference keeps
        # working for the socket's whole lifetime.
        sock = conn.sock
        assert sock is not None

        try:
            _apply_remaining_timeout(sock, deadline)
            conn.request(
                "POST",
                "/jobs",
                body=canonical_bytes,
                headers={
                    "Content-Type": "application/json",
                    "Idempotency-Key": idempotency_key,
                },
            )
        except OSError as exc:
            return SubmitHttpFailure(
                dispatch_may_have_begun=True,
                reason=f"request send failed: {exc.__class__.__name__}",
            )

        try:
            _apply_remaining_timeout(sock, deadline)
            response = conn.getresponse()
            body = _read_bounded(response, MAX_RESPONSE_BYTES, sock, deadline)
        except (OSError, http.client.HTTPException) as exc:
            return SubmitHttpFailure(
                dispatch_may_have_begun=True,
                reason=f"response was not received: {exc.__class__.__name__}",
            )

        return _classify_response(
            response.status,
            response.getheader("Content-Type"),
            body,
            idempotency_key,
            expected_digest,
        )
    finally:
        conn.close()


def _apply_remaining_timeout(sock: socket.socket, deadline: float) -> None:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("the ten-second submission deadline has already elapsed")
    try:
        sock.settimeout(remaining)
    except OSError:
        # For a `Connection: close` response, `HTTPConnection.getresponse()`
        # closes its own reference to this socket once headers are read,
        # even though `response.fp` keeps reading the same underlying
        # handle successfully afterward. On Windows, that close can make
        # this handle's own `settimeout()` fail even though `read()` on it
        # still works. The deadline check above still bounds progress;
        # reads fall back to whatever timeout was last applied successfully
        # (set immediately before `getresponse()` was called).
        pass


def _read_bounded(
    response: http.client.HTTPResponse,
    limit: int,
    sock: socket.socket,
    deadline: float,
) -> bytes | None:
    """Read at most `limit` bytes within the overall deadline.

    Uses `read1()`, not `read()`: when the response declares a
    `Content-Length`, `HTTPResponse.read(n)` blocks until `n` bytes have
    actually arrived (via its underlying `BufferedReader`), so a server that
    dribbles the body slowly one small write at a time would keep a single
    `read()` call blocked for the entire dribble, and this function's own
    per-iteration deadline re-check would never run in between. `read1()`
    instead returns after at most one underlying system call -- whatever is
    immediately available, even if less than requested -- so the deadline
    is re-applied (and can expire) between every chunk actually received.
    """

    chunks: list[bytes] = []
    total = 0
    while True:
        _apply_remaining_timeout(sock, deadline)
        chunk = response.read1(limit + 1 - total)
        if not chunk:
            break
        chunks.append(chunk)
        total += len(chunk)
        if total > limit:
            return None
    return b"".join(chunks)


def _classify_response(
    status: int,
    content_type: str | None,
    body: bytes | None,
    expected_key: str,
    expected_digest: str,
) -> SubmitHttpOutcome:
    if body is None:
        return SubmitHttpFailure(
            dispatch_may_have_begun=True,
            reason="response body exceeded the 64 KiB limit",
        )
    if status not in (200, 201):
        return SubmitHttpFailure(
            dispatch_may_have_begun=True,
            reason=f"unexpected HTTP status {status}",
        )
    if content_type is None or not _CONTENT_TYPE_RE.match(content_type.strip()):
        return SubmitHttpFailure(
            dispatch_may_have_begun=True,
            reason="response Content-Type is not application/json",
        )

    try:
        parsed = canonical_json.loads_strict(body.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        return SubmitHttpFailure(
            dispatch_may_have_begun=True,
            reason="response body is not valid JSON",
        )
    if not isinstance(parsed, dict):
        return SubmitHttpFailure(
            dispatch_may_have_begun=True,
            reason="response body root is not a JSON object",
        )
    if set(parsed.keys()) != _EXPECTED_SUCCESS_FIELDS:
        return SubmitHttpFailure(
            dispatch_may_have_begun=True,
            reason="response body does not have the exact expected field set",
        )

    replayed = parsed.get("replayed")
    if not isinstance(replayed, bool):
        return SubmitHttpFailure(
            dispatch_may_have_begun=True,
            reason="response replayed is not a boolean",
        )
    if status == 201 and replayed is not False:
        return SubmitHttpFailure(
            dispatch_may_have_begun=True,
            reason="201 response did not report replayed=false",
        )
    if status == 200 and replayed is not True:
        return SubmitHttpFailure(
            dispatch_may_have_begun=True,
            reason="200 response did not report replayed=true",
        )
    if parsed.get("status") != "processed":
        return SubmitHttpFailure(
            dispatch_may_have_begun=True,
            reason="response status is not 'processed'",
        )
    if (
        not isinstance(parsed.get("idempotency_key"), str)
        or parsed.get("idempotency_key") != expected_key
    ):
        return SubmitHttpFailure(
            dispatch_may_have_begun=True,
            reason="response idempotency_key does not match",
        )
    if (
        not isinstance(parsed.get("payload_digest"), str)
        or parsed.get("payload_digest") != expected_digest
    ):
        return SubmitHttpFailure(
            dispatch_may_have_begun=True,
            reason="response payload_digest does not match",
        )
    remote_request_id = parsed.get("remote_request_id")
    if not isinstance(remote_request_id, str):
        return SubmitHttpFailure(
            dispatch_may_have_begun=True,
            reason="response remote_request_id is not a string",
        )

    return SubmitHttpSuccess(http_status=status, remote_request_id=remote_request_id)
