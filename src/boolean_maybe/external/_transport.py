"""Shared low-level HTTP transport primitives for submission and
reconciliation.

Both operations enforce one absolute deadline covering connect, request
transmission, and receipt of the complete bounded response, and neither
follows a redirect (`http.client` never does so automatically). Only the
caller knows whether a connect-time failure is safety-relevant (submission's
`NOT_SENT`/`MAYBE_SENT` distinction); reconciliation is a GET and is always
safely retryable within its own budget, so this module reports a
connect-time failure and a later failure as two distinct outcomes and lets
each caller decide what to do with that distinction.
"""

from __future__ import annotations

import http.client
import re
import socket
import time
from dataclasses import dataclass

from .. import canonical_json

MAX_RESPONSE_BYTES = 64 * 1024

CONTENT_TYPE_JSON_RE = re.compile(
    r"^application/json(?:\s*;\s*charset=utf-8)?$", re.IGNORECASE
)


@dataclass(frozen=True)
class TransportResponse:
    status: int
    content_type: str | None
    retry_after_values: tuple[str, ...]
    body: bytes | None  # `None` means the body exceeded `MAX_RESPONSE_BYTES`.


@dataclass(frozen=True)
class TransportConnectFailed:
    """The connection never formed; no request bytes could have been sent."""

    reason: str


@dataclass(frozen=True)
class TransportFailed:
    """A failure after the connection formed; request bytes may have been sent."""

    reason: str


TransportOutcome = TransportResponse | TransportConnectFailed | TransportFailed


def send_request(
    host: str,
    port: int,
    method: str,
    path: str,
    *,
    body: bytes | None,
    headers: dict[str, str],
    timeout_seconds: float,
) -> TransportOutcome:
    """Send exactly one HTTP request and return the raw transport outcome."""

    deadline = time.monotonic() + timeout_seconds
    conn = http.client.HTTPConnection(host, port, timeout=timeout_seconds)
    try:
        try:
            conn.connect()
        except OSError as exc:
            # Nothing can have been dispatched: the connection never formed.
            return TransportConnectFailed(f"connect failed: {exc.__class__.__name__}")

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
            conn.request(method, path, body=body, headers=headers)
        except OSError as exc:
            return TransportFailed(f"request send failed: {exc.__class__.__name__}")

        try:
            _apply_remaining_timeout(sock, deadline)
            response = conn.getresponse()
            response_body = _read_bounded(response, MAX_RESPONSE_BYTES, sock, deadline)
        except (OSError, http.client.HTTPException) as exc:
            return TransportFailed(
                f"response was not received: {exc.__class__.__name__}"
            )

        retry_after_values = tuple(response.msg.get_all("Retry-After") or ())
        return TransportResponse(
            status=response.status,
            content_type=response.getheader("Content-Type"),
            retry_after_values=retry_after_values,
            body=response_body,
        )
    finally:
        conn.close()


def _apply_remaining_timeout(sock: socket.socket, deadline: float) -> None:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("the ten-second operation deadline has already elapsed")
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


def has_json_content_type(content_type: str | None) -> bool:
    return content_type is not None and bool(
        CONTENT_TYPE_JSON_RE.match(content_type.strip())
    )


def parse_json_object(body: bytes) -> dict[str, object] | None:
    try:
        parsed = canonical_json.loads_strict(body.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        return None
    if not isinstance(parsed, dict):
        return None
    return parsed


def parse_error_envelope(
    body: bytes | None, *, expected_keys: frozenset[str] | set[str]
) -> dict[str, object] | None:
    """Parse and validate `{"error": {...}}` with exactly `expected_keys`.

    Returns the inner `error` object on success, or `None` if the body is
    oversized, malformed, or does not have the documented shape with
    string-typed `code`/`message`.
    """

    if body is None:
        return None
    parsed = parse_json_object(body)
    if parsed is None or set(parsed.keys()) != {"error"}:
        return None
    error = parsed["error"]
    if not isinstance(error, dict) or set(error.keys()) != set(expected_keys):
        return None
    if not isinstance(error.get("code"), str) or not isinstance(
        error.get("message"), str
    ):
        return None
    return error
