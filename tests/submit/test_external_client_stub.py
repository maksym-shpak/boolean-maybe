"""External HTTP client tests against a minimal raw stub server.

These exercise responses a well-behaved simulator never produces (oversized
body, malformed JSON, mismatched replay flag) without needing to alter the
real simulator's behavior.
"""

from __future__ import annotations

import socket
import threading
import time
from collections.abc import Iterator

import pytest

from boolean_maybe import canonical_json
from boolean_maybe.external import client


class _StubServer:
    def __init__(self, response_bytes: bytes) -> None:
        self._response_bytes = response_bytes
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.bind(("127.0.0.1", 0))
        self._sock.listen(1)
        self.port = self._sock.getsockname()[1]
        self._thread = threading.Thread(target=self._serve_once, daemon=True)
        self._thread.start()

    def _serve_once(self) -> None:
        try:
            conn, _ = self._sock.accept()
        except OSError:
            return
        try:
            conn.settimeout(5.0)
            _read_and_drain_request(conn)
            conn.sendall(self._response_bytes)
        except OSError:
            pass
        finally:
            conn.close()

    def close(self) -> None:
        self._sock.close()
        self._thread.join(timeout=5.0)


def _read_and_drain_request(conn: socket.socket) -> bytes:
    """Read headers, then drain the declared request body.

    Closing a socket while the peer's request body write is still
    in-flight and unread can turn a clean response delivery into a
    `ConnectionResetError` on the client side (observed on Windows,
    matching the same race the simulator's own server already guards
    against in `simulator/server.py`). Draining the body first avoids that
    race regardless of platform.
    """

    buffer = b""
    while b"\r\n\r\n" not in buffer:
        chunk = conn.recv(4096)
        if not chunk:
            return buffer
        buffer += chunk

    header_text, _, body_so_far = buffer.partition(b"\r\n\r\n")
    content_length = 0
    for line in header_text.split(b"\r\n"):
        name, _, value = line.partition(b":")
        if name.strip().lower() == b"content-length":
            try:
                content_length = int(value.strip())
            except ValueError:
                content_length = 0
            break

    remaining = content_length - len(body_so_far)
    while remaining > 0:
        chunk = conn.recv(min(remaining, 65536))
        if not chunk:
            break
        remaining -= len(chunk)
    return buffer


@pytest.fixture
def stub_server() -> Iterator[type[_StubServer]]:
    servers: list[_StubServer] = []

    def factory(response_bytes: bytes) -> _StubServer:
        server = _StubServer(response_bytes)
        servers.append(server)
        return server

    yield factory  # type: ignore[misc]

    for server in servers:
        server.close()


def _submit(port: int) -> client.SubmitHttpOutcome:
    canonical_bytes = canonical_json.canonicalize({"a": 1})
    digest = canonical_json.payload_digest(canonical_bytes)
    return client.submit_job(
        "127.0.0.1", port, "job-a", canonical_bytes, digest, timeout_seconds=3.0
    )


def test_oversized_response_body_is_not_authoritative_success(stub_server) -> None:
    oversized_body = b'{"pad":"' + (b"x" * (70 * 1024)) + b'"}'
    response = (
        b"HTTP/1.1 201 Created\r\n"
        b"Content-Type: application/json\r\n"
        + f"Content-Length: {len(oversized_body)}\r\n\r\n".encode()
        + oversized_body
    )
    server = stub_server(response)

    outcome = _submit(server.port)

    assert isinstance(outcome, client.SubmitHttpFailure)
    assert outcome.dispatch_may_have_begun is True


def _valid_201_body() -> bytes:
    import json

    return json.dumps(
        {
            "idempotency_key": "job-a",
            "status": "processed",
            "payload_digest": canonical_json.payload_digest(
                canonical_json.canonicalize({"a": 1})
            ),
            "remote_request_id": "remote-1",
            "replayed": False,
        }
    ).encode()


def test_wrong_content_type_is_not_success(stub_server) -> None:
    body = _valid_201_body()
    response = (
        b"HTTP/1.1 201 Created\r\n"
        b"Content-Type: text/plain\r\n"
        + f"Content-Length: {len(body)}\r\n\r\n".encode()
        + body
    )
    server = stub_server(response)

    outcome = _submit(server.port)

    assert isinstance(outcome, client.SubmitHttpFailure)
    assert outcome.dispatch_may_have_begun is True
    assert "content-type" in outcome.reason.lower()


def test_missing_content_type_is_not_success(stub_server) -> None:
    body = _valid_201_body()
    response = (
        b"HTTP/1.1 201 Created\r\n"
        + f"Content-Length: {len(body)}\r\n\r\n".encode()
        + body
    )
    server = stub_server(response)

    outcome = _submit(server.port)

    assert isinstance(outcome, client.SubmitHttpFailure)


def test_response_with_unexpected_extra_field_is_not_success(stub_server) -> None:
    import json

    payload = {
        "idempotency_key": "job-a",
        "status": "processed",
        "payload_digest": canonical_json.payload_digest(
            canonical_json.canonicalize({"a": 1})
        ),
        "remote_request_id": "remote-1",
        "replayed": False,
        "unexpected_extra_field": "surprise",
    }
    body = json.dumps(payload).encode()
    response = (
        b"HTTP/1.1 201 Created\r\n"
        b"Content-Type: application/json\r\n"
        + f"Content-Length: {len(body)}\r\n\r\n".encode()
        + body
    )
    server = stub_server(response)

    outcome = _submit(server.port)

    assert isinstance(outcome, client.SubmitHttpFailure)
    assert outcome.dispatch_may_have_begun is True


def test_malformed_json_body_is_not_success(stub_server) -> None:
    body = b"not json"
    response = (
        b"HTTP/1.1 201 Created\r\n"
        b"Content-Type: application/json\r\n"
        + f"Content-Length: {len(body)}\r\n\r\n".encode()
        + body
    )
    server = stub_server(response)

    outcome = _submit(server.port)

    assert isinstance(outcome, client.SubmitHttpFailure)


def test_wrong_replayed_flag_for_201_is_not_success(stub_server) -> None:
    import json

    body = json.dumps(
        {
            "idempotency_key": "job-a",
            "status": "processed",
            "payload_digest": canonical_json.payload_digest(
                canonical_json.canonicalize({"a": 1})
            ),
            "remote_request_id": "remote-1",
            "replayed": True,  # wrong for a 201
        }
    ).encode()
    response = (
        b"HTTP/1.1 201 Created\r\n"
        b"Content-Type: application/json\r\n"
        + f"Content-Length: {len(body)}\r\n\r\n".encode()
        + body
    )
    server = stub_server(response)

    outcome = _submit(server.port)

    assert isinstance(outcome, client.SubmitHttpFailure)


def test_incomplete_response_is_not_success(stub_server) -> None:
    # Declares more bytes than are actually sent, then the connection closes.
    response = (
        b"HTTP/1.1 201 Created\r\n"
        b"Content-Type: application/json\r\n"
        b"Content-Length: 500\r\n\r\n"
        b'{"status":"processed"'
    )
    server = stub_server(response)

    outcome = _submit(server.port)

    assert isinstance(outcome, client.SubmitHttpFailure)
    assert outcome.dispatch_may_have_begun is True


def test_connection_close_slow_dribbled_body_is_bounded_by_the_deadline() -> None:
    # Regression test: a `Connection: close` response makes
    # `HTTPConnection.getresponse()` close its own socket reference once
    # headers are read (before the body is read at all). The deadline must
    # still bound the *body* read in that case, not silently fall back to
    # whatever timeout the socket happened to have.
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.bind(("127.0.0.1", 0))
    server_sock.listen(1)
    port = server_sock.getsockname()[1]

    def serve() -> None:
        try:
            conn, _ = server_sock.accept()
        except OSError:
            return
        try:
            conn.settimeout(5.0)
            _read_and_drain_request(conn)
            header = (
                b"HTTP/1.1 201 Created\r\n"
                b"Content-Type: application/json\r\n"
                b"Connection: close\r\n"
                b"Content-Length: 20\r\n\r\n"
            )
            conn.sendall(header)
            for _ in range(40):  # would take ~4s to fully dribble
                time.sleep(0.1)
                try:
                    conn.sendall(b"x")
                except OSError:
                    return
        except OSError:
            pass
        finally:
            conn.close()

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    try:
        canonical_bytes = canonical_json.canonicalize({"a": 1})
        digest = canonical_json.payload_digest(canonical_bytes)

        started = time.monotonic()
        outcome = client.submit_job(
            "127.0.0.1", port, "job-a", canonical_bytes, digest, timeout_seconds=0.5
        )
        elapsed = time.monotonic() - started

        assert isinstance(outcome, client.SubmitHttpFailure)
        # Bounded close to the requested 0.5s deadline (with margin for slow
        # CI), not the ~4s full dribble duration a silently-ignored deadline
        # would allow.
        assert elapsed < 1.0
    finally:
        server_sock.close()
        thread.join(timeout=5.0)


def test_unexpected_status_is_not_success(stub_server) -> None:
    body = b'{"error":{"code":"simulated_server_error","message":"boom"}}'
    response = (
        b"HTTP/1.1 500 Internal Server Error\r\n"
        b"Content-Type: application/json\r\n"
        + f"Content-Length: {len(body)}\r\n\r\n".encode()
        + body
    )
    server = stub_server(response)

    outcome = _submit(server.port)

    assert isinstance(outcome, client.SubmitHttpFailure)
    assert outcome.dispatch_may_have_begun is True


def test_empty_string_remote_request_id_is_accepted(stub_server) -> None:
    # Regression test: the specification requires "a string
    # remote_request_id", not a non-empty one. `transactions.py`'s
    # corruption check for a stored successful attempt must accept exactly
    # what this client accepts, or a fresh success with an empty
    # `remote_request_id` could never be replayed (see
    # `test_stored_empty_remote_request_id_is_not_corruption` in
    # `test_transactions.py`).
    import json

    body = json.dumps(
        {
            "idempotency_key": "job-a",
            "status": "processed",
            "payload_digest": canonical_json.payload_digest(
                canonical_json.canonicalize({"a": 1})
            ),
            "remote_request_id": "",
            "replayed": False,
        }
    ).encode()
    response = (
        b"HTTP/1.1 201 Created\r\n"
        b"Content-Type: application/json\r\n"
        + f"Content-Length: {len(body)}\r\n\r\n".encode()
        + body
    )
    server = stub_server(response)

    outcome = _submit(server.port)

    assert isinstance(outcome, client.SubmitHttpSuccess)
    assert outcome.remote_request_id == ""


def test_missing_remote_request_id_is_not_success(stub_server) -> None:
    import json

    body = json.dumps(
        {
            "idempotency_key": "job-a",
            "status": "processed",
            "payload_digest": canonical_json.payload_digest(
                canonical_json.canonicalize({"a": 1})
            ),
            "replayed": False,
        }
    ).encode()
    response = (
        b"HTTP/1.1 201 Created\r\n"
        b"Content-Type: application/json\r\n"
        + f"Content-Length: {len(body)}\r\n\r\n".encode()
        + body
    )
    server = stub_server(response)

    outcome = _submit(server.port)

    assert isinstance(outcome, client.SubmitHttpFailure)
