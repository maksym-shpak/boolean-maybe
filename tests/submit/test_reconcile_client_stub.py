"""Reconciliation HTTP client tests against a minimal raw stub server.

These exercise responses a well-behaved simulator never produces (oversized
body, malformed JSON, wrong field set) without needing to alter the real
simulator's behavior.
"""

from __future__ import annotations

import socket
import threading
from collections.abc import Iterator

import pytest

from boolean_maybe.external import reconcile


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
            _read_request_headers(conn)
            conn.sendall(self._response_bytes)
        except OSError:
            pass
        finally:
            conn.close()

    def close(self) -> None:
        self._sock.close()
        self._thread.join(timeout=5.0)


def _read_request_headers(conn: socket.socket) -> bytes:
    buffer = b""
    while b"\r\n\r\n" not in buffer:
        chunk = conn.recv(4096)
        if not chunk:
            return buffer
        buffer += chunk
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


def _reconcile(port: int) -> reconcile.ReconcileHttpOutcome:
    return reconcile.reconcile_job(
        "127.0.0.1", port, "job-a", "sha256:" + "a" * 64, timeout_seconds=3.0
    )


def test_oversized_response_body_is_protocol_uncertain(stub_server) -> None:
    oversized_body = b'{"pad":"' + (b"x" * (70 * 1024)) + b'"}'
    response = (
        b"HTTP/1.1 200 OK\r\n"
        b"Content-Type: application/json\r\n"
        + f"Content-Length: {len(oversized_body)}\r\n\r\n".encode()
        + oversized_body
    )
    server = stub_server(response)

    outcome = _reconcile(server.port)

    assert isinstance(outcome, reconcile.ReconcileProtocolUncertain)


def test_wrong_content_type_is_protocol_uncertain(stub_server) -> None:
    body = (
        b'{"idempotency_key":"job-a","status":"processed","payload_digest":"sha256:'
        + (b"a" * 64)
        + b'","remote_request_id":"remote-1"}'
    )
    response = (
        b"HTTP/1.1 200 OK\r\n"
        b"Content-Type: text/plain\r\n"
        + f"Content-Length: {len(body)}\r\n\r\n".encode()
        + body
    )
    server = stub_server(response)

    outcome = _reconcile(server.port)

    assert isinstance(outcome, reconcile.ReconcileProtocolUncertain)


def test_malformed_json_body_is_protocol_uncertain(stub_server) -> None:
    body = b"not json"
    response = (
        b"HTTP/1.1 200 OK\r\n"
        b"Content-Type: application/json\r\n"
        + f"Content-Length: {len(body)}\r\n\r\n".encode()
        + body
    )
    server = stub_server(response)

    outcome = _reconcile(server.port)

    assert isinstance(outcome, reconcile.ReconcileProtocolUncertain)


def test_extra_field_on_processed_response_is_protocol_uncertain(stub_server) -> None:
    body = (
        b'{"idempotency_key":"job-a","status":"processed","payload_digest":"sha256:'
        + b"a" * 64
        + b'","remote_request_id":"remote-1","unexpected":"x"}'
    )
    response = (
        b"HTTP/1.1 200 OK\r\n"
        b"Content-Type: application/json\r\n"
        + f"Content-Length: {len(body)}\r\n\r\n".encode()
        + body
    )
    server = stub_server(response)

    outcome = _reconcile(server.port)

    assert isinstance(outcome, reconcile.ReconcileProtocolUncertain)


def test_wrong_idempotency_key_on_processed_response_is_protocol_uncertain(
    stub_server,
) -> None:
    body = (
        b'{"idempotency_key":"job-other","status":"processed","payload_digest":"sha256:'
        + b"a" * 64
        + b'","remote_request_id":"remote-1"}'
    )
    response = (
        b"HTTP/1.1 200 OK\r\n"
        b"Content-Type: application/json\r\n"
        + f"Content-Length: {len(body)}\r\n\r\n".encode()
        + body
    )
    server = stub_server(response)

    outcome = _reconcile(server.port)

    assert isinstance(outcome, reconcile.ReconcileProtocolUncertain)


def test_extra_field_on_not_found_response_is_protocol_uncertain(stub_server) -> None:
    body = b'{"idempotency_key":"job-a","status":"not_found","unexpected":"x"}'
    response = (
        b"HTTP/1.1 404 Not Found\r\n"
        b"Content-Type: application/json\r\n"
        + f"Content-Length: {len(body)}\r\n\r\n".encode()
        + body
    )
    server = stub_server(response)

    outcome = _reconcile(server.port)

    assert isinstance(outcome, reconcile.ReconcileProtocolUncertain)


def test_delivered_500_is_server_error_regardless_of_body(stub_server) -> None:
    body = b"not even json"
    response = (
        b"HTTP/1.1 500 Internal Server Error\r\n"
        b"Content-Type: text/plain\r\n"
        + f"Content-Length: {len(body)}\r\n\r\n".encode()
        + body
    )
    server = stub_server(response)

    outcome = _reconcile(server.port)

    assert outcome == reconcile.ReconcileServerError(http_status=500)


def test_redirect_status_is_protocol_uncertain(stub_server) -> None:
    response = b"HTTP/1.1 302 Found\r\nLocation: http://127.0.0.1/elsewhere\r\nContent-Length: 0\r\n\r\n"
    server = stub_server(response)

    outcome = _reconcile(server.port)

    assert isinstance(outcome, reconcile.ReconcileProtocolUncertain)


def test_malformed_429_body_is_protocol_uncertain(stub_server) -> None:
    body = b"not a well-formed error envelope"
    response = (
        b"HTTP/1.1 429 Too Many Requests\r\n"
        b"Content-Type: application/json\r\n"
        + f"Content-Length: {len(body)}\r\n\r\n".encode()
        + body
    )
    server = stub_server(response)

    outcome = _reconcile(server.port)

    assert isinstance(outcome, reconcile.ReconcileProtocolUncertain)


def test_well_formed_429_body_with_retry_after_is_rate_limited(stub_server) -> None:
    body = b'{"error":{"code":"rate_limited","message":"slow down"}}'
    response = (
        b"HTTP/1.1 429 Too Many Requests\r\n"
        b"Content-Type: application/json\r\n"
        b"Retry-After: 2\r\n" + f"Content-Length: {len(body)}\r\n\r\n".encode() + body
    )
    server = stub_server(response)

    outcome = _reconcile(server.port)

    assert outcome == reconcile.ReconcileRateLimited(retry_after_values=("2",))
