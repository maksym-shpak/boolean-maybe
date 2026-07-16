"""Raw-socket framing tests for cases http.client will not let us construct:
repeated headers, Transfer-Encoding, and malformed Content-Length values.
"""

from __future__ import annotations

import json


def build_raw_request(
    method: str, path: str, header_lines: list[str], body: bytes
) -> bytes:
    lines = [f"{method} {path} HTTP/1.1", "Host: 127.0.0.1", *header_lines]
    head = "\r\n".join(lines) + "\r\n\r\n"
    return head.encode("ascii") + body


def parse_raw_response(raw: bytes) -> tuple[int, dict[str, str], bytes]:
    head, _, body = raw.partition(b"\r\n\r\n")
    lines = head.decode("iso-8859-1").split("\r\n")
    status = int(lines[0].split(" ")[1])
    headers: dict[str, str] = {}
    for line in lines[1:]:
        if ":" in line:
            name, _, value = line.partition(":")
            headers[name.strip()] = value.strip()
    return status, headers, body


def _json_body(payload: dict[str, object]) -> bytes:
    return json.dumps(payload).encode("utf-8")


def test_transfer_encoding_is_rejected_on_submission(simulator) -> None:
    body = _json_body({"a": 1})
    raw = build_raw_request(
        "POST",
        "/jobs",
        [
            "Content-Type: application/json",
            "Idempotency-Key: job-a",
            "Transfer-Encoding: chunked",
            f"Content-Length: {len(body)}",
        ],
        body,
    )
    status, _, body_bytes = parse_raw_response(simulator.raw_request(raw))
    assert status == 400
    assert json.loads(body_bytes)["error"]["code"] == "invalid_request"


def test_repeated_content_length_is_rejected(simulator) -> None:
    body = _json_body({"a": 1})
    raw = build_raw_request(
        "POST",
        "/jobs",
        [
            "Content-Type: application/json",
            "Idempotency-Key: job-a",
            f"Content-Length: {len(body)}",
            f"Content-Length: {len(body)}",
        ],
        body,
    )
    status, _, body_bytes = parse_raw_response(simulator.raw_request(raw))
    assert status == 400
    assert json.loads(body_bytes)["error"]["code"] == "invalid_request"


def test_missing_content_length_is_rejected(simulator) -> None:
    raw = build_raw_request(
        "POST",
        "/jobs",
        ["Content-Type: application/json", "Idempotency-Key: job-a"],
        b"",
    )
    status, _, body_bytes = parse_raw_response(simulator.raw_request(raw))
    assert status == 400
    assert json.loads(body_bytes)["error"]["code"] == "invalid_request"


def test_negative_content_length_is_rejected(simulator) -> None:
    raw = build_raw_request(
        "POST",
        "/jobs",
        [
            "Content-Type: application/json",
            "Idempotency-Key: job-a",
            "Content-Length: -1",
        ],
        b"",
    )
    status, _, body_bytes = parse_raw_response(simulator.raw_request(raw))
    assert status == 400
    assert json.loads(body_bytes)["error"]["code"] == "invalid_request"


def test_non_decimal_content_length_is_rejected(simulator) -> None:
    raw = build_raw_request(
        "POST",
        "/jobs",
        [
            "Content-Type: application/json",
            "Idempotency-Key: job-a",
            "Content-Length: 12a",
        ],
        b"",
    )
    status, _, body_bytes = parse_raw_response(simulator.raw_request(raw))
    assert status == 400
    assert json.loads(body_bytes)["error"]["code"] == "invalid_request"


def test_duplicate_content_type_is_rejected(simulator) -> None:
    body = _json_body({"a": 1})
    raw = build_raw_request(
        "POST",
        "/jobs",
        [
            "Content-Type: application/json",
            "Content-Type: application/json",
            "Idempotency-Key: job-a",
            f"Content-Length: {len(body)}",
        ],
        body,
    )
    status, _, body_bytes = parse_raw_response(simulator.raw_request(raw))
    assert status == 415
    assert json.loads(body_bytes)["error"]["code"] == "unsupported_media_type"


def test_duplicate_idempotency_key_header_is_rejected(simulator) -> None:
    body = _json_body({"a": 1})
    raw = build_raw_request(
        "POST",
        "/jobs",
        [
            "Content-Type: application/json",
            "Idempotency-Key: job-a",
            "Idempotency-Key: job-b",
            f"Content-Length: {len(body)}",
        ],
        body,
    )
    status, _, body_bytes = parse_raw_response(simulator.raw_request(raw))
    assert status == 400
    assert json.loads(body_bytes)["error"]["code"] == "invalid_idempotency_key"


def test_get_with_transfer_encoding_is_rejected(simulator) -> None:
    raw = build_raw_request(
        "GET", "/jobs/by-idempotency-key/job-a", ["Transfer-Encoding: chunked"], b""
    )
    status, _, body_bytes = parse_raw_response(simulator.raw_request(raw))
    assert status == 400
    assert json.loads(body_bytes)["error"]["code"] == "invalid_request"


def test_get_with_nonzero_content_length_is_rejected(simulator) -> None:
    raw = build_raw_request(
        "GET", "/jobs/by-idempotency-key/job-a", ["Content-Length: 5"], b""
    )
    status, _, body_bytes = parse_raw_response(simulator.raw_request(raw))
    assert status == 400
    assert json.loads(body_bytes)["error"]["code"] == "invalid_request"


def test_get_with_zero_content_length_is_accepted(simulator) -> None:
    raw = build_raw_request(
        "GET", "/jobs/by-idempotency-key/job-missing", ["Content-Length: 0"], b""
    )
    status, _, body_bytes = parse_raw_response(simulator.raw_request(raw))
    assert status == 404
    assert json.loads(body_bytes)["status"] == "not_found"


def test_content_length_header_matches_actual_body_bytes(simulator) -> None:
    body = _json_body({"a": 1})
    raw = build_raw_request(
        "POST",
        "/jobs",
        [
            "Content-Type: application/json",
            "Idempotency-Key: job-a",
            f"Content-Length: {len(body)}",
        ],
        body,
    )
    status, headers, body_bytes = parse_raw_response(simulator.raw_request(raw))
    assert status == 201
    assert int(headers["Content-Length"]) == len(body_bytes)


def test_absurdly_long_content_length_returns_413_on_submission(simulator) -> None:
    # A syntactically valid decimal Content-Length with far more digits
    # than Python's default int-string conversion limit (4300) must still
    # be classified without ever calling int() on the raw digit string.
    huge_digits = "9" * 5000
    body = _json_body({"a": 1})
    raw = build_raw_request(
        "POST",
        "/jobs",
        [
            "Content-Type: application/json",
            "Idempotency-Key: job-a",
            f"Content-Length: {huge_digits}",
        ],
        body,
    )
    status, _, body_bytes = parse_raw_response(simulator.raw_request(raw))
    assert status == 413
    assert json.loads(body_bytes)["error"]["code"] == "payload_too_large"

    events = simulator.log_stream.events()
    assert events[-1]["event"] == "request_completed"
    assert events[-1]["status"] == 413


def test_absurdly_long_content_length_returns_400_on_reconciliation(simulator) -> None:
    huge_digits = "9" * 5000
    raw = build_raw_request(
        "GET", "/jobs/by-idempotency-key/job-a", [f"Content-Length: {huge_digits}"], b""
    )
    status, _, body_bytes = parse_raw_response(simulator.raw_request(raw))
    assert status == 400
    assert json.loads(body_bytes)["error"]["code"] == "invalid_request"

    events = simulator.log_stream.events()
    assert events[-1]["event"] == "request_completed"
    assert events[-1]["status"] == 400


def test_long_zero_prefixed_content_length_is_accepted_on_submission(
    simulator,
) -> None:
    # "000...002" has thousands of characters but is mathematically 2, the
    # exact length of the body below. Leading zeros must not make an
    # otherwise-valid, small Content-Length look unbounded.
    body = b"{}"
    content_length_header = "0" * 4998 + "02"
    assert (content_length_header.lstrip("0") or "0") == str(len(body))
    raw = build_raw_request(
        "POST",
        "/jobs",
        [
            "Content-Type: application/json",
            "Idempotency-Key: job-zero-prefixed",
            f"Content-Length: {content_length_header}",
        ],
        body,
    )
    status, _, body_bytes = parse_raw_response(simulator.raw_request(raw))
    assert status == 201
    parsed = json.loads(body_bytes)
    assert parsed["idempotency_key"] == "job-zero-prefixed"
    assert parsed["replayed"] is False


def test_long_all_zero_content_length_is_accepted_on_reconciliation(
    simulator,
) -> None:
    # "000...000" has thousands of characters but is mathematically 0,
    # which GET must accept exactly like an absent or literal "0" header.
    content_length_header = "0" * 5000
    assert (content_length_header.lstrip("0") or "0") == "0"
    raw = build_raw_request(
        "GET",
        "/jobs/by-idempotency-key/job-missing",
        [f"Content-Length: {content_length_header}"],
        b"",
    )
    status, _, body_bytes = parse_raw_response(simulator.raw_request(raw))
    assert status == 404
    assert json.loads(body_bytes)["status"] == "not_found"
