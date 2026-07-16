"""Real loopback HTTP tests: status codes, headers, schemas, and validation order."""

from __future__ import annotations

import json


def _body(**overrides: object) -> bytes:
    payload: dict[str, object] = {"work": "example"}
    payload.update(overrides)
    return json.dumps(payload).encode("utf-8")


def _post_headers(idempotency_key: str) -> dict[str, str]:
    return {"Content-Type": "application/json", "Idempotency-Key": idempotency_key}


def test_submission_success_schema_and_headers(simulator) -> None:
    body = _body()
    status, parsed, headers = simulator.request(
        "POST",
        "/jobs",
        body=body,
        headers={**_post_headers("job-a"), "Content-Length": str(len(body))},
    )
    assert status == 201
    assert parsed is not None
    assert set(parsed.keys()) == {
        "idempotency_key",
        "status",
        "payload_digest",
        "remote_request_id",
        "replayed",
    }
    assert parsed["idempotency_key"] == "job-a"
    assert parsed["status"] == "processed"
    assert parsed["replayed"] is False
    assert parsed["payload_digest"].startswith("sha256:")
    assert parsed["remote_request_id"].startswith("remote-")
    assert headers["Content-Type"] == "application/json"
    assert int(headers["Content-Length"]) > 0
    assert headers["Connection"] == "close"


def test_submission_replay_schema(simulator) -> None:
    body = _body()
    equivalent_body = b'{ "work" :  "example" }'
    simulator.request("POST", "/jobs", body=body, headers=_post_headers("job-a"))
    status, parsed, _ = simulator.request(
        "POST", "/jobs", body=equivalent_body, headers=_post_headers("job-a")
    )

    assert status == 200
    assert parsed is not None
    assert set(parsed.keys()) == {
        "idempotency_key",
        "status",
        "payload_digest",
        "remote_request_id",
        "replayed",
    }
    assert parsed["replayed"] is True


def test_submission_conflict_schema(simulator) -> None:
    simulator.request(
        "POST", "/jobs", body=_body(work="one"), headers=_post_headers("job-a")
    )
    status, parsed, _ = simulator.request(
        "POST", "/jobs", body=_body(work="two"), headers=_post_headers("job-a")
    )

    assert status == 409
    assert parsed is not None
    error = parsed["error"]
    assert set(error.keys()) == {
        "code",
        "message",
        "stored_payload_digest",
        "submitted_payload_digest",
    }
    assert error["code"] == "idempotency_conflict"


def test_reconciliation_processed_and_not_found_schema(simulator) -> None:
    status, parsed, _ = simulator.request("GET", "/jobs/by-idempotency-key/job-missing")
    assert status == 404
    assert parsed == {"idempotency_key": "job-missing", "status": "not_found"}

    simulator.request("POST", "/jobs", body=_body(), headers=_post_headers("job-a"))
    status, parsed, _ = simulator.request("GET", "/jobs/by-idempotency-key/job-a")
    assert status == 200
    assert parsed is not None
    assert set(parsed.keys()) == {
        "idempotency_key",
        "status",
        "payload_digest",
        "remote_request_id",
    }
    assert parsed["status"] == "processed"


# -- Routing ---------------------------------------------------------------


def test_unknown_route_returns_404(simulator) -> None:
    status, parsed, _ = simulator.request("GET", "/unknown")
    assert status == 404
    assert parsed is not None
    assert parsed["error"]["code"] == "route_not_found"


def test_get_on_jobs_route_returns_405_with_allow_header(simulator) -> None:
    status, parsed, headers = simulator.request("GET", "/jobs")
    assert status == 405
    assert parsed is not None
    assert parsed["error"]["code"] == "method_not_allowed"
    assert headers["Allow"] == "POST"


def test_post_on_reconciliation_route_returns_405_with_allow_header(simulator) -> None:
    status, parsed, headers = simulator.request(
        "POST", "/jobs/by-idempotency-key/job-a"
    )
    assert status == 405
    assert headers["Allow"] == "GET"


def test_extra_path_segment_on_reconciliation_is_unknown_route(simulator) -> None:
    status, parsed, _ = simulator.request("GET", "/jobs/by-idempotency-key/job-a/extra")
    assert status == 404
    assert parsed["error"]["code"] == "route_not_found"


# -- Submission framing/validation ------------------------------------------


def test_missing_idempotency_key_header(simulator) -> None:
    status, parsed, _ = simulator.request(
        "POST", "/jobs", body=_body(), headers={"Content-Type": "application/json"}
    )
    assert status == 400
    assert parsed["error"]["code"] == "invalid_idempotency_key"


def test_invalid_idempotency_key_grammar(simulator) -> None:
    status, parsed, _ = simulator.request(
        "POST", "/jobs", body=_body(), headers=_post_headers("job a")
    )
    assert status == 400
    assert parsed["error"]["code"] == "invalid_idempotency_key"


def test_unsupported_content_type(simulator) -> None:
    status, parsed, _ = simulator.request(
        "POST",
        "/jobs",
        body=_body(),
        headers={"Content-Type": "text/plain", "Idempotency-Key": "job-a"},
    )
    assert status == 415
    assert parsed["error"]["code"] == "unsupported_media_type"


def test_content_type_with_charset_utf8_is_accepted(simulator) -> None:
    status, _, _ = simulator.request(
        "POST",
        "/jobs",
        body=_body(),
        headers={
            "Content-Type": "application/json; charset=utf-8",
            "Idempotency-Key": "job-a",
        },
    )
    assert status == 201


def test_oversized_body_returns_413(simulator) -> None:
    huge_value = "x" * (1024 * 1024 + 1)
    body = json.dumps({"work": huge_value}).encode("utf-8")
    status, parsed, _ = simulator.request(
        "POST", "/jobs", body=body, headers=_post_headers("job-a")
    )
    assert status == 413
    assert parsed["error"]["code"] == "payload_too_large"


def test_malformed_json_returns_400_invalid_request(simulator) -> None:
    status, parsed, _ = simulator.request(
        "POST", "/jobs", body=b"{not json", headers=_post_headers("job-a")
    )
    assert status == 400
    assert parsed["error"]["code"] == "invalid_request"


def test_non_object_root_returns_400(simulator) -> None:
    status, parsed, _ = simulator.request(
        "POST", "/jobs", body=b"[1,2,3]", headers=_post_headers("job-a")
    )
    assert status == 400
    assert parsed["error"]["code"] == "invalid_request"


def test_duplicate_member_returns_400(simulator) -> None:
    status, parsed, _ = simulator.request(
        "POST", "/jobs", body=b'{"a":1,"a":2}', headers=_post_headers("job-a")
    )
    assert status == 400
    assert parsed["error"]["code"] == "invalid_request"


def test_error_body_does_not_echo_raw_input(simulator) -> None:
    secret_marker = "super-secret-payload-marker"
    status, parsed, _ = simulator.request(
        "POST",
        "/jobs",
        body=f'{{"a": "{secret_marker}"'.encode(),
        headers=_post_headers("job-a"),
    )
    assert status == 400
    assert secret_marker not in json.dumps(parsed)


# -- Reconciliation framing/validation ---------------------------------------


def test_reconciliation_invalid_key_returns_400(simulator) -> None:
    status, parsed, _ = simulator.request("GET", "/jobs/by-idempotency-key/job%2Fa")
    assert status == 400
    assert parsed["error"]["code"] == "invalid_idempotency_key"


def test_reconciliation_wildcard_key_is_rejected(simulator) -> None:
    status, parsed, _ = simulator.request("GET", "/jobs/by-idempotency-key/*")
    assert status == 400
    assert parsed["error"]["code"] == "invalid_idempotency_key"


def test_reconciliation_percent_encoded_key_round_trips(simulator) -> None:
    simulator.request("POST", "/jobs", body=_body(), headers=_post_headers("job.a"))
    status, parsed, _ = simulator.request("GET", "/jobs/by-idempotency-key/job%2Ea")
    assert status == 200
    assert parsed["idempotency_key"] == "job.a"


# -- IPv6 loopback -----------------------------------------------------------


def _ipv6_loopback_available() -> bool:
    import socket

    try:
        with socket.socket(socket.AF_INET6, socket.SOCK_STREAM) as probe:
            probe.bind(("::1", 0))
        return True
    except OSError:
        return False


def test_ipv6_loopback_bind_and_request(make_simulator) -> None:
    if not _ipv6_loopback_available():
        import pytest

        pytest.skip("IPv6 loopback (::1) is not available in this environment")

    sim = make_simulator(host="::1")

    status, parsed, _ = sim.request(
        "POST", "/jobs", body=_body(), headers=_post_headers("job-ipv6")
    )
    assert status == 201
    assert parsed is not None
    assert parsed["idempotency_key"] == "job-ipv6"

    status, parsed, _ = sim.request("GET", "/jobs/by-idempotency-key/job-ipv6")
    assert status == 200
    assert parsed is not None
    assert parsed["status"] == "processed"
