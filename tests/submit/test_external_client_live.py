"""External HTTP client tests against a real loopback simulator instance."""

from __future__ import annotations

from boolean_maybe import canonical_json
from boolean_maybe.external import client


def test_success_201_for_a_new_key(live_simulator) -> None:
    canonical_bytes = canonical_json.canonicalize({"a": 1})
    digest = canonical_json.payload_digest(canonical_bytes)

    outcome = client.submit_job(
        live_simulator.host, live_simulator.port, "job-a", canonical_bytes, digest
    )

    assert isinstance(outcome, client.SubmitHttpSuccess)
    assert outcome.http_status == 201
    assert outcome.remote_request_id


def test_replay_200_for_an_equivalent_resubmission(live_simulator) -> None:
    canonical_bytes = canonical_json.canonicalize({"a": 1})
    digest = canonical_json.payload_digest(canonical_bytes)

    first = client.submit_job(
        live_simulator.host, live_simulator.port, "job-a", canonical_bytes, digest
    )
    assert isinstance(first, client.SubmitHttpSuccess)

    second = client.submit_job(
        live_simulator.host, live_simulator.port, "job-a", canonical_bytes, digest
    )
    assert isinstance(second, client.SubmitHttpSuccess)
    assert second.http_status == 200
    assert second.remote_request_id == first.remote_request_id


def test_non_equivalent_conflict_is_not_success(live_simulator) -> None:
    first_bytes = canonical_json.canonicalize({"a": 1})
    first_digest = canonical_json.payload_digest(first_bytes)
    client.submit_job(
        live_simulator.host, live_simulator.port, "job-a", first_bytes, first_digest
    )

    second_bytes = canonical_json.canonicalize({"a": 2})
    second_digest = canonical_json.payload_digest(second_bytes)
    outcome = client.submit_job(
        live_simulator.host,
        live_simulator.port,
        "job-a",
        second_bytes,
        second_digest,
    )

    assert isinstance(outcome, client.SubmitHttpFailure)
    assert outcome.dispatch_may_have_begun is True


def test_digest_mismatch_against_authoritative_evidence_is_not_success(
    live_simulator,
) -> None:
    canonical_bytes = canonical_json.canonicalize({"a": 1})
    real_digest = canonical_json.payload_digest(canonical_bytes)

    outcome = client.submit_job(
        live_simulator.host,
        live_simulator.port,
        "job-a",
        canonical_bytes,
        expected_digest="sha256:" + "0" * 64,
    )

    assert isinstance(outcome, client.SubmitHttpFailure)
    assert real_digest != "sha256:" + "0" * 64


def test_connection_refused_is_proven_not_sent(unused_tcp_port: int) -> None:
    canonical_bytes = canonical_json.canonicalize({"a": 1})
    digest = canonical_json.payload_digest(canonical_bytes)

    outcome = client.submit_job(
        "127.0.0.1", unused_tcp_port, "job-a", canonical_bytes, digest
    )

    assert isinstance(outcome, client.SubmitHttpFailure)
    assert outcome.dispatch_may_have_begun is False


def test_rate_limited_response_is_not_success(make_live_simulator) -> None:
    running = make_live_simulator(
        {
            "version": 1,
            "rules": [
                {
                    "operation": "submission",
                    "idempotency_key": "job-a",
                    "scenario": "429_then_success",
                }
            ],
        }
    )
    canonical_bytes = canonical_json.canonicalize({"a": 1})
    digest = canonical_json.payload_digest(canonical_bytes)

    outcome = client.submit_job(
        running.host, running.port, "job-a", canonical_bytes, digest
    )

    assert isinstance(outcome, client.SubmitHttpFailure)
    assert outcome.dispatch_may_have_begun is True
