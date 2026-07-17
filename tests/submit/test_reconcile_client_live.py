"""Reconciliation HTTP client tests against a real loopback simulator instance."""

from __future__ import annotations

from boolean_maybe import canonical_json
from boolean_maybe.external import client, reconcile


def test_match_for_a_processed_key(live_simulator) -> None:
    canonical_bytes = canonical_json.canonicalize({"a": 1})
    digest = canonical_json.payload_digest(canonical_bytes)
    submitted = client.submit_job(
        live_simulator.host, live_simulator.port, "job-a", canonical_bytes, digest
    )
    assert isinstance(submitted, client.SubmitHttpSuccess)

    outcome = reconcile.reconcile_job(
        live_simulator.host, live_simulator.port, "job-a", digest
    )

    assert isinstance(outcome, reconcile.ReconcileMatch)
    assert outcome.http_status == 200
    assert outcome.remote_request_id == submitted.remote_request_id


def test_conflict_for_a_processed_key_with_different_digest(live_simulator) -> None:
    canonical_bytes = canonical_json.canonicalize({"a": 1})
    digest = canonical_json.payload_digest(canonical_bytes)
    client.submit_job(
        live_simulator.host, live_simulator.port, "job-a", canonical_bytes, digest
    )

    other_digest = "sha256:" + "0" * 64
    outcome = reconcile.reconcile_job(
        live_simulator.host, live_simulator.port, "job-a", other_digest
    )

    assert outcome == reconcile.ReconcileConflict()


def test_not_found_for_an_unknown_key(live_simulator) -> None:
    digest = canonical_json.payload_digest(canonical_json.canonicalize({"a": 1}))

    outcome = reconcile.reconcile_job(
        live_simulator.host, live_simulator.port, "job-never-submitted", digest
    )

    assert outcome == reconcile.ReconcileNotFound()


def test_always_500_reconciliation_is_server_error(make_live_simulator) -> None:
    # The simulator's required preset catalog has no reconciliation-scoped
    # `429` scenario (`429_then_success` supports submission only); rate-
    # limited-reconciliation classification is covered against a raw stub
    # response in `test_reconcile_client_stub.py` instead.
    running = make_live_simulator(
        {
            "version": 1,
            "rules": [
                {
                    "operation": "reconciliation",
                    "idempotency_key": "job-a",
                    "scenario": "always_500",
                }
            ],
        }
    )
    digest = canonical_json.payload_digest(canonical_json.canonicalize({"a": 1}))

    outcome = reconcile.reconcile_job(running.host, running.port, "job-a", digest)

    assert outcome == reconcile.ReconcileServerError(http_status=500)


def test_reconciliation_timeout_is_transport_failure(make_live_simulator) -> None:
    running = make_live_simulator(
        {
            "version": 1,
            "rules": [
                {
                    "operation": "reconciliation",
                    "idempotency_key": "job-a",
                    "scenario": "reconciliation_timeout",
                }
            ],
        }
    )
    digest = canonical_json.payload_digest(canonical_json.canonicalize({"a": 1}))

    outcome = reconcile.reconcile_job(
        running.host, running.port, "job-a", digest, timeout_seconds=0.5
    )

    assert isinstance(outcome, reconcile.ReconcileTransportFailure)


def test_connection_refused_is_transport_failure(unused_tcp_port: int) -> None:
    digest = canonical_json.payload_digest(canonical_json.canonicalize({"a": 1}))

    outcome = reconcile.reconcile_job("127.0.0.1", unused_tcp_port, "job-a", digest)

    assert isinstance(outcome, reconcile.ReconcileTransportFailure)


def test_key_is_percent_encoded_on_the_wire(live_simulator) -> None:
    # The accepted key alphabet is already URL-unreserved, but the client
    # must still encode it per the approved specification. A key using
    # every accepted punctuation character should round-trip correctly.
    key = "job.a_b~c-1"
    canonical_bytes = canonical_json.canonicalize({"a": 1})
    digest = canonical_json.payload_digest(canonical_bytes)
    client.submit_job(
        live_simulator.host, live_simulator.port, key, canonical_bytes, digest
    )

    outcome = reconcile.reconcile_job(
        live_simulator.host, live_simulator.port, key, digest
    )

    assert isinstance(outcome, reconcile.ReconcileMatch)
