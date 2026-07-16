"""Direct service/state-machine tests for every required preset sequence.

These tests call `SimulatorService` directly (no real HTTP, no real 11-second
sleep) with an injected fake `Waiter`, per the spec's requirement that unit
tests of preset logic must not sleep for the real timeout duration.
"""

from __future__ import annotations

import json
import threading

from boolean_maybe.simulator import canonicalize, scenario as scenario_mod
from boolean_maybe.simulator.service import SimulatorService
from boolean_maybe.simulator.waiting import Waiter


class FakeWaiter(Waiter):
    """Returns immediately, reporting whether shutdown was already requested."""

    def __init__(self) -> None:
        self.call_count = 0

    def wait(self, shutdown_event: threading.Event) -> bool:
        self.call_count += 1
        return shutdown_event.is_set()


def _error(body: dict[str, object] | None) -> dict[str, object]:
    assert body is not None
    error = body["error"]
    assert isinstance(error, dict)
    return error


def _entry(payload: dict[str, object]) -> tuple[bytes, str]:
    job_entry = canonicalize.parse_job_entry(json.dumps(payload).encode("utf-8"))
    canonical_bytes = canonicalize.canonicalize(job_entry)
    digest = canonicalize.payload_digest(canonical_bytes)
    return canonical_bytes, digest


def _plan(rules: list[dict[str, str]]) -> scenario_mod.ScenarioPlan:
    return scenario_mod.parse_scenario_plan(
        json.dumps({"version": 1, "rules": rules}).encode("utf-8")
    )


def _service(rules: list[dict[str, str]]) -> tuple[SimulatorService, FakeWaiter]:
    waiter = FakeWaiter()
    service = SimulatorService(_plan(rules), waiter, threading.Event())
    return service, waiter


# -- success ------------------------------------------------------------


def test_success_submission_and_replay_and_conflict() -> None:
    service, _ = _service([])
    canonical_a, digest_a = _entry({"a": 1})
    equivalent_a, digest_equivalent = _entry({"a": 1.0})
    canonical_b, digest_b = _entry({"a": 2})

    created = service.handle_submission("job-a", canonical_a, digest_a)
    assert created.kind == "response"
    assert created.status == 201
    assert created.body is not None
    assert created.body["replayed"] is False
    assert created.body["status"] == "processed"
    assert created.processed is True
    assert created.scenario == "success"
    assert created.ordinal == 1

    replay = service.handle_submission("job-a", equivalent_a, digest_equivalent)
    assert replay.status == 200
    assert replay.body is not None
    assert replay.body["replayed"] is True
    assert replay.body["payload_digest"] == created.body["payload_digest"]
    assert replay.body["remote_request_id"] == created.body["remote_request_id"]

    conflict = service.handle_submission("job-a", canonical_b, digest_b)
    assert conflict.status == 409
    assert conflict.body is not None
    error = conflict.body["error"]
    assert isinstance(error, dict)
    assert error["code"] == "idempotency_conflict"
    assert error["stored_payload_digest"] == digest_a
    assert error["submitted_payload_digest"] == digest_b


def test_success_reconciliation_processed_and_not_found() -> None:
    service, _ = _service([])
    canonical_bytes, digest = _entry({"a": 1})

    missing = service.handle_reconciliation("job-a")
    assert missing.status == 404
    assert missing.body == {"idempotency_key": "job-a", "status": "not_found"}
    assert missing.processed is False

    service.handle_submission("job-a", canonical_bytes, digest)
    found = service.handle_reconciliation("job-a")
    assert found.status == 200
    assert found.body is not None
    assert found.body["status"] == "processed"
    assert found.body["payload_digest"] == digest
    assert found.processed is True


# -- 500_then_success -----------------------------------------------------


def test_500_then_success() -> None:
    service, _ = _service(
        [
            {
                "operation": "submission",
                "idempotency_key": "job-a",
                "scenario": "500_then_success",
            }
        ]
    )
    canonical_bytes, digest = _entry({"a": 1})

    first = service.handle_submission("job-a", canonical_bytes, digest)
    assert first.kind == "response"
    assert first.status == 500
    assert first.body is not None
    assert _error(first.body)["code"] == "simulated_server_error"
    assert first.processed is False
    assert first.ordinal == 1

    second = service.handle_submission("job-a", canonical_bytes, digest)
    assert second.status == 201
    assert second.body is not None
    assert second.body["replayed"] is False
    assert second.ordinal == 2


# -- 429_then_success -----------------------------------------------------


def test_429_then_success() -> None:
    service, _ = _service(
        [
            {
                "operation": "submission",
                "idempotency_key": "job-a",
                "scenario": "429_then_success",
            }
        ]
    )
    canonical_bytes, digest = _entry({"a": 1})

    first = service.handle_submission("job-a", canonical_bytes, digest)
    assert first.status == 429
    assert first.headers == (("Retry-After", "1"),)
    assert first.body is not None
    assert _error(first.body)["code"] == "rate_limited"
    assert first.processed is False

    second = service.handle_submission("job-a", canonical_bytes, digest)
    assert second.status == 201


# -- connect_timeout -------------------------------------------------------


def test_connect_timeout() -> None:
    service, waiter = _service(
        [
            {
                "operation": "submission",
                "idempotency_key": "job-a",
                "scenario": "connect_timeout",
            }
        ]
    )
    canonical_bytes, digest = _entry({"a": 1})

    first = service.handle_submission("job-a", canonical_bytes, digest)
    assert first.kind == "aborted"
    assert first.processed is False
    assert waiter.call_count == 1

    second = service.handle_submission("job-a", canonical_bytes, digest)
    assert second.kind == "response"
    assert second.status == 201


# -- processed_then_disconnect ---------------------------------------------


def test_processed_then_disconnect() -> None:
    service, _ = _service(
        [
            {
                "operation": "submission",
                "idempotency_key": "job-a",
                "scenario": "processed_then_disconnect",
            }
        ]
    )
    canonical_bytes, digest = _entry({"a": 1})

    first = service.handle_submission("job-a", canonical_bytes, digest)
    assert first.kind == "aborted"
    assert first.processed is True

    replay = service.handle_submission("job-a", canonical_bytes, digest)
    assert replay.kind == "response"
    assert replay.status == 200
    assert replay.body is not None
    assert replay.body["replayed"] is True

    reconciled = service.handle_reconciliation("job-a")
    assert reconciled.status == 200
    assert reconciled.body is not None
    assert reconciled.body["status"] == "processed"


# -- processed_without_response --------------------------------------------


def test_processed_without_response() -> None:
    service, waiter = _service(
        [
            {
                "operation": "submission",
                "idempotency_key": "job-a",
                "scenario": "processed_without_response",
            }
        ]
    )
    canonical_bytes, digest = _entry({"a": 1})

    first = service.handle_submission("job-a", canonical_bytes, digest)
    assert first.kind == "aborted"
    assert first.processed is True
    assert waiter.call_count == 1

    replay = service.handle_submission("job-a", canonical_bytes, digest)
    assert replay.status == 200
    assert replay.body is not None
    assert replay.body["replayed"] is True

    reconciled = service.handle_reconciliation("job-a")
    assert reconciled.status == 200


# -- processed_then_500 -----------------------------------------------------


def test_processed_then_500() -> None:
    service, _ = _service(
        [
            {
                "operation": "submission",
                "idempotency_key": "job-a",
                "scenario": "processed_then_500",
            }
        ]
    )
    canonical_bytes, digest = _entry({"a": 1})

    first = service.handle_submission("job-a", canonical_bytes, digest)
    assert first.kind == "response"
    assert first.status == 500
    assert first.processed is True

    replay = service.handle_submission("job-a", canonical_bytes, digest)
    assert replay.status == 200
    assert replay.body is not None
    assert replay.body["replayed"] is True

    reconciled = service.handle_reconciliation("job-a")
    assert reconciled.status == 200
    assert reconciled.body is not None
    assert reconciled.body["status"] == "processed"


def test_processed_then_500_conflict_still_takes_precedence_on_later_ordinal() -> None:
    service, _ = _service(
        [
            {
                "operation": "submission",
                "idempotency_key": "job-a",
                "scenario": "processed_then_500",
            }
        ]
    )
    canonical_a, digest_a = _entry({"a": 1})
    canonical_b, digest_b = _entry({"a": 2})

    service.handle_submission(
        "job-a", canonical_a, digest_a
    )  # ordinal 1: processed + 500
    conflict = service.handle_submission("job-a", canonical_b, digest_b)  # ordinal 2

    assert conflict.status == 409
    assert conflict.body is not None
    conflict_error = _error(conflict.body)
    assert conflict_error["stored_payload_digest"] == digest_a
    assert conflict_error["submitted_payload_digest"] == digest_b


# -- duplicate_remote_request_id -------------------------------------------


def test_duplicate_remote_request_id_across_distinct_keys() -> None:
    service, _ = _service(
        [
            {
                "operation": "submission",
                "idempotency_key": "*",
                "scenario": "duplicate_remote_request_id",
            }
        ]
    )
    canonical_a, digest_a = _entry({"a": 1})
    canonical_b, digest_b = _entry({"b": 2})

    first = service.handle_submission("job-a", canonical_a, digest_a)
    second = service.handle_submission("job-b", canonical_b, digest_b)

    assert first.body is not None
    assert second.body is not None
    assert first.body["remote_request_id"] == "remote-duplicate"
    assert second.body["remote_request_id"] == "remote-duplicate"
    assert first.body["idempotency_key"] != second.body["idempotency_key"]
    assert first.body["payload_digest"] != second.body["payload_digest"]

    # Otherwise normal submission/replay behavior applies.
    replay = service.handle_submission("job-a", canonical_a, digest_a)
    assert replay.status == 200
    assert replay.body is not None
    assert replay.body["remote_request_id"] == "remote-duplicate"


# -- reconciliation_timeout -------------------------------------------------


def test_reconciliation_timeout_does_not_mutate() -> None:
    service, waiter = _service(
        [
            {
                "operation": "reconciliation",
                "idempotency_key": "job-a",
                "scenario": "reconciliation_timeout",
            }
        ]
    )

    first = service.handle_reconciliation("job-a")
    assert first.kind == "aborted"
    assert first.processed is False
    assert waiter.call_count == 1

    second = service.handle_reconciliation("job-a")
    assert second.kind == "response"
    assert second.status == 404


# -- always_500 --------------------------------------------------------


def test_always_500_submission_every_request() -> None:
    service, _ = _service(
        [
            {
                "operation": "submission",
                "idempotency_key": "job-a",
                "scenario": "always_500",
            }
        ]
    )
    canonical_bytes, digest = _entry({"a": 1})

    for expected_ordinal in (1, 2, 3):
        result = service.handle_submission("job-a", canonical_bytes, digest)
        assert result.status == 500
        assert result.processed is False
        assert result.ordinal == expected_ordinal


def test_always_500_reconciliation_hides_existing_processed_record() -> None:
    service, _ = _service(
        [
            {
                "operation": "reconciliation",
                "idempotency_key": "job-a",
                "scenario": "always_500",
            }
        ]
    )
    canonical_bytes, digest = _entry({"a": 1})

    submitted = service.handle_submission("job-a", canonical_bytes, digest)
    assert submitted.status == 201

    hidden = service.handle_reconciliation("job-a")
    assert hidden.status == 500
    assert (
        hidden.processed is True
    )  # state fact is known even though hidden from the client


# -- exact/wildcard precedence and independent ordinals --------------------


def test_wildcard_and_exact_precedence_with_independent_ordinals() -> None:
    service, _ = _service(
        [
            {
                "operation": "submission",
                "idempotency_key": "*",
                "scenario": "always_500",
            },
            {
                "operation": "submission",
                "idempotency_key": "job-a",
                "scenario": "success",
            },
        ]
    )
    canonical_a, digest_a = _entry({"a": 1})
    canonical_c, digest_c = _entry({"c": 1})

    a_result = service.handle_submission("job-a", canonical_a, digest_a)
    assert a_result.status == 201  # exact rule wins

    c_result = service.handle_submission("job-c", canonical_c, digest_c)
    assert c_result.status == 500  # falls back to wildcard
    assert c_result.ordinal == 1

    a_result_2 = service.handle_submission("job-a", canonical_a, digest_a)
    assert a_result_2.ordinal == 2  # independent ordinal sequence from job-c
