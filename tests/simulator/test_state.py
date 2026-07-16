"""KeyCoordinator: per-key locks, ordinals, and processed-record storage."""

from __future__ import annotations

from boolean_maybe.simulator.state import KeyCoordinator


def test_ordinals_increment_per_operation_and_key() -> None:
    coordinator = KeyCoordinator()
    assert coordinator.next_ordinal("submission", "job-a") == 1
    assert coordinator.next_ordinal("submission", "job-a") == 2
    assert coordinator.next_ordinal("submission", "job-b") == 1
    assert coordinator.next_ordinal("reconciliation", "job-a") == 1


def test_lock_for_returns_same_lock_object_for_same_key() -> None:
    coordinator = KeyCoordinator()
    assert coordinator.lock_for("job-a") is coordinator.lock_for("job-a")
    assert coordinator.lock_for("job-a") is not coordinator.lock_for("job-b")


def test_get_record_absent_by_default() -> None:
    coordinator = KeyCoordinator()
    assert coordinator.get_record("job-a") is None


def test_create_record_is_visible_immediately() -> None:
    coordinator = KeyCoordinator()
    record = coordinator.create_record(
        "job-a", b'{"a":1}', "sha256:deadbeef", duplicate_remote_id=False
    )
    assert coordinator.get_record("job-a") is record
    assert record.remote_request_id.startswith("remote-")
    assert record.remote_request_id != "remote-duplicate"


def test_create_record_with_duplicate_remote_id_flag() -> None:
    coordinator = KeyCoordinator()
    first = coordinator.create_record(
        "job-a", b'{"a":1}', "sha256:aaaa", duplicate_remote_id=True
    )
    second = coordinator.create_record(
        "job-b", b'{"b":1}', "sha256:bbbb", duplicate_remote_id=True
    )
    assert first.remote_request_id == "remote-duplicate"
    assert second.remote_request_id == "remote-duplicate"
    assert first.idempotency_key != second.idempotency_key
    assert first.payload_digest != second.payload_digest


def test_default_remote_request_id_is_deterministic_per_key() -> None:
    coordinator = KeyCoordinator()
    record = coordinator.create_record(
        "job-a", b'{"a":1}', "sha256:aaaa", duplicate_remote_id=False
    )

    import hashlib

    expected = f"remote-{hashlib.sha256(b'job-a').hexdigest()[:24]}"
    assert record.remote_request_id == expected
