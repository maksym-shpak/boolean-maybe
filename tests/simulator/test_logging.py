"""Operational-log privacy and JSON-line atomicity tests."""

from __future__ import annotations

import json
import threading

from boolean_maybe.simulator.logs import OperationalLogger, key_fingerprint


def test_key_fingerprint_is_short_deterministic_hex() -> None:
    fingerprint = key_fingerprint("job-a")
    assert len(fingerprint) == 12
    assert fingerprint == key_fingerprint("job-a")
    assert all(ch in "0123456789abcdef" for ch in fingerprint)
    assert fingerprint != key_fingerprint("job-b")


def test_absent_optional_fields_are_omitted_not_null() -> None:
    class Stream:
        def __init__(self) -> None:
            self.text = ""

        def write(self, text: str) -> None:
            self.text += text

        def flush(self) -> None:
            return None

    stream = Stream()
    logger = OperationalLogger(stream=stream)  # type: ignore[arg-type]
    logger.emit("simulator_started")

    record = json.loads(stream.text.strip())
    assert set(record.keys()) == {"timestamp", "level", "event"}
    assert "null" not in stream.text


def test_full_event_field_set_when_all_values_supplied() -> None:
    class Stream:
        def __init__(self) -> None:
            self.text = ""

        def write(self, text: str) -> None:
            self.text += text

        def flush(self) -> None:
            return None

    stream = Stream()
    logger = OperationalLogger(stream=stream)  # type: ignore[arg-type]
    logger.emit(
        "request_completed",
        operation="submission",
        idempotency_key="job-a",
        scenario="success",
        ordinal=1,
        status=201,
        processed=True,
    )

    record = json.loads(stream.text.strip())
    assert record["operation"] == "submission"
    assert record["key_fingerprint"] == key_fingerprint("job-a")
    assert "job-a" not in stream.text
    assert record["scenario"] == "success"
    assert record["request_ordinal"] == 1
    assert record["status"] == 201
    assert record["processed"] is True


def test_concurrent_emits_never_interleave_json_lines() -> None:
    class RecordingStream:
        def __init__(self) -> None:
            self.chunks: list[str] = []

        def write(self, text: str) -> None:
            # Capture exactly what one write() call received; if the
            # logger were not holding a lock around write()+flush(), two
            # threads could otherwise interleave partial lines here.
            self.chunks.append(text)

        def flush(self) -> None:
            return None

    stream = RecordingStream()
    logger = OperationalLogger(stream=stream)  # type: ignore[arg-type]

    def worker(index: int) -> None:
        for i in range(50):
            logger.emit(
                "request_completed",
                operation="submission",
                idempotency_key=f"job-{index}-{i}",
            )

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)

    assert len(stream.chunks) == 8 * 50
    for chunk in stream.chunks:
        assert chunk.endswith("\n")
        assert chunk.count("\n") == 1
        json.loads(chunk)  # every captured write() is one complete, valid JSON line


# -- End-to-end privacy: no raw key/payload/digest leaks into logs -----------


def test_logs_never_contain_raw_key_or_payload_or_full_digest(simulator) -> None:
    secret_key = "a" * 100 + "-secret-key-suffix"
    secret_payload_marker = "super-secret-payload-value-marker"

    status, parsed, _ = simulator.request(
        "POST",
        "/jobs",
        body=json.dumps({"work": secret_payload_marker}).encode("utf-8"),
        headers={"Content-Type": "application/json", "Idempotency-Key": secret_key},
    )
    assert status == 201
    assert parsed is not None
    full_digest = parsed["payload_digest"]

    simulator.request("GET", f"/jobs/by-idempotency-key/{secret_key}")

    log_text = "".join(simulator.log_stream.lines)
    assert secret_key not in log_text
    assert secret_payload_marker not in log_text
    assert full_digest not in log_text

    events = simulator.log_stream.events()
    assert len(events) >= 2
    for event in events:
        if "key_fingerprint" in event:
            assert event["key_fingerprint"] == key_fingerprint(secret_key)
        assert "idempotency_key" not in event
        assert "payload_digest" not in event
        assert "payload" not in event


def test_malformed_body_after_valid_key_still_logs_key_fingerprint(simulator) -> None:
    # Content-Type and Idempotency-Key both validate successfully; only the
    # JSON body itself is malformed. An accepted key was available by the
    # time this request failed, so the event must still identify it.
    key = "job-malformed-body"
    status, parsed, _ = simulator.request(
        "POST",
        "/jobs",
        body=b"{not valid json",
        headers={"Content-Type": "application/json", "Idempotency-Key": key},
    )
    assert status == 400
    assert parsed is not None
    assert parsed["error"]["code"] == "invalid_request"

    events = simulator.log_stream.events()
    assert events[-1]["event"] == "request_completed"
    assert events[-1]["status"] == 400
    assert events[-1]["key_fingerprint"] == key_fingerprint(key)
