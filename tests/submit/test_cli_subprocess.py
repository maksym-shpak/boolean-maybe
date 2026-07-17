"""Subprocess tests for the `boolean-maybe submit` console entry point:
stable stdout/stderr/exit-code behavior against a real simulator subprocess.
"""

from __future__ import annotations

import contextlib
import json
import socket
import subprocess
import sys
import time
from collections.abc import Iterator
from pathlib import Path

SIMULATOR_CODE = "from boolean_maybe.simulator.cli import main; main()"
CLI_CODE = "from boolean_maybe.cli import main; main()"


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def _wait_until_accepting(port: int, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.05)
    raise AssertionError(f"simulator on 127.0.0.1:{port} never accepted connections")


@contextlib.contextmanager
def running_simulator(extra_args: list[str] | None = None) -> Iterator[int]:
    port = _find_free_port()
    proc = subprocess.Popen(
        [
            sys.executable,
            "-c",
            SIMULATOR_CODE,
            "--port",
            str(port),
            *(extra_args or []),
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        _wait_until_accepting(port)
        yield port
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)


def run_submit(
    args: list[str], timeout: float = 15.0
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-c", CLI_CODE, "submit", *args],
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def run_cli(args: list[str], timeout: float = 15.0) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-c", CLI_CODE, *args],
        capture_output=True,
        text=True,
        timeout=timeout,
    )


# -- Fresh success, replay, and JSON schema -----------------------------------


def test_fresh_success_emits_one_json_object_and_exits_0(tmp_path: Path) -> None:
    with running_simulator() as port:
        result = run_submit(
            [
                "--job-entry",
                '{"a":1}',
                "--service-url",
                f"http://127.0.0.1:{port}",
                "--database",
                str(tmp_path / "db.sqlite3"),
            ]
        )

    assert result.returncode == 0
    lines = [line for line in result.stdout.splitlines() if line.strip()]
    assert len(lines) == 1
    body = json.loads(lines[0])
    assert body["outcome"] == "succeeded"
    assert body["submitted"] is True
    assert body["state"] == "SUCCEEDED"
    assert body["attempt"]["http_status"] == 201
    assert body["result"]["status"] == "processed"
    assert body["job_id"]
    assert body["reconciliation_requests"] == 0
    assert body["attempt_history"] == [
        {
            "attempt_id": body["attempt"]["attempt_id"],
            "attempt_number": 1,
            "state": "SUCCEEDED",
            "started_at": body["attempt_history"][0]["started_at"],
            "completed_at": body["attempt_history"][0]["completed_at"],
            "http_status": 201,
            "remote_request_id": body["attempt"]["remote_request_id"],
            "error_category": None,
            "retry_after_ms": None,
            "reconciliation": {"request_count": 0, "final_category": None},
        }
    ]


def test_idempotency_conflict_omits_job_id_and_attempt_history(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "db.sqlite3"
    with running_simulator() as port:
        service_url = f"http://127.0.0.1:{port}"
        run_submit(
            [
                "--job-entry",
                '{"a":1}',
                "--idempotency-key",
                "job-a",
                "--service-url",
                service_url,
                "--database",
                str(db_path),
            ]
        )
        result = run_submit(
            [
                "--job-entry",
                '{"a":2}',
                "--idempotency-key",
                "job-a",
                "--service-url",
                service_url,
                "--database",
                str(db_path),
            ]
        )

    assert result.returncode == 1
    body = json.loads(result.stdout)
    assert body["outcome"] == "idempotency_conflict"
    assert body["submitted"] is False
    assert body["state"] is None
    assert "job_id" not in body
    assert "attempt_history" not in body
    assert body["reconciliation_requests"] == 0


def test_ambiguous_via_reconciliation_includes_job_id_and_attempt_history(
    tmp_path: Path,
) -> None:
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(
        json.dumps(
            {
                "version": 1,
                "rules": [
                    {
                        "operation": "submission",
                        "idempotency_key": "job-a",
                        "scenario": "always_500",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    with running_simulator(["--scenario-plan", str(plan_path)]) as port:
        result = run_submit(
            [
                "--job-entry",
                '{"a":1}',
                "--idempotency-key",
                "job-a",
                "--service-url",
                f"http://127.0.0.1:{port}",
                "--database",
                str(tmp_path / "db.sqlite3"),
            ]
        )

    assert result.returncode == 1
    body = json.loads(result.stdout)
    assert body["outcome"] == "ambiguous"
    assert body["submitted"] is True
    assert body["state"] == "AMBIGUOUS"
    assert body["job_id"]
    assert body["reconciliation_requests"] == 1
    assert len(body["attempt_history"]) == 1
    assert body["attempt_history"][0]["state"] == "AMBIGUOUS"
    assert body["attempt_history"][0]["error_category"] == "reconciliation_not_found"
    assert body["attempt_history"][0]["reconciliation"] == {
        "request_count": 1,
        "final_category": "reconciliation_not_found",
    }


def test_replay_returns_already_completed_and_exits_0(tmp_path: Path) -> None:
    db_path = tmp_path / "db.sqlite3"
    with running_simulator() as port:
        service_url = f"http://127.0.0.1:{port}"
        first = run_submit(
            [
                "--job-entry",
                '{"a":1}',
                "--idempotency-key",
                "job-a",
                "--service-url",
                service_url,
                "--database",
                str(db_path),
            ]
        )
        second = run_submit(
            [
                "--job-entry",
                '{"a":1}',
                "--idempotency-key",
                "job-a",
                "--service-url",
                service_url,
                "--database",
                str(db_path),
            ]
        )

    assert first.returncode == 0
    assert second.returncode == 0
    second_body = json.loads(second.stdout)
    assert second_body["outcome"] == "already_completed"
    assert second_body["submitted"] is False


def test_idempotency_conflict_exits_1(tmp_path: Path) -> None:
    db_path = tmp_path / "db.sqlite3"
    with running_simulator() as port:
        service_url = f"http://127.0.0.1:{port}"
        run_submit(
            [
                "--job-entry",
                '{"a":1}',
                "--idempotency-key",
                "job-a",
                "--service-url",
                service_url,
                "--database",
                str(db_path),
            ]
        )
        result = run_submit(
            [
                "--job-entry",
                '{"a":2}',
                "--idempotency-key",
                "job-a",
                "--service-url",
                service_url,
                "--database",
                str(db_path),
            ]
        )

    assert result.returncode == 1
    body = json.loads(result.stdout)
    assert body["outcome"] == "idempotency_conflict"
    assert body["submitted"] is False


# -- Validation failures (exit 2) --------------------------------------------


def test_invalid_job_entry_exits_2_and_sends_no_http(tmp_path: Path) -> None:
    with running_simulator() as port:
        result = run_submit(
            [
                "--job-entry",
                "not json",
                "--service-url",
                f"http://127.0.0.1:{port}",
                "--database",
                str(tmp_path / "db.sqlite3"),
            ]
        )

    assert result.returncode == 2
    assert not (tmp_path / "db.sqlite3").exists()


def test_invalid_idempotency_key_exits_2(tmp_path: Path) -> None:
    with running_simulator() as port:
        result = run_submit(
            [
                "--job-entry",
                "{}",
                "--idempotency-key",
                "not a valid key!",
                "--service-url",
                f"http://127.0.0.1:{port}",
                "--database",
                str(tmp_path / "db.sqlite3"),
            ]
        )

    assert result.returncode == 2


def test_hostname_service_url_is_rejected_with_exit_2(tmp_path: Path) -> None:
    result = run_submit(
        [
            "--job-entry",
            "{}",
            "--service-url",
            "http://localhost:8080",
            "--database",
            str(tmp_path / "db.sqlite3"),
        ]
    )

    assert result.returncode == 2
    assert not (tmp_path / "db.sqlite3").exists()


def test_missing_required_option_exits_2() -> None:
    result = run_submit([])
    assert result.returncode == 2


# -- Help and bare invocation --------------------------------------------------


def test_help_exits_0() -> None:
    result = run_cli(["--help"])
    assert result.returncode == 0


def test_bare_invocation_prints_help_and_exits_0() -> None:
    result = run_cli([])
    assert result.returncode == 0


# -- Stable stdout/stderr separation ------------------------------------------


def test_success_writes_only_one_line_to_stdout_and_no_traceback_to_stderr(
    tmp_path: Path,
) -> None:
    with running_simulator() as port:
        result = run_submit(
            [
                "--job-entry",
                "{}",
                "--service-url",
                f"http://127.0.0.1:{port}",
                "--database",
                str(tmp_path / "db.sqlite3"),
            ]
        )

    assert result.returncode == 0
    assert "Traceback" not in result.stderr
    assert result.stdout.count("\n") == 1


def test_retry_exhausted_when_service_unreachable(tmp_path: Path) -> None:
    # A connect failure is proven `NOT_SENT` and is therefore a safe-retry
    # candidate: all three lifetime attempts are consumed against the same
    # unreachable port before the Job becomes definitively `FAILED_PERMANENT`.
    # `submitted` is `False` because every individual attempt proved its own
    # dispatch never began.
    unreachable_port = _find_free_port()  # nothing listening on this port
    result = run_submit(
        [
            "--job-entry",
            "{}",
            "--service-url",
            f"http://127.0.0.1:{unreachable_port}",
            "--database",
            str(tmp_path / "db.sqlite3"),
        ],
        timeout=30.0,
    )

    assert result.returncode == 1
    body = json.loads(result.stdout)
    assert body["outcome"] == "retry_exhausted"
    assert body["submitted"] is False
    assert body["state"] == "FAILED_PERMANENT"
