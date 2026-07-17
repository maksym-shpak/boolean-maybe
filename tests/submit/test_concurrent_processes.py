"""Concurrent-process race tests: two real `boolean-maybe submit` processes
using the same database file and the same supplied key, racing against a
real simulator subprocess.
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
def running_simulator() -> Iterator[int]:
    port = _find_free_port()
    proc = subprocess.Popen(
        [sys.executable, "-c", SIMULATOR_CODE, "--port", str(port)],
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


def _start_submit(args: list[str]) -> subprocess.Popen[str]:
    return subprocess.Popen(
        [sys.executable, "-c", CLI_CODE, "submit", *args],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def test_concurrent_same_key_same_payload_creates_exactly_one_job(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "db.sqlite3"
    with running_simulator() as port:
        service_url = f"http://127.0.0.1:{port}"
        common_args = [
            "--job-entry",
            '{"race":true}',
            "--idempotency-key",
            "race-key",
            "--service-url",
            service_url,
            "--database",
            str(db_path),
        ]

        procs = [_start_submit(common_args) for _ in range(4)]
        outputs = [proc.communicate(timeout=20) for proc in procs]
        exit_codes = [proc.returncode for proc in procs]

    bodies = [json.loads(stdout) for stdout, _stderr in outputs if stdout.strip()]
    assert len(bodies) == 4
    assert all(code in (0, 1) for code in exit_codes)

    job_ids = {body["job_id"] for body in bodies if "job_id" in body}
    assert len(job_ids) == 1

    succeeded = [b for b in bodies if b.get("outcome") == "succeeded"]
    already_completed = [b for b in bodies if b.get("outcome") == "already_completed"]
    job_in_progress = [b for b in bodies if b.get("outcome") == "job_in_progress"]

    # Exactly one process initiated the HTTP submission; every other process
    # either replays the completed result or observes an actively-owned Job.
    assert len(succeeded) == 1
    assert len(already_completed) + len(job_in_progress) == 3
    for body in already_completed:
        assert body["submitted"] is False
    for body in job_in_progress:
        assert body["submitted"] is False


def test_concurrent_same_key_non_equivalent_payload_never_creates_two_jobs(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "db.sqlite3"
    with running_simulator() as port:
        service_url = f"http://127.0.0.1:{port}"

        proc_a = _start_submit(
            [
                "--job-entry",
                '{"variant":"a"}',
                "--idempotency-key",
                "race-key-2",
                "--service-url",
                service_url,
                "--database",
                str(db_path),
            ]
        )
        proc_b = _start_submit(
            [
                "--job-entry",
                '{"variant":"b"}',
                "--idempotency-key",
                "race-key-2",
                "--service-url",
                service_url,
                "--database",
                str(db_path),
            ]
        )
        out_a = proc_a.communicate(timeout=20)
        out_b = proc_b.communicate(timeout=20)

    body_a = json.loads(out_a[0])
    body_b = json.loads(out_b[0])
    outcomes = sorted([body_a["outcome"], body_b["outcome"]])

    # Whichever payload wins the pre-side-effect race gets its own Job
    # created (and its HTTP request always completes, since the loser's
    # conflict check fires purely on payload mismatch, independent of the
    # winner's Job state); the other is always rejected as a conflict before
    # any attempt or HTTP request of its own.
    assert outcomes == ["idempotency_conflict", "succeeded"]
