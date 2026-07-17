"""Crash-boundary subprocess tests covering the full interruption matrix:
immediately before the pre-side-effect commit, immediately after it (during
HTTP), and after remote processing but before local finalization commits. A
later invocation must never treat the resulting `SUBMITTING` Job as
eligible or send another request.
"""

from __future__ import annotations

import contextlib
import json
import socket
import sqlite3
import subprocess
import sys
import threading
import time
from collections.abc import Iterator
from pathlib import Path

from boolean_maybe import canonical_json
from boolean_maybe.persistence import connection as connection_mod

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
def running_simulator(plan_path: Path) -> Iterator[int]:
    port = _find_free_port()
    proc = subprocess.Popen(
        [
            sys.executable,
            "-c",
            SIMULATOR_CODE,
            "--port",
            str(port),
            "--scenario-plan",
            str(plan_path),
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


def _job_row(db_path: Path, key: str) -> tuple[str, str] | None:
    # `timeout=0`: even a plain read needs at least a SHARED lock in
    # rollback-journal mode, incompatible with another connection's
    # EXCLUSIVE lock. Without a short timeout, a poll loop calling this
    # while some other connection holds that lock would itself queue up
    # and block for Python's default 5-second busy handling -- long enough
    # to straddle the very state transition the poll loop is trying to
    # observe. Failing fast and treating contention as "not observed yet"
    # keeps polling responsive instead.
    if not db_path.exists():
        return None
    try:
        conn = sqlite3.connect(str(db_path), timeout=0)
    except sqlite3.OperationalError:
        return None
    try:
        row = conn.execute(
            "SELECT job_id, state FROM jobs WHERE idempotency_key = ?", (key,)
        ).fetchone()
        return tuple(row) if row is not None else None
    except sqlite3.OperationalError:
        return None
    finally:
        conn.close()


def _attempt_state(db_path: Path, job_id: str) -> str | None:
    conn = sqlite3.connect(str(db_path), timeout=5)
    try:
        row = conn.execute(
            "SELECT state FROM submission_attempts WHERE job_id = ?", (job_id,)
        ).fetchone()
        return row[0] if row is not None else None
    finally:
        conn.close()


def test_process_killed_during_http_leaves_submitting_and_blocks_reattempt(
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
                        "idempotency_key": "job-crash",
                        "scenario": "connect_timeout",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    db_path = tmp_path / "db.sqlite3"

    with running_simulator(plan_path) as port:
        service_url = f"http://127.0.0.1:{port}"
        args = [
            "--job-entry",
            '{"a":1}',
            "--idempotency-key",
            "job-crash",
            "--service-url",
            service_url,
            "--database",
            str(db_path),
        ]
        proc = subprocess.Popen(
            [sys.executable, "-c", CLI_CODE, "submit", *args],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            # Wait until the pre-side-effect commit is durable (the process
            # is now blocked inside the connect_timeout HTTP wait).
            deadline = time.monotonic() + 10.0
            row = None
            while time.monotonic() < deadline:
                row = _job_row(db_path, "job-crash")
                if row is not None and row[1] == "SUBMITTING":
                    break
                time.sleep(0.05)
            assert row is not None and row[1] == "SUBMITTING", (
                "pre-side-effect commit never became visible before the deadline"
            )
            job_id = row[0]
            assert _attempt_state(db_path, job_id) == "STARTED"

            proc.kill()
            proc.wait(timeout=5)
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait(timeout=5)

        # Durable evidence of the (possibly sent) request survives the kill.
        row_after_kill = _job_row(db_path, "job-crash")
        assert row_after_kill == (job_id, "SUBMITTING")
        assert _attempt_state(db_path, job_id) == "STARTED"

        # A later invocation must never treat this Job as eligible, and must
        # not send another HTTP request (the simulator ordinal for this key
        # would otherwise advance to 2 and normal behavior would apply).
        second_args = [
            "--job-entry",
            '{"a":1}',
            "--idempotency-key",
            "job-crash",
            "--service-url",
            service_url,
            "--database",
            str(db_path),
        ]
        second = subprocess.run(
            [sys.executable, "-c", CLI_CODE, "submit", *second_args],
            capture_output=True,
            text=True,
            timeout=15,
        )

    assert second.returncode == 1
    body = json.loads(second.stdout)
    assert body["outcome"] == "job_not_eligible"
    assert body["submitted"] is False
    assert body["state"] == "SUBMITTING"

    # Still exactly the same Job/attempt: no second attempt was created.
    final_row = _job_row(db_path, "job-crash")
    assert final_row == (job_id, "SUBMITTING")


def test_process_killed_before_pre_side_effect_commit_persists_nothing(
    tmp_path: Path,
) -> None:
    # A helper connection holds the database's write lock for the entire
    # test, so the CLI process's pre-side-effect `BEGIN IMMEDIATE` can only
    # ever be *waiting* to acquire it, never past it. Killing the process
    # while it is stuck there simulates interruption strictly before that
    # transaction could commit.
    db_path = tmp_path / "db.sqlite3"
    connection_mod.open_connection(db_path).close()

    blocker = sqlite3.connect(
        str(db_path), autocommit=True, timeout=0, check_same_thread=False
    )
    blocker.execute("BEGIN EXCLUSIVE")
    try:
        with running_simulator(_success_plan(tmp_path)) as port:
            args = [
                "--job-entry",
                '{"a":1}',
                "--idempotency-key",
                "job-before-commit",
                "--service-url",
                f"http://127.0.0.1:{port}",
                "--database",
                str(db_path),
            ]
            proc = subprocess.Popen(
                [sys.executable, "-c", CLI_CODE, "submit", *args],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            try:
                # Give the process ample time to reach and block on the
                # pre-side-effect `BEGIN IMMEDIATE` before killing it.
                time.sleep(1.0)
                assert proc.poll() is None, (
                    "process exited early instead of blocking on the held lock"
                )
                proc.kill()
                proc.wait(timeout=5)
            finally:
                if proc.poll() is None:
                    proc.kill()
                    proc.wait(timeout=5)
    finally:
        blocker.execute("COMMIT")
        blocker.close()

    conn = sqlite3.connect(str(db_path))
    try:
        row = conn.execute(
            "SELECT COUNT(*) FROM jobs WHERE idempotency_key = 'job-before-commit'"
        ).fetchone()
        assert row == (0,)
    finally:
        conn.close()


def _success_plan(tmp_path: Path) -> Path:
    plan_path = tmp_path / "success-plan.json"
    plan_path.write_text(json.dumps({"version": 1, "rules": []}), encoding="utf-8")
    return plan_path


class _LockingSuccessServer:
    """A stub `POST /jobs` server that returns one authoritative success
    response, but only after acquiring an exclusive lock on the same
    SQLite database the CLI process under test is using -- and holds that
    lock for a while after sending the response.

    This deterministically reproduces "remote processing already
    succeeded, but local finalization cannot yet commit": by the time the
    client receives the response and attempts the post-side-effect
    transaction, this lock is already held.
    """

    def __init__(
        self,
        db_path: Path,
        idempotency_key: str,
        canonical_bytes: bytes,
        hold_seconds: float,
    ) -> None:
        self._db_path = db_path
        self._idempotency_key = idempotency_key
        self._canonical_bytes = canonical_bytes
        self._hold_seconds = hold_seconds
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.bind(("127.0.0.1", 0))
        self._sock.listen(1)
        self.port = self._sock.getsockname()[1]
        # Set once the request is drained, proving the client's
        # pre-side-effect commit already happened (it dispatches HTTP only
        # after that commit succeeds). The test waits on this instead of
        # polling the database file directly: on Windows, a read attempt
        # against a database another connection holds under `BEGIN
        # EXCLUSIVE` does not reliably fail fast even with `timeout=0`, so
        # polling would itself stall for the whole lock hold and could
        # observe the state only *after* release.
        # Set only after the authoritative 201 has actually been handed to
        # the socket, with the lock already held. Waiting on this before
        # killing the client (rather than just after the request is
        # received) is what actually proves the kill lands after remote
        # success: lock acquisition or response delivery could otherwise
        # still be pending on a slow host.
        self.response_sent = threading.Event()
        self._thread = threading.Thread(target=self._serve_once, daemon=True)
        self._thread.start()

    def _serve_once(self) -> None:
        try:
            conn, _ = self._sock.accept()
        except OSError:
            return
        blocker = None
        try:
            conn.settimeout(10.0)
            self._read_and_drain_request(conn)

            # The client's pre-side-effect commit must already be durable
            # by the time it could have dispatched this request; acquiring
            # the lock here, before responding, guarantees it is held by
            # the time the client attempts its post-side-effect commit.
            # A real (non-zero) timeout matters: a `timeout=0` connection
            # fails this `BEGIN EXCLUSIVE` immediately on any transient
            # contention (e.g. the test's own polling reads), which would
            # silently skip locking rather than retry -- defeating the
            # whole point of this stub.
            blocker = sqlite3.connect(
                str(self._db_path),
                autocommit=True,
                timeout=5.0,
                check_same_thread=False,
            )
            blocker.execute("BEGIN EXCLUSIVE")

            digest = canonical_json.payload_digest(self._canonical_bytes)
            body = json.dumps(
                {
                    "idempotency_key": self._idempotency_key,
                    "status": "processed",
                    "payload_digest": digest,
                    "remote_request_id": "remote-locking-stub",
                    "replayed": False,
                }
            ).encode()
            response = (
                b"HTTP/1.1 201 Created\r\n"
                b"Content-Type: application/json\r\n"
                + f"Content-Length: {len(body)}\r\n\r\n".encode()
                + body
            )
            conn.sendall(response)
            self.response_sent.set()
            time.sleep(self._hold_seconds)
        except OSError:
            pass
        finally:
            conn.close()
            if blocker is not None:
                blocker.execute("COMMIT")
                blocker.close()

    @staticmethod
    def _read_and_drain_request(conn: socket.socket) -> None:
        buffer = b""
        while b"\r\n\r\n" not in buffer:
            chunk = conn.recv(4096)
            if not chunk:
                return
            buffer += chunk
        header_text, _, body_so_far = buffer.partition(b"\r\n\r\n")
        content_length = 0
        for line in header_text.split(b"\r\n"):
            name, _, value = line.partition(b":")
            if name.strip().lower() == b"content-length":
                try:
                    content_length = int(value.strip())
                except ValueError:
                    content_length = 0
                break
        remaining = content_length - len(body_so_far)
        while remaining > 0:
            chunk = conn.recv(min(remaining, 65536))
            if not chunk:
                break
            remaining -= len(chunk)

    def close(self) -> None:
        self._sock.close()
        self._thread.join(timeout=15.0)


def test_process_killed_after_remote_success_during_finalization_leaves_submitting(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "db.sqlite3"
    connection_mod.open_connection(db_path).close()

    idempotency_key = "job-finalize-crash"
    canonical_bytes = canonical_json.canonicalize({"a": 1})
    server = _LockingSuccessServer(
        db_path, idempotency_key, canonical_bytes, hold_seconds=3.0
    )
    try:
        args = [
            "--job-entry",
            '{"a":1}',
            "--idempotency-key",
            idempotency_key,
            "--service-url",
            f"http://127.0.0.1:{server.port}",
            "--database",
            str(db_path),
        ]
        proc = subprocess.Popen(
            [sys.executable, "-c", CLI_CODE, "submit", *args],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            # Wait until the stub has actually handed the authoritative 201
            # to the socket (with its lock already held) -- not merely
            # until it received the request -- so the kill below is
            # guaranteed to land after remote success, not just after the
            # request was dispatched. Only a short fixed margin remains for
            # the client to receive that already-sent response and attempt
            # (and block on) its post-side-effect transaction.
            assert server.response_sent.wait(timeout=10.0), (
                "stub server never sent its response"
            )
            time.sleep(0.5)

            proc.kill()
            proc.wait(timeout=5)
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait(timeout=5)
    finally:
        server.close()

    # Remote processing happened (the stub sent authoritative evidence),
    # but local finalization never committed: the Job/attempt must remain
    # exactly as the pre-side-effect transaction left them.
    row_after_kill = _job_row(db_path, idempotency_key)
    assert row_after_kill is not None
    job_id, state = row_after_kill
    assert state == "SUBMITTING"
    assert _attempt_state(db_path, job_id) == "STARTED"

    with running_simulator(_success_plan(tmp_path)) as port:
        second = subprocess.run(
            [
                sys.executable,
                "-c",
                CLI_CODE,
                "submit",
                "--job-entry",
                '{"a":1}',
                "--idempotency-key",
                idempotency_key,
                "--service-url",
                f"http://127.0.0.1:{port}",
                "--database",
                str(db_path),
            ],
            capture_output=True,
            text=True,
            timeout=15,
        )

    assert second.returncode == 1
    body = json.loads(second.stdout)
    assert body["outcome"] == "job_not_eligible"
    assert body["submitted"] is False
    assert body["state"] == "SUBMITTING"
