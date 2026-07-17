"""Persistence tests: PRAGMA verification, migration version 1, reopen,
newer-version refusal, schema-mismatch detection, concurrent initialization,
and busy-timeout exhaustion.
"""

from __future__ import annotations

import stat
import sqlite3
import sys
import threading
from pathlib import Path

import pytest

from boolean_maybe.persistence import connection as connection_mod
from boolean_maybe.persistence import schema
from boolean_maybe.persistence.errors import PersistenceError, SchemaVersionError


def test_fresh_database_migrates_to_version_1_with_verified_pragmas(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "nested" / "boolean-maybe.sqlite3"
    conn = connection_mod.open_connection(db_path)
    try:
        assert db_path.exists()
        (version,) = conn.execute("PRAGMA user_version").fetchone()
        assert version == schema.SCHEMA_VERSION

        (journal_mode,) = conn.execute("PRAGMA journal_mode").fetchone()
        assert str(journal_mode).lower() == "delete"
        (synchronous,) = conn.execute("PRAGMA synchronous").fetchone()
        assert int(synchronous) == 2
        (foreign_keys,) = conn.execute("PRAGMA foreign_keys").fetchone()
        assert int(foreign_keys) == 1
        (busy_timeout,) = conn.execute("PRAGMA busy_timeout").fetchone()
        assert int(busy_timeout) == schema.BUSY_TIMEOUT_MS

        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
        assert {"jobs", "submission_attempts"} <= tables
    finally:
        conn.close()


def test_reopening_version_1_is_non_destructive(tmp_path: Path) -> None:
    db_path = tmp_path / "boolean-maybe.sqlite3"
    first = connection_mod.open_connection(db_path)
    try:
        first.execute("BEGIN IMMEDIATE")
        first.execute(
            "INSERT INTO jobs (job_id, idempotency_key, payload_canonical, state, created_at, updated_at) "
            "VALUES ('job-1', 'key-1', X'7b7d', 'PENDING', '2026-01-01T00:00:00.000000Z', '2026-01-01T00:00:00.000000Z')"
        )
        first.execute("COMMIT")
    finally:
        first.close()

    second = connection_mod.open_connection(db_path)
    try:
        row = second.execute(
            "SELECT job_id FROM jobs WHERE job_id = 'job-1'"
        ).fetchone()
        assert row == ("job-1",)
    finally:
        second.close()


def test_newer_schema_version_is_refused(tmp_path: Path) -> None:
    db_path = tmp_path / "boolean-maybe.sqlite3"
    conn = connection_mod.open_connection(db_path)
    conn.execute(f"PRAGMA user_version = {schema.SCHEMA_VERSION + 1}")
    conn.close()

    with pytest.raises(PersistenceError):
        connection_mod.open_connection(db_path)


def test_schema_mismatch_at_version_1_is_refused(tmp_path: Path) -> None:
    db_path = tmp_path / "boolean-maybe.sqlite3"
    conn = connection_mod.open_connection(db_path)
    conn.execute("BEGIN IMMEDIATE")
    conn.execute("ALTER TABLE jobs ADD COLUMN unexpected_column TEXT")
    conn.execute("COMMIT")
    conn.close()

    with pytest.raises(SchemaVersionError):
        connection_mod.open_connection(db_path)


def test_non_unique_replacement_index_is_refused(tmp_path: Path) -> None:
    # Regression test: swapping the required unique partial index for an
    # ordinary non-unique index must not silently pass verification.
    db_path = tmp_path / "boolean-maybe.sqlite3"
    conn = connection_mod.open_connection(db_path)
    conn.execute("BEGIN IMMEDIATE")
    conn.execute("DROP INDEX one_started_attempt_per_job")
    conn.execute(
        "CREATE INDEX one_started_attempt_per_job ON submission_attempts(job_id) "
        "WHERE state = 'STARTED'"
    )
    conn.execute("COMMIT")
    conn.close()

    with pytest.raises(SchemaVersionError):
        connection_mod.open_connection(db_path)


def test_non_partial_replacement_index_is_refused(tmp_path: Path) -> None:
    # Regression test: swapping the required partial (WHERE-qualified)
    # index for a full unique index over the same column must not silently
    # pass verification, since it would no longer permit multiple non-
    # STARTED attempts for the same Job.
    db_path = tmp_path / "boolean-maybe.sqlite3"
    conn = connection_mod.open_connection(db_path)
    conn.execute("BEGIN IMMEDIATE")
    conn.execute("DROP INDEX one_started_attempt_per_job")
    conn.execute(
        "CREATE UNIQUE INDEX one_started_attempt_per_job ON submission_attempts(job_id)"
    )
    conn.execute("COMMIT")
    conn.close()

    with pytest.raises(SchemaVersionError):
        connection_mod.open_connection(db_path)
    conn.close()

    with pytest.raises(SchemaVersionError):
        connection_mod.open_connection(db_path)


def test_two_processes_racing_to_initialize_produce_one_valid_schema(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "boolean-maybe.sqlite3"
    results: list[object] = []
    results_lock = threading.Lock()
    barrier = threading.Barrier(4)

    def worker() -> None:
        barrier.wait()
        try:
            conn = connection_mod.open_connection(db_path)
        except PersistenceError as exc:
            with results_lock:
                results.append(exc)
            return
        try:
            (version,) = conn.execute("PRAGMA user_version").fetchone()
            with results_lock:
                results.append(version)
        finally:
            conn.close()

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)

    versions = [r for r in results if isinstance(r, int)]
    assert len(versions) >= 1
    assert all(version == schema.SCHEMA_VERSION for version in versions)


def test_busy_timeout_exhaustion_fails_safely(tmp_path: Path) -> None:
    db_path = tmp_path / "boolean-maybe.sqlite3"
    # Establish version 1 first so the blocking connection below only needs
    # to hold the write lock, not also race the one-time migration.
    connection_mod.open_connection(db_path).close()

    # Held for the entire test (never released): SQLite's busy_timeout retry
    # window is a soft, imprecise deadline, so racing it against a timed
    # release is flaky. Holding the lock unconditionally makes the failure
    # deterministic.
    blocker = sqlite3.connect(
        str(db_path), autocommit=True, timeout=0, check_same_thread=False
    )
    blocker.execute("BEGIN EXCLUSIVE")
    try:
        with pytest.raises(PersistenceError):
            connection_mod.open_connection(db_path)
    finally:
        blocker.execute("COMMIT")
        blocker.close()


def test_directory_at_database_path_is_refused(tmp_path: Path) -> None:
    db_path = tmp_path / "boolean-maybe.sqlite3"
    db_path.mkdir()  # a directory occupies the exact target path

    with pytest.raises(PersistenceError):
        connection_mod.open_connection(db_path)


@pytest.mark.skipif(
    sys.platform == "win32",
    reason=(
        "os.chmod on Windows only toggles the read-only file attribute; it "
        "does not reliably block SQLite's write access the way POSIX "
        "permission bits do, so this scenario is not portably reproducible"
    ),
)
def test_read_only_database_file_is_refused(tmp_path: Path) -> None:
    db_path = tmp_path / "boolean-maybe.sqlite3"
    connection_mod.open_connection(db_path).close()

    db_path.chmod(stat.S_IREAD)
    try:
        with pytest.raises(PersistenceError):
            connection_mod.open_connection(db_path)
    finally:
        db_path.chmod(stat.S_IREAD | stat.S_IWRITE)


@pytest.mark.skipif(
    sys.platform == "win32",
    reason=(
        "os.chmod on Windows does not remove directory write/create "
        "permission the way POSIX permission bits do, so a read-only "
        "parent directory is not portably reproducible here"
    ),
)
def test_read_only_parent_directory_is_refused(tmp_path: Path) -> None:
    read_only_dir = tmp_path / "read-only"
    read_only_dir.mkdir()
    db_path = read_only_dir / "boolean-maybe.sqlite3"

    read_only_dir.chmod(stat.S_IREAD | stat.S_IEXEC)
    try:
        with pytest.raises(PersistenceError):
            connection_mod.open_connection(db_path)
    finally:
        read_only_dir.chmod(stat.S_IREAD | stat.S_IWRITE | stat.S_IEXEC)
