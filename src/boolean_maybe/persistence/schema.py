"""SQLite migration version 1: DDL, PRAGMA policy, and schema verification.

`docs/specs/features/submit-single-job.md` documents the connection PRAGMA
policy and this exact schema. Migration `1` creates the `jobs` and
`submission_attempts` tables and the `one_started_attempt_per_job` unique
index atomically under an exclusive schema transaction; version `1` opens
without mutation; a newer or mismatched version fails before product work.
"""

from __future__ import annotations

import re
import sqlite3

from .errors import SchemaVersionError

_WHITESPACE_RE = re.compile(r"\s+")

SCHEMA_VERSION = 1
BUSY_TIMEOUT_MS = 5000

MIGRATION_1_STATEMENTS: tuple[str, ...] = (
    """
    CREATE TABLE jobs (
        job_id TEXT PRIMARY KEY,
        idempotency_key TEXT NOT NULL UNIQUE,
        payload_canonical BLOB NOT NULL,
        state TEXT NOT NULL CHECK (
            state IN (
                'PENDING', 'SUBMITTING', 'RETRY_SCHEDULED',
                'SUCCEEDED', 'FAILED_PERMANENT', 'AMBIGUOUS'
            )
        ),
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        CHECK (length(payload_canonical) <= 1048576),
        CHECK (updated_at >= created_at)
    )
    """,
    """
    CREATE TABLE submission_attempts (
        attempt_id TEXT PRIMARY KEY,
        job_id TEXT NOT NULL REFERENCES jobs(job_id),
        attempt_number INTEGER NOT NULL CHECK (attempt_number > 0),
        state TEXT NOT NULL CHECK (
            state IN (
                'STARTED', 'SUCCEEDED', 'RETRYABLE_FAILURE',
                'PERMANENT_FAILURE', 'AMBIGUOUS'
            )
        ),
        started_at TEXT NOT NULL,
        completed_at TEXT,
        http_status INTEGER,
        remote_request_id TEXT,
        error_category TEXT,
        retry_after_ms INTEGER CHECK (retry_after_ms IS NULL OR retry_after_ms >= 0),
        owner_token TEXT,
        fencing_generation INTEGER NOT NULL CHECK (fencing_generation > 0),
        lease_expires_at TEXT,
        UNIQUE (job_id, attempt_number),
        CHECK (
            (state = 'STARTED' AND completed_at IS NULL
                AND owner_token IS NOT NULL AND lease_expires_at IS NOT NULL)
            OR
            (state <> 'STARTED' AND completed_at IS NOT NULL
                AND owner_token IS NULL AND lease_expires_at IS NULL)
        ),
        CHECK (completed_at IS NULL OR completed_at >= started_at)
    )
    """,
    """
    CREATE UNIQUE INDEX one_started_attempt_per_job
    ON submission_attempts(job_id)
    WHERE state = 'STARTED'
    """,
)

# (name, type, notnull, pk) per `PRAGMA table_info`. SQLite does not imply
# NOT NULL for a non-INTEGER PRIMARY KEY, so `job_id`/`attempt_id` are 0 here.
_JOBS_COLUMNS = frozenset(
    {
        ("job_id", "TEXT", 0, 1),
        ("idempotency_key", "TEXT", 1, 0),
        ("payload_canonical", "BLOB", 1, 0),
        ("state", "TEXT", 1, 0),
        ("created_at", "TEXT", 1, 0),
        ("updated_at", "TEXT", 1, 0),
    }
)

_SUBMISSION_ATTEMPTS_COLUMNS = frozenset(
    {
        ("attempt_id", "TEXT", 0, 1),
        ("job_id", "TEXT", 1, 0),
        ("attempt_number", "INTEGER", 1, 0),
        ("state", "TEXT", 1, 0),
        ("started_at", "TEXT", 1, 0),
        ("completed_at", "TEXT", 0, 0),
        ("http_status", "INTEGER", 0, 0),
        ("remote_request_id", "TEXT", 0, 0),
        ("error_category", "TEXT", 0, 0),
        ("retry_after_ms", "INTEGER", 0, 0),
        ("owner_token", "TEXT", 0, 0),
        ("fencing_generation", "INTEGER", 1, 0),
        ("lease_expires_at", "TEXT", 0, 0),
    }
)


def ensure_schema(conn: sqlite3.Connection) -> None:
    """Migrate an empty database to version 1, or verify an existing one.

    Must run inside the caller's exclusive schema transaction.
    """

    (version,) = conn.execute("PRAGMA user_version").fetchone()
    if version == 0:
        for statement in MIGRATION_1_STATEMENTS:
            conn.execute(statement)
        conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
        return
    if version == SCHEMA_VERSION:
        _verify_schema(conn)
        return
    raise SchemaVersionError(
        f"database schema version {version} is newer than the supported version {SCHEMA_VERSION}"
    )


def _verify_schema(conn: sqlite3.Connection) -> None:
    _verify_table_columns(conn, "jobs", _JOBS_COLUMNS)
    _verify_table_columns(conn, "submission_attempts", _SUBMISSION_ATTEMPTS_COLUMNS)

    # Column/type/notnull/pk equality (checked above) cannot see CHECK
    # constraints or foreign keys, and `PRAGMA index_list` alone cannot see
    # a partial index's predicate. Comparing the exact `CREATE TABLE`/
    # `CREATE INDEX` text that this migration itself issued (normalized to
    # tolerate whitespace-only formatting differences) additionally detects
    # a dropped or altered CHECK, foreign key, or index predicate.
    _verify_object_sql(conn, "table", "jobs", MIGRATION_1_STATEMENTS[0])
    _verify_object_sql(conn, "table", "submission_attempts", MIGRATION_1_STATEMENTS[1])
    _verify_object_sql(
        conn, "index", "one_started_attempt_per_job", MIGRATION_1_STATEMENTS[2]
    )

    index_rows = conn.execute("PRAGMA index_list('submission_attempts')").fetchall()
    matching = [row for row in index_rows if row[1] == "one_started_attempt_per_job"]
    if not matching:
        raise SchemaVersionError(
            "submission_attempts is missing the required unique index"
        )
    # `index_list` row shape: (seq, name, unique, origin, partial).
    (_, _, unique, _, partial) = matching[0]
    if not unique:
        raise SchemaVersionError("one_started_attempt_per_job is not a unique index")
    if not partial:
        raise SchemaVersionError(
            "one_started_attempt_per_job is not a partial (WHERE-qualified) index"
        )


def _verify_object_sql(
    conn: sqlite3.Connection, object_type: str, name: str, expected_ddl: str
) -> None:
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type = ? AND name = ?",
        (object_type, name),
    ).fetchone()
    if row is None or row[0] is None:
        raise SchemaVersionError(f"{object_type} {name!r} is missing")
    if _normalize_sql(row[0]) != _normalize_sql(expected_ddl):
        raise SchemaVersionError(
            f"{object_type} {name!r} does not match the version 1 definition"
        )


def _normalize_sql(sql: str) -> str:
    return _WHITESPACE_RE.sub(" ", sql).strip()


def _verify_table_columns(
    conn: sqlite3.Connection, table: str, expected: frozenset[tuple[str, str, int, int]]
) -> None:
    rows = conn.execute(f"PRAGMA table_info('{table}')").fetchall()
    if not rows:
        raise SchemaVersionError(f"table {table!r} is missing")
    actual = {(row[1], str(row[2]).upper(), int(row[3]), int(row[5])) for row in rows}
    if actual != expected:
        raise SchemaVersionError(f"table {table!r} schema does not match version 1")
