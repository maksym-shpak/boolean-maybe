"""SQLite migration versions 1 and 2: DDL, PRAGMA policy, and schema
verification.

`docs/specs/features/submit-single-job.md` documents the connection PRAGMA
policy and the version-1 schema. Migration `1` creates the `jobs` and
`submission_attempts` tables and the `one_started_attempt_per_job` unique
index atomically under an exclusive schema transaction.

`docs/specs/features/reliable-job-submission.md` documents migration `2`,
which is additive: it preserves every version-1 row and adds the
`service_rate_limit_gate` singleton and the append-oriented
`attempt_observations` history, committing atomically with the same
exclusive schema transaction. A fresh database applies both migrations in
sequence; a version-1 database applies only migration `2`; version `2`
opens without mutation; a newer or mismatched version fails before product
work.
"""

from __future__ import annotations

import re
import sqlite3

from .errors import SchemaVersionError

_WHITESPACE_RE = re.compile(r"\s+")

SCHEMA_VERSION = 2
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

MIGRATION_2_STATEMENTS: tuple[str, ...] = (
    """
    CREATE TABLE service_rate_limit_gate (
        singleton_id INTEGER PRIMARY KEY CHECK (singleton_id = 1),
        not_before TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE attempt_observations (
        observation_id TEXT PRIMARY KEY,
        attempt_id TEXT NOT NULL
            REFERENCES submission_attempts(attempt_id),
        sequence_id TEXT NOT NULL,
        sequence_number INTEGER NOT NULL CHECK (sequence_number > 0),
        operation TEXT NOT NULL
            CHECK (operation IN ('SUBMISSION', 'RECONCILIATION')),
        request_ordinal INTEGER NOT NULL CHECK (request_ordinal > 0),
        observed_at TEXT NOT NULL,
        dispatch_certainty TEXT
            CHECK (dispatch_certainty IN ('NOT_SENT', 'MAYBE_SENT')),
        evidence_category TEXT,
        http_status INTEGER
            CHECK (http_status IS NULL OR http_status BETWEEN 100 AND 599),
        remote_request_id TEXT,
        retry_after_ms INTEGER
            CHECK (retry_after_ms IS NULL OR retry_after_ms >= 0),
        retry_after_diagnostic TEXT,
        observed_fencing_generation INTEGER NOT NULL
            CHECK (observed_fencing_generation > 0),
        is_late INTEGER NOT NULL DEFAULT 0 CHECK (is_late IN (0, 1)),
        consumed_at TEXT,
        UNIQUE (attempt_id, sequence_number),
        UNIQUE (sequence_id, request_ordinal)
    )
    """,
    """
    CREATE INDEX attempt_observations_attempt_order
    ON attempt_observations (attempt_id, sequence_number)
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

_SERVICE_RATE_LIMIT_GATE_COLUMNS = frozenset(
    {
        ("singleton_id", "INTEGER", 0, 1),
        ("not_before", "TEXT", 1, 0),
        ("updated_at", "TEXT", 1, 0),
    }
)

_ATTEMPT_OBSERVATIONS_COLUMNS = frozenset(
    {
        ("observation_id", "TEXT", 0, 1),
        ("attempt_id", "TEXT", 1, 0),
        ("sequence_id", "TEXT", 1, 0),
        ("sequence_number", "INTEGER", 1, 0),
        ("operation", "TEXT", 1, 0),
        ("request_ordinal", "INTEGER", 1, 0),
        ("observed_at", "TEXT", 1, 0),
        ("dispatch_certainty", "TEXT", 0, 0),
        ("evidence_category", "TEXT", 0, 0),
        ("http_status", "INTEGER", 0, 0),
        ("remote_request_id", "TEXT", 0, 0),
        ("retry_after_ms", "INTEGER", 0, 0),
        ("retry_after_diagnostic", "TEXT", 0, 0),
        ("observed_fencing_generation", "INTEGER", 1, 0),
        ("is_late", "INTEGER", 1, 0),
        ("consumed_at", "TEXT", 0, 0),
    }
)


def ensure_schema(conn: sqlite3.Connection) -> None:
    """Migrate an empty or version-1 database to version 2, or verify an
    existing version-2 database.

    Must run inside the caller's exclusive schema transaction.
    """

    (version,) = conn.execute("PRAGMA user_version").fetchone()
    if version == 0:
        for statement in MIGRATION_1_STATEMENTS:
            conn.execute(statement)
        for statement in MIGRATION_2_STATEMENTS:
            conn.execute(statement)
        conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
        return
    if version == 1:
        # Fail closed on a tampered or corrupted version-1 database rather
        # than silently grafting migration 2 onto it: verify the existing
        # version-1 objects match their original definition first.
        _verify_v1_schema(conn)
        for statement in MIGRATION_2_STATEMENTS:
            conn.execute(statement)
        conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
        return
    if version == SCHEMA_VERSION:
        _verify_schema(conn)
        return
    raise SchemaVersionError(
        f"database schema version {version} is newer than the supported version {SCHEMA_VERSION}"
    )


def _verify_v1_schema(conn: sqlite3.Connection) -> None:
    _verify_table_columns(conn, "jobs", _JOBS_COLUMNS)
    _verify_table_columns(conn, "submission_attempts", _SUBMISSION_ATTEMPTS_COLUMNS)

    # Column/type/notnull/pk equality (checked above) cannot see CHECK
    # constraints or foreign keys, and `PRAGMA index_list` alone cannot see
    # a partial index's predicate. Comparing the exact `CREATE TABLE`/
    # `CREATE INDEX` text that this migration itself issued (normalized to
    # tolerate whitespace-only formatting differences) additionally detects
    # a dropped or altered CHECK, foreign key, or index predicate.
    _verify_object_sql(
        conn, "table", "jobs", MIGRATION_1_STATEMENTS[0], version_label="version 1"
    )
    _verify_object_sql(
        conn,
        "table",
        "submission_attempts",
        MIGRATION_1_STATEMENTS[1],
        version_label="version 1",
    )
    _verify_object_sql(
        conn,
        "index",
        "one_started_attempt_per_job",
        MIGRATION_1_STATEMENTS[2],
        version_label="version 1",
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


def _verify_schema(conn: sqlite3.Connection) -> None:
    _verify_v1_schema(conn)
    _verify_table_columns(
        conn, "service_rate_limit_gate", _SERVICE_RATE_LIMIT_GATE_COLUMNS
    )
    _verify_table_columns(conn, "attempt_observations", _ATTEMPT_OBSERVATIONS_COLUMNS)

    _verify_object_sql(
        conn,
        "table",
        "service_rate_limit_gate",
        MIGRATION_2_STATEMENTS[0],
        version_label="version 2",
    )
    _verify_object_sql(
        conn,
        "table",
        "attempt_observations",
        MIGRATION_2_STATEMENTS[1],
        version_label="version 2",
    )
    _verify_object_sql(
        conn,
        "index",
        "attempt_observations_attempt_order",
        MIGRATION_2_STATEMENTS[2],
        version_label="version 2",
    )

    observation_index_rows = conn.execute(
        "PRAGMA index_list('attempt_observations')"
    ).fetchall()
    observation_matching = [
        row
        for row in observation_index_rows
        if row[1] == "attempt_observations_attempt_order"
    ]
    if not observation_matching:
        raise SchemaVersionError("attempt_observations is missing the required index")


def _verify_object_sql(
    conn: sqlite3.Connection,
    object_type: str,
    name: str,
    expected_ddl: str,
    *,
    version_label: str,
) -> None:
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type = ? AND name = ?",
        (object_type, name),
    ).fetchone()
    if row is None or row[0] is None:
        raise SchemaVersionError(f"{object_type} {name!r} is missing")
    if _normalize_sql(row[0]) != _normalize_sql(expected_ddl):
        raise SchemaVersionError(
            f"{object_type} {name!r} does not match the {version_label} definition"
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
        raise SchemaVersionError(f"table {table!r} column schema does not match")
