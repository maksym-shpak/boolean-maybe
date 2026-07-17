"""SQLite connection setup: PRAGMA verification and schema migration.

Every connection uses true autocommit mode (Python 3.12's `autocommit=True`)
so that `BEGIN IMMEDIATE`/`BEGIN EXCLUSIVE`/`COMMIT`/`ROLLBACK` issued by this
package are the only transaction control in effect; no connection, cursor, or
transaction is held open across HTTP or another await by the caller.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from . import schema
from .errors import PersistenceError

_EXPECTED_SYNCHRONOUS_FULL = 2


def open_connection(database_path: Path) -> sqlite3.Connection:
    """Open (creating if needed) the database at `database_path`.

    Creates a missing parent directory but never replaces or deletes an
    existing directory or file at the target. Verifies the required PRAGMA
    policy and applies or verifies migration version 1, all before any
    product work begins.
    """

    try:
        database_path.parent.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise PersistenceError(
            f"cannot create the database directory: {exc.__class__.__name__}"
        ) from exc

    try:
        # `check_same_thread=False`: the application workflow calls this
        # connection sequentially through separate `asyncio.to_thread()`
        # invocations, which the default executor may run on different
        # worker threads. Access is never concurrent -- each awaited call
        # completes before the next begins -- so this only disables an
        # overly strict same-thread guard, not real thread-safety.
        conn = sqlite3.connect(
            str(database_path), autocommit=True, timeout=0, check_same_thread=False
        )
    except sqlite3.Error as exc:
        raise PersistenceError(
            f"cannot open the database file: {exc.__class__.__name__}"
        ) from exc

    try:
        _configure_pragmas(conn)
        _ensure_schema_locked(conn)
    except BaseException:
        conn.close()
        raise
    return conn


def _configure_pragmas(conn: sqlite3.Connection) -> None:
    try:
        # `busy_timeout` must be set first: `sqlite3.connect(..., timeout=0)`
        # otherwise leaves every earlier statement with no lock-contention
        # tolerance at all, so a concurrent process holding the migration's
        # exclusive lock could make `journal_mode`/`synchronous` fail
        # immediately instead of waiting.
        conn.execute(f"PRAGMA busy_timeout = {schema.BUSY_TIMEOUT_MS}")
        conn.execute("PRAGMA journal_mode = DELETE")
        conn.execute("PRAGMA synchronous = FULL")
        conn.execute("PRAGMA foreign_keys = ON")

        (journal_mode,) = conn.execute("PRAGMA journal_mode").fetchone()
        (synchronous,) = conn.execute("PRAGMA synchronous").fetchone()
        (foreign_keys,) = conn.execute("PRAGMA foreign_keys").fetchone()
        (busy_timeout,) = conn.execute("PRAGMA busy_timeout").fetchone()
    except sqlite3.Error as exc:
        raise PersistenceError(
            f"cannot verify database PRAGMA policy: {exc.__class__.__name__}"
        ) from exc

    if str(journal_mode).lower() != "delete":
        raise PersistenceError(f"unexpected journal_mode: {journal_mode!r}")
    if int(synchronous) != _EXPECTED_SYNCHRONOUS_FULL:
        raise PersistenceError(f"unexpected synchronous setting: {synchronous!r}")
    if int(foreign_keys) != 1:
        raise PersistenceError("foreign_keys PRAGMA did not enable")
    if int(busy_timeout) != schema.BUSY_TIMEOUT_MS:
        raise PersistenceError(f"unexpected busy_timeout: {busy_timeout!r}")


def _ensure_schema_locked(conn: sqlite3.Connection) -> None:
    try:
        conn.execute("BEGIN EXCLUSIVE")
    except sqlite3.OperationalError as exc:
        raise PersistenceError(
            f"could not begin the schema transaction: {exc.__class__.__name__}"
        ) from exc

    try:
        schema.ensure_schema(conn)
    except sqlite3.Error as exc:
        conn.execute("ROLLBACK")
        raise PersistenceError(
            f"schema migration failed: {exc.__class__.__name__}"
        ) from exc
    except PersistenceError:
        conn.execute("ROLLBACK")
        raise

    conn.execute("COMMIT")
