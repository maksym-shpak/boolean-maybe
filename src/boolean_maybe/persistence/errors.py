"""Persistence-layer error types.

All are safe to summarize to the CLI user without leaking SQL statements,
tracebacks, or payload content; callers must render only a generic message.
"""

from __future__ import annotations


class PersistenceError(Exception):
    """A local persistence operation failed or its result was uncertain."""


class SchemaVersionError(PersistenceError):
    """The database schema is newer than supported, or does not match version 1."""


class DatabaseCorruptionError(PersistenceError):
    """Persisted data violates an invariant that the schema alone cannot enforce."""
