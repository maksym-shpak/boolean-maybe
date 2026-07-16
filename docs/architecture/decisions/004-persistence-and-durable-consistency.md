# ADR-004: Persistence and Durable Consistency

## Status

Accepted

## Date

2026-07-16

## Context

`boolean-maybe` requires one local authoritative store that survives short-lived CLI invocations, prevents duplicate external submissions across concurrent CLI processes, and preserves enough evidence to recover conservatively after interruption.

The application must durably record a Job and its `STARTED` SubmissionAttempt before an external request can begin. It must also avoid holding a database transaction open during unreliable network I/O. These requirements create two persistence boundaries around one external side effect and an unavoidable crash window between durable intent and definitive local finalization.

The assessment does not need an ORM, external database server, distributed lock service, or high-availability storage. The storage model should remain small, inspectable, and runnable on Windows, macOS, and Linux without external infrastructure.

## Decision

### Storage technology and location

* Authoritative local application state is stored in one SQLite database file using Python's standard-library `sqlite3` module.
* SQLAlchemy or another ORM is not introduced. The persistence adapter owns explicit SQL, row mapping, transactions, and migrations behind the application persistence boundary.
* Every CLI process that coordinates the same Jobs must use the same database file. The file path and its user-facing default are defined by the applicable CLI feature specification.
* The database must reside on a local filesystem accessible by all participating local processes. Network filesystems and concurrent access from different hosts are unsupported.
* The simulated external service uses separate state and must never read or write the application database.

### SQLite connection policy

Every application connection must establish and verify these settings before use:

* `PRAGMA journal_mode = DELETE` rollback-journal mode rather than WAL;
* `PRAGMA synchronous = FULL`;
* `PRAGMA foreign_keys = ON`;
* a finite busy timeout;
* SQLite autocommit mode with explicit `BEGIN IMMEDIATE`, `COMMIT`, and `ROLLBACK` statements rather than implicit legacy `sqlite3` transaction behavior.

Rollback-journal mode is selected because the workload is small and write transactions are short. It avoids requiring a particular bundled SQLite maintenance release for safe multi-process WAL behavior. A future move to WAL requires a superseding ADR with an explicit minimum SQLite version, checkpoint policy, sidecar-file handling, and cross-platform concurrency verification.

The exact busy-timeout duration is an operational setting defined by an approved feature specification. Exhausting it is a local persistence failure; the workflow must not begin an external request whose pre-side-effect transaction did not commit.

### Transaction discipline

State-changing workflows use explicit short write transactions initiated with `BEGIN IMMEDIATE`. This obtains SQLite's single-writer reservation before the workflow reads state that controls whether it may write or initiate an external effect.

Each transaction must either commit all of its state changes or roll them back. Database constraints and transaction predicates must enforce core identity, uniqueness, relationship, lifecycle, and single-active-attempt invariants even when multiple CLI processes race.

No database transaction or cursor may remain open while awaiting HTTP, retry delay, user input, or other potentially long work.

### Pre-side-effect consistency boundary

Before an external submission attempt, one `BEGIN IMMEDIATE` transaction must:

1. resolve or create the Job by `idempotency_key`;
2. validate RFC 8785 Job Entry equivalence for an existing key;
3. return an existing completed result or reject non-equivalent reuse without creating an attempt;
4. verify that the Job is currently eligible for a new attempt;
5. allocate the next unique attempt identity and attempt number;
6. insert the `SubmissionAttempt` in `STARTED`;
7. transition the Job to `SUBMITTING`;
8. commit successfully.

The external request may begin only after that commit returns successfully. A rollback, busy-timeout exhaustion, disk error, constraint failure, or uncertain commit result must stop the workflow before external submission.

The committed `SUBMITTING` Job and `STARTED` attempt are evidence that an external request was authorized and may have started. They are deliberately not evidence that the network call actually began or that remote processing occurred.

### Post-side-effect consistency boundary

After the external client returns an observation, a separate short `BEGIN IMMEDIATE` transaction must:

1. identify the same Job and active SubmissionAttempt;
2. verify that the invocation still owns the current unexpired lease and fencing generation;
3. persist the attempt's observed outcome and sanitized remote evidence;
4. transition the Job according to the domain lifecycle and approved retry policy;
5. preserve the earliest retry eligibility time when applicable;
6. commit the attempt completion and Job transition together.

If the ownership or fencing precondition fails, the stale invocation must not finalize the attempt through the normal completion path. Its observation is handled only through the explicit late-evidence workflow defined by the persistence and recovery feature specifications.

If this transaction fails or its commit result is uncertain after an external request may have occurred, the workflow must not invent a definitive local outcome or automatically send another request. Recovery treats the persisted pre-side-effect evidence conservatively.

### Multi-process concurrency

SQLite's single-writer rule and `BEGIN IMMEDIATE` serialize state-changing decisions across CLI processes. Database uniqueness constraints must cover at least:

* `Job.job_id`;
* `Job.idempotency_key`;
* `SubmissionAttempt.attempt_id`;
* the pair of `SubmissionAttempt.job_id` and `attempt_number`;
* the invariant that no Job can acquire a second active attempt while it is `SUBMITTING`.

A process that loses a race must re-read the committed authoritative state and return, wait, or report a local contention result as defined by the feature specification. It must not make a duplicate external request based on stale state.

Connections are scoped to persistence operations and are not shared concurrently across threads or processes. Because `sqlite3` is synchronous while application workflows are asynchronous under ADR-002, the persistence adapter must isolate potentially blocking database work from the event-loop thread using `asyncio.to_thread()` or an equivalent bounded adapter mechanism. This does not change SQLite's one-writer semantics.

### Active invocation ownership and fencing

Job state alone cannot distinguish a crashed `SUBMITTING` process from a live process currently awaiting HTTP. The persistence implementation therefore maintains application-internal invocation leases and fencing generations.

* Each state-changing CLI invocation uses a unique opaque invocation token and a durable lease with an expiry value that participating processes on the same host can compare safely.
* The pre-side-effect transaction associates the new `STARTED` attempt with the current invocation lease and fencing generation.
* Only the current unexpired lease owner may initiate the external request for that attempt.
* Another process that observes an unexpired owner must treat the attempt as active. It may inspect state but must not recover, finalize, or retry that attempt.
* A recovery workflow may claim an attempt only after its owner lease expires. The claim occurs in a `BEGIN IMMEDIATE` transaction and advances the fencing generation atomically.
* Lease expiry and takeover never prove that the prior request was unsent. A reclaimed attempt remains potentially sent and follows the conservative recovery evidence rules below.
* A prior owner whose lease expired or whose fencing generation is stale must not initiate another request or overwrite state finalized by the new owner. Any late remote observation must enter an explicit, auditable recovery-safe evidence path defined by the persistence and recovery feature specifications.
* Lease renewal and release are persistence coordination operations, not Job lifecycle transitions.

The exact lease duration, renewal cadence, fencing representation, time-source strategy, and late-observation workflow are defined by an approved persistence/recovery feature specification. The time-source strategy must prevent wall-clock adjustments, suspend/resume, or host reboot from causing an unsafe early takeover; a monotonic deadline requires a durable boot identity or equivalent restart rule. These behaviors must be tested with overlapping processes, forced lease expiry, clock adjustment, and restart.

### Crash recovery evidence

SQLite rollback recovery determines whether each local transaction committed; it cannot determine whether an external HTTP request began or was processed.

The recovery rules are:

* a rolled-back or absent pre-side-effect transaction means no attempt was durably authorized and no external request was allowed to begin;
* a completed attempt and matching Job transition are authoritative local evidence of the observation previously persisted by the client;
* a persisted `SUBMITTING` Job with a `STARTED` attempt and an unexpired owner lease is active and must not be recovered by another process;
* after its owner lease expires and recovery atomically claims it, the attempt is potentially sent regardless of whether interruption occurred immediately before or after the network call;
* such a Job must not return to `PENDING` or become automatically retryable;
* matching authoritative `200 processed` reconciliation evidence from ADR-003 may finalize it as successful through an approved recovery workflow;
* authoritative key-conflict evidence may finalize it as a permanent failure through an approved recovery workflow;
* `404 not_found`, timeout, disconnect, `5xx`, rate limiting, missing evidence, or failed reconciliation cannot exclude prior processing and therefore preserve an ambiguous outcome.

The feature specification defines when reconciliation is attempted and how recovery results are presented, but it may not weaken these evidence rules.

### Schema evolution

* The application owns an ordered, monotonic sequence of schema migrations stored with the source code.
* The current schema version is recorded in SQLite's application-managed `PRAGMA user_version` field.
* Database initialization and pending migrations run before product workflows.
* Migration coordination uses an exclusive schema-change transaction so only one process can migrate at a time; other processes wait within the finite busy timeout or fail safely.
* Each migration and its `user_version` update must commit atomically. A failed migration rolls back and prevents product workflows from running.
* The application refuses to open a database whose schema version is newer than it supports.
* Destructive or lossy migrations require an accepted ADR or approved feature specification and an explicit backup/rollback plan.

The physical tables, columns, indexes, and SQL migration contents are implementation contracts defined by the persistence feature specification, not by this ADR.

### Retention

Retention and deletion policy remains deliberately deferred. Until an approved decision defines it, the application performs no automatic deletion or compaction of Jobs, SubmissionAttempts, or evidence required for diagnostics and recovery.

SQLite maintenance such as `VACUUM`, archival, export, or history pruning must not be introduced implicitly as part of normal workflow execution.

## Consequences

Benefits:

* The application gets transactional local durability and multi-process locking without external infrastructure or a runtime dependency.
* Explicit pre- and post-side-effect transactions make the HTTP crash windows visible and reviewable.
* Database constraints provide a final duplicate-prevention boundary when concurrent processes race.
* Durable leases and fencing prevent recovery from confusing an active HTTP attempt with an abandoned one.
* Standard-library `sqlite3` keeps the assessment small and exposes transaction behavior directly.
* Versioned migrations allow the persisted model to evolve without silently reinterpreting existing state.

Trade-offs:

* SQLite permits only one writer at a time; concurrent CLI processes may wait or fail with local contention.
* Rollback-journal mode can briefly block readers during writer commit and provides less read/write concurrency than WAL.
* Synchronous `sqlite3` calls require bounded thread offloading from asynchronous workflows.
* Lease expiry and fencing add internal persistence metadata, clock-based coordination, and late-observation edge cases.
* There is no transaction spanning SQLite and HTTP. A committed `STARTED` attempt therefore remains conservative evidence, not proof that the request was sent.
* Without a retention decision, local history and the database file can grow over time.
* Explicit SQL and migrations require discipline that an ORM might otherwise partially automate.

## Impact on Core Entities

This decision does not add or change a core entity, required field, lifecycle state, relationship, ownership rule, or compatibility rule.

Invocation leases and fencing generations are persistence-internal coordination records. They do not identify a Job or SubmissionAttempt, do not change domain ownership, and must not be exposed as authoritative product state.

It defines the persistence mechanism that enforces existing contracts:

* unique local Job and SubmissionAttempt identities;
* unique local `idempotency_key` binding;
* monotonically increasing, per-Job attempt numbers;
* one active `STARTED` attempt per Job;
* atomic local transition into `SUBMITTING` before an external side effect;
* append-oriented completed attempt history.

No update to `docs/domain/core-entities.md` is required on acceptance unless an implementation specification proposes a new stable field or changes an existing invariant.

## Alternatives Considered

1. **SQLAlchemy over SQLite.**
   Not selected because the assessment has one local store and a small stable domain model. An ORM would add a runtime dependency and abstraction without removing the need to reason explicitly about transactions, locking, migrations, and HTTP crash windows.

2. **Aiosqlite or another asynchronous SQLite wrapper.**
   Not selected in this ADR because standard-library `sqlite3` plus bounded thread isolation satisfies ADR-002 without adding a runtime dependency. A later implementation may justify a wrapper only through a superseding ADR.

3. **SQLite WAL mode.**
   Not selected because the assessment does not require its additional read/write concurrency, WAL adds checkpoint and sidecar-file concerns, and safe multi-process use depends on the bundled SQLite maintenance version. Short rollback-journal transactions are the more conservative default.

4. **A JSON or append-only file store.**
   Rejected because cross-process locking, uniqueness, atomic multi-record transitions, indexing, and schema evolution would need custom implementation.

5. **A client-server database.**
   Rejected because it adds external infrastructure and operational scope disproportionate to a local assessment CLI.

6. **Holding one transaction open across the HTTP request.**
   Rejected because unreliable network waits would hold the single writer lock, amplify contention, and still could not provide an atomic commit across SQLite and the remote service.

7. **Defaulting interrupted `STARTED` attempts back to retryable.**
   Rejected because local persistence cannot prove that the external request did not begin or was not processed.

8. **Automatic destructive schema migration or history pruning.**
   Rejected because data loss, compatibility, and retention require explicit decisions and recovery plans.

## Scope

This decision applies to:

* the local persistence technology and adapter boundary;
* SQLite connection, journal, durability, and transaction policy;
* local multi-process writer coordination;
* active invocation leases and fencing during external side-effect windows;
* durable boundaries before and after an external submission;
* crash-recovery evidence classification;
* schema versioning and migration coordination;
* deliberate deferral of retention and deletion.

This decision does not select or define:

* physical table, column, index, or migration SQL;
* the user-facing database path or configuration option;
* exact busy-timeout duration or contention output contract;
* exact lease duration, renewal cadence, fencing schema, time-source/boot-identity mechanism, or late-observation presentation;
* retry counts, backoff, rate-limit, or timeout formulas;
* when reconciliation is automatically or manually invoked;
* retention duration, archival, backup command, or deletion policy;
* simulator persistence, which remains separate from application state.

## Follow-up on Acceptance

If this ADR is accepted:

* update `docs/architecture/architecture-overview.md` with SQLite/`sqlite3`, rollback-journal durability, `BEGIN IMMEDIATE` writer serialization, pre/post HTTP transaction boundaries, crash-evidence rules, and migration ownership;
* remove or narrow the deferred architecture questions about local persistence technology and crash-recovery algorithm while retaining retention as deferred;
* add this ADR to the overview's Related Architecture Decisions section;
* require the persistence feature specification to define schema SQL, constraints, migration contents, connection setup verification, bounded async offloading, invocation leases, fencing, and multi-process/crash-boundary tests;
* require tests for concurrent same-key submission, writer contention, live-owner protection, lease expiry and takeover, stale-owner fencing, late remote observations, crash before pre-side-effect commit, crash after pre-side-effect commit, crash after remote processing before finalization, uncertain finalization commit, migration races, rollback on migration failure, and refusal of newer schema versions.

## Related Documents

* `docs/product/product-brief.md`
* `docs/product/glossary.md`
* `docs/domain/core-entities.md`
* `docs/architecture/architecture-overview.md`
* `docs/architecture/decisions/001-python-runtime-packaging-and-development-tooling.md`
* `docs/architecture/decisions/002-execution-model-and-application-boundaries.md`
* `docs/architecture/decisions/003-simulated-external-service-contract.md`

External technical references:

* [Python `sqlite3` documentation](https://docs.python.org/3.12/library/sqlite3.html)
* [SQLite transaction semantics](https://www.sqlite.org/lang_transaction.html)
* [SQLite WAL documentation](https://www.sqlite.org/wal.html)
* [SQLite PRAGMA reference](https://www.sqlite.org/pragma.html)

Supersedes: none

Superseded by: none
