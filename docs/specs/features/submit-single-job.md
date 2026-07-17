# Feature Specification: Submit a Single Job

## Status

Implemented

## Summary

Implement the first end-to-end application vertical for one Job Entry: the CLI validates input, resolves an Idempotency Key, durably records the authoritative local Job and a `STARTED` SubmissionAttempt before HTTP, submits once to the default-success Simulated External Service, atomically records success, and returns a stable JSON result. A later invocation with the same key and an equivalent Job Entry returns the persisted successful result without another SubmissionAttempt or HTTP request.

## References / Context

* `docs/product/product-brief.md`
* `docs/product/glossary.md`
* `docs/domain/core-entities.md`
* `docs/architecture/architecture-overview.md`
* `docs/architecture/decisions/001-python-runtime-packaging-and-development-tooling.md`
* `docs/architecture/decisions/002-execution-model-and-application-boundaries.md`
* `docs/architecture/decisions/003-simulated-external-service-contract.md`
* `docs/architecture/decisions/004-persistence-and-durable-consistency.md`
* `docs/architecture/decisions/005-identity-and-duplicate-prevention.md`
* `docs/architecture/decisions/006-retry-rate-limits-ambiguity-and-recovery.md`
* `docs/specs/features/simulated-external-service.md`

## Problem

The repository has an installable CLI bootstrap and a working Simulated External Service, but it does not yet prove the product's foundational path through validation, durable local state, one external side effect, authoritative state transition, and machine-readable output. Without this vertical, later retry, Batch, reconciliation, and recovery work has no tested single-Job workflow to reuse.

## Goal

Provide a small, durable, automation-friendly `submit` command that proves this sequence:

```text
CLI -> validation -> persistence -> HTTP -> state transition -> JSON result
```

The implementation must establish the correct boundaries for later reliability work without implementing the complete retry engine.

## Non-goals

* Submission retry, retry delay, `Retry-After`, service-wide rate-limit coordination, or the three-attempt engine.
* Automatic or explicit Reconciliation.
* Recovery or takeover of an interrupted `SUBMITTING` Job.
* Batch input or concurrent Job orchestration within one CLI invocation.
* Inspection, listing, manual resolution, deletion, retention, backup, or database maintenance commands.
* File or standard-input Job Entry input, multiple output formats, or human-oriented rendering.
* Authentication, authorization, TLS, non-loopback external services, or production deployment.
* Arbitrary Job Entry field schemas beyond accepting one canonicalizable JSON object.
* Structured operational logging; this feature defines command results and concise diagnostics only.
* Changing the simulator contract, Job or SubmissionAttempt fields, or lifecycle states.

## Users / Actors

* A shell user or automation process submitting one Job Entry.
* The asynchronous single-Job submission workflow.
* The local SQLite persistence adapter.
* The external-service HTTP client.
* The separately running Simulated External Service.

## Scope

In scope:

* one `submit` subcommand and one inline Job Entry;
* optional user-provided Idempotency Key and workflow-owned key generation;
* RFC 8785 validation, canonicalization, equivalence, and SHA-256 digest calculation;
* initial SQLite schema and migration version `1` for Jobs and SubmissionAttempts;
* separate durable transactions before and after one HTTP request;
* one `POST /jobs` request with a ten-second deadline;
* authoritative `201` or equivalent-replay `200` success classification;
* `PENDING -> SUBMITTING -> SUCCEEDED` and `STARTED -> SUCCEEDED` persistence;
* local `already_completed` replay from a matching `SUCCEEDED` Job;
* stable JSON command results and exit codes;
* unit, persistence, HTTP contract, subprocess, race, and crash-boundary tests;
* concise README documentation for the command and local state.

Out of scope:

* completing or retrying a Job after any non-success HTTP or transport observation;
* starting work for an existing Job in `PENDING`, `SUBMITTING`, `RETRY_SCHEDULED`, `FAILED_PERMANENT`, or `AMBIGUOUS`;
* lease expiry takeover, fencing-generation advancement, or late-evidence persistence;
* schema support for the deferred service-wide rate-limit gate or retry eligibility beyond existing core fields.

### Explicit approval decisions

Changing this specification to `Approved` explicitly confirms both decisions below; they must not be treated as incidental consequences of approving the rest of the feature:

1. **Staged service-wide rate-limit gate exception.** Migration version `1` has no service-gate record because this vertical cannot observe or persist `429` handling and never creates a retry. Its pre-side-effect transaction therefore omits the literal ADR-004/ADR-006 service `not_before` gate lookup and treats the absent gate as having no restriction. This is a narrow, conscious staging exception to the requirement that every submission authorization check the durable gate. It is safe only while no implemented workflow can establish gate state. The feature that first handles `429`, retry eligibility, or Reconciliation must add the durable gate in a later migration and make its check a prerequisite for every first or repeated submission before that feature is accepted.
2. **Initial lease duration.** Version `1` uses a fixed 60-second, non-renewed invocation lease. Approval confirms this initial coordination value with the conservative no-takeover behavior defined below. A later recovery feature must review whether to retain or migrate this duration when it introduces renewal, expiry takeover, boot/time-source handling, and late evidence.

## Expected Behavior

### Command contract

The installed command syntax is:

```text
boolean-maybe submit --job-entry JSON [--idempotency-key KEY] [--database PATH] [--service-url URL]
```

Rules:

* `submit` accepts exactly one `--job-entry` value. The value is one UTF-8 JSON document encoded by the operating system's argument handling.
* `--idempotency-key` is optional. The CLI adapter passes it unchanged to the application request; key validation and generation belong to the workflow.
* `--database` defaults to `.boolean-maybe/boolean-maybe.sqlite3` relative to the invocation's current working directory. Relative explicit paths are also resolved from that directory. The adapter creates a missing parent directory but does not replace a directory or non-database file at the target.
* `--service-url` defaults to `http://127.0.0.1:8080`. It accepts only an `http` origin with a loopback IP literal, an explicit or default port, and no credentials, query, fragment, or non-root path. Hostnames, including `localhost`, are rejected.
* Unknown options, missing values, extra positional arguments, and help follow the standard parser contract. Help exits `0`; invalid command syntax exits `2`.
* The synchronous CLI adapter calls one asynchronous application workflow through `asyncio.run()` exactly once. It does not call persistence or HTTP directly.

No environment variable silently overrides these values in this feature.

### Job Entry validation and equivalence

The inline value must:

* be valid JSON whose root is an object;
* contain no duplicate member names at any nesting level;
* be no more than 1 MiB when encoded as UTF-8;
* satisfy the I-JSON constraints used by the simulator, including finite numbers, integers in `[-(2^53)+1, (2^53)-1]`, and valid Unicode scalar values;
* be accepted by the existing direct `rfc8785` dependency.

The workflow canonicalizes the complete object to RFC 8785 UTF-8 bytes. Those bytes are the persisted payload representation and the equivalence value. The payload digest is `sha256:` followed by 64 lowercase hexadecimal SHA-256 characters over the canonical bytes.

Strict JSON parsing, I-JSON tree validation, RFC 8785 canonicalization, and digest calculation must live in one application-neutral module used by both the CLI application and simulator. The implementation may move existing simulator helpers into that shared module and retain compatibility imports, but it must not maintain two independent implementations of these rules. Simulator state, scenario selection, HTTP parsing, and response behavior remain simulator-owned and must not enter the shared module.

Invalid JSON, a duplicate member, a non-object root, oversized input, or non-canonicalizable input is rejected before opening the database, creating a Job, or initiating HTTP.

This first feature defines no required or optional business fields inside a Job Entry. Any canonicalizable JSON object, including `{}`, is valid.

### Idempotency Key contract

A user-provided key uses the simulator-compatible grammar:

* 1 through 128 ASCII characters;
* only `A-Z`, `a-z`, `0-9`, `.`, `_`, `~`, and `-`;
* exact, case-sensitive comparison with no trimming, case-folding, decoding, or normalization.

When no key is supplied, the workflow generates:

```text
job_<32 lowercase hexadecimal characters>
```

The suffix is produced from a cryptographically secure random 128-bit value. It is not derived from the Job Entry. A generated collision with any persisted key causes a new value to be generated before Job creation. Tests inject the generator and prove both pre-read and uniqueness-race collision handling.

A supplied invalid key is rejected before persistence or HTTP. A supplied key already bound to a non-equivalent canonical payload returns an idempotency conflict before creating an attempt or sending HTTP. An intentional equivalent reuse follows the existing Job-state rules below.

### Local identities and timestamps

New identifiers use independent cryptographically secure UUID version 4 values serialized as lowercase canonical strings:

* `job_id`: `xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx`;
* `attempt_id`: the same representation;
* internal invocation token: the same representation.

Identifier generation is injectable for collision tests. A `job_id` or `attempt_id` collision causes regeneration; it never reuses another entity.

Timestamps are UTC RFC 3339 strings with six fractional digits and the `Z` suffix. The workflow uses one injectable UTC clock and enforces `updated_at >= created_at` and `completed_at >= started_at` even if the supplied test clock returns equal instants.

### SQLite database and migration version 1

Use standard-library `sqlite3`; add no ORM or database runtime dependency. Every connection must establish and verify:

```text
PRAGMA journal_mode = DELETE
PRAGMA synchronous = FULL
PRAGMA foreign_keys = ON
PRAGMA busy_timeout = 5000
```

Connections use autocommit mode and explicit transactions. Product writes use `BEGIN IMMEDIATE`. No connection, transaction, or cursor remains open across HTTP or another await. Potentially blocking adapter operations run through `asyncio.to_thread()`; a workflow performs them sequentially, so this feature creates no unbounded database task set.

Initialization uses an exclusive schema transaction. Version `0` creates version `1` atomically and sets `PRAGMA user_version = 1`. Version `1` opens without mutation. A newer version, a failed migration, or an object whose schema does not match version `1` fails before product work. No destructive migration, deletion, compaction, or `VACUUM` is performed.

Migration `1` creates these application-owned tables and constraints (equivalent SQL layout is allowed only when it preserves every named column and constraint):

```sql
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
);

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
);

CREATE UNIQUE INDEX one_started_attempt_per_job
ON submission_attempts(job_id)
WHERE state = 'STARTED';
```

`payload_canonical` must decode to the complete canonical JSON object before a stored Job is used. Invalid persisted bytes or an impossible row combination is database corruption and must not authorize HTTP.

`owner_token`, `fencing_generation`, and `lease_expires_at` are persistence coordination metadata, not core-entity fields or command output. Initial fencing generation is `1`. This feature uses a 60-second lease and does not renew or take over a lease. The ten-second HTTP deadline leaves bounded time for normal finalization. Every normal completion transaction must match the owner token and fencing generation and observe an unexpired lease. If the lease is lost or expires, the workflow must not update the Job or attempt; it returns a safe operational failure and leaves the durable `SUBMITTING`/`STARTED` evidence for a later recovery feature. Another invocation never treats expiry as permission to submit, reconcile, finalize, or advance fencing in this feature. Consequently, wall-clock adjustment, suspend/resume, or reboot may conservatively strand work but cannot cause early takeover or another HTTP request.

### Pre-side-effect transaction

After validation, the workflow opens one short `BEGIN IMMEDIATE` transaction and resolves the exact key:

1. If the key was generated for this invocation and already exists, roll back, generate a new key, and repeat. Never reuse the winning Job.
2. If a supplied key has no Job, insert a new `PENDING` Job with the canonical payload.
3. If a supplied key has a Job with a non-equivalent payload, commit no changes and return `idempotency_conflict`.
4. If an equivalent existing Job is `SUCCEEDED`, read its successful attempt evidence, commit no changes, and return `already_completed`.
5. If an equivalent existing Job has any other state, commit no changes and return the state-specific `job_not_eligible` result below. Duplicate detection alone does not authorize HTTP.
6. For a newly inserted `PENDING` Job, allocate attempt number `1`, create one `STARTED` SubmissionAttempt with the current invocation ownership metadata, transition the Job to `SUBMITTING`, and commit all changes together.

The external request may begin only after step 6 commits successfully and the current invocation still owns the unexpired lease. A rollback, busy-timeout exhaustion, disk error, constraint failure, migration failure, or uncertain commit result stops before HTTP.

This feature never creates attempt number greater than `1`.

### HTTP request and success validation

The external-service adapter sends exactly one request:

```text
POST /jobs HTTP/1.1
Content-Type: application/json
Idempotency-Key: <exact persisted key>
Content-Length: <canonical byte length>

<canonical Job Entry bytes>
```

Rules:

* use Python standard-library HTTP facilities isolated from the event-loop thread; add no HTTP dependency;
* apply one ten-second deadline covering connect, request transmission, and receipt of the complete response;
* disable redirects and do not send the payload to another origin;
* accept at most 64 KiB of response body; a larger or incomplete response is not authoritative success;
* require UTF-8 JSON, one object, no duplicate members, and the exact simulator success fields and types;
* accept only `201` with `replayed: false` or `200` with `replayed: true`;
* require `status = "processed"`, exact matching `idempotency_key`, exact matching locally computed `payload_digest`, and a string `remote_request_id`;
* ignore no mismatch: an unexpected status, schema, replay flag, key, or digest is not success.

The workflow classifies only public HTTP evidence; it never reads simulator scenario configuration.

### Post-side-effect success transaction

After authoritative success, a separate short `BEGIN IMMEDIATE` transaction must atomically:

1. re-read the exact Job and attempt;
2. require Job state `SUBMITTING`, attempt state `STARTED`, matching invocation token, fencing generation `1`, and an unexpired lease;
3. update the attempt to `SUCCEEDED`, set `completed_at`, `http_status` to the observed `201` or `200`, set the observed `remote_request_id`, keep `error_category` and `retry_after_ms` null, and clear ownership/lease columns;
4. update the Job to `SUCCEEDED` and set `updated_at` to the same completion instant;
5. commit both changes together.

Only after this commit succeeds may the CLI report success. If it fails or the commit result is uncertain, the CLI returns an operational failure and must not issue another HTTP request. It must not claim that the Job failed remotely.

### Non-success and interrupted paths in this vertical

This feature deliberately does not implement the ADR-006 retry and Reconciliation engine. Therefore any HTTP status, transport failure, timeout, cancellation after possible dispatch, malformed response, lost lease, or failed/uncertain post-side-effect commit that is not the authoritative success path above:

* must not produce `outcome = "succeeded"` or `already_completed`;
* must not initiate a second submission or a Reconciliation request;
* must not reset the Job to `PENDING`, create another attempt, or overwrite the `STARTED` attempt;
* returns the `submission_incomplete` operational result when the CLI remains able to render a result;
* leaves the committed `SUBMITTING` Job and `STARTED` attempt unchanged for a later approved recovery feature.

Cancellation before the pre-side-effect commit propagates without persisted product state. Cancellation after the commit follows the conservative rule above. These interim behaviors are safe but intentionally incomplete; later features must replace them with the ADR-006 classifications and fenced recovery without changing this feature's success and replay contracts.

### Existing Job behavior

For a supplied key and equivalent payload:

| Existing Job state | Behavior | HTTP or new attempt |
| --- | --- | --- |
| `SUCCEEDED` | Return `already_completed` from the persisted successful attempt. | None |
| `PENDING` | Return `job_not_eligible`. | None |
| `SUBMITTING` | Return `job_not_eligible`. | None |
| `RETRY_SCHEDULED` | Return `job_not_eligible`. | None |
| `FAILED_PERMANENT` | Return `job_not_eligible`. | None |
| `AMBIGUOUS` | Return `job_not_eligible`. | None |

A `SUCCEEDED` Job must have exactly one successful attempt usable by this feature. Missing or contradictory evidence is database corruption, not `already_completed`.

The stored replay is materialized without HTTP from:

* persisted Job identity, key, state, and canonical payload;
* recomputed payload digest;
* the successful attempt's attempt identity, number, HTTP status, and remote request ID.

### JSON output

Every product result is one compact UTF-8 JSON object followed by one newline on stdout. Key order is not a contract. No progress or diagnostic text is written to stdout.

Fresh success:

```json
{
  "outcome": "succeeded",
  "submitted": true,
  "job_id": "<local UUID>",
  "idempotency_key": "<resolved key>",
  "state": "SUCCEEDED",
  "attempt": {
    "attempt_id": "<local UUID>",
    "attempt_number": 1,
    "http_status": 201,
    "remote_request_id": "remote-<value>"
  },
  "result": {
    "status": "processed",
    "payload_digest": "sha256:<digest>",
    "remote_request_id": "remote-<value>"
  }
}
```

The fresh path may contain `attempt.http_status = 200` when the simulator already holds an equivalent record for a newly created local Job. This is still `outcome = "succeeded"` and `submitted = true` because the current CLI invocation made the HTTP request.

Stored replay uses the same schema, identities, state, attempt evidence, and result evidence, with only:

```json
{
  "outcome": "already_completed",
  "submitted": false
}
```

The snippet shows the two changed fields, not a partial response.

Expected non-success product results use:

```json
{
  "outcome": "idempotency_conflict | job_not_eligible | submission_incomplete",
  "submitted": "<boolean>",
  "idempotency_key": "<supplied or resolved key>",
  "state": "<persisted state or null>",
  "error": {
    "code": "<same value as outcome>",
    "message": "<concise safe message>"
  }
}
```

For `submission_incomplete` after the pre-side-effect commit, `submitted` is `true` only when request dispatch may have begun; it is `false` when the adapter proves it did not begin. Uncertain evidence defaults to `true`. `state` reports the persisted `SUBMITTING` state, not a guessed remote outcome. Expected error objects never contain the Job Entry, raw response body, database statement, traceback, or secret-bearing headers.

For `idempotency_conflict`, `submitted` is exactly `false` and `state` is exactly `null`; the command does not disclose the state of the differently bound Job. For `job_not_eligible`, `submitted` is exactly `false` and `state` is the persisted state of the equivalent Job.

Parser/validation failures may use the same error envelope without `idempotency_key` or `state` when unavailable. Concise human-safe diagnostics may also be written to stderr; traceback output is disabled for expected failures.

### Exit codes

The stable process exit codes are:

| Code | Meaning |
| ---: | --- |
| `0` | `succeeded`, `already_completed`, or help. |
| `1` | A valid command could not complete successfully: `idempotency_conflict`, `job_not_eligible`, `submission_incomplete`, local persistence failure, HTTP failure, or unexpected internal failure. |
| `2` | Command syntax, Job Entry, Idempotency Key, database path, or service URL validation failed before product work. |

No other exit code is intentionally returned by normal command handling.

## System Flow

```mermaid
sequenceDiagram
  participant C as CLI
  participant W as Submission Workflow
  participant P as SQLite Persistence
  participant H as External Service Client
  participant S as Simulated External Service

  C->>W: Job Entry, optional key, database, service origin
  W->>W: Parse, validate, canonicalize, resolve key
  W->>P: BEGIN IMMEDIATE; resolve/create Job
  alt Matching SUCCEEDED Job
    P-->>W: Persisted successful evidence
    W-->>C: already_completed JSON; exit 0
  else New eligible Job
    W->>P: Insert Job + STARTED attempt; set SUBMITTING; COMMIT
    W->>H: Submit exact persisted key and canonical payload
    H->>S: POST /jobs
    S-->>H: Authoritative 201 or replay 200
    H-->>W: Validated success evidence
    W->>P: BEGIN IMMEDIATE; complete attempt + Job; COMMIT
    W-->>C: succeeded JSON; exit 0
  end
```

## Acceptance Criteria

### CLI and validation

* Given one canonicalizable JSON object, when `submit` is invoked against the default-success simulator, then the command emits exactly one success JSON object and exits `0`.
* Given missing, malformed, duplicate-member, non-object, oversized, non-I-JSON, or non-canonicalizable input, then the command exits `2`, emits no success result, creates no database product rows, and sends no HTTP request.
* Given valid and invalid database paths or service URLs, then defaults and validation behave exactly as specified and invalid values send no HTTP request.
* Given help, unknown options, missing option values, or extra arguments, then parser output and exit codes are stable and no product work begins.

### Identity and duplicate prevention

* Given no key, then the workflow generates the specified non-payload-derived key, returns it, persists it, and transmits it unchanged.
* Given a valid supplied key, then the exact value is returned, persisted, and transmitted unchanged.
* Given generated key, Job ID, or attempt ID collisions, including a uniqueness race, then generation repeats without reusing another entity or sending from stale state.
* Given a supplied key bound to a non-equivalent payload, then the command returns `idempotency_conflict`, exits `1`, creates no attempt, preserves the original Job, and sends no HTTP request.
* The `idempotency_conflict` result contains `submitted = false` and `state = null`; it does not disclose the conflicting Job's state.
* Given concurrent processes using the same supplied key and equivalent payload, then exactly one local Job and one SubmissionAttempt are created and at most one HTTP request is sent; the loser re-reads authoritative state and returns either the completed replay or `job_not_eligible` without sending.

### Durable side-effect boundaries

* Given a new valid submission, then the database contains the Job, its `STARTED` attempt, `SUBMITTING` state, and invocation ownership before the simulator observes the request.
* Given failure or uncertain commit before the pre-side-effect transaction completes, then no HTTP request begins.
* Given authoritative success, then the post-side-effect transaction atomically records attempt `SUCCEEDED`, Job `SUCCEEDED`, completion evidence, and cleared ownership.
* Given post-side-effect transaction failure, uncertain commit, lease loss, or stale fencing, then the CLI does not report success and does not issue another request.
* Given any unsupported non-success observation after dispatch may have begun, then durable state remains `SUBMITTING`/`STARTED`, `submission_incomplete` is returned when possible, and no retry or Reconciliation request occurs.
* Given a process terminated after the pre-side-effect commit, then a later invocation never interprets the existing Job as eligible and never sends another HTTP request.

### HTTP success contract

* Given simulator `201` with matching authoritative fields, then the workflow records and returns success.
* Given simulator `200` equivalent replay for a new local Job, then the workflow records and returns success with `submitted = true` and the observed `http_status = 200`.
* Given mismatched key or digest, wrong replay flag, malformed or oversized body, redirect, unexpected status, timeout, disconnect, or incomplete response, then the workflow does not record or report success.
* Given a remote request ID equal to one stored for another Job, then both Jobs remain distinct and no uniqueness error occurs.

### Stored result replay

* Given an existing `SUCCEEDED` Job, the same supplied key, and an RFC 8785-equivalent Job Entry with different member order or whitespace, then the CLI returns `already_completed`, `submitted = false`, the stored identities/evidence, and exit `0` without creating a Job or attempt or making HTTP.
* Given corrupted or incomplete persisted successful evidence, then the command returns an operational failure rather than inventing `already_completed`.
* Given any equivalent non-`SUCCEEDED` Job, then the command returns `job_not_eligible`, reports the persisted state, creates no attempt, and sends no HTTP request.
* Every `job_not_eligible` result contains `submitted = false` and the exact persisted state of the equivalent Job.

### Persistence and migration

* Given a missing database and parent directory, then migration `1` initializes them before product work and the resulting database passes all required PRAGMA and schema checks.
* Given two processes racing to initialize, then one valid version-1 schema results and neither observes a partial migration.
* Given version `1`, reopening is non-destructive; given a newer version, incompatible schema, migration error, or five-second busy-timeout exhaustion, product work fails safely without HTTP.
* A read-only or otherwise unwritable database location, a directory at the database-file path, busy-timeout exhaustion, migration failure, schema mismatch, and other runtime persistence failures each exit `1`, never `2`, after command/path syntax has already validated.
* Given process restart after success, then the persisted Job and attempt still materialize the same `already_completed` result.

### Output and exit codes

* Every fresh success and stored replay exactly satisfies its JSON schema, writes one newline-terminated object to stdout, and exits `0`.
* Expected invalid input exits `2`; valid-command operational and product failures exit `1`; expected failures emit no traceback or sensitive payload/response content.
* `submitted` describes only whether the current invocation initiated or may have initiated HTTP; it never describes historical submission by another invocation.

## Edge Cases

* Empty `{}` Job Entry and nested objects/arrays at canonicalization boundaries.
* Equivalent member ordering, insignificant whitespace, Unicode escapes, and RFC 8785 numeric representations.
* Duplicate JSON members below the root; invalid Unicode; non-finite or out-of-range numeric input.
* Keys at 1 and 128 characters; forbidden `*`, whitespace, non-ASCII, separators, and 129-character values.
* Explicit relative and absolute database paths, missing parent directory, read-only location, directory-as-file target, and lock contention.
* Simulator already holding an equivalent record while the local database has none.
* Simulator restart after local success; local replay must not call the reset simulator.
* Two processes racing on the same key before and after the winning pre-side-effect commit.
* Collision injection before insert and uniqueness collision at insert for all generated identities.
* Process interruption immediately before commit, immediately after commit, during HTTP, after remote processing, and during finalization.
* HTTP response exactly at and above 64 KiB, duplicate response members, wrong content type, wrong key/digest, absent remote request ID, and redirect response.
* Forward wall-clock adjustment, suspend/resume, or lease expiry during HTTP; these fail conservatively without takeover.
* Duplicate `remote_request_id` values across different Jobs.

## Affected Areas

* CLI adapter: `src/boolean_maybe/cli.py`
* Application workflow and request/result types: new modules under `src/boolean_maybe/application/`
* Domain validation and state transition rules: new modules under `src/boolean_maybe/domain/`
* Shared strict JSON, I-JSON, RFC 8785, and digest behavior: one application-neutral module used by both application and simulator code
* SQLite persistence and migration: new modules under `src/boolean_maybe/persistence/`
* External-service client: new modules under `src/boolean_maybe/external/`
* Tests: new CLI, workflow, persistence, HTTP, race, and subprocess tests under `tests/`
* Documentation: root `README.md`; after human approval, synchronization of the resolved feature-level questions in `docs/product/product-brief.md`, `docs/domain/core-entities.md`, and `docs/architecture/architecture-overview.md`
* Packaging: no new entry point or dependency; existing `boolean-maybe` and `rfc8785` declarations remain.

## Related Architecture Decisions

* ADR-001: installed CLI, Python 3.12, dependency policy, and locked verification.
* ADR-002: synchronous CLI and one asynchronous application workflow boundary.
* ADR-003: HTTP submission, canonical equivalence, and authoritative success evidence.
* ADR-004: SQLite, migrations, transaction boundaries, ownership, and fencing.
* ADR-005: identity roles, generated/supplied key behavior, conflict prevention, and stored replay.
* ADR-006: ten-second request deadline and conservative treatment of uncertain observations; its retry and Reconciliation engine is deferred.

## Affected Core Entities

This feature persists and transitions the existing `Job` and `SubmissionAttempt` core entities without changing their stable contracts.

* `Job`: creates all required fields, stores the immutable canonical representation of `payload`, transitions `PENDING -> SUBMITTING -> SUCCEEDED`, and reuses a matching existing `SUCCEEDED` Job.
* `SubmissionAttempt`: creates one attempt with number `1`, transitions `STARTED -> SUCCEEDED`, and stores timestamps, HTTP status, and optional non-unique Remote Request ID.

The internal ownership, fencing, and lease columns are persistence coordination metadata and do not become core-entity fields. `payload_digest` is recomputed evidence and does not become authoritative identity or a required persisted field. No update to `docs/domain/core-entities.md` is required if implementation preserves this contract.

After human approval, the open-question list in `docs/domain/core-entities.md` must be narrowed to record that this feature defines the first single-Job payload as any canonicalizable JSON object, the 1-to-128-character Idempotency Key grammar, and inline JSON as the only input method for this command. This is documentation synchronization, not a core-entity contract change.

## Data / State Changes

This is the first authoritative application persistence schema. It creates durable Job and SubmissionAttempt records in SQLite migration version `1`. Successful state and attempt evidence survive CLI restart. There is no prior application data to migrate, and no automatic deletion or compaction is added.

The only fully completed lifecycle path in this feature is:

```text
Job: PENDING -> SUBMITTING -> SUCCEEDED
SubmissionAttempt: STARTED -> SUCCEEDED
```

Unsupported or interrupted post-dispatch paths deliberately preserve `SUBMITTING`/`STARTED` for later recovery rather than inventing a terminal outcome.

After human approval, the product brief's open questions about the exact Job Entry fields and required CLI input methods must be narrowed to the decisions in this feature. The architecture overview needs only a compact reference to the new concrete command/schema contract; no new ADR or architectural pattern is required.

## API / Interface Changes

* The existing `boolean-maybe` entry point gains the `submit` subcommand and the options defined above.
* Stdout gains the documented JSON result schemas.
* Process exit codes `0`, `1`, and `2` gain the documented stable meanings.
* The application initiates the existing simulator `POST /jobs` contract.
* The local SQLite schema version becomes `1`.

## Security / Permissions

* Job Entry content is accepted as trusted-local user input but treated as potentially sensitive. It is persisted because it is the authoritative Job payload, but it is not written to diagnostics or error results.
* The HTTP client accepts only an unauthenticated loopback `http` origin and never follows redirects.
* The database may contain sensitive payloads. The README must state that this feature does not provide application-level encryption or permission hardening and that users must protect the file with local filesystem permissions.
* Idempotency keys and remote request IDs are identifiers, not credentials. Expected diagnostics should prefer the Job ID and must not expose full payloads, raw response bodies, SQL, or tracebacks.
* Database files, parent directories, or existing contents are never deleted or overwritten to recover from an error.

## Copy / Terminology

Use glossary terms `Job`, `Job Entry`, `Idempotency Key`, `SubmissionAttempt`, `Remote Request ID`, and `Simulated External Service`. Use exact lifecycle enum values in uppercase and exact outcome/error codes in lowercase `snake_case`. Do not describe local replay as a new submission, remote request IDs as unique identity, or `submission_incomplete` as proof of remote failure.

## Test Expectations

Tests must cover every acceptance criterion at the lowest practical level and include:

* strict JSON parsing, I-JSON validation, RFC 8785 conformance/equivalence, size limits, and digest calculation;
* key grammar, generated format, provenance, and deterministic collision injection;
* synchronous CLI-to-async-workflow entry exactly once and stable stdout/stderr/exit-code subprocess behavior;
* migration SQL, PRAGMA verification, constraints, row mapping, corruption detection, reopen, newer-version refusal, rollback, and five-second contention timeout;
* pre/post transaction atomicity and proof that no HTTP begins before durable authorization;
* real loopback integration with simulator `201`, simulator `200` replay, malformed success evidence, duplicate remote IDs, and local stored replay;
* concurrent-process same-key races using the same database file, with deterministic coordination rather than timing-only assertions;
* crash-boundary subprocess tests that always clean up child processes and verify persisted state;
* injected clock, ID/key generators, HTTP adapter, and persistence faults without real ten-second waits;
* Windows, macOS, and Linux compatible path, process, SQLite-locking, and HTTP behavior.

Required repository verification:

```text
uv sync --locked
uv lock --locked
uv run --locked ruff check .
uv run --locked ruff format --check .
uv run --locked pyright
uv run --locked pytest -q
uv build
```

The implementation handoff must report every command and exit status. Tests must not weaken or bypass existing simulator coverage.

## Migration / Compatibility

This is the first application schema and first product command behavior. There is no earlier Job database to migrate. Future schema changes must use a higher atomic migration version, preserve accepted core-entity contracts, and refuse unsupported newer versions. Future retry/recovery features must remain compatible with successful version-1 records and the stable success/replay output contracts.

The existing `boolean-maybe-simulator` command and HTTP contract remain unchanged.

## Risks

* The vertical deliberately leaves uncertain post-dispatch work in `SUBMITTING`/`STARTED`; it is safe against duplicate submission but not operationally complete until recovery is implemented.
* A current-working-directory database default is predictable for repository evaluation but means invocations from different directories do not coordinate unless they pass the same `--database` path.
* Inline JSON is compact in scope but subject to shell quoting and operating-system command-length limits; file and stdin input remain later interface decisions.
* Standard-library SQLite and HTTP adapters require careful async thread isolation, transaction cleanup, bounded response reading, and cross-platform tests.
* A 60-second non-renewed lease may expire after suspend or extreme local delay; this feature intentionally fails conservatively and never takes over.

## Open Questions

None.

## Implementation Notes

Keep CLI parsing/rendering, application orchestration, domain validation, persistence, and HTTP transport behind separate interfaces. Extract the required shared strict-JSON/canonicalization module without coupling application behavior to simulator state or scenario controls. Do not add retry, Reconciliation, recovery takeover, Batch behavior, a CLI framework, an ORM, or a new runtime dependency while implementing this specification.
