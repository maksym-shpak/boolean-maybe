# Feature Specification: Reliable Job Submission

## Status

Implemented

## Summary

Extend the implemented single-Job `submit` vertical with the evidence-driven reliability policy from ADR-006: definitive failure classification, bounded safe submission retries, full-jitter backoff, `Retry-After`, a durable service-wide rate-limit gate, automatic reconciliation after uncertain dispatch, restart recovery of interrupted attempts, conservative persistence-failure handling, and structured durable attempt diagnostics.

The feature preserves one local Job, one immutable Job Entry, and one idempotency key for the logical submission. It never turns uncertainty into permission for another remote side effect.

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
* `docs/specs/features/submit-single-job.md`

## Problem

The current single-Job vertical proves durable creation, one successful HTTP submission, finalization, and stored-result replay. Every non-success after its pre-side-effect commit deliberately remains `SUBMITTING`/`STARTED`, however, and the staged schema has no durable rate-limit gate or structured reconciliation evidence.

Without the reliability workflow, the CLI cannot safely distinguish a request proven not sent from one that may have been processed, cannot honor `429` across processes, cannot bound retries durably, and cannot recover an interrupted attempt without risking duplicate remote work.

## Goal

For one Job Entry submitted through the existing CLI command:

1. classify every supported local, HTTP, and transport observation without overstating certainty;
2. retry submission only after delivered `429` or adapter-proven `NOT_SENT`, at most three SubmissionAttempts over the Job lifetime;
3. coordinate `Retry-After` and policy backoff through durable Job eligibility and a service-wide gate;
4. reconcile every `MAYBE_SENT` submission by the same idempotency key without creating another SubmissionAttempt;
5. recover an interrupted `STARTED` attempt after safe lease takeover and reconcile it instead of resubmitting;
6. persist a sanitized, ordered history sufficient to explain submission and reconciliation decisions; and
7. return a stable JSON result and exit code for success, scheduling, permanent failure, ambiguity, active ownership, and local operational failure.

## Non-goals

* Batch input, Batch identity, Batch persistence, or aggregate Batch output.
* A background worker, daemon, scheduler, or automatic database-wide scan.
* A new CLI command for explicit reconciliation, inspection, manual resolution, or user-approved resubmission from `AMBIGUOUS`.
* Any automatic transition out of `SUCCEEDED`, `FAILED_PERMANENT`, or `AMBIGUOUS`.
* More than three SubmissionAttempts or more than three GETs in one reconciliation sequence.
* Automatic retry after `5xx`, timeout, disconnect, incomplete response, redirect, malformed response, or unclassified transport evidence.
* Configurable retry budgets, backoff formula, jitter mode, request deadline, lease parameters, or service-gate scope.
* General remote services, TLS, authentication, redirects, proxies, DNS-based service discovery, or non-loopback service URLs.
* Changes to the simulator scenario language or its authoritative evidence contract.
* Retention, pruning, archival, `VACUUM`, or deletion of Job, attempt, or reliability evidence.
* Claiming exactly-once remote processing.

## Users / Actors

* A CLI user submitting one Job Entry.
* The application submission workflow, which owns classification, retry, reconciliation, and Job transitions.
* The persistence adapter, which owns transactions, migration, rate-limit coordination, leases, fencing, and sanitized evidence storage.
* The external service client, which reports HTTP evidence and dispatch certainty but does not choose Job transitions.
* The Simulated External Service, which supplies deterministic unreliable submission and reconciliation behavior.
* Concurrent or restarted CLI processes using the same SQLite database.

## Scope

### In scope

* Extend the existing `boolean-maybe submit` command; its arguments and validation remain unchanged.
* Continue an equivalent existing Job in `PENDING` or eligible `RETRY_SCHEDULED`.
* Return stored results for `SUCCEEDED`, and stable terminal results for `FAILED_PERMANENT` and `AMBIGUOUS`, without HTTP.
* Handle delivered submission `400`, `409`, `429`, `5xx`, authoritative `200`/`201`, unexpected HTTP responses, timeouts, disconnects, and proven pre-dispatch failures.
* Parse and persist bounded `Retry-After` evidence.
* Perform full-jitter waits with an injected random source and monotonic in-process clock.
* Add migration version `2` for the service-wide gate and sanitized reliability observations.
* Renew, recover, fence, and release the existing 60-second attempt lease.
* Recover only the equivalent Job selected by the current `submit` invocation.
* Record and safely consume late observations from a stale owner.
* Preserve the existing success and input-error output contracts while adding reliability result and history fields.

### Out of scope

* Starting another attempt for `SUBMITTING`, `FAILED_PERMANENT`, or `AMBIGUOUS`.
* Treating reconciliation `404` as permission to submit.
* Persisting a new `reconciliation-scheduled` Job state.
* Duplicating authoritative retry eligibility on the Job row.
* Storing raw response bodies, raw `Retry-After`, arbitrary headers, credentials, or a second copy of the Job Entry in diagnostic records.

### Explicit feature decisions requiring human approval

Approval of this specification confirms all of the following concrete choices delegated by ADR-004 and ADR-006:

1. **Automatic reconciliation only.** This feature performs reconciliation only while classifying an active uncertain attempt or recovering an interrupted one. It does not approve explicit reconciliation of a terminal `AMBIGUOUS` Job.
2. **Matching-invocation recovery only.** Restart recovery is triggered by `submit` with the same explicit idempotency key and equivalent Job Entry. No startup scan or background recovery is introduced.
3. **Lease parameters.** The version-1 duration remains 60 seconds, becomes renewable, and uses a 20-second maximum renewal interval while the owner waits. Every HTTP authorization renews the lease to 60 seconds before dispatch.
4. **Safe takeover quarantine.** An invocation must observe an expired owner token, fencing generation, and lease expiry, wait through a 10-second local monotonic quarantine, and then observe those exact values unchanged before it may claim the attempt. Ten seconds matches the maximum deadline of an already authorized HTTP operation; fencing and the late-evidence path cover a response/finalization race at that boundary. The claim rechecks the captured values in `BEGIN IMMEDIATE` and increments fencing. The quarantine counts toward, rather than extends, ADR-006's 30-second invocation wait ceiling, leaving up to 20 seconds for recovery-reconciliation delays. This restart rule prevents wall-clock jumps, suspend/resume, or host reboot from creating immediate unsafe takeover without introducing a platform-specific boot identifier.
5. **Late evidence.** A stale owner may append sanitized evidence but may not complete an attempt. A current owner may consume authoritative late evidence while the attempt remains `STARTED`; a completed attempt is never silently reclassified.
6. **Structured history surface.** Reliability output adds `attempt_history` while retaining the existing `attempt` and `result` objects. Internal observation identifiers, owner tokens, fencing values, and lease timestamps are never exposed.

## Expected Behavior

### Command and identity contract

The command remains:

```text
boolean-maybe submit --job-entry JSON [--idempotency-key KEY] [--database PATH] [--service-url URL]
```

All validation, canonical JSON, idempotency-key generation and collision behavior, default paths, service URL restrictions, stdout/stderr rules, and generated identifiers from `submit-single-job.md` remain authoritative.

Every submission retry uses the existing Job, immutable canonical payload, and exact idempotency key. It creates the next monotonic SubmissionAttempt number. Reconciliation is a GET associated with the current attempt and never creates a SubmissionAttempt.

Restart recovery requires the user-supplied key because a newly generated key denotes a new logical submission. A matching `SUBMITTING` Job discovered only after a generated-key collision is treated as a collision and causes key regeneration, not recovery.

### Existing Job routing

After canonical equivalence is verified and before any external request:

| Persisted state | Behavior |
| --- | --- |
| `PENDING` | Authorize the first attempt only after the service gate permits it. |
| `RETRY_SCHEDULED` | Derive eligibility from the latest `RETRYABLE_FAILURE`; authorize the next attempt only after both Job eligibility and service gate permit it. |
| `SUBMITTING` with unexpired lease | Return `job_in_progress`; send no POST or GET. |
| `SUBMITTING` with expired lease | Apply the quarantine and fenced recovery rules, then reconcile the same `STARTED` attempt. |
| `SUCCEEDED` | Return `already_completed` with stored authoritative evidence; send no HTTP and create no attempt. |
| `FAILED_PERMANENT` | Return `retry_exhausted` when three attempts ending in `RETRYABLE_FAILURE` prove budget exhaustion; otherwise return `failed_permanent`. Send no HTTP and create no attempt. |
| `AMBIGUOUS` | Return `ambiguous` with stored history; send no HTTP and create no attempt. |

A state/attempt mismatch, missing required completion evidence, noncanonical persisted payload, invalid attempt numbering, missing retry eligibility, or multiple active attempts is database corruption and stops before HTTP.

### Dispatch certainty

The external adapter returns an observation plus one of:

| Certainty | Concrete mapping in this adapter | Workflow meaning |
| --- | --- | --- |
| `NOT_SENT` | `HTTPConnection.connect()` failed before it returned successfully, or cancellation occurred after durable attempt creation but before `connect()` began. | The service could not have processed this request; safe-retry candidate. |
| `MAYBE_SENT` | `connect()` returned successfully and any later operation failed or was cancelled; or the adapter cannot prove the earlier condition. | No automatic POST; reconcile. |

Connection refusal, address failure, and connection-establishment timeout are `NOT_SENT` only when raised by the explicit `connect()` phase. Failure during request serialization is prevented by prior canonicalization; any otherwise unclassified adapter exception defaults to `MAYBE_SENT` after authorization.

Simulator scenario names are never evidence. A timeout or disconnect is not `NOT_SENT` merely because no response arrived.

### HTTP operation deadline and response validation

Every POST and GET has its own absolute ten-second monotonic deadline covering connect, request transmission, response headers, and the complete bounded response body. Retry and rate-limit sleeps occur outside this deadline. Automatic redirects remain disabled.

The response size limit, `application/json` Content-Type requirement, strict JSON decoding, exact-field validation, exact JSON types, idempotency-key comparison, digest comparison, and remote-request-ID treatment from the implemented client remain in force. Reconciliation validates the ADR-003 `200 processed`, `404 not_found`, `429`, and `5xx` response schemas with the same strictness. A complete but non-authoritative response is `protocol_uncertain`, not invented success or conflict.

### Submission classification and transitions

The current owner records each observation and applies this matrix atomically where it completes the attempt:

| Submission observation | Attempt transition | Job transition | Next action |
| --- | --- | --- | --- |
| Authoritative matching `201`, or replayed matching `200` | `SUCCEEDED` | `SUCCEEDED` | Return success. |
| Delivered valid `400 validation_error` | `PERMANENT_FAILURE`, `validation_rejected` | `FAILED_PERMANENT` | Stop. |
| Delivered authoritative `409 idempotency_conflict` | `PERMANENT_FAILURE`, `idempotency_conflict` | `FAILED_PERMANENT` | Stop. |
| Delivered valid `429` | `RETRYABLE_FAILURE`, `rate_limited` | `RETRY_SCHEDULED` if fewer than three attempts; otherwise `FAILED_PERMANENT` | Advance the gate and wait/return/exhaust as below. |
| Transport `NOT_SENT` | `RETRYABLE_FAILURE`, `transport_not_sent` | `RETRY_SCHEDULED` if fewer than three attempts; otherwise `FAILED_PERMANENT` | Wait/return/exhaust as below. |
| Delivered `5xx` | Remain `STARTED` | Remain `SUBMITTING` | Reconcile; diagnostic `server_uncertain`. |
| Timeout, disconnect, incomplete response, or other `MAYBE_SENT` transport failure | Remain `STARTED` | Remain `SUBMITTING` | Reconcile; diagnostic `transport_maybe_sent`. |
| Redirect, malformed, oversized, wrong-Content-Type, or otherwise unexpected complete response | Remain `STARTED` | Remain `SUBMITTING` | Reconcile; diagnostic `protocol_uncertain`. |

Successful attempts have null `error_category`. Intermediate observations never overwrite the final category.

### Submission budget and retry scheduling

The Job has at most three persisted SubmissionAttempts over its lifetime: initial plus two automatic retries. The count never resets across invocations, recovery, or process races.

After safe-retry request ordinal `n`, use integer milliseconds:

```text
cap_ms(n) = min(30_000, 500 * 2^(n - 1))
policy_delay_ms(n) = uniform integer in [0, cap_ms(n)]
```

The injected random source makes both bounds testable. For the implemented three-attempt budget, retry waits use caps `500` and `1_000`; a terminal third `429` computes the `2_000`-millisecond policy value only for gate advancement and diagnostics.

For an attempt that leaves its Job `RETRY_SCHEDULED`, `retry_after_ms` is the effective non-negative interval measured from that attempt's `completed_at`. Eligibility is derived only as `completed_at + retry_after_ms`. The third safe-retry candidate remains `RETRYABLE_FAILURE`, transitions its Job to `FAILED_PERMANENT`, reports `retry_exhausted`, and creates no further eligibility even if diagnostic delay is retained.

### `Retry-After`

For delivered `429`, accept exactly one `Retry-After` value in either form:

* non-negative decimal delta-seconds; or
* an IMF-fixdate HTTP date with an explicit UTC interpretation.

At observation time, convert a valid future value no greater than 3,600,000 milliseconds to a non-negative interval. Millisecond fractions are rounded up so the accepted server instant is never shortened. The effective delay is `max(policy_delay_ms, server_delay_ms)`.

Missing, repeated, syntactically invalid, negative, elapsed, overflowing, or greater-than-one-hour values are ignored in favor of policy jitter and recorded with a sanitized diagnostic code. The raw header is not persisted. An accepted value is never capped to one hour; a value beyond one hour is invalid.

### Durable service-wide rate-limit gate

Any delivered `429` from POST or GET advances the one-service gate to the later of its current value and `observation_time + effective_delay`. The gate update and affected attempt/Job update commit in one transaction when both change.

The singleton is scoped to the database's one simulated remote authority. Different accepted `--service-url` loopback addresses or ports do not partition or bypass it.

Every first or repeated submission authorization computes:

```text
max(Job retry eligibility when applicable, service gate not_before)
```

inside its `BEGIN IMMEDIATE` transaction before allocating an attempt. Every reconciliation authorization similarly checks the gate and sequence-local eligibility while verifying current lease/fencing ownership. No database transaction remains open across a wait or HTTP call. After authorization commits, dispatch follows immediately with no unrelated work or delay.

If a new Job is created while the gate is closed, it remains `PENDING` with no attempt. If the remaining delay fits the invocation's remaining wait budget, the workflow waits and reauthorizes; otherwise it returns `submission_deferred` with `state = PENDING`.

### In-process waiting

One CLI invocation may intentionally sleep for at most 30 seconds in total. HTTP execution time and SQLite busy waiting are separately bounded and are not counted as retry sleep. All sleeps use an injected monotonic clock; durable eligibility uses UTC wall-clock instants.

Before and after each sleep, the workflow rereads durable authorization state. A wall-clock movement may lengthen or shorten the remaining scheduling delay, but cannot change evidence classification or bypass the gate transaction. Forward and backward wall-clock changes must be tested.

Submission behavior when the entire remaining delay does not fit:

* `PENDING` returns `submission_deferred`;
* `RETRY_SCHEDULED` returns `retry_scheduled`.

Reconciliation behavior when its required delay does not fit is terminal `AMBIGUOUS`; no reconciliation-scheduled state is created.

### Automatic reconciliation

After `MAYBE_SENT`, retain the same `STARTED` attempt and current lease. Issue at most three GETs to:

```text
GET /jobs/by-idempotency-key/{idempotency_key}
```

The path component is encoded by the client even though the accepted key alphabet is URL-unreserved.

For one reconciliation sequence:

| Observation | Result |
| --- | --- |
| Authoritative `200 processed` with matching key and digest | Complete the active attempt and Job as `SUCCEEDED`. |
| Authoritative `200 processed` proving the same key is bound to a different digest | Complete the attempt as `PERMANENT_FAILURE` with `idempotency_conflict`, and Job as `FAILED_PERMANENT`. |
| Delivered valid `404 not_found` | Complete attempt and Job as `AMBIGUOUS` with `reconciliation_not_found`; do not retry GET or POST. |
| Delivered `429` | Record `rate_limited`, advance the gate, and retry GET if budget and wait ceiling permit. |
| Delivered `5xx` | Record `server_uncertain` and retry GET if budget and wait ceiling permit. |
| Timeout, disconnect, incomplete, malformed, oversized, or unexpected response | Record the matching transport/protocol diagnostic and retry GET if budget and wait ceiling permit. |

Every GET consumes one of the three reconciliation requests, including `429`. Reconciliation retry delay uses request ordinal `n` and the same full-jitter formula and `Retry-After` rules. Exhaustion or unsupported waiting completes the attempt and Job as `AMBIGUOUS` with `reconciliation_inconclusive`.

Reconciliation success resolves the original SubmissionAttempt; it does not add a new attempt and never sends another POST. A remote request ID alone is not authoritative.

### Lease renewal and request authorization

The 60-second lease remains persistence-internal. The owner renews it:

* immediately before every POST or GET authorization;
* after every HTTP observation before applying a next step; and
* during intentional waits often enough that no interval between successful renewals exceeds 20 seconds.

Long sleeps are divided into monotonic chunks of at most 20 seconds. Renewal is a short transaction matching Job `SUBMITTING`, attempt `STARTED`, owner token, and fencing generation. It extends expiry to wall-clock `now + 60 seconds` and changes no domain state.

Every dispatch authorization verifies current ownership and an unexpired renewed lease inside `BEGIN IMMEDIATE`, commits, and dispatches immediately. Every normal finalization again matches owner and fencing. If renewal, authorization, or finalization loses ownership, the invocation sends no next request and returns `ownership_lost`; it never steals ownership through the normal path.

### Restart recovery and safe takeover

Recovery is reached only for an equivalent existing `SUBMITTING` Job selected with an explicit key.

1. Read and validate its single `STARTED` attempt and ownership evidence.
2. If the lease is unexpired, return `job_in_progress` without POST or GET.
3. If expired, capture owner token, fencing generation, and expiry, then wait for a 10-second monotonic quarantine without holding a transaction.
4. During quarantine, any changed or renewed ownership evidence aborts this claim; the workflow reroutes from current state without reusing elapsed evidence.
5. In `BEGIN IMMEDIATE`, require the exact captured owner token, generation, and expiry to remain unchanged and expired under the current wall clock. Replace the owner token, increment fencing by exactly one, and issue a fresh 60-second lease.
6. Treat the claimed attempt as `MAYBE_SENT` regardless of its crash boundary and run one bounded reconciliation sequence. Never create another SubmissionAttempt or POST.

The quarantine consumes 10 seconds of the invocation's 30-second sleep budget, so a successfully claimed recovery sequence retains up to 20 seconds for reconciliation backoff and valid `Retry-After` waits. It is deliberately required after process or host restart; no persisted monotonic value is compared across boots. A forward wall-clock jump can only begin quarantine, not bypass it. A backward jump can delay takeover. After suspend/resume, both the unchanged-evidence check and fencing still apply. If a reconciliation delay does not fit the actual remaining budget, the normal terminal `AMBIGUOUS` rule still applies.

### Late observations

An invocation rechecks ownership before dispatch and finalization. If it loses fencing while a request is already in flight, it may append the resulting sanitized observation tagged with its observed generation and `is_late = 1`, but it may not update attempt or Job state through the normal completion path.

The current owner checks unconsumed late observations before each reconciliation GET and before terminal ambiguous finalization:

* authoritative matching success may complete the active attempt and Job as `SUCCEEDED`;
* authoritative submission `400` or conflict evidence may complete permanent failure;
* authoritative submission `429` may complete the attempt under normal safe-retry budget rules and advances the gate;
* uncertain late evidence remains diagnostic and cannot authorize POST or invent a terminal result.

Consumption is atomic with the resulting transition and records which observation supplied the evidence. If the attempt is already completed, late evidence remains append-only diagnostic history and does not reclassify it. Two owners racing to consume evidence are serialized and fenced.

### Cancellation and process interruption

Cancellation before `connect()` begins after attempt creation is `NOT_SENT`: complete the owned attempt as `RETRYABLE_FAILURE` under normal budget rules, persist any eligibility, and propagate cancellation. It still counts toward the lifetime attempt budget.

Cancellation after `connect()` returned is `MAYBE_SENT`: record available diagnostic evidence, leave Job `SUBMITTING` and attempt `STARTED`, stop automatic reconciliation, release no claim that would imply safety, and propagate cancellation. Later recovery reconciles.

Cancellation during a retry wait creates no attempt or request and preserves `PENDING`/`RETRY_SCHEDULED`. Cancellation during recovery quarantine or before a reconciliation authorization leaves the prior owner evidence unchanged. No cancellation is swallowed merely to finish retries.

### Local persistence failures

* Failure or uncertain commit before submission authorization means no POST is permitted and returns `local_persistence_failure` with `submitted = false`.
* Failure while checking a closed gate or retry eligibility creates no attempt and sends no request.
* Failure after an attempt authorization commit but before adapter dispatch is completed as `NOT_SENT` only if the adapter boundary proves `connect()` never began; otherwise it remains recoverable `STARTED` evidence.
* Failure or uncertain commit after an HTTP observation never causes another automatic POST. The workflow returns `local_persistence_failure`; durable state is reread only when safe, and a later invocation follows stored evidence rather than the in-memory assumption.
* A delivered `429` gate update must not be reported as durable unless its commit is known successful.
* Busy-timeout exhaustion, read-only paths, directory-at-path conflicts, migration failures, disk errors, corrupted evidence, and schema mismatch are exit-code `1` operational failures with sanitized output.

### Persistence migration version 2

Version `2` is additive and preserves every version-1 Job and SubmissionAttempt row. It creates:

```sql
CREATE TABLE service_rate_limit_gate (
    singleton_id INTEGER PRIMARY KEY CHECK (singleton_id = 1),
    not_before TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

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
);

CREATE INDEX attempt_observations_attempt_order
ON attempt_observations (attempt_id, sequence_number);
```

`sequence_id` groups one submission observation or one bounded reconciliation sequence; it is operational identity, not a core entity. `sequence_number` orders all HTTP observations for one attempt. `request_ordinal` applies the relevant jitter/budget formula within that sequence. The final `reconciliation_inconclusive` classification is stored on the completed SubmissionAttempt rather than as a synthetic HTTP observation.

`observation_id` and `sequence_id` are independent cryptographically secure UUID version 4 values in the same lowercase canonical representation already specified for local Job and attempt identifiers. A generated collision is retried before insertion and never reuses or merges evidence.

SQL formatting, whitespace, and identifier quoting may differ from the displayed migration only when normalized schema verification proves the same named columns, types, checks, foreign keys, uniqueness rules, and index properties. An implementation may not omit, weaken, or replace any semantic constraint or index predicate.

The migration and `PRAGMA user_version = 2` commit atomically under the ADR-004 exclusive migration transaction. Schema verification checks exact normalized table/index SQL plus foreign keys and index properties. Opening newer, malformed, or partially migrated schema still fails closed. No version-1 row is rewritten or reinterpreted.

### Structured attempt history

All product results for a resolved existing or newly created Job include `attempt_history`, ordered by `attempt_number`. Each item contains:

```json
{
  "attempt_id": "<local UUID>",
  "attempt_number": 1,
  "state": "SUCCEEDED | RETRYABLE_FAILURE | PERMANENT_FAILURE | AMBIGUOUS | STARTED",
  "started_at": "<UTC timestamp>",
  "completed_at": "<UTC timestamp or null>",
  "http_status": 201,
  "remote_request_id": "<string or null>",
  "error_category": "<stable category or null>",
  "retry_after_ms": null,
  "reconciliation": {
    "request_count": 1,
    "final_category": "<sanitized category or null>"
  }
}
```

The history exposes core attempt fields and an aggregate reconciliation summary, not raw internal observations. A direct attempt has reconciliation count `0`. `final_category` is the last non-success reconciliation category from the ADR-006 baseline (`rate_limited`, `server_uncertain`, `transport_maybe_sent`, `protocol_uncertain`, `reconciliation_not_found`, `reconciliation_inconclusive`, or `idempotency_conflict`), or null when no such diagnostic occurred. It never replaces the attempt's final `error_category`.

The existing top-level successful `attempt` object remains and identifies the attempt that supplied definitive success. Existing `result` evidence remains unchanged. Stored replay reconstructs both from durable evidence without HTTP.

### JSON outcomes

Every result remains one compact UTF-8 JSON object plus newline on stdout. The common reliability envelope is:

```json
{
  "outcome": "<outcome>",
  "submitted": false,
  "job_id": "<local UUID>",
  "idempotency_key": "<resolved key>",
  "state": "<persisted Job state>",
  "attempt_history": [],
  "error": {
    "code": "<stable code>",
    "message": "<concise safe message>"
  }
}
```

`submitted` is `true` exactly when this invocation began or may have begun at least one submission POST. Reconciliation GETs alone do not make it true. The outcome also includes `reconciliation_requests`, the number of GETs initiated by this invocation.

For every non-success envelope, `error.code` is exactly equal to `outcome`. Detailed evidence classification remains in `attempt_history[].error_category` and the reconciliation summary. `job_id`, `idempotency_key`, `state`, and history are omitted only when the failure occurred before they were available.

| Outcome | State | Meaning |
| --- | --- | --- |
| `succeeded` | `SUCCEEDED` | This invocation obtained definitive success, directly or by reconciliation. |
| `already_completed` | `SUCCEEDED` | Stored definitive result; no HTTP. |
| `submission_deferred` | `PENDING` | First attempt blocked beyond the remaining wait ceiling by the service gate. |
| `retry_scheduled` | `RETRY_SCHEDULED` | Safe retry remains durably eligible in the future. |
| `retry_exhausted` | `FAILED_PERMANENT` | The third safe-retry candidate exhausted the lifetime attempt budget. |
| `failed_permanent` | `FAILED_PERMANENT` | Definitive validation rejection/conflict, or stored permanent outcome. |
| `ambiguous` | `AMBIGUOUS` | Remote processing cannot be determined; no automatic retry. |
| `job_in_progress` | `SUBMITTING` | Another unexpired owner holds the active attempt. |
| `ownership_lost` | current persisted state | This invocation lost lease/fencing and stopped new work. |
| `local_persistence_failure` | persisted state or `null` | Required local state could not be read or durably advanced. |
| `idempotency_conflict` | `null` | The supplied key is locally bound to a non-equivalent Job Entry; no Job state/history is disclosed and no HTTP is sent. |

Fresh direct and replay success preserve the exact required fields from `submit-single-job.md`, adding only `attempt_history` and `reconciliation_requests`. Reconciled success uses the same success schema with `submitted` reflecting POST activity in this invocation and `attempt.http_status` equal to the authoritative status that completed the attempt: direct POST status when retained, otherwise reconciliation `200`. The observation history preserves both operations.

Input-validation and local idempotency-conflict envelopes remain as previously specified. Expected errors never expose payloads, SQL, raw response bodies, headers, invocation tokens, fencing, leases, or tracebacks.

User cancellation or external process termination may end outside the normal JSON/`0`-`2` result contract according to ADR-002 and platform conventions; it is not a normal product outcome.

### Exit codes

The existing stable codes remain:

| Code | Outcomes |
| ---: | --- |
| `0` | `succeeded`, `already_completed`, or help. |
| `1` | Every other valid-command outcome, including deferred/scheduled, exhausted, permanent, ambiguous, active-owner, ownership, HTTP, persistence, and unexpected internal failures. |
| `2` | Command syntax or input validation failure before product work. |

## System Flow

```text
CLI validation and canonicalization
  -> open/migrate/verify SQLite
  -> resolve existing Job or create PENDING
  -> route terminal, active, recovery, first-attempt, or retry path
  -> wait outside transaction if authorized timing has not arrived
  -> BEGIN IMMEDIATE authorization
       -> verify payload/state/budget/gate/lease
       -> create STARTED attempt when submission is safe
       -> commit SUBMITTING ownership
  -> renew/verify lease
  -> POST with ten-second deadline
  -> persist structured observation
       -> definitive completion, or
       -> safe RETRY_SCHEDULED, or
       -> retain STARTED and run bounded GET reconciliation
  -> atomically finalize attempt and Job
  -> render JSON and stable exit code
```

No SQLite transaction spans HTTP, waiting, random generation, or user input.

## Acceptance Criteria

### Classification and safe retry

* Given a POST returns authoritative matching `201` or replayed `200`, when it is finalized, then the attempt and Job become `SUCCEEDED`, and no retry is created.
* Given a delivered valid `400`, when classified, then the attempt becomes `PERMANENT_FAILURE` with `validation_rejected`, the Job becomes `FAILED_PERMANENT`, and no retry or reconciliation occurs.
* Given a delivered authoritative `409`, when classified, then the attempt becomes `PERMANENT_FAILURE` with `idempotency_conflict`, the Job becomes `FAILED_PERMANENT`, and no retry occurs.
* Given `connect()` fails before it succeeds, when the adapter reports `NOT_SENT`, then the attempt becomes `RETRYABLE_FAILURE`, and only the same key/payload may receive the next attempt.
* Given `connect()` succeeded and send/read later times out or disconnects, when evidence is classified, then it is `MAYBE_SENT`, no new POST is created, and reconciliation begins.
* Given a delivered `500`, when classified, then the active attempt remains `STARTED`, reconciliation begins, and no direct retry is sent even if the simulator scenario would succeed on another POST.
* Given an unknown adapter failure, when dispatch certainty cannot be proved, then classification defaults to `MAYBE_SENT`.

### Rate limiting and bounded retries

* Given the first POST returns `429` and the next same-key POST succeeds, when the effective delay fits the invocation ceiling, then exactly two attempts exist, both POSTs use the same key and canonical payload, and the Job becomes `SUCCEEDED`.
* Given three safe-retry candidates across one or more restarts, when the third is recorded, then exactly three attempts exist, the Job becomes `FAILED_PERMANENT`, the outcome is `retry_exhausted`, and no fourth POST is possible.
* Given deterministic random values at zero and at the cap, when delays are calculated, then the inclusive full-jitter bounds are respected for both submission and reconciliation.
* Given a valid future delta-seconds or IMF-fixdate `Retry-After` no greater than one hour, when a `429` is recorded, then the effective delay is no earlier than both server and jitter instants.
* Given missing, duplicate, elapsed, negative, malformed, overflowing, or greater-than-one-hour `Retry-After`, when parsed, then policy jitter is used and a sanitized anomaly is persisted without the raw header.
* Given a required submission delay does not fit the remaining 30-second wait budget, when the workflow stops, then eligibility remains durable and no premature attempt or POST is created.

### Shared gate

* Given Job A receives `429`, when unrelated Job B attempts its first submission using the same database before `not_before`, then B creates no attempt and sends no POST.
* Given multiple processes race at gate expiry, when they authorize work, then every authorization checks the durable gate in `BEGIN IMMEDIATE`; no process bypasses the current persisted value.
* Given concurrent `429` observations propose different instants, when they commit, then the gate retains their maximum.
* Given a reconciliation `429`, when recorded, then it advances the same gate used by first and repeated submissions.

### Reconciliation and ambiguity

* Given a request was processed but its response was lost, when the CLI reconciles by idempotency key, then it records the authoritative remote result, completes the same attempt as `SUCCEEDED`, and does not create or send another remote Job.
* Given a processed request returns `500`, when reconciliation returns matching `200`, then the Job becomes `SUCCEEDED` and exactly one SubmissionAttempt exists.
* Given submission is `MAYBE_SENT` and reconciliation proves a digest conflict for the same key, when finalized, then the attempt becomes `PERMANENT_FAILURE`, the Job becomes `FAILED_PERMANENT`, and no POST retry occurs.
* Given submission is `MAYBE_SENT` and reconciliation returns `404`, when classified, then the attempt and Job become `AMBIGUOUS`, and no automatic retry is performed.
* Given submission and all three reconciliation requests are inconclusive, then the Job becomes `AMBIGUOUS`, the attempt records `reconciliation_inconclusive`, and no automatic retry is performed.
* Given reconciliation requires a delay beyond the remaining wait ceiling, then the Job becomes `AMBIGUOUS` without a persisted reconciliation-scheduled state.
* Given reconciliation first returns `429` and then matching `200`, when budget and delay permit, then two GETs and one SubmissionAttempt are recorded, the gate is advanced, and the Job succeeds.
* Given a malformed reconciliation body or non-authoritative mismatch, when classified, then it consumes reconciliation budget as inconclusive and never invents conflict or success.

### Recovery, fencing, and late evidence

* Given an equivalent `SUBMITTING` Job has an unexpired lease, when another process submits the same key, then it returns `job_in_progress` and sends neither POST nor GET.
* Given the owner lease is expired, when a recovery invocation observes unchanged evidence for the full 10-second monotonic quarantine and claims it, then fencing increments exactly once, at least 20 seconds of the invocation wait ceiling remain, and recovery sends GET only.
* Given takeover succeeds and the first recovery GET requires a retry delay that fits the remaining budget, when reconciliation continues, then the second and, if still eligible, third GET may run under the normal three-request sequence budget.
* Given the lease is renewed or ownership changes during quarantine, when recovery rechecks, then it does not claim using the old observation.
* Given the process was killed before the pre-side-effect commit, when the same explicit key is submitted after restart, then normal first-attempt processing is allowed.
* Given the process was killed after the pre-side-effect commit at any boundary before local finalization, when recovery claims the attempt, then it treats it as `MAYBE_SENT`, reconciles, and never creates a replacement attempt.
* Given a stale owner returns authoritative evidence after fencing changed, when it records late evidence, then it cannot finalize the Job; the current owner may consume that evidence atomically while the attempt remains `STARTED`.
* Given late evidence arrives after the attempt completed, then it remains diagnostic and does not reclassify the completed attempt.
* Given forward/backward wall-clock movement, suspend/resume, or restart, when takeover is considered, then none bypasses the monotonic quarantine, exact-evidence recheck, or fencing authorization.

### Persistence and crash boundaries

* Given local persistence fails before known authorization commit, when the command exits, then no HTTP request is sent and `submitted` is false.
* Given a post-observation commit fails or its result is uncertain, when the command exits, then it does not claim the in-memory transition and does not send another POST.
* Given a process is killed during retry sleep, when restarted with the same key, then durable attempt count and eligibility are preserved and the budget does not reset.
* Given migration version 1, when version 2 opens it, then all existing rows remain intact and the gate/observation structures are added atomically.
* Given malformed version-2 SQL, indexes, foreign keys, or a newer `user_version`, when opening the database, then product work fails before HTTP.

### Output and history

* Given any resolved Job result, when JSON is rendered, then attempt history is ordered, exact-typed, sanitized, and consistent with persisted rows.
* Given reconciliation occurred, when history is rendered, then its GET count and final diagnostic are visible without exposing raw bodies, headers, lease, owner, or fencing metadata.
* Given success was obtained by reconciliation in a recovery-only invocation, when rendered, then `submitted` is false, `reconciliation_requests` is positive, and exit code is `0`.
* Given `RETRY_SCHEDULED`, `FAILED_PERMANENT`, or `AMBIGUOUS`, when the same equivalent command runs again, then the output reflects stored state/history and cannot reset terminal state or budget.
* Given any valid but non-successful outcome, then exit code is `1`; given success/replay it is `0`; given input validation failure it is `2`.

### Cancellation and concurrency

* Given cancellation before `connect()` starts, when cleanup succeeds, then the attempt counts as `transport_not_sent` and normal safe-retry eligibility is preserved.
* Given cancellation after possible dispatch, when propagated, then the attempt remains recoverable `STARTED` and no reconciliation or new POST begins in that invocation.
* Given cancellation during a wait, then no request is authorized after cancellation and durable timing evidence remains intact.
* Given concurrent same-key invocations in `PENDING` or `RETRY_SCHEDULED`, then at most one allocates the next attempt and sends its POST.

## Edge Cases

* A policy delay of exactly `0` still passes through a fresh authorization transaction.
* A service gate or Job eligibility exactly equal to wall-clock `now` is eligible.
* An accepted `Retry-After` exactly one hour in the future is valid; any positive amount beyond it is invalid.
* A third `429` advances the gate even though no submission retry remains.
* A duplicate remote request ID across Jobs or attempts never affects identity or matching.
* A reconciliation `404` after a `500` may produce `AMBIGUOUS` even if the simulator did not process the POST.
* An authoritative replayed `200` on the first local POST is success, not a retry or local conflict.
* An empty remote request ID remains a valid string if the remote contract allows it.
* The wall clock moves while sleeping; the monotonic sleep budget remains bounded and durable authorization is recomputed.
* SQLite busy timeout expires during lease renewal, gate update, takeover, observation append, or finalization.
* A stale process loses fencing between authorization commit and actual socket dispatch; dispatch must be immediate, and any observation is late evidence.
* The process terminates after gate update but before output, after attempt completion but before Job transition commit, or after final commit but before output.
* Observation history is corrupted, reordered, references a missing attempt, duplicates sequence ordinals, or claims impossible final evidence.

## Affected Areas

* `src/boolean_maybe/application/submit.py`
* `src/boolean_maybe/cli.py`
* `src/boolean_maybe/domain/`
* `src/boolean_maybe/external/client.py`
* `src/boolean_maybe/persistence/schema.py`
* `src/boolean_maybe/persistence/transactions.py`
* New focused application/persistence modules are allowed when they preserve the existing boundaries, for example retry policy, reconciliation, lease coordination, or result projection.
* `tests/submit/`
* `README.md`
* `docs/architecture/architecture-overview.md`

## Related Architecture Decisions

* ADR-001: retain Python 3.12+, standard-library runtime dependencies, `uv`, and existing quality gates.
* ADR-002: CLI remains an adapter; blocking SQLite is offloaded; cancellation remains explicit.
* ADR-003: only its submission/reconciliation evidence may prove processing, conflict, rejection, rate limiting, or point-in-time absence.
* ADR-004: short SQLite transactions, durable pre-side-effect evidence, leases/fencing, schema migration, and conservative uncertain commits remain mandatory.
* ADR-005: one Job/key/payload identity is preserved across retries and reconciliation.
* ADR-006: classification, budgets, backoff, deadlines, gate, reconciliation, recovery, and stop conditions are implemented literally.

No new ADR is required if implementation follows this specification. A different lease/time-source strategy, new dependency, manual ambiguous-state operation, or changed numeric policy requires separate human approval and potentially a superseding ADR.

## Affected Core Entities

This feature exercises the existing `Job` and `SubmissionAttempt` lifecycle without changing their identities, fields, ownership, or allowed states.

* `PENDING -> SUBMITTING` begins the first attempt.
* `RETRY_SCHEDULED -> SUBMITTING` begins the next safe attempt after eligibility.
* `SUBMITTING -> SUCCEEDED | RETRY_SCHEDULED | FAILED_PERMANENT | AMBIGUOUS` follows observed evidence and budget.
* Completed SubmissionAttempts remain append-oriented and are never silently overwritten.
* Retry eligibility remains derived from `completed_at + retry_after_ms`.

The service gate, lease renewals, reconciliation sequences, and observation rows are persistence-internal coordination/diagnostic records, not core entities. `attempt_history` is a projection of existing core attempts plus non-authoritative reconciliation summaries. No new stable core field is introduced.

## Data / State Changes

* Add migration version `2` exactly as specified.
* Preserve and verify version-1 data.
* Begin using all existing Job and SubmissionAttempt terminal/retry states already defined by core entities.
* Add sanitized append-oriented HTTP/reconciliation observations.
* Add a singleton durable service-rate-limit gate.
* Retain 60-second lease expiry while adding renewal and fenced takeover behavior.
* Perform no automatic retention or deletion.

## API / Interface Changes

* No CLI arguments or endpoint contracts change.
* `submit` can now continue eligible existing Jobs and recover matching interrupted Jobs.
* JSON adds reliability outcomes, `attempt_history`, and `reconciliation_requests`.
* Exit-code meanings remain `0`, `1`, and `2`.
* The external client gains reconciliation and explicit dispatch-certainty observations; these are internal adapter contracts.

## Security / Permissions

* Preserve loopback-only HTTP validation and filesystem permissions inherited from the single-Job feature.
* Do not log or duplicate Job Entry payloads in attempts or observation history.
* Do not persist raw response bodies, headers, `Retry-After`, tracebacks, SQL, or secrets.
* Treat payload digests as operational evidence, not identity or a security boundary.
* Sanitize persistence and protocol diagnostics before stdout/stderr.
* Do not expose owner tokens, fencing generations, lease timestamps, or internal sequence IDs in product output.

## Copy / Terminology

Use glossary terms exactly: Job, Job Entry, SubmissionAttempt, Idempotency Key, Retry, Retryable Failure, Permanent Failure, Rate Limit, Reconciliation, and Ambiguous Outcome. Machine-readable enum values and codes remain English.

## Test Expectations

Tests must be deterministic, layered, and scenario-driven:

* unit tests for dispatch mapping, classification matrix, strict reconciliation schemas, Retry-After parsing, integer full-jitter bounds, scheduling arithmetic, clock changes, and output projection;
* migration/schema-verification tests from empty and version-1 databases, including malformed tables/indexes/foreign keys and concurrent migration;
* transaction tests for gate maximum, eligibility derivation, attempt budget, atomic Job/attempt changes, lease renewal, quarantine recheck, takeover fencing, late-evidence append/consumption, and corrupted evidence;
* live/stub HTTP tests for `400`, `409`, `429`, `5xx`, redirects, wrong Content-Type, malformed/extra fields, slow headers/body, timeout, disconnect, connection refusal, and all reconciliation responses;
* workflow tests for every acceptance scenario with injected UTC clock, monotonic clock, sleeper, random source, identifiers, and adapter;
* subprocess tests for durable restart behavior, three-attempt exhaustion across invocations, active-owner exclusion, same-key races, gate coordination, cancellation, and crash boundaries before/after authorization, dispatch, observation, and finalization;
* simulator integration tests for `500_then_success`, `429_then_success`, `connect_timeout`, `processed_then_disconnect`, `processed_without_response`, `processed_then_500`, `duplicate_remote_request_id`, `reconciliation_timeout`, and `always_500`, interpreted only through wire evidence;
* POSIX and Windows tests for SQLite read-only/directory paths, process locks, cancellation/signals where supported, and monotonic timing tolerances;
* the full existing suite, lint, formatting, type checking, locked dependency checks, and build.

No existing test may be deleted, weakened, skipped more broadly, or changed merely to accept a new result. Any updated stage-5 expectation must demonstrate the explicitly superseding reliability behavior. Timing tests use generous upper bounds while still failing the known unbounded implementation.

## Migration / Compatibility

Migration version `2` is additive and rollback-safe as one transaction; failure leaves version `1` unchanged. No downgrade is supported. The application refuses a newer schema.

Existing successful Jobs replay identically apart from additive history fields. Existing `SUBMITTING`/`STARTED` rows created by version 1 are recoverable only through the same lease-expiry quarantine and are always treated as `MAYBE_SENT`. Existing terminal rows are not reclassified.

The specification intentionally supersedes stage-5 `job_not_eligible` behavior for equivalent `PENDING`, `RETRY_SCHEDULED`, `FAILED_PERMANENT`, `AMBIGUOUS`, and recoverable `SUBMITTING` Jobs, and supersedes `submission_incomplete` for observations now classified by this feature. It does not weaken stored success, idempotency conflict, validation, or durable side-effect contracts.

## Documentation Updates

Implementation must update `README.md` with the bounded retry/reconciliation behavior, reliability outcomes, history fields, and a concise simulator example.

After human approval and implementation review, `docs/architecture/architecture-overview.md` must narrow its remaining deferred items for exact CLI recovery trigger, lease parameters/time-source strategy, migration-2 gate representation, and late-evidence presentation by referencing this specification. No product-brief, glossary, or core-entities change is expected unless implementation changes the approved contracts.

## Risks

* This is the largest stateful feature and combines network uncertainty, clocks, randomness, persistence, and multi-process races.
* A conservative `MAYBE_SENT` or reconciliation `404` can produce `AMBIGUOUS` even when the remote service did no work.
* The shared gate can delay unrelated Jobs.
* A 10-second takeover quarantine makes crash recovery intentionally slower and consumes one third of the invocation wait ceiling, but it covers the maximum already-authorized HTTP deadline and prevents immediate unsafe takeover without platform-specific boot identity.
* Wall-clock changes may move durable eligibility; they must never change dispatch certainty or bypass transactional authorization.
* Late authoritative evidence arriving after terminal completion remains diagnostic because completed attempts cannot be silently reclassified.
* Without retention, observation history grows indefinitely.
* Cross-platform process interruption, socket deadline, and SQLite-lock behavior require CI on Windows, macOS, and Linux.

## Open Questions

No blocking open questions remain in this draft. The six explicit feature decisions above require human confirmation through approval of the specification.

## Implementation Notes

* Prefer small policy/value modules and explicit transaction functions over embedding retry or lifecycle decisions in the CLI or HTTP adapter.
* Keep the random source, UTC clock, monotonic clock, and sleeper injectable; production defaults may use standard-library implementations.
* Reuse the existing canonical JSON and strict response validation modules.
* Do not add a runtime dependency solely for retries, HTTP, clocks, or database access.
* Preserve stage-aware exception classification: whether POST may have begun is decided at the adapter boundary, not by a broad CLI catch.
* Implementation must stop and request human direction if the exact lease, late-evidence, output, or migration contract cannot be implemented without changing this specification.
