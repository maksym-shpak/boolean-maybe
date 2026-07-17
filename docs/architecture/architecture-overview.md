# Architecture Overview

## Purpose

This document describes the stable high-level architecture of `boolean-maybe`: its runtime boundaries, component responsibilities, dependency direction, important data flows, and ownership of authoritative state.

It intentionally does not select concrete libraries, physical database schemas, class names, exact CLI commands, complete HTTP schemas, or implementation details beyond accepted architecture decisions. Remaining details belong to approved feature specifications.

Product intent is defined in `docs/product/product-brief.md`, domain terminology in `docs/product/glossary.md`, and stable domain contracts in `docs/domain/core-entities.md`.

## System Summary

`boolean-maybe` consists of a short-lived local Python application, exposed through a CLI adapter, and a separate simulated external HTTP service. The CLI adapter accepts Job Entries and invokes application workflows. Those workflows persist authoritative local execution state and communicate with the external service through a dedicated client boundary.

The simulator exposes `POST /jobs` for idempotent submission and `GET /jobs/by-idempotency-key/{idempotency_key}` for reconciliation. It deliberately applies deterministic failure scenarios to both operations so the local application must handle retryable failures, ambiguous outcomes, process interruption, and non-authoritative remote evidence.

The local application owns Job identity and execution state. The simulator represents the unreliable external system and does not share the application's persistence or become authoritative for local identity.

## Technology Constraints

| Area | Current constraint | Deliberately undecided |
| --- | --- | --- |
| Language | Python 3.12 (CPython, `>=3.12,<3.13`); packaged with `uv` and Hatchling under a `src/boolean_maybe/` layout; formatting/linting via Ruff, static typing via Pyright, tests via pytest (see ADR-001) | Feature-level supporting libraries |
| User interface | Command-line application | Exact commands and input/output schemas |
| External integration | HTTP submission and reconciliation by idempotency key; RFC 8785 evidence semantics from ADR-003 and bounded retry, rate-limit, ambiguity, and recovery policy from ADR-006 | Client library, concrete adapter error mappings, and feature-level wire-schema details beyond required evidence |
| Persistence | One local SQLite database through the standard-library `sqlite3` adapter; rollback-journal mode, full synchronous durability, explicit short transactions, and multi-process leases/fencing define the consistency boundary (see ADR-004) | Physical schema, exact contention and lease parameters, time-source mechanism, and retention policy |
| Execution model | Synchronous CLI adapter enters one asynchronous application workflow per invocation; domain rules remain synchronous; Batch uses bounded structured concurrency (see ADR-002) | Exact Batch concurrency limit and adapter-specific non-blocking mechanisms |
| Runtime | On-demand local application process with a CLI adapter, plus a separate simulator process; the CLI is installed as the `boolean-maybe` console entry point (see ADR-001), and the simulator is installed as the `boolean-maybe-simulator` console entry point per the accepted simulated-external-service feature specification | None beyond what that feature specification defines |

## Architectural Principles

1. **Application logic is independent of the CLI.**
   The CLI translates user input into workflow requests and renders results; it does not own submission, retry, reconciliation, or lifecycle rules.

2. **Execution boundaries are explicit.**
   The synchronous CLI adapter enters the asynchronous application once per invocation. Application workflows own I/O orchestration, while domain rules remain synchronous and independent of CLI and async infrastructure.

3. **Local state is authoritative.**
   The local application owns Job and SubmissionAttempt identity and lifecycle state. Remote responses are evidence, not replacements for local identity.

4. **Durable intent precedes possible external effects.**
   Before an external request begins, durable state identifies the Job and corresponding SubmissionAttempt and prevents recovery from treating a potentially sent request as unsent.

5. **Uncertainty remains explicit.**
   Missing or inconclusive remote evidence produces an ambiguous outcome rather than invented success or failure. `AMBIGUOUS` is never retried automatically.

6. **All submission paths use one reliability model.**
   Single-entry, batch, retry, and reconciliation flows preserve the same Job and SubmissionAttempt invariants.

7. **Batch concurrency is bounded and structured.**
   Concurrent Job workflows are limited explicitly, every task is awaited within the invocation, and expected per-Job outcomes remain isolated from sibling Jobs.

8. **The simulator remains isolated and deterministic.**
   It runs separately, owns only simulated remote behavior, and reproduces configured failure scenarios without sharing application state.

## System Context

```mermaid
flowchart LR
  User[Operator or Automation] --> CLI[CLI Adapter]
  CLI --> Workflows[Application Workflows]
  Workflows --> LocalState[(Local Durable State)]
  Workflows --> Client[External Service Client]
  Client -->|HTTP submission and reconciliation| Simulator[Simulated External Service]
  Simulator --> Scenarios[Deterministic Failure Scenarios]
```

The operator or automation interacts only with the CLI contract. The local application, through its application workflows, reads and updates local durable state and observes the simulator through HTTP. The CLI adapter does not access persistence or the simulator directly. Local durable state and simulator state are separate; neither process accesses the other's storage directly.

## Component Map

```mermaid
flowchart TD
  CLI[CLI Adapter] --> App[Application Workflows]

  App --> Submit[Job Submission Workflow]
  App --> Batch[Batch Workflow]
  App --> Reconcile[Reconciliation Workflow]

  Submit --> Persistence[Persistence Boundary]
  Batch --> Persistence
  Reconcile --> Persistence

  Submit --> Client[External Service Client]
  Reconcile --> Client

  Client --> SubmitEndpoint[Simulator: Submission Endpoint]
  Client --> ReconcileEndpoint[Simulator: Reconciliation Endpoint]
  SubmitEndpoint --> FailureScenarios[Deterministic Failure Scenarios]
  ReconcileEndpoint --> FailureScenarios

  Batch --> Submit
```

Arrows represent allowed runtime dependencies or calls. Application workflows depend on abstract persistence and external-service boundaries, not on their concrete implementations. The simulator is reached only through the external service client and has no dependency on CLI-adapter or application internals.

## Components and Responsibilities

| Component | Responsibilities | Must not own |
| --- | --- | --- |
| CLI adapter | Parse inputs, synchronously enter one top-level asynchronous application workflow, and render machine-readable results and documented exit codes. | Event-loop orchestration beyond the single application entry, Job lifecycle rules, retry decisions, persistence operations, or HTTP reliability behavior. |
| Application workflows | Asynchronously coordinate use cases and enforce domain contracts across persistence and external interactions. | Transport-specific presentation, physical storage details, or independent domain state. |
| Job submission workflow | Resolve idempotency identity, detect existing Jobs, create and complete SubmissionAttempts, classify observed outcomes, and apply bounded retry semantics. | CLI formatting, batch-specific state, or simulator scenario control. |
| Batch workflow | Accept multiple Job Entries, coordinate bounded structured execution of the single-Job submission workflow, preserve Job isolation, and produce aggregate reporting. | Unbounded or fire-and-forget tasks, a separate submission/retry model, or authority over Job state. |
| Reconciliation workflow | Query external evidence by idempotency key and apply only explicit, auditable resolutions allowed by an approved specification. | Implicit retries from `AMBIGUOUS` or unverified reclassification. |
| Persistence boundary | Durably store and retrieve authoritative local state while preserving required identity, lifecycle, and consistency guarantees. | Independent domain transitions or external HTTP behavior. |
| External service client | Translate workflow requests and remote observations across the HTTP boundary without assigning stronger certainty than the evidence supports. | Local identity, Job lifecycle decisions, or retry policy ownership. |
| Simulated external service | Bind idempotency keys to RFC 8785-equivalent Job Entries, provide idempotent `POST /jobs` and reconciliation `GET /jobs/by-idempotency-key/{idempotency_key}`, and execute deterministic unreliable behaviors for both operations. | Local Job identity, local execution state, CLI behavior, application persistence, or scenario outcomes that contradict actual key binding. |

## Dependency Direction and Boundaries

The CLI depends on application workflows. It may not call persistence or the simulator directly.

Application workflows depend on the stable domain contracts and on abstract persistence and external-service boundaries. Concrete storage and HTTP details remain outside workflow logic.

The CLI creates one event-loop lifecycle per invocation and crosses into the asynchronous application exactly once. Domain rules remain synchronous; potentially long infrastructure I/O must not block the application event loop.

The Batch workflow delegates each eligible Job to the Job submission workflow. Reconciliation is a separate explicit workflow and cannot silently become a retry path.

The persistence implementation and external service client adapt infrastructure to the needs of the workflows. They do not define domain state transitions.

The simulator is an independently running process behind HTTP endpoints. It does not import, call, or share storage with the local application.

## Important Data Flows

### Job Submission

```mermaid
sequenceDiagram
  participant C as CLI
  participant W as Job Submission Workflow
  participant P as Persistence Boundary
  participant E as External Service Client

  C->>W: Submit Job Entry and optional idempotency key
  W->>P: Find or create authoritative Job
  alt Matching SUCCEEDED Job exists
    P-->>W: Stored Job and result
    W-->>C: already_completed, submitted=false
  else New external attempt is allowed
    W->>P: Durably record Job and STARTED attempt
    W->>E: Submit using idempotency key
    E-->>W: Observed response, failure, or uncertainty
    W->>P: Persist attempt outcome and Job transition
    W-->>C: Current result
  end
```

Persistence uses separate short transactions before and after the HTTP side effect and never holds a transaction open across network I/O. Before submission, it commits the Job, a `STARTED` SubmissionAttempt, `SUBMITTING` state, and current invocation ownership; after an observation, the current lease and fencing owner atomically records the attempt outcome and Job transition. A committed pre-side-effect transaction authorizes the request but does not prove that it began or was processed.

This sequence is a simplified happy-path boundary view. It does not enumerate every state-dependent outcome, including an existing `SUBMITTING` or `AMBIGUOUS` Job or a `RETRY_SCHEDULED` Job that has not reached its eligibility time.

### Batch Orchestration

The CLI passes multiple Job Entries to the Batch workflow. The workflow resolves each entry to a Job and delegates eligible work to the Job submission workflow. Each Job retains independent authoritative state; failure or ambiguity of one Job cannot alter another Job. Whether Batch itself is persisted and how membership or aggregate results are represented remain deferred decisions.

### Reconciliation

The CLI explicitly invokes the reconciliation workflow for an eligible Job. The workflow reads local state, queries the simulator by idempotency key through the external service client, and records only conclusions supported by observed evidence. A delivered `200 processed` response is authoritative positive evidence only when its key and canonical payload digest match the local Job Entry. A digest mismatch proves a key conflict, not local success. A `404 not_found` response is only a point-in-time negative observation and cannot by itself authorize automatic retry. Because reconciliation is also unreliable, an inconclusive operation preserves ambiguity rather than triggering an implicit submission or inventing a definitive outcome.

### Retry, Rate Limits, and Ambiguity

Submission retries are allowed only after a definitive `429` or transport evidence proving `NOT_SENT`. A timeout, disconnect, `5xx`, malformed response, or other `MAYBE_SENT` evidence never authorizes direct resubmission; the workflow instead performs bounded reconciliation while the same SubmissionAttempt remains `STARTED`. Matching authoritative evidence completes success, authoritative key conflict completes permanent failure, and `404`, exhausted reconciliation, or unsupported waiting completes `AMBIGUOUS`.

Each Job has a durable lifetime budget of three SubmissionAttempts. Submission and reconciliation requests each have a ten-second deadline; a reconciliation sequence has at most three requests. Retry waits use full-jitter exponential backoff with a 500-millisecond base, while accepted `Retry-After` values up to one hour establish a lower bound. The CLI waits at most 30 seconds in one invocation and otherwise persists safe submission eligibility or stops reconciliation as ambiguous.

A durable service-wide rate-limit gate coordinates Batch tasks and CLI processes using the same database. Submission authorization requires both Job eligibility and the service gate to have arrived inside the ADR-004 pre-side-effect transaction. `AMBIGUOUS` remains terminal for automatic processing.

### Simulator Contract and Failure Evidence

The first processed submission atomically binds an idempotency key to the RFC 8785 canonical bytes of its complete Job Entry. An equivalent replay returns the stored result without processing again; a non-equivalent reuse returns `409 Conflict` without changing the binding. A SHA-256 canonical payload digest may be exchanged as comparison evidence but is not local identity or a security boundary.

Delivered successful submission or matching `200 processed` reconciliation is definitive positive processing evidence. Delivered `400`, `409`, and `429` responses definitively describe rejection, conflict, or rate limiting for that request. A reconciliation `404`, remote request ID alone, `5xx`, incomplete response, timeout, or disconnect cannot establish a definitive remote outcome. The same `5xx`, timeout, or disconnect may occur before or after remote processing.

Failure scenarios are selected by a simulator-owned deterministic plan using operation, exact key or wildcard, and per-operation/per-key request ordinal. Exact-key entries take precedence over wildcard entries. Per-key serialization preserves deterministic behavior under concurrent Batch execution. Scenario controls remain outside Job Entry and product-facing request fields and cannot override actual idempotency binding or payload equivalence.

### Recovery After Interruption

A later CLI invocation triggers an application workflow that reads authoritative local state through the persistence boundary. An unexpired invocation lease identifies active ownership; after expiry, a recovery workflow may claim the attempt only by atomically advancing its fencing generation. The claimed attempt is always `MAYBE_SENT` and is never automatically resubmitted. Recovery uses the bounded reconciliation sequence from ADR-006 and preserves ambiguity unless authoritative ADR-003 evidence supports success or permanent conflict. Exact commands, lease timing, and late-evidence presentation belong to approved feature specifications.

## Data and State Ownership

| Data or state | Authority | Architectural rule |
| --- | --- | --- |
| Job and execution state | Local application through the persistence boundary | `job_id` is the opaque, immutable local identity and is not exposed as simulator protocol identity. |
| SubmissionAttempt history | Local application through the persistence boundary | Recorded before a possible side effect and append-oriented after completion. |
| Job Entry | Job | Immutable after Job creation. |
| Idempotency key | Job | Immutable identity of one logical submission and all its attempts; generated by the application workflow when not supplied by the user, never derived from payload, and transmitted unchanged to the simulator. |
| Retry eligibility | Most recent retryable SubmissionAttempt | Derived as `completed_at + retry_after_ms`; no duplicate authoritative Job-level value. |
| Service rate-limit gate | Local persistence coordination metadata | Durable across CLI processes; delays submission and reconciliation authorization but is not a core entity or lifecycle state. |
| Remote request ID and remote responses | External service, stored locally as evidence | Optional, potentially duplicated, and never authoritative local identity. |
| Simulator processing state | Simulated external service | Keyed by idempotency key, binds one canonical Job Entry to one processed result, and is accessible only through submission and reconciliation operations. Its reset or retention boundary limits the duration of available evidence. |
| Batch data | Undecided | Candidate domain concept; persistence and cardinality require later approval. |

## Runtime and Deployment Model

The system has two local runtime units:

1. The local application runs on demand through its synchronous CLI adapter. Each invocation creates one event-loop lifecycle, awaits one top-level asynchronous application workflow, emits its result, and exits. No application task may outlive the invocation, and reliability cannot depend on the process remaining alive.
2. The simulated external service runs as a separate HTTP process and retains enough simulated remote state to support idempotent submission and reconciliation by idempotency key.

The repository must remain runnable without external infrastructure. Hosting, process supervision, the user-facing database location, physical persistence schema, and simulator process packaging remain outside this overview.

## Security and Privacy Boundary

The current product targets a trusted local development or evaluation environment; authentication and authorization are outside scope.

Job payloads may contain sensitive user data. Full payloads do not cross into operational logs by default, SubmissionAttempt records do not duplicate raw payloads, and stored error or response information must not expose secrets. The simulator receives Job Entry data only through its HTTP submission boundary and must not gain access to local persistence.

## Architectural Constraints

* Preserve the separation between CLI presentation and application/domain behavior.
* Enter the asynchronous application exactly once from the synchronous CLI adapter; do not leak CLI-framework or event-loop concerns into domain interfaces.
* Do not bypass the Job submission workflow from batch orchestration.
* Do not create unbounded or fire-and-forget Batch tasks; await all owned tasks before the invocation exits.
* Preserve expected per-Job failure and ambiguity as isolated results rather than allowing them to cancel sibling Jobs.
* Propagate unexpected cancellation after required cleanup and durable-state handling; do not silently swallow cancellation.
* Do not let persistence or HTTP adapters introduce independent lifecycle transitions.
* Use the single local SQLite database and transaction policy defined by ADR-004; do not hold a database transaction open across HTTP, retry delay, or user input.
* Require current lease ownership and fencing generation before initiating a request or finalizing its normal post-side-effect transaction.
* Keep `job_id`, `idempotency_key`, payload-equivalence evidence, and remote request IDs in their distinct identity roles defined by ADR-005.
* Reject intentional reuse of an idempotency key with a non-equivalent Job Entry before creating an attempt or initiating HTTP.
* Retry submission only after definitive `429` or proven `NOT_SENT`; reconcile `MAYBE_SENT` evidence without creating another SubmissionAttempt.
* Enforce ADR-006 lifetime budgets, request deadlines, durable retry eligibility, and the service-wide rate-limit gate across process restarts.
* Do not share storage between the application and simulator.
* Do not use remote request IDs as local identities or uniqueness keys.
* Do not infer safe retry from reconciliation `404`, a remote request ID, or other non-authoritative evidence.
* Do not let simulator scenario controls override actual idempotency binding or payload-equivalence semantics.
* Do not send an external request before the required durable Job and SubmissionAttempt state exists.
* Do not automatically retry `AMBIGUOUS` Jobs or treat reconciliation as an implicit retry.
* Do not claim exactly-once remote processing; unreliable idempotency and reconciliation reduce duplicate risk but do not eliminate uncertainty.
* Keep physical schemas, package layout, CLI surface, concrete adapter error mappings, exact lease parameters, and retention policy outside this overview until approved separately.

## Known Risks and Trade-offs

* A short-lived CLI must recover from durable evidence rather than in-memory coordination.
* The external service may process a request without returning a usable response, so some outcomes remain ambiguous even with idempotency and reconciliation capabilities.
* Unreliable reconciliation may preserve ambiguity instead of resolving it.
* The service-wide rate-limit gate may conservatively delay unrelated Jobs, and anomalous `Retry-After` values are bounded by ADR-006.
* Local-only persistence avoids external infrastructure but does not provide distributed coordination or high availability.
* Keeping Batch as a candidate concept avoids premature persistence commitments but defers parts of batch recovery and reporting design.
* Deterministic simulator scenarios improve repeatability but cannot reproduce every failure mode of a real external service.

## Deferred Architecture Decisions

The following questions require ADRs before affected features are implemented:

* Is Batch persisted, and if so, what are its identity, membership, cardinality, lifecycle, and aggregate-state semantics?
* What retention and deletion rules apply to authoritative local history and simulator state?

Exact CLI commands, input methods, response schemas, the positive Batch concurrency limit and its user-facing configurability, Batch ordering and duplicate presentation, persistence schemas and coordination parameters, manual recovery/reconciliation operations, and explicit user operations from `AMBIGUOUS` belong to approved feature specifications rather than this overview.

`docs/specs/features/submit-single-job.md` defines the first such concrete contract: the `submit` command, its JSON result and exit-code schemas, and SQLite migration version `1`. Later features may add commands and migration versions without changing this overview.

## Related Architecture Decisions

* `docs/architecture/decisions/001-python-runtime-packaging-and-development-tooling.md` — Python runtime, packaging, and development tooling baseline.
* `docs/architecture/decisions/002-execution-model-and-application-boundaries.md` — synchronous CLI boundary, asynchronous application workflows, and bounded structured Batch concurrency.
* `docs/architecture/decisions/003-simulated-external-service-contract.md` — simulator endpoints, idempotency and payload-equivalence rules, reconciliation evidence, and deterministic failure scenarios.
* `docs/architecture/decisions/004-persistence-and-durable-consistency.md` — SQLite persistence, durable side-effect boundaries, multi-process fencing, crash evidence, and migration policy.
* `docs/architecture/decisions/005-identity-and-duplicate-prevention.md` — local Job identity, logical submission identity, key provenance, duplicate prevention, and stored-result replay.
* `docs/architecture/decisions/006-retry-rate-limits-ambiguity-and-recovery.md` — evidence-driven retry safety, bounded reconciliation, rate-limit coordination, ambiguity, and interrupted-attempt recovery.
