# ADR-002: Execution Model and Application Boundaries

## Status

Accepted

## Date

2026-07-16

## Context

`boolean-maybe` is an on-demand CLI application that performs network and persistence I/O, supports single-Job and multi-Job workflows, and must remain responsive to interruption without introducing a background worker.

The architecture overview already separates the CLI adapter from application workflows, persistence, and the external service client. It also requires Batch orchestration to reuse the single-Job submission workflow and preserve independent Job state. The execution model must make these boundaries implementable without coupling application logic to a CLI framework.

A fully synchronous model would be simple for single-Job operations but would make bounded overlap of I/O-bound Batch work depend on threads or later architectural rework. A fully asynchronous CLI surface would expose event-loop concerns at the presentation boundary without adding product value.

## Decision

### Synchronous CLI boundary

* The installed console entry point remains a synchronous function owned by the CLI adapter.
* The CLI adapter parses input, constructs a workflow request, invokes one top-level asynchronous application workflow, and renders the returned result.
* A CLI invocation owns one event-loop lifecycle and enters the asynchronous application through `asyncio.run()` or an equivalent standard-library runner exactly once.
* CLI framework objects, callbacks, exceptions, and context types must not cross into application or domain interfaces.
* The CLI adapter must not call persistence or the external service client directly and must not own concurrency, retry, reconciliation, or Job lifecycle decisions.

### Asynchronous application workflows

* Job submission, Batch orchestration, reconciliation, and recovery orchestration are asynchronous application workflows.
* Workflow interfaces use application-owned request and result types rather than CLI-framework types.
* Workflows coordinate I/O through explicit persistence and external-service ports. Their concrete adapters may use synchronous or asynchronous libraries, but they must satisfy the asynchronous workflow contract without blocking the event loop during potentially long I/O.
* Domain rules, entity invariants, state-transition validation, and deterministic classification logic remain synchronous and independent of `asyncio`.
* Async orchestration must not become a second source of domain state or lifecycle rules.

### Structured and bounded Batch concurrency

* Batch orchestration may run multiple eligible single-Job submission workflows concurrently because their dominant work is expected to be I/O-bound.
* The number of concurrently active Job workflows must be bounded by an explicit positive limit.
* The exact default limit and its user-facing configurability are defined by the Batch feature specification. This ADR defines only that the limit exists and is enforced within one CLI invocation.
* Batch concurrency must use structured task ownership: every task is awaited before the workflow returns, and no fire-and-forget Job task may outlive the invocation.
* Expected Job-level outcomes, including permanent failure and ambiguity, are captured as independent Job results and must not cancel or overwrite sibling Jobs.
* Unexpected cancellation must be propagated after required cleanup and durable-state handling. Coroutines must not silently swallow `asyncio.CancelledError`.
* Batch orchestration delegates actual submission to the same Job submission workflow used for a single Job; it does not duplicate submission, retry, or persistence rules.

### Resource and lifecycle boundaries

* Infrastructure resources used by a workflow are created and closed within the application invocation or within an explicitly scoped adapter lifetime.
* The application does not keep a process-global event loop, background task registry, scheduler, or long-running worker.
* Rate-limit coordination, retry delays, timeouts, and persistence consistency remain governed by their own ADRs and feature specifications. Their implementations must integrate with this async workflow boundary without blocking unrelated eligible work.
* CPU-bound work is not a justification for additional concurrency in this decision. Any later process or thread execution model requires separate justification.

## Consequences

Benefits:

* Single-Job and Batch execution share one application workflow model.
* I/O waits for multiple Jobs can overlap while concurrency remains bounded and reviewable.
* The synchronous console entry point remains simple for shell users and CLI tooling.
* Domain behavior stays deterministic, directly testable, and independent of event-loop or framework concerns.
* Structured task ownership prevents background work from silently surviving the short-lived CLI invocation.

Trade-offs:

* Application workflow tests need async test support or standard-library event-loop runners.
* Persistence and HTTP adapters must integrate safely with an async caller even if their selected libraries are synchronous.
* Cancellation and cleanup become explicit correctness concerns, especially around durable state and potentially started external effects.
* Expected Job failures must be represented as results rather than escaping in a way that unintentionally cancels sibling work.
* Async orchestration adds complexity that would not be necessary for single-Job execution alone; Batch I/O overlap is the justification for accepting it.

## Impact on Core Entities

None.

This decision does not add or change core entities, fields, identities, invariants, lifecycle states, relationships, ownership, compatibility rules, or data security boundaries. It preserves the existing requirement that every Job has independent authoritative state and that Batch orchestration uses the single-Job workflow.

No update to `docs/domain/core-entities.md` is required.

## Alternatives Considered

1. **Synchronous CLI and synchronous application workflows.**
   Not selected because bounded Batch overlap would then require a thread-based orchestration model or sequential network waits. It remains a reasonable simpler model for a single-Job-only product, but Batch orchestration is in scope.

2. **Asynchronous CLI entry point and asynchronous workflows.**
   Not selected because shell invocation and CLI frameworks are naturally synchronous boundaries. Exposing async concerns there would couple presentation to the execution model without improving product behavior.

3. **Synchronous domain and application core with an async-only Batch wrapper.**
   Not selected because submission and reconciliation perform the same I/O operations in both single and Batch flows. Maintaining separate sync and async orchestration paths would risk duplicated reliability logic.

4. **Thread-based Batch concurrency.**
   Not selected as the primary model because the expected concurrency is I/O-bound and the application already benefits from async HTTP and persistence boundaries. Threads may still be used inside a concrete adapter when an approved synchronous dependency must be isolated from the event loop.

5. **Unbounded task creation for all Batch entries.**
   Rejected because it would make resource use, rate-limit coordination, and persistence contention depend directly on input size.

6. **A background worker or persistent event loop.**
   Rejected because it contradicts the on-demand runtime model and the product non-goal of a long-running background worker.

## Scope

This decision applies to:

* the synchronous CLI-to-application transition;
* the asynchronous application workflow contract;
* separation of domain logic from CLI and async infrastructure;
* structured task ownership and bounded concurrency for Batch execution;
* event-loop and resource lifetime within one CLI invocation.

This decision does not select or define:

* a CLI framework or concrete commands;
* an HTTP client or persistence library;
* physical persistence transactions or schemas;
* the Batch concurrency limit or user-facing option;
* retry counts, delays, timeouts, rate-limit algorithms, or recovery policy;
* exact Batch interruption, ordering, or aggregate-result behavior;
* simulator server framework or process-launch mechanism.

## Follow-up on Acceptance

If this ADR is accepted:

* update `docs/architecture/architecture-overview.md` to identify the synchronous CLI adapter, async application workflow boundary, and bounded structured Batch concurrency as decided constraints;
* remove or narrow the deferred architecture question about the Batch concurrency model while retaining the feature-level question of the exact limit;
* add this ADR to the overview's Related Architecture Decisions section;
* require affected feature specifications to describe cancellation, per-Job result isolation, and tests for the selected concurrency limit where applicable.

## Related Documents

* `docs/product/product-brief.md`
* `docs/product/glossary.md`
* `docs/domain/core-entities.md`
* `docs/architecture/architecture-overview.md`
* `docs/architecture/decisions/001-python-runtime-packaging-and-development-tooling.md`

External technical references:

* [Python 3.12 asyncio runners](https://docs.python.org/3.12/library/asyncio-runner.html)
* [Python 3.12 coroutines, tasks, and structured concurrency](https://docs.python.org/3.12/library/asyncio-task.html)

Supersedes: none

Superseded by: none
