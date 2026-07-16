# Product Brief

## Product Name

**boolean-maybe**

## One-Sentence Description

A resilient command-line application for submitting and managing jobs through a deliberately unreliable external API.

## What We Are Building

`boolean-maybe` is a command-line application that accepts Job Entries and submits them to a simulated external HTTP service whose behavior is intentionally unreliable. A Job Entry is a user-provided JSON object describing one unit of work.

The application manages each job locally, records submission attempts, prevents duplicate submissions, applies bounded retry policies, respects rate limits, and preserves execution state across process restarts.

The project also includes a lightweight simulated external service capable of reproducing defined failure scenarios such as server errors, timeouts, rate limiting, processed requests without responses, duplicate remote request IDs, and unexpected disconnects.

The simulator exists to exercise and demonstrate the reliability behavior of the CLI. It is not the primary product and must remain small, deterministic, and easy to run.

## Target Users

The primary users are automation engineers, platform engineers, and technical operators who need to submit jobs to an unreliable external service from scripts, terminals, or automated workflows.

A secondary user is the engineer reviewing or maintaining the application, who needs to understand its behavior, failure semantics, and operational trade-offs without reverse-engineering the implementation.

## User Problems

* A request may fail before reaching the external service or after the service has already processed it.
* Blind retries may create duplicate submissions.
* Remote request identifiers may be duplicated and therefore cannot be trusted as local job identities.
* Rate limiting requires coordinated delays rather than immediate repeated requests.
* A CLI process may stop while jobs are pending or in progress.
* Users need to distinguish successful, failed, retryable, and ambiguous outcomes.
* Logs and exit codes must be suitable for both human diagnosis and automated workflows.

## Primary Use Cases

* Submit a single job entry and receive a clear machine-readable result.
* Submit multiple job entries while preserving independent state for each job.
* Retry transient failures according to a bounded retry policy.
* Resume or inspect jobs after the CLI process has stopped.
* Detect an existing logical submission and avoid creating a duplicate submission.
* Identify submissions whose remote outcome cannot be determined safely.
* Diagnose execution through structured JSON logs and meaningful exit codes.
* Reproduce external-service failure scenarios locally.

## Product Goals

* Make job submission predictable even when the external service is not.
* Preserve enough local state to support safe recovery after failures or restarts.
* Prevent duplicate submissions whenever the available external-service contract makes prevention possible.
* Represent uncertain remote outcomes explicitly rather than reporting false success or false failure.
* Provide retry behavior that is bounded, explainable, and sensitive to rate limits.
* Make the CLI suitable for shell scripts and automated systems.
* Keep the implementation focused, maintainable, and easy to evaluate.
* Document assumptions, guarantees, limitations, failure scenarios, and trade-offs clearly.

## Non-Goals

* Building a general-purpose workflow orchestration platform.
* Providing a long-running background worker or distributed job queue.
* Providing a graphical or web-based user interface.
* Supporting multiple unrelated external-service protocols.
* Providing distributed high availability.
* Guaranteeing exactly-once remote processing. Idempotent submission and reconciliation reduce duplicate risk but do not provide this guarantee when those operations are themselves unreliable.
* Automatically resolving every ambiguous outcome.
* Optimizing for large-scale or high-throughput production workloads.
* Hiding unresolved remote state behind a generic success or failure result.

## Product Principles

1. **Uncertainty must be explicit.**
   When the application cannot determine whether the external service processed a request, it must preserve and report an ambiguous outcome.

2. **Local identity is authoritative.**
   Jobs and submission attempts use client-generated identities. A remote request ID is recorded as external evidence but is not trusted as a unique local identifier.

3. **Retries must be deliberate.**
   Retries are performed only for classified retryable outcomes, use bounded delays, respect rate-limit information, and preserve the logical identity of the submission.

4. **State must precede side effects.**
   The application records sufficient durable state before initiating an external request so that interruption does not silently erase evidence of an in-progress submission.

5. **Failures must be safe.**
   Local persistence or state-transition failures must stop external submission rather than risk untracked side effects.

6. **Single-job behavior is foundational.**
   Batch execution orchestrates the same submission behavior used for one job rather than implementing a separate reliability model.

7. **Operational contracts must be stable.**
   Command output, structured logs, and exit-code meanings must be documented and suitable for automation.

8. **Scope must remain proportional.**
   The project should demonstrate reliability and engineering judgment without introducing infrastructure unrelated to the assessment.

## Key Domain Concepts

* Ambiguous Outcome
* Batch
* Duplicate Submission
* Execution State
* External Service
* Idempotency Key
* Job
* Job Entry
* Permanent Failure
* Rate Limit
* Reconciliation
* Remote Request ID
* Retry
* Retryable Failure
* Simulated External Service
* Structured Log
* Submission
* Submission Attempt

Detailed definitions are maintained in:

```text
docs/product/glossary.md
```

Stable entity contracts and lifecycle rules are maintained in:

```text
docs/domain/core-entities.md
```

## Success Criteria

The product is successful when:

* A user can install and run the CLI using documented commands.
* A user can submit one or more valid job entries.
* Repeating the same logical submission does not silently create a duplicate submission.
* Transient failures and rate limits result in bounded, observable retry behavior.
* Completed execution state survives process restarts.
* Interrupted or uncertain requests are represented without inventing a definitive outcome.
* Duplicate remote request IDs do not corrupt or merge unrelated local jobs.
* Each command returns documented, meaningful exit codes.
* Operational events are emitted as structured JSON logs.
* The simulated service can reproduce the documented failure scenarios.
* The README clearly explains setup, assumptions, guarantees, limitations, failure handling, trade-offs, and likely improvements.

## Constraints

* The implementation language is Python.
* The product is operated through a command-line interface.
* The external dependency is represented by a local simulated HTTP service.
* Execution state must be persisted locally.
* Logs must be available in structured JSON form.
* The repository must be runnable by a reviewer without external infrastructure.
* The implementation must prioritize reliability and clarity over feature completeness.
* Tooling and runtime dependencies must remain proportionate to the project scope.

## Current Assumptions

* A Job Entry is a user-provided JSON object describing one unit of work; its exact feature-specific schema must be defined by the applicable approved feature specification.
* The CLI and simulated external service run in a trusted local development or evaluation environment.
* Authentication and authorization are outside the current scope.
* The simulated service exposes enough configurable behavior to reproduce deterministic failure scenarios.
* The same idempotency key must not be accepted with materially different job content.
* Remote request IDs may be absent or duplicated.
* Some failures may leave the remote processing result unknowable.
* A user or later reconciliation operation may be required to resolve ambiguous outcomes.
* Batch workloads are small enough for bounded local concurrency.
* One local persistence store is sufficient for the assessment.

## Approved Product Decisions

* The application workflow generates an idempotency key by default, while the CLI also accepts a user-provided key. An idempotency key must never be derived from the Job Entry payload.
* The simulated external service supports idempotent submission and reconciliation by idempotency key. Both endpoints remain operationally unreliable so that requests may still produce retryable or ambiguous client-observed outcomes.
* When a request matches an existing `SUCCEEDED` Job by idempotency key and equivalent Job Entry, the CLI returns the stored result with `outcome` set to `already_completed`, `submitted` set to `false`, and exit code `0`. In this result, `submitted=false` means the current CLI invocation did not send another external request.

## Open Product Questions

* What exact data fields constitute a job entry?
* Which CLI input methods are required: inline JSON, file input, standard input, or a subset of these?
* Which operations should be available for ambiguous jobs: inspection only, explicit retry, reconciliation, or manual resolution?
* What level of batch concurrency is appropriate as the default?
* Should human-readable output be supported in addition to machine-readable JSON command results?
