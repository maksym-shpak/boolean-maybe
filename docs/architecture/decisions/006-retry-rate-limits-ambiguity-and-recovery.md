# ADR-006: Retry, Rate Limits, Ambiguity, and Recovery

## Status

Accepted

## Date

2026-07-16

## Context

`boolean-maybe` must make bounded progress against an external service that may reject a request definitively, ask the client to wait, fail before dispatch, or fail after processing without delivering usable evidence. Treating all transport and server failures as retryable would risk duplicate remote work; treating all failures as permanent would discard the value of idempotency and reconciliation.

ADR-003 defines the evidence authority of submission and reconciliation responses. ADR-004 defines durable attempt boundaries, invocation leases, fencing, and the rule that an interrupted `STARTED` attempt is potentially sent. ADR-005 requires every retry and reconciliation operation to preserve the Job's idempotency key. This ADR defines how application workflows classify observations, spend retry budgets, respect rate limits, reconcile uncertain dispatch, recover interrupted work, and stop automatic processing.

The decision distinguishes submission retries, which may cause an external side effect, from reconciliation retries, which only observe remote state. Neither operation is assumed reliable.

The numeric limits are assessment-sized defaults rather than production tuning: ten seconds exposes deterministic timeout scenarios without prolonged hangs; three submission attempts and three reconciliation requests demonstrate bounded recovery without request storms; 500-millisecond full-jitter backoff keeps ordinary tests and manual evaluation responsive; 30 seconds bounds one CLI invocation's waiting; and one hour permits realistic delayed scheduling while rejecting effectively unbounded service-wide suspension. Changing these limits requires a later accepted decision because they affect observable reliability behavior and test duration.

## Decision

### Dispatch certainty

The external-service adapter reports one of two dispatch-certainty classes with every transport failure:

| Dispatch certainty | Meaning | Submission consequence |
| --- | --- | --- |
| `NOT_SENT` | The adapter can prove that no part of the HTTP request was dispatched and the service could not have processed it. Examples include failure to resolve, connect, or establish transport before request dispatch. | Safe-retry candidate. |
| `MAYBE_SENT` | Any request bytes may have been dispatched, or the adapter cannot prove otherwise. | Unsafe to resubmit automatically; enter reconciliation. |

The default for missing, library-specific, or uncertain evidence is `MAYBE_SENT`. A timeout, disconnect, cancellation, incomplete response, or protocol failure is not `NOT_SENT` merely because the application did not receive a response. The client feature specification must document and test every concrete adapter signal mapped to `NOT_SENT`; it may not infer certainty from simulator scenario configuration.

Each HTTP submission or reconciliation request has its own finite ten-second operation deadline. The deadline includes connection establishment, request transmission, and receipt of the complete response required for classification; retry delays and `Retry-After` waiting occur outside it. Deadline expiry is a timeout and defaults to `MAYBE_SENT` unless the adapter proves `NOT_SENT`. It is not user cancellation. An approved feature specification may expose a shorter user-configurable value, but it must not remove the deadline or exceed ten seconds without a later accepted decision.

### Submission observation classification

The submission workflow classifies public evidence as follows:

| Observed submission evidence | Attempt classification path | Automatic action |
| --- | --- | --- |
| Delivered `201` or replay `200` with matching key and payload digest | `SUCCEEDED` | Finalize Job as `SUCCEEDED`; stop. |
| Delivered validation `400` | `PERMANENT_FAILURE` | Finalize Job as `FAILED_PERMANENT`; stop. |
| Delivered `409` key conflict | `PERMANENT_FAILURE` | Finalize Job as `FAILED_PERMANENT`; stop. |
| Delivered `429` | `RETRYABLE_FAILURE` if submission budget remains | Persist rate-limit eligibility and retry only after it; otherwise exhaust the Job budget. |
| Transport failure proven `NOT_SENT` | `RETRYABLE_FAILURE` if submission budget remains | Schedule a safe retry; otherwise exhaust the Job budget. |
| Delivered `5xx`, incomplete or unexpected response, timeout, disconnect, or any transport failure classified `MAYBE_SENT` | Keep the active attempt unresolved while reconciliation runs | Never resubmit automatically from this evidence. |

An unexpected complete submission response that is not authoritative under ADR-003 is a protocol failure with `MAYBE_SENT` semantics. Automatic HTTP redirects are disabled; a redirect response is unexpected evidence rather than permission to dispatch the Job Entry to another location. A remote request ID alone never changes classification.

Baseline machine-readable observation categories map as follows:

| `error_category` | Exact evidence |
| --- | --- |
| `validation_rejected` | Delivered submission `400`. |
| `idempotency_conflict` | Delivered submission `409`, or authoritative reconciliation evidence that the key is bound to a non-equivalent payload. |
| `rate_limited` | Any delivered `429`, for submission or reconciliation. |
| `transport_not_sent` | Submission transport failure proven `NOT_SENT`. |
| `transport_maybe_sent` | Submission transport failure without a complete response and classified `MAYBE_SENT`, including timeout or disconnect. |
| `server_uncertain` | Delivered submission or reconciliation `5xx`. |
| `protocol_uncertain` | Unexpected, incomplete, malformed, redirected, or non-authoritative complete response. |
| `reconciliation_not_found` | Delivered reconciliation `404 not_found`. |
| `reconciliation_inconclusive` | Final ambiguous classification after reconciliation budget exhaustion or an unsupported wait, excluding `404`; intermediate evidence retains its category above. |

When an observation completes a SubmissionAttempt, its `error_category` records the category that caused the final attempt classification. Intermediate reconciliation observations, including `rate_limited`, remain sanitized diagnostics and do not overwrite a later definitive success/conflict or the final `reconciliation_inconclusive` category. Successful attempts need no error category. Feature specifications may add more specific backward-compatible categories, but they may not remap or weaken these baseline evidence classes.

### Submission retry budget

One Job has a durable lifetime budget of three SubmissionAttempts in total: the initial attempt plus at most two automatic safe retries. The budget is counted from persisted SubmissionAttempt records and therefore cannot reset across CLI invocations, process restarts, or concurrent processes.

Only `RETRYABLE_FAILURE` caused by definitive `429` or proven `NOT_SENT` evidence may consume the next automatic submission attempt. A retry preserves the same Job, Job Entry, and idempotency key and receives the next monotonic attempt number.

When a safe-retry candidate completes:

* if fewer than three SubmissionAttempts exist, the attempt remains `RETRYABLE_FAILURE` and the Job becomes `RETRY_SCHEDULED` with a durable earliest eligibility time;
* if it is the third SubmissionAttempt, the attempt remains `RETRYABLE_FAILURE`, the Job becomes `FAILED_PERMANENT`, and the workflow reports retry-budget exhaustion without creating another attempt.

For every `RETRYABLE_FAILURE` that leaves its Job `RETRY_SCHEDULED`, `retry_after_ms` records the non-negative effective interval from that attempt's `completed_at` to its calculated eligibility instant. The Job's authoritative earliest eligibility is derived as `completed_at + retry_after_ms`; no duplicate Job-level eligibility field is introduced. A budget-exhausting `RETRYABLE_FAILURE` may retain diagnostic delay evidence but creates no eligibility. The attempt and Job transition must persist atomically under ADR-004.

Budget exhaustion is a definitive local policy outcome, not evidence that the last request was remotely processed or permanently rejected. A later ordinary CLI invocation must not reset or extend the budget.

### Exponential backoff and jitter

After request ordinal `n` in the relevant submission or reconciliation sequence, where `n = 1` is the delay after the first request, the policy computes:

```text
backoff_cap(n) = min(30 seconds, 500 milliseconds * 2^(n - 1))
policy_delay(n) = uniform random duration in [0, backoff_cap(n)]
```

This is full jitter. Randomness affects scheduling only, never classification or identity. Tests must use an injected deterministic random source and cover both interval bounds.

With the selected budgets, actual retry waits use only `n = 1` and `n = 2`, producing caps of 500 milliseconds and 1 second. A terminal third `429` still calculates `n = 3` only to advance the shared service gate, even though no request remains in that sequence. The 30-second exponential cap provides bounded headroom if a later accepted decision increases either budget; it does not increase the current number of retries.

For a definitive `429`, the workflow parses `Retry-After` as either delta-seconds or an HTTP date. A syntactically valid header whose future delay is no greater than one hour establishes a server-required earliest retry instant. The effective eligibility is the later of that instant and the policy-delay instant. A missing, invalid, already elapsed, negative, overflowing, or greater-than-one-hour value falls back to `policy_delay(n)` and is recorded as anomalous diagnostics. The same bounded value advances the shared gate, so one response cannot suspend all Jobs indefinitely.

The application never caps or shortens an accepted future `Retry-After`; values beyond the explicit one-hour contract are invalid rather than truncated. It may wait inside the current invocation only when the effective remaining delay is at most 30 seconds. For a longer delay, it persists the eligibility time and returns the scheduled result instead of keeping the CLI process alive. A later invocation may continue only after durable eligibility has arrived.

`Retry-After` HTTP dates and durable eligibility use UTC wall-clock instants because they must survive process and host restart. The workflow converts an accepted HTTP date to a bounded interval at observation time and persists that interval in `retry_after_ms`. Wall-clock correction may make a safe retry occur earlier or later than intended, but it cannot weaken the `NOT_SENT`/`429` evidence that made submission retry safe. Feature specifications must test forward and backward clock adjustments and use a monotonic clock for in-process waiting.

### Shared rate-limit gate

Rate-limit evidence applies conservatively to the simulated external service as one remote authority. A delivered `429` from submission or reconciliation advances a durable service-wide `not_before` gate to the later of its current value and the bounded newly calculated eligibility.

The effective authorization time for a submission is `max(Job retry eligibility, service not_before gate)`, omitting Job eligibility for a first attempt. Both conditions must have arrived before another SubmissionAttempt can be allocated.

The gate check is an additional prerequisite inside the same ADR-004 pre-side-effect `BEGIN IMMEDIATE` transaction, after resolving the Job and before allocating `attempt_number`, inserting `STARTED`, or transitioning to `SUBMITTING`. If the effective authorization time has not arrived, that transaction creates no SubmissionAttempt and authorizes no HTTP request. This decision extends the ADR-004 pre-side-effect step list; ADR-004 records the synchronized prerequisite.

The effective authorization time for a reconciliation retry is `max(reconciliation policy/Retry-After eligibility, service not_before gate)`; the initial lookup has no sequence-local eligibility. Reconciliation uses its own short `BEGIN IMMEDIATE` authorization transaction immediately before each GET. That transaction verifies that the effective authorization time has arrived and, for an active attempt, verifies current lease/fencing ownership; it commits before HTTP and holds no database transaction during the request. A successful authorization commit permits that request to proceed immediately. A `429` observed by another process after this commit cannot revoke the already authorized request, just as a `429` cannot retroactively delay a dispatched request. No retry delay or unrelated work may occur between authorization commit and dispatch.

Batch tasks and concurrent CLI processes must use these authorization transactions and may not bypass the gate. A submission workflow may wait or return `RETRY_SCHEDULED` according to the 30-second in-process ceiling; an active reconciliation sequence may wait within the ceiling or stop as `AMBIGUOUS` under the rules below. Updating the gate and the affected Job/attempt scheduling evidence occurs in one local transaction where both are changed.

The gate is persistence-internal coordination metadata, not a core entity or Job lifecycle state. Simulator responses do not retroactively delay requests already dispatched by another process before the `429` was observed.

### Automatic reconciliation after uncertain submission

When submission evidence is `MAYBE_SENT`, the workflow does not complete the active SubmissionAttempt immediately and does not create another one. While retaining the current ADR-004 lease and fencing ownership, it performs bounded reconciliation with the same idempotency key:

1. Send reconciliation only after the shared rate-limit gate permits it.
2. If delivered `200 processed` evidence has the matching key and payload digest, complete the active attempt as `SUCCEEDED` and the Job as `SUCCEEDED`.
3. If delivered `200 processed` evidence proves a payload-digest mismatch for the same key, complete the attempt as `PERMANENT_FAILURE` and the Job as `FAILED_PERMANENT`.
4. If delivered `404 not_found`, complete the attempt and Job as `AMBIGUOUS`; do not submit again.
5. If reconciliation returns `429`, a `5xx`, an incomplete or unexpected response, timeout, or disconnect, retry reconciliation within its separate budget and delay policy.
6. If the reconciliation budget is exhausted or a required delay exceeds the 30-second in-process ceiling, complete the attempt and Job as `AMBIGUOUS`.

Reconciliation `404` is not retried automatically within this sequence because simulator processing records become visible atomically when processing commits under ADR-003; the contract has no eventual-consistency visibility lag. The negative observation still cannot prove that submission is safe, so the conservative terminal result is `AMBIGUOUS`. Malformed or mismatched evidence that does not meet ADR-003's authoritative-conflict requirements is inconclusive and follows step 5 rather than inventing success or failure.

Automatic reconciliation is part of classifying the same active SubmissionAttempt, not another SubmissionAttempt and not a submission retry. This avoids silently reclassifying a completed attempt.

### Reconciliation retry budget

One automatic reconciliation sequence may issue at most three reconciliation requests: the initial lookup plus at most two retries. Each request, including one that returns `429`, consumes this sequence budget.

After failed reconciliation request ordinal `n`, the workflow uses the same full-jitter formula defined above before any remaining retry. A valid `Retry-After` on reconciliation `429` supplies the same lower-bound behavior and updates the shared rate-limit gate. The reconciliation budget is scoped to the active classification or recovery sequence; it does not consume the Job's three-SubmissionAttempt budget.

A reconciliation delay beyond the 30-second in-process ceiling deliberately ends the current attempt as `AMBIGUOUS` instead of introducing a persisted `reconciliation-scheduled` lifecycle state. This is a simplicity trade-off: a later explicit reconciliation operation may start a new bounded read-only sequence, but automatic processing does not keep a `STARTED` attempt ownerless until a distant time.

An explicit later reconciliation operation for an `AMBIGUOUS` Job, if approved by a feature specification, starts a new bounded reconciliation sequence. It may resolve the Job only from authoritative ADR-003 evidence and may never submit or make the Job automatically retryable. Repeated user-invoked reconciliation is observable and does not silently run in the background.

### Recovery of interrupted `SUBMITTING` and `STARTED`

Recovery follows ADR-004 ownership and fencing before examining remote evidence:

1. A `SUBMITTING` Job with a `STARTED` attempt and an unexpired owner lease is active; another process stops recovery for that Job without sending or reconciling.
2. After lease expiry, recovery atomically claims the attempt and advances its fencing generation.
3. The claimed attempt is `MAYBE_SENT` even if interruption may have occurred before the network call. Recovery never creates a replacement SubmissionAttempt and never resubmits it automatically.
4. Recovery runs the bounded reconciliation sequence above under its current lease and fencing ownership.
5. Matching authoritative processed evidence finalizes success; authoritative key-conflict evidence finalizes permanent failure; `404`, exhausted or delayed reconciliation, and any remaining uncertainty finalize `AMBIGUOUS`.

If interruption occurred before the ADR-004 pre-side-effect transaction committed, no SubmissionAttempt was durably authorized and normal first-attempt processing may begin. A stale invocation that later reports an observation cannot use the normal completion path; its evidence enters the explicit fenced late-evidence workflow required by ADR-004.

### Cancellation and process interruption

User cancellation stops new automatic work promptly and propagates according to ADR-002 after durable cleanup:

* before a submission request can possibly dispatch, the owned attempt may be completed as `RETRYABLE_FAILURE` with `transport_not_sent` and normal budget rules;
* after possible dispatch, cancellation takes precedence over starting or continuing automatic reconciliation; the workflow leaves durable `SUBMITTING`/`STARTED` evidence for fenced recovery rather than resubmitting or claiming a definitive outcome;
* cancellation during a retry delay or before a reconciliation request sends no new request and preserves the already persisted eligibility or active recovery evidence.

The application does not swallow cancellation to finish all retries. Persistence cleanup, lease handling, and recording of already observed authoritative evidence precede propagation where possible.

A pre-dispatch cancellation still counts the already persisted SubmissionAttempt toward the lifetime budget. This keeps the budget derivable solely from durable attempt history and prevents repeated cancellation from creating unbounded attempts; the attempt records `NOT_SENT` evidence rather than pretending no attempt existed.

### Automatic stopping conditions

The application stops automatic work for a Job when any of these conditions occurs:

* the Job reaches `SUCCEEDED`, `FAILED_PERMANENT`, or `AMBIGUOUS`;
* the three-SubmissionAttempt budget is exhausted;
* a retry remains safely scheduled but its eligibility exceeds the 30-second in-process wait ceiling;
* uncertain submission is resolved by `404`, exhausts reconciliation, or requires a reconciliation/rate-limit wait beyond that ceiling;
* another unexpired invocation owns the active attempt;
* lease/fencing ownership is lost;
* user cancellation or process termination prevents safe continuation;
* persistence cannot durably record the next action or the commit result is uncertain.

`RETRY_SCHEDULED` means automatic submission remains eligible only after its persisted time and within the remaining lifetime budget; it does not require the current process to wait. `AMBIGUOUS` is terminal for automatic processing. Inspection, explicit reconciliation, manual resolution, or user-approved resubmission from `AMBIGUOUS` require an approved feature specification and cannot be inferred from this ADR.

## Consequences

Benefits:

* Submission retries occur only when public evidence proves that the failed request was not processed.
* Uncertain dispatch is reconciled before the application admits ambiguity, without creating a duplicate attempt.
* Durable lifetime budgets prevent process restarts or concurrent invocations from creating unbounded submission attempts.
* Full jitter reduces synchronized retries, while `Retry-After` and a shared durable gate coordinate rate limits across Batch tasks and CLI processes.
* Recovery uses the same evidence matrix as live execution and cannot confuse an abandoned attempt with a safe retry.
* The CLI stops predictably instead of waiting indefinitely or hiding unresolved remote state.

Trade-offs:

* A server error or transport failure may end as `AMBIGUOUS` even when the simulator did not process the request, because the public evidence cannot prove that fact.
* A delivered reconciliation `404` deliberately prevents automatic resubmission and may require later human-directed work.
* Three total submission attempts and three reconciliation requests favor bounded evaluation over aggressive availability.
* A service-wide rate-limit gate may delay unrelated Jobs more conservatively than the simulator strictly requires.
* Persisted scheduling and cross-process rate-limit coordination add infrastructure metadata and clock-handling tests.
* Keeping an attempt `STARTED` during automatic reconciliation extends lease ownership and requires renewal without holding a database transaction open.

## Impact on Core Entities

No new core entity or field is added, but one existing `SubmissionAttempt.retry_after_ms` invariant is made explicit and requires synchronization on acceptance.

This decision supplies the policies already anticipated by the existing model:

* `retry_after_ms` is required when `RETRYABLE_FAILURE` transitions its Job to `RETRY_SCHEDULED` and records the effective interval from `completed_at` to calculated eligibility;
* a `RETRY_SCHEDULED` Job derives its authoritative earliest eligibility as the most recent attempt's `completed_at + retry_after_ms`;
* retry exhaustion transitions the Job to `FAILED_PERMANENT` without rewriting the last attempt's evidence;
* uncertain submission and inconclusive reconciliation complete the attempt and Job as `AMBIGUOUS`;
* `STARTED` remains active while uncertain submission is reconciled or awaits fenced recovery.

`docs/domain/core-entities.md` records the conditional `retry_after_ms` requirement and derived Job eligibility rule. Physical storage and indexing of that derivation, and representation of service-wide rate-limit metadata, remain persistence feature contracts under ADR-004. Any proposal to add a duplicate authoritative Job eligibility field requires a later approved core-entity change and must not be introduced silently.

No lifecycle state, identity, relationship, or ownership rule changes. `AMBIGUOUS` remains terminal for automatic processing.

## Alternatives Considered

1. **Retry every timeout, disconnect, and `5xx` with the same idempotency key.**
   Rejected because ADR-003 makes these observations ambiguous and simulator state may be reset or unavailable; idempotency reduces risk but does not authorize claiming every resubmission is safe.

2. **Never retry submission.**
   Rejected because definitive `429` and proven pre-dispatch failures establish that the request was not processed and can be retried safely within a budget.

3. **Mark every uncertain transport failure `AMBIGUOUS` immediately.**
   Rejected because bounded reconciliation may supply authoritative processed or conflict evidence without another submission.

4. **Treat reconciliation `404` as permission to resubmit.**
   Rejected by ADR-003 because it is only a point-in-time negative observation and simulator reset limits evidence lifetime.

5. **Use fixed delays without jitter.**
   Rejected because concurrent Batch tasks and CLI processes could synchronize repeated requests.

6. **Apply an in-memory rate-limit delay only to the Job that received `429`.**
   Rejected because it would not coordinate sibling Batch tasks or other CLI processes using the same service and local store.

7. **Reset retry budgets on every CLI invocation.**
   Rejected because process restarts would create unbounded attempts and make policy outcomes depend on invocation boundaries.

8. **Count reconciliation requests as SubmissionAttempts.**
   Rejected because reconciliation is observational and must not distort delivery history or consume the side-effect budget.

9. **Leave an inconclusive recovered attempt permanently `STARTED`.**
   Rejected because after bounded recovery it would remain active without an owner and obscure the explicit ambiguous outcome.

10. **Hold the CLI open for every future `Retry-After`.**
    Rejected because the application is an on-demand process with durable scheduling; long waits belong in persisted eligibility rather than process lifetime.

## Scope

This decision applies to:

* submission and reconciliation error classification;
* dispatch certainty and safe versus unsafe submission retries;
* submission and reconciliation retry budgets;
* exponential backoff, full jitter, operation deadline, and `Retry-After` handling;
* cross-Job and cross-process rate-limit coordination;
* automatic reconciliation after uncertain dispatch;
* ambiguity classification and automatic stopping conditions;
* recovery of interrupted `SUBMITTING` Jobs and `STARTED` attempts;
* cancellation behavior around possible external side effects.

This decision does not select or define:

* a concrete HTTP library or exception hierarchy;
* exact CLI commands, output schemas, or exit-code mapping beyond existing approved contracts;
* user configurability beyond the bounded timeout allowance stated above;
* explicit manual resolution or user-approved resubmission from `AMBIGUOUS`;
* simulator scenario configuration or state-retention policy;
* physical persistence schema, lease duration, or clock/boot-identity mechanism;
* Batch identity, persistence, ordering, or aggregate-result behavior;
* retention, archival, or deletion policy.

## Follow-up on Acceptance

If this ADR is accepted:

* update `docs/architecture/architecture-overview.md` with the evidence-driven retry matrix, bounded automatic reconciliation, budgets, durable rate-limit gate, and stopping rules;
* remove the deferred architecture question about timeout, retry-budget, rate-limit, and retry-exhaustion policy;
* add this ADR to the overview's Related Architecture Decisions section;
* update ADR-004's pre-side-effect transaction to include the service-gate and Job-eligibility authorization prerequisite defined here;
* update `docs/domain/core-entities.md` with the conditional `retry_after_ms` requirement and the derived `RETRY_SCHEDULED` eligibility rule;
* require submission/recovery feature specifications to define concrete adapter mappings, sanitized diagnostics, result presentation, cancellation cleanup, and persisted scheduling representation;
* require deterministic tests for all evidence classes, both dispatch-certainty classes, three-attempt exhaustion across restarts, full-jitter bounds, valid/invalid `Retry-After`, long-delay process exit, shared-gate Batch and multi-process races, uncertain submission followed by every reconciliation outcome, reconciliation exhaustion, live-owner exclusion, lease takeover, stale-owner late evidence, cancellation at each dispatch boundary, and uncertain local commits.

No product-brief or glossary update is required: retry, rate limit, reconciliation, retryable/permanent failure, and ambiguous outcome already have the required product meanings. The new dispatch-certainty labels, gate, budgets, and timing formulas are internal architecture contracts rather than new product-facing concepts.

## Related Documents

* `docs/product/product-brief.md`
* `docs/product/glossary.md`
* `docs/domain/core-entities.md`
* `docs/architecture/architecture-overview.md`
* `docs/architecture/decisions/002-execution-model-and-application-boundaries.md`
* `docs/architecture/decisions/003-simulated-external-service-contract.md`
* `docs/architecture/decisions/004-persistence-and-durable-consistency.md`
* `docs/architecture/decisions/005-identity-and-duplicate-prevention.md`

External technical references:

* [RFC 9110: HTTP Semantics — Retry-After](https://www.rfc-editor.org/rfc/rfc9110.html#name-retry-after)
* [RFC 6585: Additional HTTP Status Codes — 429 Too Many Requests](https://www.rfc-editor.org/rfc/rfc6585.html#section-4)

Supersedes: none

Superseded by: none
