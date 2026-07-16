# ADR-003: Simulated External Service Contract

## Status

Accepted

## Date

2026-07-16

## Context

The CLI can offer only the guarantees supported by the external service contract and by the evidence that the client actually observes. The simulated external service must therefore define idempotency, reconciliation, and failure effects precisely enough to distinguish safe retry from genuine uncertainty without becoming a second authority for local Job identity.

The product has already decided that the simulator supports idempotent submission and reconciliation by idempotency key, that both operations remain operationally unreliable, and that remote request IDs may be absent or duplicated. The contract must preserve those decisions while keeping failure scenarios deterministic and reproducible.

The simulator is a local assessment component, not a general-purpose production API. Its contract should be small, explicit, and sufficient to demonstrate the limits of client-side reliability.

## Decision

### HTTP operations

The simulator exposes two product-facing operations:

| Operation | HTTP contract | Purpose |
| --- | --- | --- |
| Submit Job Entry | `POST /jobs` with `Content-Type: application/json` and required `Idempotency-Key` header | Bind an idempotency key to one Job Entry and process it at most once within the simulator state lifetime. |
| Reconcile by idempotency key | `GET /jobs/by-idempotency-key/{idempotency_key}` | Return the simulator's current evidence for that key without initiating submission or processing. |

The idempotency key in the reconciliation path is URI-encoded as an opaque value. Exact key length, character, and validation rules are defined by the applicable feature specification.

The detailed JSON schemas may add diagnostic fields in an approved feature specification, but every successful processed response must include at least:

* the `idempotency_key`;
* `status = "processed"`;
* the canonical `payload_digest` defined below;
* a `remote_request_id`, which may be absent or duplicated;
* for submission responses, whether the response represents the first observed processing or an idempotent replay.

### Simulator-side payload equivalence

* A Job Entry must be a JSON object that is valid input to the JSON Canonicalization Scheme in RFC 8785, including its I-JSON constraints and verified errata.
* The canonical payload is the UTF-8 RFC 8785 representation of the complete Job Entry.
* Two Job Entries are equivalent if and only if their canonical payload bytes are identical.
* `payload_digest` is `sha256:` followed by the lowercase hexadecimal SHA-256 digest of those canonical bytes.
* Object member order and insignificant JSON serialization whitespace do not affect equivalence. Array order and distinctions preserved by RFC 8785 do affect equivalence.
* JSON input that cannot be represented canonically, including ambiguous duplicate object member names, must be rejected before processing.
* The digest is comparison evidence and operational metadata, not a security boundary or a local identity.

This rule defines equivalence at the simulator submission boundary and the corresponding comparison rule used by the client for idempotency-key conflict detection. It does not add `payload_digest` as a required persisted field on `Job`.

### Idempotent submission

Simulator processing state is keyed by `idempotency_key`. Operations for the same key are serialized so that concurrent requests cannot process the key twice.

For `POST /jobs`:

1. If the key has no processed record and the selected scenario permits processing, the simulator atomically binds the key to the canonical payload, records one processed result, and returns `201 Created` when the response is delivered.
2. If the key is already bound to an equivalent payload, the simulator does not process it again and returns the stored result with `200 OK` and replay evidence when the response is delivered.
3. If the key is already bound to a non-equivalent payload, the simulator does not mutate the existing record and returns `409 Conflict` with the stored and submitted payload digests.
4. Malformed or non-canonicalizable input is rejected with `400 Bad Request` before processing.
5. A rate-limited request returns `429 Too Many Requests` with `Retry-After` and is not processed.

The simulator may fail before or after committing the processed record. Therefore, absence of a usable success response does not establish that processing did not occur.

Idempotency is guaranteed only while the corresponding simulator processing record exists. Simulator state lifetime and reset behavior must be explicit in the simulator feature specification and runtime documentation; a reset must not be presented as evidence that an earlier request was never processed.

### Reconciliation semantics

`GET /jobs/by-idempotency-key/{idempotency_key}` never creates or changes a processed record.

* If a record exists and the response is delivered, the simulator returns `200 OK` with `status = "processed"`, the bound payload digest, and any stored remote request ID.
* If no record exists at lookup time and the response is delivered, the simulator returns `404 Not Found` with `status = "not_found"`.
* Reconciliation may also be rate-limited, fail with a server error, time out, or disconnect according to the deterministic scenario plan.

A delivered `200 processed` response is authoritative positive evidence of remote processing only when its idempotency key and payload digest match the local Job Entry. It may resolve an ambiguous Job to a successful remote outcome through an approved feature workflow.

A digest mismatch is authoritative evidence that the key is bound to a different payload, not evidence that the local Job succeeded.

A `404 not_found` response is a point-in-time negative observation, not authoritative proof that no earlier processing occurred. It must not, by itself, make an ambiguous Job eligible for automatic retry. A missing response, timeout, disconnect, `429`, or server error is likewise inconclusive.

Remote request IDs are never authoritative reconciliation keys or proof of uniqueness. The reconciliation operation cannot be queried by remote request ID.

### Evidence authority

| Observed evidence | Contract conclusion |
| --- | --- |
| Delivered submission `201` or replay `200` with matching key and payload digest | Definitive evidence that the equivalent Job Entry was processed. |
| Delivered reconciliation `200 processed` with matching key and payload digest | Definitive evidence that the equivalent Job Entry was processed. |
| Delivered `409 Conflict` | Definitive evidence that the key is already bound to a non-equivalent payload; no processing occurred for the conflicting request. |
| Delivered validation `400` or rate-limit `429` | Definitive evidence that this request was not processed. |
| Delivered reconciliation `404 not_found` | Point-in-time negative observation only; insufficient by itself to prove that no earlier processing occurred or to authorize automatic retry. |
| Delivered `5xx`, incomplete response, timeout, or disconnect | Ambiguous with respect to remote processing because the same observation can occur before or after commit. |
| Remote request ID alone | Non-authoritative evidence; never proof of identity, uniqueness, or processing for a local Job. |

The client must not infer stronger conclusions from implementation knowledge of a configured scenario than it could infer from this public evidence contract.

### Deterministic failure scenarios

Scenario selection is controlled by a simulator-owned scenario plan, separate from the product-facing request payload and idempotency key. The mechanism used to provide the plan at simulator startup is defined by its feature specification.

The scenario plan may select failure timing, response delivery, rate limiting, and remote request ID behavior, but it must not override actual idempotency binding or payload-equivalence semantics. It cannot force fresh processing for an already bound key, an idempotency conflict for an equivalent payload, or an equivalent replay for a non-equivalent payload.

Each planned action is selected deterministically by:

* operation: submission or reconciliation;
* an exact idempotency key or an explicit wildcard;
* the one-based request ordinal for that operation and key.

Per-key ordinals and serialized same-key processing make scenario selection stable even when different keys are requested concurrently. An exhausted plan uses an explicitly documented default action rather than randomness.

An exact-key plan entry takes precedence over a wildcard entry for the same operation and ordinal. Duplicate entries with equal precedence are invalid configuration. When no entry matches, the default action is normal submission or normal reconciliation according to current simulator state.

The simulator must support at least these scenario effects:

| Scenario effect | Remote state effect | Client observation |
| --- | --- | --- |
| Normal submission | Process once, or replay the stored result | Delivered `201` or `200` response |
| Validation rejection | No processing | Delivered `400` response |
| Idempotency conflict | Existing record unchanged | Delivered `409` response |
| Rate limited | No processing | Delivered `429` response with `Retry-After` |
| Server error before processing | No processing | Delivered `5xx` response |
| Server error after processing | Processed record committed | Delivered `5xx` response |
| Disconnect or timeout before processing | No processing | No complete response |
| Processed without response | Processed record committed | Disconnect or timeout before a complete response |
| Duplicate remote request ID | Scenario-selected ID may equal an ID used by another key | Otherwise normal processed response or reconciliation evidence |
| Reconciliation processed | No mutation | Delivered authoritative positive `200` evidence |
| Reconciliation not found | No mutation | Delivered non-authoritative negative `404` observation |
| Reconciliation failure | No mutation | `429`, `5xx`, timeout, or disconnect |

The client cannot observe the simulator's internal scenario name or state-effect column. It must classify outcomes only from transport evidence and the public contract. In particular, the same observed `5xx`, timeout, or disconnect can occur before or after processing and is therefore not definitive unless the public response contract explicitly says otherwise.

### Remote request ID behavior

* A remote request ID is generated or selected only when a processed record is created.
* Idempotent replay returns the stored ID when one exists; it does not create a new one.
* Scenario configuration may deliberately assign the same remote request ID to unrelated idempotency keys.
* Remote request IDs have no uniqueness constraint and cannot merge, overwrite, or re-identify local Jobs.

## Consequences

Benefits:

* The simulator demonstrates both safe idempotent replay and failures that remain genuinely ambiguous.
* Submission and reconciliation evidence have explicit authority limits, preventing false success and unsafe retry.
* Standard canonicalization gives client and simulator one portable payload-equivalence rule without making a digest a domain identity.
* Per-key deterministic scenarios remain reproducible under bounded concurrent Batch execution.
* Duplicate remote request IDs can be tested without corrupting idempotency semantics.

Trade-offs:

* RFC 8785 and SHA-256 support add implementation and interoperability work to both client and simulator.
* Some otherwise valid JSON cannot be accepted because canonicalization requires I-JSON-compatible input.
* Negative reconciliation deliberately cannot resolve every ambiguous outcome or authorize automatic retry.
* Simulator state reset limits the duration of its idempotency evidence and must be visible operationally.
* Detailed scenario planning is more explicit than random fault injection but requires test data and documentation.

## Impact on Core Entities

This decision does not add a core entity or required field. In particular, `payload_digest` remains derived external evidence and must not become a required `Job` or `SubmissionAttempt` field solely because of this ADR.

The accepted decision would define the Job Entry equivalence rule referenced by the existing `idempotency_key` invariant. On acceptance, `docs/domain/core-entities.md` must be updated to:

* replace the open question about equivalence and canonicalization with a reference to RFC 8785 canonical-byte equality;
* preserve the rule that non-equivalent key reuse is rejected before an external request;
* state that a digest may be computed or persisted by an approved design but is not authoritative identity or a security boundary.

No lifecycle state, relationship, or ownership change is required.

## Alternatives Considered

1. **Treat raw request bytes as payload identity.**
   Rejected because member ordering and insignificant whitespace would make semantically equivalent Job Entries conflict.

2. **Use implementation-specific sorted-key JSON.**
   Rejected because number and string serialization can differ across runtimes. RFC 8785 supplies an explicit cross-language canonical representation.

3. **Trust idempotency key alone and ignore payload mismatch.**
   Rejected because accidental key reuse could silently return an unrelated processed result.

4. **Treat successful `404 not_found` reconciliation as proof that retry is safe.**
   Rejected because state reset, retention, races, or prior evidence loss can make absence weaker than proof of non-processing.

5. **Use remote request ID for reconciliation.**
   Rejected because the product explicitly allows absent and duplicate remote request IDs.

6. **Select failure scenarios randomly per request.**
   Rejected because nondeterminism would make reliability tests difficult to reproduce and diagnose.

7. **Let each request choose its own scenario through the product payload.**
   Rejected because simulator controls would contaminate the external service contract and Job Entry schema.

8. **Expose only positive-path idempotent responses.**
   Rejected because the assessment must demonstrate processed-without-response, ambiguous transport evidence, rate limiting, and duplicate remote IDs.

## Scope

This decision applies to:

* simulator submission and reconciliation endpoints;
* simulator-side idempotency and payload-equivalence semantics;
* minimum processed evidence and its authority;
* deterministic failure selection and state effects;
* duplicate remote request ID and processed-without-response behavior.

This decision does not select or define:

* a simulator web framework, HTTP client library, or process-launch command;
* physical simulator storage, schema, migrations, or retention duration;
* detailed request and response schemas beyond the minimum contract evidence;
* CLI commands or user-facing reconciliation operations;
* client retry counts, delays, timeouts, rate-limit scheduling, or recovery transitions;
* authentication, authorization, or deployment outside the trusted local assessment environment.

## Follow-up on Acceptance

If this ADR is accepted:

* update `docs/architecture/architecture-overview.md` with the selected endpoints, idempotency binding, canonical equivalence rule, evidence-authority limits, and deterministic per-key scenario model;
* remove or narrow the deferred questions about Job Entry equivalence and simulator HTTP evidence semantics;
* update `docs/domain/core-entities.md` as described in Impact on Core Entities;
* add this ADR to the overview's Related Architecture Decisions section;
* require simulator and external-client feature specifications to provide contract tests for equivalent replay, non-equivalent conflict, processed-without-response followed by reconciliation, negative reconciliation, duplicate remote IDs, and deterministic concurrent scenario selection.

## Related Documents

* `docs/product/product-brief.md`
* `docs/product/glossary.md`
* `docs/domain/core-entities.md`
* `docs/architecture/architecture-overview.md`
* `docs/architecture/decisions/001-python-runtime-packaging-and-development-tooling.md`
* `docs/architecture/decisions/002-execution-model-and-application-boundaries.md`

External technical references:

* [RFC 8785: JSON Canonicalization Scheme](https://www.rfc-editor.org/info/rfc8785/)
* [RFC 8785 verified errata](https://www.rfc-editor.org/errata/rfc8785)
* [RFC 9110: HTTP Semantics](https://www.rfc-editor.org/info/rfc9110/)
* [RFC 6585: Additional HTTP Status Codes](https://www.rfc-editor.org/rfc/rfc6585.html)

Supersedes: none

Superseded by: none
