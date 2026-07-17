# boolean-maybe

A resilient command-line application for submitting and managing jobs through a deliberately unreliable external API.

This repository currently provides:

* `boolean-maybe submit`, the first end-to-end single-Job submission command, described below;
* `boolean-maybe-simulator`, a small local HTTP simulator of that unreliable external service, described below.

See `docs/product/product-brief.md`, `docs/product/glossary.md`, `docs/domain/core-entities.md`, and `docs/architecture/` for product, domain, and architecture context.

## Prerequisites

* Python 3.12
* [`uv`](https://docs.astral.sh/uv/)

## Setup

```sh
uv sync --locked
```

This installs both console entry points, `boolean-maybe` and `boolean-maybe-simulator`, into the project's managed virtual environment using the committed lockfile.

## Submitting a Job (`boolean-maybe submit`)

`submit` proves the product's foundational path for one Job Entry: validate, durably record the Job and a `STARTED` attempt *before* any HTTP request, submit once to the Simulated External Service, atomically record success, and print a stable JSON result. A later invocation with the same key and an equivalent Job Entry replays the persisted successful result without another attempt or HTTP request. This first feature intentionally does not implement retry, reconciliation, or recovery of an interrupted submission; see `docs/specs/features/submit-single-job.md` for the complete contract.

Command syntax:

```text
boolean-maybe submit --job-entry JSON [--idempotency-key KEY] [--database PATH] [--service-url URL]
```

* `--job-entry` is one inline UTF-8 JSON object (any canonicalizable object, including `{}`); it is rejected before any database is opened or HTTP request is sent if it is not valid, canonicalizable, RFC 8785/I-JSON-compliant input up to 1 MiB.
* `--idempotency-key` is optional: 1-128 characters from `A-Z a-z 0-9 . _ ~ -`. When omitted, a `job_<32 lowercase hex>` key is generated (never derived from the Job Entry).
* `--database` defaults to `.boolean-maybe/boolean-maybe.sqlite3` relative to the current directory. A missing parent directory is created; an existing directory or file is never replaced or deleted.
* `--service-url` defaults to `http://127.0.0.1:8080` and accepts only an unauthenticated loopback `http` origin (no hostnames, credentials, query, fragment, or non-root path).

Example, against a simulator started with `uv run --locked boolean-maybe-simulator`:

```sh
uv run --locked boolean-maybe submit --job-entry '{"work":"example"}'
```

```json
{"outcome":"succeeded","submitted":true,"job_id":"...","idempotency_key":"job_...","state":"SUCCEEDED","attempt":{"attempt_id":"...","attempt_number":1,"http_status":201,"remote_request_id":"remote-..."},"result":{"status":"processed","payload_digest":"sha256:...","remote_request_id":"remote-..."}}
```

Repeating the same command with `--idempotency-key job-a` twice returns `outcome: "already_completed"` and `submitted: false` on the second call, without contacting the simulator again.

Exit codes: `0` for `succeeded`, `already_completed`, or help; `1` when a valid command could not complete successfully (`idempotency_conflict`, `job_not_eligible`, `submission_incomplete`, or another local/operational failure); `2` when command syntax or Job Entry/Idempotency Key/database path/service URL validation fails before any product work begins.

**This feature provides no application-level encryption or permission hardening.** The SQLite database may contain sensitive Job Entry payloads; protect the database file with local filesystem permissions.

## Simulated External Service

`boolean-maybe-simulator` is a separate HTTP process, built on the Python standard library's `http.server` rather than a web framework, that reproduces the deterministic failure scenarios described by `docs/architecture/decisions/003-simulated-external-service-contract.md` and `docs/specs/features/simulated-external-service.md`. It exists only to exercise the CLI's reliability behavior in later features; it is not a mock of the CLI, not a production API, and not an authority for local Job state.

**The simulator is loopback-only, unauthenticated, intentionally unreliable, and must never be exposed beyond local development or evaluation use.** All processed-record state and per-key request ordinals are held in memory only and are discarded whenever the process restarts; a fresh process has no evidence of anything a previous process handled.

### Run with default (`success`) behavior

```sh
uv run --locked boolean-maybe-simulator
```

By default this binds to `http://127.0.0.1:8080` and applies normal state-dependent behavior to every request (equivalent to the `success` preset): a new idempotency key is processed and a repeated equivalent submission is replayed.

Command syntax:

```text
boolean-maybe-simulator [--host 127.0.0.1] [--port 8080] [--scenario-plan PATH]
```

* `--host` must be a loopback IP literal (`ipaddress.ip_address(value).is_loopback`), e.g. `127.0.0.1` or `::1`. Hostnames such as `localhost` are rejected.
* `--port` is an integer from 1 to 65535 (default `8080`).
* `--scenario-plan` is an optional path to a version-1 JSON scenario plan (below). The plan is fully validated before the socket binds; an invalid plan, host, or port exits with code `2` and starts no server.

### Endpoints

`POST /jobs` — submit a Job Entry:

```text
POST /jobs
Content-Type: application/json
Idempotency-Key: <1-128 chars from A-Z a-z 0-9 . _ ~ ->

{ "...": "any RFC 8785-canonicalizable JSON object" }
```

* A new key is processed once and returns `201` with `replayed: false`.
* An RFC 8785-equivalent resubmission of the same key returns `200` with the stored result and `replayed: true`, without processing again.
* The same key bound to a non-equivalent Job Entry returns `409` with both payload digests, and never mutates the existing binding.

`GET /jobs/by-idempotency-key/{idempotency_key}` — reconcile by the same key (percent-encoded per RFC 3986 if needed):

* Returns `200` with `status: "processed"` and the stored evidence if a record exists.
* Returns `404` with `status: "not_found"` if it does not. This is a point-in-time observation only, not proof that no earlier process ever handled the key.

Both endpoints may also return the documented validation errors (`400`, `404 route_not_found`, `405`, `413`, `415`) or one of the ten deterministic failure presets configured by a scenario plan; see the feature specification for the complete contract, error codes, and preset table.

### Scenario plans

Without `--scenario-plan`, every request uses normal state-dependent behavior. To exercise deterministic failures, supply a version-1 JSON plan:

```json
{
  "version": 1,
  "rules": [
    { "operation": "submission", "idempotency_key": "job-a", "scenario": "429_then_success" },
    { "operation": "reconciliation", "idempotency_key": "*", "scenario": "reconciliation_timeout" }
  ]
}
```

* `idempotency_key` is either one accepted exact key or the wildcard `*`; an exact match always takes precedence over a wildcard match for the same operation.
* Each `scenario` is one of: `success`, `500_then_success`, `429_then_success`, `connect_timeout`, `processed_then_disconnect`, `processed_without_response`, `processed_then_500`, `duplicate_remote_request_id`, `reconciliation_timeout`, `always_500`.
* The plan is immutable for the lifetime of the process; there is no runtime override.

Run with:

```sh
uv run --locked boolean-maybe-simulator --scenario-plan path/to/plan.json
```

### Example: one submission and reconciliation with `curl`

Bash / zsh:

```sh
curl -i -X POST http://127.0.0.1:8080/jobs \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: job-a' \
  -d '{"work":"example"}'

curl -i http://127.0.0.1:8080/jobs/by-idempotency-key/job-a
```

PowerShell quoting differs (use double quotes for the outer string and escape inner double quotes, or use `curl.exe` with single-quoted JSON as above if available):

```powershell
curl.exe -i -X POST http://127.0.0.1:8080/jobs `
  -H "Content-Type: application/json" `
  -H "Idempotency-Key: job-a" `
  -d '{\"work\":\"example\"}'

curl.exe -i http://127.0.0.1:8080/jobs/by-idempotency-key/job-a
```

### Stopping the simulator

`Ctrl+C` performs a clean shutdown: the server stops accepting connections, interrupts any in-progress timeout preset, closes active connections, waits at most two seconds for in-flight requests, and exits with code `0`. All in-memory state is discarded; the next run starts empty with ordinals reset to zero.

### Operational logs

The simulator writes one JSON object per line to stderr for `simulator_started`, `request_completed`, `request_aborted`, `configuration_rejected`, and `simulator_stopped`. Logs never contain raw idempotency keys, Job Entry payloads, canonical payload bytes, full payload digests, response bodies, or scenario-plan content; an accepted key is identified only by a 12-character `key_fingerprint`.
