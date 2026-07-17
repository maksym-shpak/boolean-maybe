"""Console entry point for the `boolean-maybe` CLI.

    boolean-maybe submit --job-entry JSON [--idempotency-key KEY]
                          [--database PATH] [--service-url URL]

The synchronous CLI adapter parses arguments, resolves `--database` and
`--service-url` defaults, and calls the asynchronous submission workflow
through `asyncio.run()` exactly once. It does not call persistence or HTTP
directly and owns no Job lifecycle, retry, or reconciliation decisions.

Exit codes: `0` for `succeeded`, `already_completed`, or help; `1` when a
valid command could not complete successfully; `2` when command syntax or
Job Entry/Idempotency Key/database path/service URL validation fails before
product work begins.
"""

from __future__ import annotations

import argparse
import asyncio
import ipaddress
import json
import sys
from pathlib import Path
from urllib.parse import urlsplit

from .application import submit as submit_workflow

PROG = "boolean-maybe"
DEFAULT_DATABASE_RELATIVE = Path(".boolean-maybe") / "boolean-maybe.sqlite3"
DEFAULT_SERVICE_URL = "http://127.0.0.1:8080"


class _CommandInputError(ValueError):
    """A command-level input (database path or service URL) is invalid."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog=PROG)
    subparsers = parser.add_subparsers(dest="command")

    submit_parser = subparsers.add_parser("submit", help="submit one inline Job Entry")
    submit_parser.add_argument("--job-entry", required=True, dest="job_entry")
    submit_parser.add_argument(
        "--idempotency-key", default=None, dest="idempotency_key"
    )
    submit_parser.add_argument("--database", default=None, dest="database")
    submit_parser.add_argument(
        "--service-url", default=DEFAULT_SERVICE_URL, dest="service_url"
    )

    return parser


def _resolve_database_path(raw: str | None) -> Path:
    try:
        if raw is None:
            return (Path.cwd() / DEFAULT_DATABASE_RELATIVE).resolve()
        candidate = Path(raw)
        base = candidate if candidate.is_absolute() else Path.cwd() / candidate
        return base.resolve()
    except (ValueError, OSError) as exc:
        raise _CommandInputError(
            "invalid_database_path", f"--database is not a usable path: {exc}"
        ) from exc


def _parse_service_url(raw: str) -> tuple[str, int]:
    parts = urlsplit(raw)

    if parts.scheme != "http":
        raise _CommandInputError(
            "invalid_service_url", "--service-url must use the http scheme"
        )
    if parts.username is not None or parts.password is not None:
        raise _CommandInputError(
            "invalid_service_url", "--service-url must not contain credentials"
        )
    if parts.query or parts.fragment:
        raise _CommandInputError(
            "invalid_service_url",
            "--service-url must not contain a query or fragment",
        )
    if parts.path not in ("", "/"):
        raise _CommandInputError(
            "invalid_service_url", "--service-url must not contain a non-root path"
        )

    try:
        hostname = parts.hostname
        port = parts.port
    except ValueError as exc:
        raise _CommandInputError(
            "invalid_service_url", "--service-url host or port is malformed"
        ) from exc

    if hostname is None:
        raise _CommandInputError(
            "invalid_service_url", "--service-url must include a host"
        )

    try:
        host_address = ipaddress.ip_address(hostname)
    except ValueError as exc:
        raise _CommandInputError(
            "invalid_service_url",
            "--service-url host must be a loopback IP literal, not a hostname",
        ) from exc
    if not host_address.is_loopback:
        raise _CommandInputError(
            "invalid_service_url", "--service-url host must be a loopback IP literal"
        )

    return hostname, (port if port is not None else 80)


def _print_json(body: dict[str, object]) -> None:
    print(json.dumps(body, separators=(",", ":")), file=sys.stdout)


def _emit_command_input_error(error: _CommandInputError) -> None:
    _print_json(
        {
            "outcome": error.code,
            "submitted": False,
            "error": {"code": error.code, "message": error.message},
        }
    )


def _emit_validation_error(exc: submit_workflow.ValidationError) -> None:
    _print_json(
        {
            "outcome": "invalid_input",
            "submitted": False,
            "error": {"code": "invalid_input", "message": str(exc)},
        }
    )


def _emit_unexpected_failure() -> None:
    _print_json(
        {
            "outcome": "submission_incomplete",
            "submitted": True,
            "idempotency_key": None,
            "state": None,
            "error": {
                "code": "submission_incomplete",
                "message": "an unexpected internal error occurred",
            },
        }
    )


def _emit_outcome(outcome: submit_workflow.SubmitOutcome) -> int:
    if isinstance(outcome, submit_workflow.SubmitSuccess):
        _print_json(
            {
                "outcome": outcome.outcome,
                "submitted": outcome.submitted,
                "job_id": outcome.job_id,
                "idempotency_key": outcome.idempotency_key,
                "state": outcome.state,
                "attempt": {
                    "attempt_id": outcome.attempt_id,
                    "attempt_number": outcome.attempt_number,
                    "http_status": outcome.http_status,
                    "remote_request_id": outcome.remote_request_id,
                },
                "result": {
                    "status": "processed",
                    "payload_digest": outcome.payload_digest,
                    "remote_request_id": outcome.remote_request_id,
                },
            }
        )
        return 0

    _print_json(
        {
            "outcome": outcome.outcome,
            "submitted": outcome.submitted,
            "idempotency_key": outcome.idempotency_key,
            "state": outcome.state,
            "error": {"code": outcome.outcome, "message": outcome.message},
        }
    )
    return 1


def main(argv: list[str] | None = None) -> None:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command is None:
        parser.print_help()
        sys.exit(0)
        return

    try:
        service_host, service_port = _parse_service_url(args.service_url)
        database_path = _resolve_database_path(args.database)
    except _CommandInputError as exc:
        _emit_command_input_error(exc)
        sys.exit(2)
        return

    request = submit_workflow.SubmitRequest(
        job_entry_raw=args.job_entry,
        idempotency_key=args.idempotency_key,
        database_path=database_path,
        service_host=service_host,
        service_port=service_port,
    )

    try:
        outcome = asyncio.run(submit_workflow.run_submit(request))
    except submit_workflow.ValidationError as exc:
        _emit_validation_error(exc)
        sys.exit(2)
        return
    except Exception:
        _emit_unexpected_failure()
        sys.exit(1)
        return

    sys.exit(_emit_outcome(outcome))
