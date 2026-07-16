"""Standard-library HTTP server wiring: routing, framing/validation, delivery.

No web framework is used. Concurrency is bounded above the standard
unbounded `socketserver.ThreadingMixIn` by a fixed, non-configurable
semaphore of at most 32 active request-handler threads. Clean shutdown
stops accepting connections, signals the shared shutdown event (which
interrupts any in-progress timeout waiter), force-closes active request
sockets, and waits at most two seconds for handler threads to finish.
"""

from __future__ import annotations

import http.server
import ipaddress
import json
import re
import socket
import socketserver
import threading
import time
from typing import cast
from urllib.parse import urlsplit

from . import canonicalize, idempotency
from .logs import OperationalLogger
from .service import Outcome, SimulatorService

DEFAULT_MAX_WORKERS = 32
MAX_BODY_BYTES = 1024 * 1024

JOBS_PATH = "/jobs"
RECONCILE_PREFIX = "/jobs/by-idempotency-key/"

_CONTENT_LENGTH_RE = re.compile(r"^[0-9]+$")
# Comfortably above any real body length (1 MiB is 7 digits) and safely
# below Python's int-string conversion digit limit (4300 by default); a
# value with more *significant* digits than this (i.e. after stripping
# leading zeros, which are still a valid decimal integer) can be
# classified without calling int() on an attacker-controlled, arbitrarily
# long digit string.
_MAX_CONTENT_LENGTH_DIGITS = 15
_CONTENT_TYPE_RE = re.compile(
    r"^application/json(?:\s*;\s*charset=utf-8)?$", re.IGNORECASE
)


class ValidationFailure(Exception):
    def __init__(
        self,
        status: int,
        code: str,
        message: str,
        headers: tuple[tuple[str, str], ...] = (),
    ) -> None:
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message
        self.headers = headers


class BoundedThreadingHTTPServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True
    # http.server.HTTPServer defaults this to True; on Windows, SO_REUSEADDR
    # can let a second bind silently "succeed" against a port another
    # process is actively listening on instead of raising OSError. Keep
    # bind failures loud and portable by disabling it.
    allow_reuse_address = False
    request_queue_size = 128

    def __init__(
        self,
        server_address: tuple[str, int],
        handler_class: type[http.server.BaseHTTPRequestHandler],
        *,
        max_workers: int = DEFAULT_MAX_WORKERS,
    ) -> None:
        # `--host` accepts loopback IPv6 literals (e.g. `::1`), but
        # HTTPServer.address_family defaults to AF_INET. Select the family
        # matching the requested host before the base class creates and
        # binds the socket.
        if ipaddress.ip_address(server_address[0]).version == 6:
            self.address_family = socket.AF_INET6

        self._request_semaphore = threading.BoundedSemaphore(max_workers)
        self._active_lock = threading.Lock()
        self._active_threads: set[threading.Thread] = set()
        self._active_sockets: set[socket.socket] = set()
        super().__init__(server_address, handler_class)

    # Attributes assigned by create_server(); declared here for clarity.
    service: SimulatorService
    logger: OperationalLogger
    shutdown_event: threading.Event

    def process_request(self, request, client_address) -> None:
        # Poll with a bounded timeout instead of blocking indefinitely: if
        # all `max_workers` slots are busy (e.g. every active handler is in
        # an 11-second timeout wait), this keeps the accept loop able to
        # notice `shutdown_event` promptly instead of deadlocking clean
        # shutdown behind a semaphore that only frees once those handlers
        # are interrupted.
        while not self._request_semaphore.acquire(timeout=0.1):
            if self.shutdown_event.is_set():
                if isinstance(request, socket.socket):  # TCP server: always true
                    try:
                        request.close()
                    except OSError:
                        pass
                return
        super().process_request(request, client_address)

    def process_request_thread(self, request, client_address) -> None:
        assert isinstance(
            request, socket.socket
        )  # TCP server: never the UDP (bytes, socket) form
        thread = threading.current_thread()
        with self._active_lock:
            self._active_threads.add(thread)
            self._active_sockets.add(request)
        try:
            super().process_request_thread(request, client_address)
        finally:
            with self._active_lock:
                self._active_threads.discard(thread)
                self._active_sockets.discard(request)
            self._request_semaphore.release()

    def handle_error(self, request, client_address) -> None:
        # An unhandled exception mid-request means no complete HTTP response
        # was delivered; log it as an abort, never a fabricated "completed"
        # status. Never print a raw traceback: it may contain request data.
        self.logger.emit("request_aborted", level="error")

    def shutdown_and_close(self, timeout: float = 2.0) -> None:
        deadline = time.monotonic() + timeout

        # Signal first: a handler blocked in an in-progress timeout wait
        # must be released (and its semaphore slot freed) before the accept
        # loop's own bounded semaphore poll in `process_request` can notice
        # `shutdown_event` and stop, and before `self.shutdown()` below can
        # return promptly.
        self.shutdown_event.set()

        # Stop the serve_forever loop cleanly. This is a no-op if
        # serve_forever already returned (e.g. via KeyboardInterrupt in the
        # same thread), and blocks until it exits if still running in
        # another thread (e.g. under test).
        self.shutdown()

        try:
            self.server_close()
        except OSError:
            pass

        with self._active_lock:
            sockets = list(self._active_sockets)
            threads = list(self._active_threads)

        for sock in sockets:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                sock.close()
            except OSError:
                pass

        for thread in threads:
            remaining = max(0.0, deadline - time.monotonic())
            thread.join(timeout=remaining)


class SimulatorRequestHandler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "boolean-maybe-simulator/1"

    @property
    def _server(self) -> BoundedThreadingHTTPServer:
        # BaseRequestHandler.server is declared as the generic BaseServer;
        # this handler is only ever constructed by BoundedThreadingHTTPServer.
        return cast(BoundedThreadingHTTPServer, self.server)

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002 - stdlib signature
        return None

    def do_GET(self) -> None:
        self._route()

    def do_POST(self) -> None:
        self._route()

    def do_PUT(self) -> None:
        self._route()

    def do_DELETE(self) -> None:
        self._route()

    def do_PATCH(self) -> None:
        self._route()

    def do_HEAD(self) -> None:
        self._route()

    def do_OPTIONS(self) -> None:
        self._route()

    def _route(self) -> None:
        self.close_connection = True
        path = urlsplit(self.path).path
        method = self.command

        if path == JOBS_PATH:
            if method != "POST":
                self._send_method_not_allowed(("POST",), operation="submission")
                return
            self._handle_submission()
            return

        if (
            path.startswith(RECONCILE_PREFIX)
            and "/" not in path[len(RECONCILE_PREFIX) :]
        ):
            if method != "GET":
                self._send_method_not_allowed(("GET",), operation="reconciliation")
                return
            self._handle_reconciliation(path[len(RECONCILE_PREFIX) :])
            return

        self._send_route_not_found()

    def _handle_submission(self) -> None:
        idem_key: str | None = None
        try:
            self._validate_no_transfer_encoding()
            content_length = self._parse_content_length(
                require_present=True, max_value=MAX_BODY_BYTES
            )
            self._validate_json_content_type()
            idem_key = self._validate_submission_idempotency_key()
            raw_body = self.rfile.read(content_length) if content_length else b""
            job_entry = canonicalize.parse_job_entry(raw_body)
            canonical_bytes = canonicalize.canonicalize(job_entry)
        except ValidationFailure as failure:
            if failure.status == 413:
                # The client may still be writing an oversized body. On
                # some platforms (observed on Windows), closing a socket
                # that still has unread incoming data causes a connection
                # reset instead of a clean response delivery. Draining a
                # bounded amount first lets the client finish its write so
                # the 413 response arrives intact.
                self._drain_body_best_effort()
            self._send_validation_failure(failure, operation="submission")
            return
        except canonicalize.JobEntryValidationError:
            self._send_validation_failure(
                ValidationFailure(
                    400,
                    "invalid_request",
                    "request body is not a valid canonicalizable Job Entry",
                ),
                operation="submission",
                idempotency_key=idem_key,
            )
            return

        digest = canonicalize.payload_digest(canonical_bytes)
        outcome = self._server.service.handle_submission(
            idem_key, canonical_bytes, digest
        )
        self._deliver(outcome, operation="submission", idempotency_key=idem_key)

    def _handle_reconciliation(self, encoded_key: str) -> None:
        try:
            self._validate_no_transfer_encoding()
            self._validate_get_content_length()
            idem_key = idempotency.decode_reconciliation_key(encoded_key)
        except ValidationFailure as failure:
            self._send_validation_failure(failure, operation="reconciliation")
            return
        except ValueError:
            self._send_validation_failure(
                ValidationFailure(
                    400,
                    "invalid_idempotency_key",
                    "path key is not an accepted idempotency key",
                ),
                operation="reconciliation",
            )
            return

        outcome = self._server.service.handle_reconciliation(idem_key)
        self._deliver(outcome, operation="reconciliation", idempotency_key=idem_key)

    # -- Validation helpers -------------------------------------------------

    def _validate_no_transfer_encoding(self) -> None:
        if self.headers.get_all("Transfer-Encoding"):
            raise ValidationFailure(
                400, "invalid_request", "Transfer-Encoding is not accepted"
            )

    def _parse_content_length(
        self, *, require_present: bool, max_value: int | None
    ) -> int:
        values = self.headers.get_all("Content-Length")
        if not values:
            if require_present:
                raise ValidationFailure(
                    400, "invalid_request", "Content-Length header is required"
                )
            return 0
        if len(values) > 1:
            raise ValidationFailure(
                400, "invalid_request", "Content-Length header must not repeat"
            )
        value = values[0]
        if not _CONTENT_LENGTH_RE.fullmatch(value):
            raise ValidationFailure(
                400, "invalid_request", "Content-Length must be a valid decimal integer"
            )
        # Leading zeros are still a valid decimal integer (e.g. "0002" is 2),
        # so the digit count that matters for the safe-conversion bound is
        # the count after stripping them, not the raw header length.
        significant_digits = value.lstrip("0") or "0"
        if len(significant_digits) > _MAX_CONTENT_LENGTH_DIGITS:
            # Too many significant digits to safely convert; this is
            # certainly larger than max_value without parsing it as an int.
            if max_value is not None:
                raise ValidationFailure(
                    413, "payload_too_large", "request body exceeds the 1 MiB limit"
                )
            raise ValidationFailure(
                400, "invalid_request", "Content-Length is not a supported value"
            )
        length = int(significant_digits)
        if max_value is not None and length > max_value:
            raise ValidationFailure(
                413, "payload_too_large", "request body exceeds the 1 MiB limit"
            )
        return length

    def _validate_get_content_length(self) -> None:
        values = self.headers.get_all("Content-Length")
        if not values:
            return
        if len(values) > 1:
            raise ValidationFailure(
                400, "invalid_request", "Content-Length header must not repeat"
            )
        value = values[0]
        if not _CONTENT_LENGTH_RE.fullmatch(value):
            raise ValidationFailure(
                400, "invalid_request", "Content-Length must be a valid decimal integer"
            )
        significant_digits = value.lstrip("0") or "0"
        if len(significant_digits) > _MAX_CONTENT_LENGTH_DIGITS:
            raise ValidationFailure(
                400, "invalid_request", "GET requests must not declare a non-zero body"
            )
        if int(significant_digits) != 0:
            raise ValidationFailure(
                400, "invalid_request", "GET requests must not declare a non-zero body"
            )

    def _validate_json_content_type(self) -> None:
        values = self.headers.get_all("Content-Type")
        if (
            not values
            or len(values) > 1
            or not _CONTENT_TYPE_RE.match(values[0].strip())
        ):
            raise ValidationFailure(
                415, "unsupported_media_type", "Content-Type must be application/json"
            )

    def _validate_submission_idempotency_key(self) -> str:
        values = self.headers.get_all("Idempotency-Key")
        if not values or len(values) > 1:
            raise ValidationFailure(
                400,
                "invalid_idempotency_key",
                "exactly one Idempotency-Key header is required",
            )
        key = values[0]
        if not idempotency.is_accepted_key(key):
            raise ValidationFailure(
                400,
                "invalid_idempotency_key",
                "Idempotency-Key does not match the accepted grammar",
            )
        return key

    # -- Response delivery ----------------------------------------------

    def _send_route_not_found(self) -> None:
        self._send_validation_failure(
            ValidationFailure(404, "route_not_found", "no such route"), operation=None
        )

    def _send_method_not_allowed(
        self, allowed: tuple[str, ...], *, operation: str
    ) -> None:
        self._send_validation_failure(
            ValidationFailure(
                405,
                "method_not_allowed",
                "method not allowed for this route",
                headers=(("Allow", ", ".join(allowed)),),
            ),
            operation=operation,
        )

    def _send_validation_failure(
        self,
        failure: ValidationFailure,
        *,
        operation: str | None,
        idempotency_key: str | None = None,
    ) -> None:
        body: dict[str, object] = {
            "error": {"code": failure.code, "message": failure.message}
        }
        self._write_json_response(failure.status, body, failure.headers)
        self._server.logger.emit(
            "request_completed",
            level="error",
            operation=operation,
            idempotency_key=idempotency_key,
            status=failure.status,
        )

    def _deliver(
        self, outcome: Outcome, *, operation: str, idempotency_key: str
    ) -> None:
        if outcome.kind == "response":
            assert outcome.status is not None
            assert outcome.body is not None
            self._write_json_response(outcome.status, outcome.body, outcome.headers)
            level = "info" if outcome.status < 400 else "error"
            self._server.logger.emit(
                "request_completed",
                level=level,
                operation=operation,
                idempotency_key=idempotency_key,
                scenario=outcome.scenario,
                ordinal=outcome.ordinal,
                status=outcome.status,
                processed=outcome.processed,
            )
        else:
            self._server.logger.emit(
                "request_aborted",
                level="error",
                operation=operation,
                idempotency_key=idempotency_key,
                scenario=outcome.scenario,
                ordinal=outcome.ordinal,
                processed=outcome.processed,
            )

    def _drain_body_best_effort(self, limit: int = MAX_BODY_BYTES) -> None:
        # Bound the read by both byte count and a short socket timeout: the
        # client may have declared more than it ever actually sends, and
        # without a timeout a client that stops short would hang this read
        # forever waiting for bytes that never arrive.
        try:
            self.connection.settimeout(0.5)
        except OSError:
            return
        try:
            remaining = limit
            while remaining > 0:
                chunk = self.rfile.read(min(remaining, 65536))
                if not chunk:
                    break
                remaining -= len(chunk)
        except OSError:
            pass

    def _write_json_response(
        self, status: int, body: dict[str, object], headers: tuple[tuple[str, str], ...]
    ) -> None:
        body_bytes = json.dumps(body).encode("utf-8")
        self.close_connection = True
        try:
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body_bytes)))
            self.send_header("Connection", "close")
            for name, value in headers:
                self.send_header(name, value)
            self.end_headers()
            self.wfile.write(body_bytes)
        except OSError:
            # Broken pipe / connection reset while writing; any state
            # mutation already happened and must remain committed.
            pass


def create_server(
    host: str,
    port: int,
    service: SimulatorService,
    logger: OperationalLogger,
    shutdown_event: threading.Event,
    *,
    max_workers: int = DEFAULT_MAX_WORKERS,
) -> BoundedThreadingHTTPServer:
    server = BoundedThreadingHTTPServer(
        (host, port), SimulatorRequestHandler, max_workers=max_workers
    )
    server.service = service
    server.logger = logger
    server.shutdown_event = shutdown_event
    return server
