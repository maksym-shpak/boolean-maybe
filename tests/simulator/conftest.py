"""Shared fixtures for real loopback HTTP tests against the simulator server."""

from __future__ import annotations

import http.client
import json
import threading
from collections.abc import Iterator

import pytest

from boolean_maybe.simulator.logs import OperationalLogger
from boolean_maybe.simulator.scenario import (
    EMPTY_PLAN,
    ScenarioPlan,
    parse_scenario_plan,
)
from boolean_maybe.simulator.server import BoundedThreadingHTTPServer, create_server
from boolean_maybe.simulator.service import SimulatorService
from boolean_maybe.simulator.waiting import Waiter


class RunningSimulator:
    def __init__(
        self,
        server: BoundedThreadingHTTPServer,
        thread: threading.Thread,
        log_stream: "FakeStream",
        host: str = "127.0.0.1",
    ) -> None:
        self.server = server
        self.thread = thread
        self.log_stream = log_stream
        self.host = host

    @property
    def port(self) -> int:
        return self.server.server_address[1]

    def request(
        self,
        method: str,
        path: str,
        *,
        body: bytes | None = None,
        headers: dict[str, str] | None = None,
    ) -> tuple[int, dict[str, object] | None, http.client.HTTPMessage]:
        conn = http.client.HTTPConnection(self.host, self.port, timeout=5)
        try:
            conn.request(method, path, body=body, headers=headers or {})
            response = conn.getresponse()
            raw = response.read()
            parsed: dict[str, object] | None = json.loads(raw) if raw else None
            return response.status, parsed, response.headers
        finally:
            conn.close()

    def raw_request(self, raw_bytes: bytes, *, read_timeout: float = 5.0) -> bytes:
        """Send raw bytes over a fresh socket and return whatever comes back."""

        import socket

        sock = socket.create_connection((self.host, self.port), timeout=read_timeout)
        try:
            sock.sendall(raw_bytes)
            sock.settimeout(read_timeout)
            chunks = []
            try:
                while True:
                    chunk = sock.recv(4096)
                    if not chunk:
                        break
                    chunks.append(chunk)
            except OSError:
                pass
            return b"".join(chunks)
        finally:
            sock.close()


class FakeStream:
    def __init__(self) -> None:
        self.lines: list[str] = []
        self._lock = threading.Lock()

    def write(self, text: str) -> None:
        with self._lock:
            self.lines.append(text)

    def flush(self) -> None:
        return None

    def events(self) -> list[dict[str, object]]:
        with self._lock:
            joined = "".join(self.lines)
        return [json.loads(line) for line in joined.splitlines() if line]


def _start_simulator(
    plan: ScenarioPlan,
    *,
    waiter: Waiter | None = None,
    max_workers: int = 32,
    host: str = "127.0.0.1",
) -> RunningSimulator:
    from boolean_maybe.simulator.waiting import RealWaiter

    shutdown_event = threading.Event()
    service = SimulatorService(plan, waiter or RealWaiter(), shutdown_event)
    log_stream = FakeStream()
    logger = OperationalLogger(stream=log_stream)  # type: ignore[arg-type]
    server = create_server(
        host, 0, service, logger, shutdown_event, max_workers=max_workers
    )
    thread = threading.Thread(
        target=server.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True
    )
    thread.start()
    return RunningSimulator(server, thread, log_stream, host=host)


@pytest.fixture
def simulator() -> Iterator[RunningSimulator]:
    running = _start_simulator(EMPTY_PLAN)
    try:
        yield running
    finally:
        running.server.shutdown_and_close(timeout=2.0)
        running.thread.join(timeout=3.0)


@pytest.fixture
def make_simulator() -> Iterator[object]:
    created: list[RunningSimulator] = []

    def factory(
        plan_document: dict[str, object] | None = None,
        *,
        waiter: Waiter | None = None,
        max_workers: int = 32,
        host: str = "127.0.0.1",
    ) -> RunningSimulator:
        plan = (
            EMPTY_PLAN
            if plan_document is None
            else parse_scenario_plan(json.dumps(plan_document).encode())
        )
        running = _start_simulator(
            plan, waiter=waiter, max_workers=max_workers, host=host
        )
        created.append(running)
        return running

    yield factory

    for running in created:
        running.server.shutdown_and_close(timeout=2.0)
        running.thread.join(timeout=3.0)
