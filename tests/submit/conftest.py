"""Shared fixtures for the `submit` feature: a real in-process simulator and
a temporary SQLite database path.
"""

from __future__ import annotations

import socket
import threading
from collections.abc import Iterator
from pathlib import Path

import pytest

from boolean_maybe.simulator.logs import OperationalLogger
from boolean_maybe.simulator.scenario import (
    EMPTY_PLAN,
    ScenarioPlan,
    parse_scenario_plan,
)
from boolean_maybe.simulator.server import BoundedThreadingHTTPServer, create_server
from boolean_maybe.simulator.service import SimulatorService
from boolean_maybe.simulator.waiting import RealWaiter


class RunningSimulator:
    def __init__(
        self, server: BoundedThreadingHTTPServer, thread: threading.Thread
    ) -> None:
        self.server = server
        self.thread = thread

    @property
    def host(self) -> str:
        return "127.0.0.1"

    @property
    def port(self) -> int:
        return self.server.server_address[1]


def _start_simulator(plan: ScenarioPlan) -> RunningSimulator:
    shutdown_event = threading.Event()
    service = SimulatorService(plan, RealWaiter(), shutdown_event)
    logger = OperationalLogger(stream=_NullStream())  # type: ignore[arg-type]
    server = create_server("127.0.0.1", 0, service, logger, shutdown_event)
    thread = threading.Thread(
        target=server.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True
    )
    thread.start()
    return RunningSimulator(server, thread)


class _NullStream:
    def write(self, text: str) -> None:
        return None

    def flush(self) -> None:
        return None


@pytest.fixture
def live_simulator() -> Iterator[RunningSimulator]:
    running = _start_simulator(EMPTY_PLAN)
    try:
        yield running
    finally:
        running.server.shutdown_and_close(timeout=2.0)
        running.thread.join(timeout=3.0)


@pytest.fixture
def make_live_simulator() -> Iterator[object]:
    created: list[RunningSimulator] = []

    def factory(plan_document: dict[str, object] | None = None) -> RunningSimulator:
        import json

        plan = (
            EMPTY_PLAN
            if plan_document is None
            else parse_scenario_plan(json.dumps(plan_document).encode())
        )
        running = _start_simulator(plan)
        created.append(running)
        return running

    yield factory

    for running in created:
        running.server.shutdown_and_close(timeout=2.0)
        running.thread.join(timeout=3.0)


@pytest.fixture
def database_path(tmp_path: Path) -> Path:
    return tmp_path / "boolean-maybe.sqlite3"


@pytest.fixture
def unused_tcp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]
