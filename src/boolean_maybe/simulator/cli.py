"""Console entry point for `boolean-maybe-simulator`.

    boolean-maybe-simulator [--host 127.0.0.1] [--port 8080] [--scenario-plan PATH]

The complete scenario plan is read and validated before the socket binds.
Invalid arguments or configuration exit with code 2 without starting the
server; a bind or unexpected startup failure exits with code 1; a clean
SIGINT shutdown exits with code 0 and discards all in-memory state.
"""

from __future__ import annotations

import argparse
import ipaddress
import sys
import threading
from pathlib import Path

from . import scenario as scenario_mod
from .logs import OperationalLogger
from .server import create_server
from .service import SimulatorService
from .waiting import RealWaiter

PROG = "boolean-maybe-simulator"


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog=PROG)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", default=8080, type=int)
    parser.add_argument(
        "--scenario-plan", default=None, type=Path, dest="scenario_plan"
    )
    return parser.parse_args(argv)


def _fail(message: str, *, code: int) -> None:
    print(f"{PROG}: {message}", file=sys.stderr)
    sys.exit(code)


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)

    try:
        host_address = ipaddress.ip_address(args.host)
    except ValueError:
        _fail(f"--host must be a loopback IP literal, got {args.host!r}", code=2)
        return
    if not host_address.is_loopback:
        _fail(f"--host must be a loopback IP literal, got {args.host!r}", code=2)
        return

    if not (1 <= args.port <= 65535):
        _fail(f"--port must be between 1 and 65535, got {args.port}", code=2)
        return

    logger = OperationalLogger()

    plan = scenario_mod.EMPTY_PLAN
    if args.scenario_plan is not None:
        try:
            plan = scenario_mod.load_scenario_plan(args.scenario_plan)
        except scenario_mod.ScenarioPlanError as exc:
            logger.emit("configuration_rejected", level="error")
            _fail(f"invalid scenario plan: {exc}", code=2)
            return

    shutdown_event = threading.Event()
    service = SimulatorService(plan, RealWaiter(), shutdown_event)

    try:
        server = create_server(args.host, args.port, service, logger, shutdown_event)
    except OSError as exc:
        _fail(f"failed to bind {args.host}:{args.port}: {exc}", code=1)
        return

    logger.emit("simulator_started")

    try:
        server.serve_forever(poll_interval=0.1)
    except KeyboardInterrupt:
        pass
    finally:
        server.shutdown_and_close(timeout=2.0)
        logger.emit("simulator_stopped")

    sys.exit(0)
