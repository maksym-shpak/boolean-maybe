"""Core simulator service: ordinals, scenario selection, and state effects.

This module owns no HTTP or logging concerns. It is deliberately callable
directly (without a real socket) so preset logic can be tested with an
injected `Waiter` instead of sleeping for the real 11-second duration.

For one idempotency key, ordinal assignment, scenario selection, state
inspection, conflict decision, and scenario state effect all happen while
holding that key's lock, including any timeout wait. This keeps same-key
submission and reconciliation serialized while different keys proceed
concurrently.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import Literal

from . import scenario as scenario_mod
from .state import KeyCoordinator
from .waiting import Waiter

SUBMISSION = scenario_mod.SUBMISSION
RECONCILIATION = scenario_mod.RECONCILIATION

_RATE_LIMITED_BODY: dict[str, object] = {
    "error": {
        "code": "rate_limited",
        "message": "The simulated service is rate limited.",
    }
}
_SERVER_ERROR_BODY: dict[str, object] = {
    "error": {
        "code": "simulated_server_error",
        "message": "The simulated service failed.",
    }
}


@dataclass(frozen=True)
class Outcome:
    kind: Literal["response", "aborted"]
    status: int | None = None
    body: dict[str, object] | None = None
    headers: tuple[tuple[str, str], ...] = field(default_factory=tuple)
    processed: bool = False
    scenario: str = "success"
    ordinal: int = 0


class SimulatorService:
    def __init__(
        self,
        plan: scenario_mod.ScenarioPlan,
        waiter: Waiter,
        shutdown_event: threading.Event,
    ) -> None:
        self._plan = plan
        self._waiter = waiter
        self._shutdown_event = shutdown_event
        self._coordinator = KeyCoordinator()

    def handle_submission(
        self, key: str, canonical_bytes: bytes, digest: str
    ) -> Outcome:
        lock = self._coordinator.lock_for(key)
        with lock:
            ordinal = self._coordinator.next_ordinal(SUBMISSION, key)
            scenario_name = self._plan.resolve(SUBMISSION, key)
            log_scenario = scenario_name or "success"
            action = scenario_mod.resolve_action(scenario_name, SUBMISSION, ordinal)

            existing = self._coordinator.get_record(key)
            if existing is not None and existing.canonical_bytes != canonical_bytes:
                return Outcome(
                    kind="response",
                    status=409,
                    body={
                        "error": {
                            "code": "idempotency_conflict",
                            "message": "The idempotency key is already bound to a non-equivalent Job Entry.",
                            "stored_payload_digest": existing.payload_digest,
                            "submitted_payload_digest": digest,
                        }
                    },
                    processed=True,
                    scenario=log_scenario,
                    ordinal=ordinal,
                )

            if action is scenario_mod.Action.RETURN_500:
                return Outcome(
                    kind="response",
                    status=500,
                    body=_SERVER_ERROR_BODY,
                    processed=existing is not None,
                    scenario=log_scenario,
                    ordinal=ordinal,
                )

            if action is scenario_mod.Action.RETURN_429:
                return Outcome(
                    kind="response",
                    status=429,
                    body=_RATE_LIMITED_BODY,
                    headers=(("Retry-After", "1"),),
                    processed=existing is not None,
                    scenario=log_scenario,
                    ordinal=ordinal,
                )

            if action is scenario_mod.Action.TIMEOUT_NO_PROCESS:
                self._waiter.wait(self._shutdown_event)
                return Outcome(
                    kind="aborted",
                    processed=existing is not None,
                    scenario=log_scenario,
                    ordinal=ordinal,
                )

            if action in (
                scenario_mod.Action.PROCESS_THEN_DISCONNECT,
                scenario_mod.Action.PROCESS_THEN_TIMEOUT,
                scenario_mod.Action.PROCESS_THEN_500,
            ):
                if existing is None:
                    self._coordinator.create_record(
                        key, canonical_bytes, digest, duplicate_remote_id=False
                    )

                if action is scenario_mod.Action.PROCESS_THEN_DISCONNECT:
                    return Outcome(
                        kind="aborted",
                        processed=True,
                        scenario=log_scenario,
                        ordinal=ordinal,
                    )
                if action is scenario_mod.Action.PROCESS_THEN_TIMEOUT:
                    self._waiter.wait(self._shutdown_event)
                    return Outcome(
                        kind="aborted",
                        processed=True,
                        scenario=log_scenario,
                        ordinal=ordinal,
                    )
                return Outcome(
                    kind="response",
                    status=500,
                    body=_SERVER_ERROR_BODY,
                    processed=True,
                    scenario=log_scenario,
                    ordinal=ordinal,
                )

            # Action.NORMAL
            if existing is not None:
                return Outcome(
                    kind="response",
                    status=200,
                    body=_submission_body(existing, replayed=True),
                    processed=True,
                    scenario=log_scenario,
                    ordinal=ordinal,
                )
            record = self._coordinator.create_record(
                key,
                canonical_bytes,
                digest,
                duplicate_remote_id=(scenario_name == "duplicate_remote_request_id"),
            )
            return Outcome(
                kind="response",
                status=201,
                body=_submission_body(record, replayed=False),
                processed=True,
                scenario=log_scenario,
                ordinal=ordinal,
            )

    def handle_reconciliation(self, key: str) -> Outcome:
        lock = self._coordinator.lock_for(key)
        with lock:
            ordinal = self._coordinator.next_ordinal(RECONCILIATION, key)
            scenario_name = self._plan.resolve(RECONCILIATION, key)
            log_scenario = scenario_name or "success"
            action = scenario_mod.resolve_action(scenario_name, RECONCILIATION, ordinal)
            existing = self._coordinator.get_record(key)

            if action is scenario_mod.Action.RETURN_500:
                return Outcome(
                    kind="response",
                    status=500,
                    body=_SERVER_ERROR_BODY,
                    processed=existing is not None,
                    scenario=log_scenario,
                    ordinal=ordinal,
                )

            if action is scenario_mod.Action.TIMEOUT_NO_PROCESS:
                self._waiter.wait(self._shutdown_event)
                return Outcome(
                    kind="aborted",
                    processed=existing is not None,
                    scenario=log_scenario,
                    ordinal=ordinal,
                )

            if existing is not None:
                return Outcome(
                    kind="response",
                    status=200,
                    body={
                        "idempotency_key": key,
                        "status": "processed",
                        "payload_digest": existing.payload_digest,
                        "remote_request_id": existing.remote_request_id,
                    },
                    processed=True,
                    scenario=log_scenario,
                    ordinal=ordinal,
                )
            return Outcome(
                kind="response",
                status=404,
                body={"idempotency_key": key, "status": "not_found"},
                processed=False,
                scenario=log_scenario,
                ordinal=ordinal,
            )


def _submission_body(record, *, replayed: bool) -> dict[str, object]:
    return {
        "idempotency_key": record.idempotency_key,
        "status": "processed",
        "payload_digest": record.payload_digest,
        "remote_request_id": record.remote_request_id,
        "replayed": replayed,
    }
