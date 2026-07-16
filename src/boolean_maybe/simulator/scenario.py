"""Scenario-plan schema validation and deterministic per-ordinal action selection.

The plan is immutable local startup configuration: version-1 JSON with
`rules` selecting a named preset by exact idempotency key or the `*`
wildcard, per operation. An exact-key rule takes precedence over a wildcard
rule for the same operation; when neither matches, normal state-dependent
behavior applies and is not configurable.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any

from . import strict_json
from .idempotency import is_accepted_key

SUBMISSION = "submission"
RECONCILIATION = "reconciliation"
WILDCARD_KEY = "*"

MAX_PLAN_BYTES = 1024 * 1024
MAX_RULES = 1000

SCENARIO_OPERATIONS: dict[str, frozenset[str]] = {
    "success": frozenset({SUBMISSION, RECONCILIATION}),
    "500_then_success": frozenset({SUBMISSION}),
    "429_then_success": frozenset({SUBMISSION}),
    "connect_timeout": frozenset({SUBMISSION}),
    "processed_then_disconnect": frozenset({SUBMISSION}),
    "processed_without_response": frozenset({SUBMISSION}),
    "processed_then_500": frozenset({SUBMISSION}),
    "duplicate_remote_request_id": frozenset({SUBMISSION}),
    "reconciliation_timeout": frozenset({RECONCILIATION}),
    "always_500": frozenset({SUBMISSION, RECONCILIATION}),
}


class ScenarioPlanError(ValueError):
    """The scenario-plan file or document is invalid configuration."""


class Action(Enum):
    NORMAL = "normal"
    RETURN_500 = "return_500"
    RETURN_429 = "return_429"
    TIMEOUT_NO_PROCESS = "timeout_no_process"
    PROCESS_THEN_DISCONNECT = "process_then_disconnect"
    PROCESS_THEN_TIMEOUT = "process_then_timeout"
    PROCESS_THEN_500 = "process_then_500"


@dataclass(frozen=True)
class ScenarioPlan:
    exact_rules: dict[tuple[str, str], str]
    wildcard_rules: dict[str, str]

    def resolve(self, operation: str, idempotency_key: str) -> str | None:
        scenario = self.exact_rules.get((operation, idempotency_key))
        if scenario is not None:
            return scenario
        return self.wildcard_rules.get(operation)


EMPTY_PLAN = ScenarioPlan(exact_rules={}, wildcard_rules={})


def load_scenario_plan(path: Path) -> ScenarioPlan:
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise ScenarioPlanError(f"cannot read scenario-plan file: {exc}") from exc
    return parse_scenario_plan(raw)


def parse_scenario_plan(raw: bytes) -> ScenarioPlan:
    if len(raw) > MAX_PLAN_BYTES:
        raise ScenarioPlanError("scenario plan exceeds the 1 MiB size limit")

    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ScenarioPlanError("scenario plan is not valid UTF-8") from exc

    try:
        document = strict_json.loads_strict(text)
    except ValueError as exc:
        # Never interpolate the underlying exception: a duplicate-member
        # error embeds the raw (possibly sensitive) member name, and
        # scenario-plan content must never appear in diagnostics or logs.
        raise ScenarioPlanError("scenario plan is not valid JSON") from exc

    return _validate_document(document)


def _validate_document(document: Any) -> ScenarioPlan:
    if not isinstance(document, dict):
        raise ScenarioPlanError("scenario plan root must be a JSON object")
    if set(document.keys()) != {"version", "rules"}:
        raise ScenarioPlanError(
            "scenario plan must contain exactly 'version' and 'rules'"
        )

    version = document["version"]
    if isinstance(version, bool) or not isinstance(version, int) or version != 1:
        raise ScenarioPlanError("scenario plan 'version' must equal integer 1")

    rules_raw = document["rules"]
    if not isinstance(rules_raw, list):
        raise ScenarioPlanError("scenario plan 'rules' must be an array")
    if len(rules_raw) > MAX_RULES:
        raise ScenarioPlanError("scenario plan exceeds the 1000 rule limit")

    exact_rules: dict[tuple[str, str], str] = {}
    wildcard_rules: dict[str, str] = {}
    seen_selectors: set[tuple[str, str]] = set()

    for entry in rules_raw:
        if not isinstance(entry, dict):
            raise ScenarioPlanError("each scenario rule must be a JSON object")
        if set(entry.keys()) != {"operation", "idempotency_key", "scenario"}:
            raise ScenarioPlanError(
                "each scenario rule must contain exactly 'operation', 'idempotency_key', and 'scenario'"
            )

        operation = entry["operation"]
        key = entry["idempotency_key"]
        scenario = entry["scenario"]

        if operation not in (SUBMISSION, RECONCILIATION):
            raise ScenarioPlanError(
                "each rule 'operation' must be submission or reconciliation"
            )
        if not isinstance(key, str) or not (
            key == WILDCARD_KEY or is_accepted_key(key)
        ):
            # Never echo the submitted value: it may be a real idempotency
            # key, and scenario-plan content must not appear in diagnostics.
            raise ScenarioPlanError(
                "each rule 'idempotency_key' must be an accepted key or the '*' wildcard"
            )
        if not isinstance(scenario, str) or scenario not in SCENARIO_OPERATIONS:
            raise ScenarioPlanError("each rule 'scenario' must be a known preset name")
        if operation not in SCENARIO_OPERATIONS[scenario]:
            raise ScenarioPlanError("a rule's 'scenario' must support its 'operation'")

        selector = (operation, key)
        if selector in seen_selectors:
            raise ScenarioPlanError(
                "duplicate rule for the same operation and idempotency-key selector"
            )
        seen_selectors.add(selector)

        if key == WILDCARD_KEY:
            wildcard_rules[operation] = scenario
        else:
            exact_rules[(operation, key)] = scenario

    return ScenarioPlan(exact_rules=exact_rules, wildcard_rules=wildcard_rules)


def resolve_action(scenario: str | None, operation: str, ordinal: int) -> Action:
    if scenario is None or scenario == "success":
        return Action.NORMAL
    if scenario == "always_500":
        return Action.RETURN_500

    if operation == SUBMISSION:
        if scenario == "500_then_success":
            return Action.RETURN_500 if ordinal == 1 else Action.NORMAL
        if scenario == "429_then_success":
            return Action.RETURN_429 if ordinal == 1 else Action.NORMAL
        if scenario == "connect_timeout":
            return Action.TIMEOUT_NO_PROCESS if ordinal == 1 else Action.NORMAL
        if scenario == "processed_then_disconnect":
            return Action.PROCESS_THEN_DISCONNECT if ordinal == 1 else Action.NORMAL
        if scenario == "processed_without_response":
            return Action.PROCESS_THEN_TIMEOUT if ordinal == 1 else Action.NORMAL
        if scenario == "processed_then_500":
            return Action.PROCESS_THEN_500 if ordinal == 1 else Action.NORMAL
        if scenario == "duplicate_remote_request_id":
            return Action.NORMAL
    elif operation == RECONCILIATION:
        if scenario == "reconciliation_timeout":
            return Action.TIMEOUT_NO_PROCESS if ordinal == 1 else Action.NORMAL

    raise ScenarioPlanError(  # pragma: no cover - excluded by prior plan validation
        "scenario is not valid for this operation"
    )
