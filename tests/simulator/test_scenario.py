"""Scenario-plan schema validation, precedence, and action resolution tests."""

from __future__ import annotations

import json

import pytest

from boolean_maybe.simulator import scenario as scenario_mod


def _plan(document: dict[str, object]) -> scenario_mod.ScenarioPlan:
    return scenario_mod.parse_scenario_plan(json.dumps(document).encode("utf-8"))


VALID_DOCUMENT = {
    "version": 1,
    "rules": [
        {
            "operation": "submission",
            "idempotency_key": "job-a",
            "scenario": "429_then_success",
        },
        {
            "operation": "reconciliation",
            "idempotency_key": "job-a",
            "scenario": "reconciliation_timeout",
        },
    ],
}


def test_valid_plan_parses() -> None:
    plan = _plan(VALID_DOCUMENT)
    assert plan.resolve("submission", "job-a") == "429_then_success"
    assert plan.resolve("reconciliation", "job-a") == "reconciliation_timeout"


def test_exact_rule_takes_precedence_over_wildcard() -> None:
    plan = _plan(
        {
            "version": 1,
            "rules": [
                {
                    "operation": "submission",
                    "idempotency_key": "*",
                    "scenario": "always_500",
                },
                {
                    "operation": "submission",
                    "idempotency_key": "job-a",
                    "scenario": "success",
                },
            ],
        }
    )
    assert plan.resolve("submission", "job-a") == "success"
    assert plan.resolve("submission", "job-b") == "always_500"


def test_no_matching_rule_resolves_to_none() -> None:
    plan = scenario_mod.EMPTY_PLAN
    assert plan.resolve("submission", "job-a") is None
    assert plan.resolve("reconciliation", "job-a") is None


@pytest.mark.parametrize(
    "document",
    [
        {"version": 1},  # missing 'rules'
        {"version": 1, "rules": [], "extra": True},  # unknown top-level field
        {"version": 2, "rules": []},  # unsupported version
        {"version": 1.0, "rules": []},  # version not an integer
        {"version": True, "rules": []},  # bool is not an accepted integer
        {"version": 1, "rules": {}},  # rules not an array
        "not-an-object",
        [1, 2, 3],
    ],
)
def test_malformed_document_shape_is_rejected(document: object) -> None:
    with pytest.raises(scenario_mod.ScenarioPlanError):
        scenario_mod._validate_document(document)


def test_unknown_field_in_rule_is_rejected() -> None:
    with pytest.raises(scenario_mod.ScenarioPlanError):
        _plan(
            {
                "version": 1,
                "rules": [
                    {
                        "operation": "submission",
                        "idempotency_key": "job-a",
                        "scenario": "success",
                        "extra": 1,
                    }
                ],
            }
        )


def test_missing_field_in_rule_is_rejected() -> None:
    with pytest.raises(scenario_mod.ScenarioPlanError):
        _plan(
            {
                "version": 1,
                "rules": [{"operation": "submission", "idempotency_key": "job-a"}],
            }
        )


def test_unsupported_operation_is_rejected() -> None:
    with pytest.raises(scenario_mod.ScenarioPlanError):
        _plan(
            {
                "version": 1,
                "rules": [
                    {
                        "operation": "delete",
                        "idempotency_key": "job-a",
                        "scenario": "success",
                    }
                ],
            }
        )


def test_invalid_key_selector_is_rejected() -> None:
    with pytest.raises(scenario_mod.ScenarioPlanError):
        _plan(
            {
                "version": 1,
                "rules": [
                    {
                        "operation": "submission",
                        "idempotency_key": "job a",
                        "scenario": "success",
                    }
                ],
            }
        )


def test_unknown_scenario_is_rejected() -> None:
    with pytest.raises(scenario_mod.ScenarioPlanError):
        _plan(
            {
                "version": 1,
                "rules": [
                    {
                        "operation": "submission",
                        "idempotency_key": "job-a",
                        "scenario": "teapot",
                    }
                ],
            }
        )


def test_operation_incompatible_scenario_is_rejected() -> None:
    with pytest.raises(scenario_mod.ScenarioPlanError):
        _plan(
            {
                "version": 1,
                "rules": [
                    {
                        "operation": "reconciliation",
                        "idempotency_key": "job-a",
                        "scenario": "duplicate_remote_request_id",
                    }
                ],
            }
        )


def test_duplicate_rule_same_operation_and_key_is_rejected() -> None:
    with pytest.raises(scenario_mod.ScenarioPlanError):
        _plan(
            {
                "version": 1,
                "rules": [
                    {
                        "operation": "submission",
                        "idempotency_key": "job-a",
                        "scenario": "success",
                    },
                    {
                        "operation": "submission",
                        "idempotency_key": "job-a",
                        "scenario": "always_500",
                    },
                ],
            }
        )


def test_duplicate_wildcard_rule_is_rejected() -> None:
    with pytest.raises(scenario_mod.ScenarioPlanError):
        _plan(
            {
                "version": 1,
                "rules": [
                    {
                        "operation": "submission",
                        "idempotency_key": "*",
                        "scenario": "success",
                    },
                    {
                        "operation": "submission",
                        "idempotency_key": "*",
                        "scenario": "always_500",
                    },
                ],
            }
        )


def test_duplicate_json_member_in_plan_is_rejected() -> None:
    raw = b'{"version": 1, "version": 1, "rules": []}'
    with pytest.raises(scenario_mod.ScenarioPlanError):
        scenario_mod.parse_scenario_plan(raw)


def test_plan_exceeding_size_limit_is_rejected() -> None:
    oversized = (
        b'{"version": 1, "rules": [' + b"0" * (scenario_mod.MAX_PLAN_BYTES + 1) + b"]}"
    )
    with pytest.raises(scenario_mod.ScenarioPlanError):
        scenario_mod.parse_scenario_plan(oversized)


def test_plan_exceeding_rule_count_limit_is_rejected() -> None:
    rules = [
        {
            "operation": "submission",
            "idempotency_key": f"job-{i}",
            "scenario": "success",
        }
        for i in range(scenario_mod.MAX_RULES + 1)
    ]
    with pytest.raises(scenario_mod.ScenarioPlanError):
        _plan({"version": 1, "rules": rules})


def test_plan_at_rule_count_limit_is_accepted() -> None:
    rules = [
        {
            "operation": "submission",
            "idempotency_key": f"job-{i}",
            "scenario": "success",
        }
        for i in range(scenario_mod.MAX_RULES)
    ]
    plan = _plan({"version": 1, "rules": rules})
    assert plan.resolve("submission", "job-0") == "success"


# -- Action resolution --------------------------------------------------


@pytest.mark.parametrize(
    ("scenario", "ordinal", "expected"),
    [
        ("success", 1, scenario_mod.Action.NORMAL),
        ("success", 5, scenario_mod.Action.NORMAL),
        ("500_then_success", 1, scenario_mod.Action.RETURN_500),
        ("500_then_success", 2, scenario_mod.Action.NORMAL),
        ("429_then_success", 1, scenario_mod.Action.RETURN_429),
        ("429_then_success", 2, scenario_mod.Action.NORMAL),
        ("connect_timeout", 1, scenario_mod.Action.TIMEOUT_NO_PROCESS),
        ("connect_timeout", 2, scenario_mod.Action.NORMAL),
        ("processed_then_disconnect", 1, scenario_mod.Action.PROCESS_THEN_DISCONNECT),
        ("processed_then_disconnect", 2, scenario_mod.Action.NORMAL),
        ("processed_without_response", 1, scenario_mod.Action.PROCESS_THEN_TIMEOUT),
        ("processed_without_response", 2, scenario_mod.Action.NORMAL),
        ("processed_then_500", 1, scenario_mod.Action.PROCESS_THEN_500),
        ("processed_then_500", 2, scenario_mod.Action.NORMAL),
        ("duplicate_remote_request_id", 1, scenario_mod.Action.NORMAL),
        ("duplicate_remote_request_id", 5, scenario_mod.Action.NORMAL),
        ("always_500", 1, scenario_mod.Action.RETURN_500),
        ("always_500", 5, scenario_mod.Action.RETURN_500),
    ],
)
def test_submission_action_resolution(
    scenario: str, ordinal: int, expected: scenario_mod.Action
) -> None:
    assert (
        scenario_mod.resolve_action(scenario, scenario_mod.SUBMISSION, ordinal)
        is expected
    )


@pytest.mark.parametrize(
    ("scenario", "ordinal", "expected"),
    [
        (None, 1, scenario_mod.Action.NORMAL),
        ("success", 1, scenario_mod.Action.NORMAL),
        ("reconciliation_timeout", 1, scenario_mod.Action.TIMEOUT_NO_PROCESS),
        ("reconciliation_timeout", 2, scenario_mod.Action.NORMAL),
        ("always_500", 1, scenario_mod.Action.RETURN_500),
        ("always_500", 9, scenario_mod.Action.RETURN_500),
    ],
)
def test_reconciliation_action_resolution(
    scenario: str | None, ordinal: int, expected: scenario_mod.Action
) -> None:
    assert (
        scenario_mod.resolve_action(scenario, scenario_mod.RECONCILIATION, ordinal)
        is expected
    )


def test_none_scenario_resolves_to_normal_for_submission() -> None:
    assert (
        scenario_mod.resolve_action(None, scenario_mod.SUBMISSION, 1)
        is scenario_mod.Action.NORMAL
    )


# -- Error messages never echo raw plan content ------------------------------


SECRET_KEY = "super-secret-production-idempotency-key-value"


@pytest.mark.parametrize(
    "document",
    [
        {
            "version": 1,
            "rules": [
                {
                    "operation": "submission",
                    "idempotency_key": "not a valid key " + SECRET_KEY,
                    "scenario": "success",
                }
            ],
        },
        {
            "version": 1,
            "rules": [
                {
                    "operation": SECRET_KEY,
                    "idempotency_key": "job-a",
                    "scenario": "success",
                }
            ],
        },
        {
            "version": 1,
            "rules": [
                {
                    "operation": "submission",
                    "idempotency_key": "job-a",
                    "scenario": SECRET_KEY,
                }
            ],
        },
        {
            "version": 1,
            "rules": [
                {
                    "operation": "reconciliation",
                    "idempotency_key": "job-a",
                    "scenario": "duplicate_remote_request_id",  # submission-only
                }
            ],
        },
        {
            "version": 1,
            "rules": [
                {
                    "operation": "submission",
                    "idempotency_key": SECRET_KEY,
                    "scenario": "success",
                },
                {
                    "operation": "submission",
                    "idempotency_key": SECRET_KEY,
                    "scenario": "always_500",
                },
            ],
        },
    ],
)
def test_plan_validation_errors_never_echo_configured_values(
    document: dict[str, object],
) -> None:
    with pytest.raises(scenario_mod.ScenarioPlanError) as exc_info:
        _plan(document)
    message = str(exc_info.value)
    assert SECRET_KEY not in message
    assert "job-a" not in message


def test_duplicate_member_error_never_echoed_into_plan_error() -> None:
    raw = (
        f'{{"version": 1, "{SECRET_KEY}": 1, "{SECRET_KEY}": 2, "rules": []}}'.encode()
    )
    with pytest.raises(scenario_mod.ScenarioPlanError) as exc_info:
        scenario_mod.parse_scenario_plan(raw)
    assert SECRET_KEY not in str(exc_info.value)
