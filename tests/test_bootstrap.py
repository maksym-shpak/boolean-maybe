"""Repository bootstrap smoke tests.

These tests verify that the packaging baseline defined by ADR-001 works:
the package is importable and the declared console-script entry point
resolves and runs. They intentionally do not assert any product-level
CLI behavior, output, or exit-code contract.
"""

from importlib.metadata import entry_points

import boolean_maybe


def test_package_is_importable() -> None:
    assert boolean_maybe is not None


def test_console_script_entry_point_resolves_and_runs() -> None:
    matches = [
        ep for ep in entry_points(group="console_scripts") if ep.name == "boolean-maybe"
    ]

    assert len(matches) == 1
    assert matches[0].value == "boolean_maybe.cli:main"
    matches[0].load()()  # runs without raising; no return/exit-code contract asserted
