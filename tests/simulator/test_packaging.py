"""Packaging tests: resolve and invoke both console scripts from a real
built wheel installed into a fresh, isolated virtual environment (not the
repository's editable dev install).
"""

from __future__ import annotations

import http.client
import json
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


@pytest.fixture(scope="module")
def installed_venv(tmp_path_factory: pytest.TempPathFactory) -> Path:
    build_dir = tmp_path_factory.mktemp("packaging-build")
    subprocess.run(
        ["uv", "build", "--wheel", "--out-dir", str(build_dir)],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    wheels = list(build_dir.glob("*.whl"))
    assert len(wheels) == 1, f"expected exactly one built wheel, found {wheels}"
    wheel_path = wheels[0]

    venv_dir = tmp_path_factory.mktemp("packaging-venv")
    subprocess.run(
        ["uv", "venv", str(venv_dir)], check=True, capture_output=True, text=True
    )

    venv_python = venv_dir / (
        "Scripts/python.exe" if sys.platform == "win32" else "bin/python"
    )
    subprocess.run(
        ["uv", "pip", "install", "--python", str(venv_python), str(wheel_path)],
        check=True,
        capture_output=True,
        text=True,
    )
    return venv_dir


def _script_path(venv_dir: Path, name: str) -> Path:
    if sys.platform == "win32":
        return venv_dir / "Scripts" / f"{name}.exe"
    return venv_dir / "bin" / name


def test_wheel_and_sdist_both_declare_both_console_scripts(tmp_path: Path) -> None:
    subprocess.run(
        ["uv", "build", "--out-dir", str(tmp_path)],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    wheels = list(tmp_path.glob("*.whl"))
    sdists = list(tmp_path.glob("*.tar.gz"))
    assert len(wheels) == 1
    assert len(sdists) == 1

    extract_dir = tmp_path / "wheel-extracted"
    shutil.unpack_archive(str(wheels[0]), str(extract_dir), format="zip")
    entry_points_files = list(extract_dir.glob("*.dist-info/entry_points.txt"))
    assert len(entry_points_files) == 1
    entry_points_text = entry_points_files[0].read_text(encoding="utf-8")
    assert "boolean-maybe = boolean_maybe.cli:main" in entry_points_text
    assert (
        "boolean-maybe-simulator = boolean_maybe.simulator.cli:main"
        in entry_points_text
    )


def test_boolean_maybe_console_script_resolves_and_runs(installed_venv: Path) -> None:
    script = _script_path(installed_venv, "boolean-maybe")
    assert script.exists()

    result = subprocess.run([str(script)], capture_output=True, text=True, timeout=10)
    assert result.returncode == 0


def test_boolean_maybe_simulator_console_script_resolves_and_runs(
    installed_venv: Path,
) -> None:
    script = _script_path(installed_venv, "boolean-maybe-simulator")
    assert script.exists()

    port = _find_free_port()
    proc = subprocess.Popen(
        [str(script), "--port", str(port)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        deadline = time.monotonic() + 5.0
        connected = False
        while time.monotonic() < deadline:
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                    connected = True
                    break
            except OSError:
                time.sleep(0.05)
        assert connected, (
            "installed simulator console script never started accepting connections"
        )

        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
        conn.request("GET", "/jobs/by-idempotency-key/job-a")
        response = conn.getresponse()
        body = json.loads(response.read())
        conn.close()
        assert response.status == 404
        assert body == {"idempotency_key": "job-a", "status": "not_found"}
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)
