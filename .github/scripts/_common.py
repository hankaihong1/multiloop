"""Shared helpers for cross-platform CI and distribution Python scripts."""

from __future__ import annotations

import contextlib
import os
import pathlib
import subprocess
import sys
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator

ROOT = pathlib.Path(__file__).resolve().parents[2]
DEFAULT_VERSIONS = ("3.14t", "3.15t")


def parse_versions(cli_versions: list[str]) -> list[str]:
    """Resolve target Python versions from CLI args, PYTHON_VERSIONS env var, or defaults."""
    if cli_versions:
        return cli_versions
    env_val = os.environ.get("PYTHON_VERSIONS", "").strip()
    if env_val:
        return env_val.split()
    return list(DEFAULT_VERSIONS)


def parse_major_minor(version_tag: str) -> tuple[int, int]:
    """Parse ``'3.14t'`` or ``'3.15'`` into ``(major, minor)``."""
    cleaned = version_tag.removesuffix("t")
    parts = cleaned.split(".")
    if len(parts) != 2 or not all(part.isdigit() for part in parts):
        raise ValueError(f"Expected '<major>.<minor>[t]' version spec, got {version_tag!r}")
    return int(parts[0]), int(parts[1])


def venv_python_path(venv_dir: pathlib.Path) -> pathlib.Path:
    """Return the Python executable inside ``venv_dir``."""
    if sys.platform == "win32":
        return venv_dir / "Scripts" / "python.exe"
    return venv_dir / "bin" / "python"


def base_uv_env(venv_dir: pathlib.Path | None = None) -> dict[str, str]:
    """Build a clean environment for child ``uv`` invocations."""
    env = os.environ.copy()
    env.pop("UV_PYTHON", None)
    env.pop("PYO3_PYTHON", None)
    if venv_dir is None:
        env.pop("VIRTUAL_ENV", None)
        env.pop("UV_PROJECT_ENVIRONMENT", None)
    else:
        env["UV_PROJECT_ENVIRONMENT"] = str(venv_dir)
        env["VIRTUAL_ENV"] = str(venv_dir)
        bin_dir = str(venv_python_path(venv_dir).parent)
        existing_path = env.get("PATH", "")
        env["PATH"] = f"{bin_dir}{os.pathsep}{existing_path}" if existing_path else bin_dir
    return env


def run_cmd(cmd: list[str], env: dict[str, str] | None = None) -> None:
    """Print and execute ``cmd`` in the repository root, failing loudly on non-zero exit."""
    print(f"+ {' '.join(cmd)}", flush=True)
    subprocess.run(cmd, cwd=ROOT, env=env, check=True)


def find_managed_python(version: str) -> str:
    """Locate (and install if missing) a ``uv``-managed interpreter for ``version``."""
    env = base_uv_env()
    find_cmd = ["uv", "python", "find", "--system", "--managed-python", version]
    probe = subprocess.run(find_cmd, cwd=ROOT, env=env, text=True, capture_output=True, check=False)
    if probe.returncode != 0:
        run_cmd(["uv", "python", "install", version], env=env)
        probe = subprocess.run(
            find_cmd, cwd=ROOT, env=env, text=True, capture_output=True, check=True
        )
    resolved = probe.stdout.strip().splitlines()[-1].strip()
    if not resolved:
        raise RuntimeError(f"uv python find returned empty path for {version}")
    return resolved


def ensure_venv(version: str, venv_dir: pathlib.Path) -> tuple[pathlib.Path, dict[str, str]]:
    """Provision/sync ``venv_dir`` for ``version`` and return ``(venv_python, child_env)``."""
    managed_python = find_managed_python(version)
    env = base_uv_env(venv_dir)
    run_cmd(
        [
            "uv",
            "sync",
            "--locked",
            "--no-install-project",
            "--group",
            "dev",
            "--python",
            managed_python,
        ],
        env=env,
    )
    venv_py = venv_python_path(venv_dir)
    if not venv_py.exists():
        raise RuntimeError(f"Expected venv Python executable missing: {venv_py}")
    env["PYO3_PYTHON"] = str(venv_py)
    return venv_py, env


@contextlib.contextmanager
def ci_group(title: str) -> Iterator[None]:
    """Emit collapsible GitHub Actions log groups or clear local section headers."""
    in_gha = os.environ.get("GITHUB_ACTIONS") == "true"
    if in_gha:
        print(f"::group::{title}", flush=True)
    else:
        print(f"\n=== {title} ===", flush=True)
    try:
        yield
    finally:
        if in_gha:
            print("::endgroup::", flush=True)
