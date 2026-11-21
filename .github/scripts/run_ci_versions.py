"""Build and run Rust + Python test suites sequentially across Python versions on one OS."""

from __future__ import annotations

import argparse
import sys

from _common import ROOT, ci_group, ensure_venv, parse_major_minor, parse_versions, run_cmd


def _verify_runtime(venv_py: str, version_tag: str, env: dict[str, str]) -> None:
    major, minor = parse_major_minor(version_tag)
    expect_nogil = version_tag.endswith("t")
    snippet = (
        "import sys; "
        f"assert sys.version_info[:2] == ({major}, {minor}), sys.version; "
        f"assert (hasattr(sys, '_is_gil_enabled') and not sys._is_gil_enabled()) == {expect_nogil!r}, "
        "'unexpected GIL mode'"
    )
    run_cmd(["uv", "run", "--no-sync", "--python", venv_py, "python", "-c", snippet], env=env)


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Run CI builds and tests across Python versions.")
    parser.add_argument("versions", nargs="*", help="Target Python versions (e.g. 3.14t 3.15t).")
    args = parser.parse_args(argv)

    versions = parse_versions(args.versions)
    for ver in versions:
        with ci_group(f"Python {ver}: build & test"):
            venv_dir = ROOT / f".venv-ci-{ver}"
            venv_py, env = ensure_venv(ver, venv_dir)
            py_str = str(venv_py)
            _verify_runtime(py_str, ver, env)
            run_cmd(
                [
                    "uv",
                    "run",
                    "--no-sync",
                    "--python",
                    py_str,
                    "maturin",
                    "develop",
                    "--release",
                    "--locked",
                ],
                env=env,
            )
            run_cmd(
                [
                    "uv",
                    "run",
                    "--no-sync",
                    "--python",
                    py_str,
                    "python",
                    ".github/scripts/run_rust_tests.py",
                ],
                env=env,
            )
            run_cmd(["uv", "run", "--no-sync", "--python", py_str, "pytest"], env=env)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
