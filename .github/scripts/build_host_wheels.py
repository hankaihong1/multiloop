"""Build distribution wheels for all target free-threaded Python versions on one OS."""

from __future__ import annotations

import argparse
import sys

from _common import ROOT, ci_group, ensure_venv, find_managed_python, parse_versions, run_cmd


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Build wheels for target Python versions.")
    parser.add_argument("--target", default="", help="Optional Rust/maturin target triple or arch.")
    parser.add_argument("--out", default="dist", help="Output directory for built wheels.")
    parser.add_argument("versions", nargs="*", help="Target Python versions (e.g. 3.14t 3.15t).")
    args = parser.parse_args(argv)

    versions = parse_versions(args.versions)
    with ci_group(f"Build wheels ({', '.join(versions)})"):
        interpreters = [find_managed_python(ver) for ver in versions]
        venv_py, env = ensure_venv(versions[0], ROOT / ".venv-build")
        cmd = [
            "uv",
            "run",
            "--no-sync",
            "--python",
            str(venv_py),
            "maturin",
            "build",
            "--release",
            "--locked",
            "--out",
            args.out,
            "--compatibility",
            "pypi",
        ]
        if args.target:
            cmd.extend(["--target", args.target])
        cmd.append("--interpreter")
        cmd.extend(interpreters)
        run_cmd(cmd, env=env)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
