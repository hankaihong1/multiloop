"""Install and test built wheels or sdist archives across free-threaded Python versions."""

from __future__ import annotations

import argparse
import sys

from _common import ROOT, ci_group, ensure_venv, parse_major_minor, parse_versions, run_cmd


def _smoke_test(venv_py: str, version_tag: str, env: dict[str, str]) -> None:
    major, minor = parse_major_minor(version_tag)
    expect_nogil = version_tag.endswith("t")
    snippet = (
        "import sys, multiloop, multiloop._multiloop_core as m; "
        f"assert sys.version_info[:2] == ({major}, {minor}), sys.version; "
        "assert 'site-packages' in (multiloop.__file__ or ''), multiloop.__file__; "
        "assert 'site-packages' in (m.__file__ or ''), m.__file__; "
        f"assert (hasattr(sys, '_is_gil_enabled') and not sys._is_gil_enabled()) == {expect_nogil!r}, "
        "'unexpected GIL mode'; "
        "print('ok:', sys.version_info[:2], m.__file__)"
    )
    run_cmd(["uv", "run", "--no-sync", "--python", venv_py, "python", "-c", snippet], env=env)


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Verify built wheels or sdist across versions.")
    parser.add_argument("--mode", choices=("wheels", "sdist"), required=True)
    parser.add_argument("--dist-dir", default="dist", help="Directory containing dist archives.")
    parser.add_argument("versions", nargs="*", help="Target Python versions (e.g. 3.14t 3.15t).")
    args = parser.parse_args(argv)

    dist_dir = (ROOT / args.dist_dir).resolve()
    versions = parse_versions(args.versions)
    sdist_path = ""
    if args.mode == "sdist":
        tarballs = sorted(dist_dir.glob("*.tar.gz"))
        if len(tarballs) != 1:
            raise RuntimeError(f"Expected exactly 1 sdist tarball in {dist_dir}, got {tarballs}")
        sdist_path = str(tarballs[0])

    for ver in versions:
        with ci_group(f"Verify {args.mode} ({ver})"):
            venv_dir = ROOT / f".venv-dist-{args.mode}-{ver}"
            venv_py, env = ensure_venv(ver, venv_dir)
            py_str = str(venv_py)
            if args.mode == "wheels":
                run_cmd(
                    [
                        "uv",
                        "pip",
                        "install",
                        "multiloop",
                        "--no-index",
                        "--no-deps",
                        "--only-binary",
                        ":all:",
                        "--find-links",
                        str(dist_dir),
                        "--reinstall",
                        "--python",
                        py_str,
                    ],
                    env=env,
                )
            else:
                run_cmd(
                    [
                        "uv",
                        "pip",
                        "install",
                        "--no-index",
                        "--no-build-isolation",
                        "--no-deps",
                        "--reinstall",
                        "--python",
                        py_str,
                        sdist_path,
                    ],
                    env=env,
                )
            _smoke_test(py_str, ver, env)
            run_cmd(
                ["uv", "run", "--no-sync", "--python", py_str, "pytest", "-m", "not slow"],
                env=env,
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
