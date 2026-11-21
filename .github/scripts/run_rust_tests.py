"""Run Rust unit tests with the active Python interpreter's shared library path.

Standalone ``cargo test`` binaries link against ``libpython`` rather than loading
symbols from a host process. Managed interpreters (such as ``uv`` Python installs)
place ``libpython`` outside the default system loader search path, so this helper
prepends the base interpreter library directory before invoking ``cargo test``.
"""

from __future__ import annotations

import os
import subprocess
import sys
import sysconfig


def _python_lib_dir() -> tuple[str, str]:
    """Return ``(env_var_name, directory_path)`` for locating ``libpython``."""
    if sys.platform == "win32":
        # On Windows, python3xx.dll resides in the base interpreter root rather
        # than the venv copy or LIBDIR (which holds import .lib files).
        return "PATH", sys.base_prefix

    lib_dir = sysconfig.get_config_var("LIBDIR")
    if not isinstance(lib_dir, str) or not lib_dir:
        lib_dir = os.path.join(sys.base_prefix, "lib")

    env_var = "DYLD_FALLBACK_LIBRARY_PATH" if sys.platform == "darwin" else "LD_LIBRARY_PATH"
    return env_var, lib_dir


def main(argv: list[str]) -> int:
    env_var, lib_dir = _python_lib_dir()
    env = os.environ.copy()
    existing = env.get(env_var, "")
    env[env_var] = f"{lib_dir}{os.pathsep}{existing}" if existing else lib_dir

    cmd = [
        "cargo",
        "test",
        "--lib",
        "--no-default-features",
        "--features",
        "test-init",
        "--locked",
        *argv,
    ]
    result = subprocess.run(cmd, env=env, check=False)
    return result.returncode


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
