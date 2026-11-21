"""Verify package version and release metadata consistency across repository files."""

from __future__ import annotations

import argparse
import pathlib
import re
import sys
import tomllib

ROOT = pathlib.Path(__file__).resolve().parents[2]


def _read_pyproject_version() -> str:
    data = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    project = data.get("project")
    if not isinstance(project, dict) or not isinstance(project.get("version"), str):
        raise RuntimeError("Missing [project].version in pyproject.toml")
    return project["version"]


def _read_cargo_version() -> str:
    data = tomllib.loads((ROOT / "Cargo.toml").read_text(encoding="utf-8"))
    package = data.get("package")
    if not isinstance(package, dict) or not isinstance(package.get("version"), str):
        raise RuntimeError("Missing [package].version in Cargo.toml")
    return package["version"]


def _read_citation_version() -> str:
    text = (ROOT / "CITATION.cff").read_text(encoding="utf-8")
    match = re.search(r'^version:\s*["\']?([^\s"\'#]+)["\']?', text, re.MULTILINE)
    if match is None:
        raise RuntimeError("Missing version field in CITATION.cff")
    return match.group(1)


def _read_python_dunder_version() -> str:
    text = (ROOT / "src" / "multiloop" / "__init__.py").read_text(encoding="utf-8")
    match = re.search(r'^__version__\s*=\s*["\']([^"\']+)["\']', text, re.MULTILINE)
    if match is None:
        raise RuntimeError("Missing __version__ in src/multiloop/__init__.py")
    return match.group(1)


def _check_changelog(version: str) -> None:
    text = (ROOT / "CHANGELOG.md").read_text(encoding="utf-8")
    pattern = rf"^##\s+\[{re.escape(version)}\](?:\s|$)"
    if re.search(pattern, text, re.MULTILINE) is None:
        raise RuntimeError(f"CHANGELOG.md does not contain a '## [{version}]' section")


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Verify release version parity across manifests.")
    parser.add_argument(
        "--tag",
        default="",
        help="Optional git tag/ref name (e.g. v0.1.3) to compare against manifest version.",
    )
    parser.add_argument(
        "--require-changelog",
        action="store_true",
        help="Require a matching '## [<version>]' entry in CHANGELOG.md.",
    )
    args = parser.parse_args(argv)

    py_version = _read_pyproject_version()
    cargo_version = _read_cargo_version()
    citation_version = _read_citation_version()
    dunder_version = _read_python_dunder_version()

    errors: list[str] = []
    if cargo_version != py_version:
        errors.append(
            f"Version mismatch: pyproject.toml={py_version} != Cargo.toml={cargo_version}"
        )
    if citation_version != py_version:
        errors.append(
            f"Version mismatch: pyproject.toml={py_version} != CITATION.cff={citation_version}"
        )
    if dunder_version != py_version:
        errors.append(
            f"Version mismatch: pyproject.toml={py_version} != __init__.py={dunder_version}"
        )
    if args.tag:
        tag_version = args.tag.removeprefix("v")
        if tag_version != py_version:
            errors.append(f"Version mismatch: tag={args.tag} != pyproject.toml={py_version}")
    if args.require_changelog:
        try:
            _check_changelog(py_version)
        except RuntimeError as exc:
            errors.append(str(exc))

    if errors:
        for err in errors:
            print(f"ERROR: {err}", file=sys.stderr)
        return 1

    print(f"OK: release metadata consistent at version {py_version}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
