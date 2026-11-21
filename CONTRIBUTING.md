# Contributing to multiloop

First off, thank you for taking the time to contribute! 🎉

This document outlines how to set up a development environment, run the test
suite, satisfy the linting/type-checking gates, and get your changes merged.

Please read our [Code of Conduct](CODE_OF_CONDUCT.md) before participating —
we expect all contributors to uphold it.

---

## Table of Contents

- [Development Setup](#development-setup)
- [Building the Rust Extension](#building-the-rust-extension)
- [Testing](#testing)
- [Linting & Type Checking](#linting--type-checking)
- [PR & Release Process](#pr--release-process)

---

## Development Setup

`multiloop` targets **free-threaded Python (`>= 3.14t`, no-GIL)** with a Rust
core built via PyO3 and `maturin`. All Python tooling is managed through
**`uv`** — please do not use `pip`.

### Prerequisites

- **Python** `>= 3.14t` (free-threaded build; `uv` will provision `3.14t` from `.python-version`)
- **Rust** toolchain (`stable`, with `clippy` and `rustfmt`) — needed to compile `_multiloop_core`
- **`uv`** — the package and environment manager used throughout this repo

### Install dev dependencies

```bash
make sync             # or: uv sync --locked --group dev
```

This installs the locked `dev` dependency group from `pyproject.toml`, including
`maturin`, `mypy`, `pyright`, `pytest`, `pytest-asyncio`, `pytest-benchmark`,
`pytest-repeat`, `pytest-timeout`, and `ruff`.

---

## Building the Rust Extension

The core engine (`_multiloop_core`) is a Rust `cdylib` (`PyO3` + `flume` +
`parking_lot`) that must be compiled before Python code can import `multiloop`.

Build it with the release profile:

```bash
make develop          # or: uv run maturin develop --release --locked
```

> The `--release` profile is important — it enables `opt-level = 3`, thin LTO,
> and single codegen units as configured in `Cargo.toml`, which is what produces
> the performance characteristics the benchmarks rely on.

---

## Testing

The repository runs both Rust unit tests (`cargo test`) and the Python test
suite (`pytest`).

Run everything:

```bash
make test             # runs make test-rust + uv run --no-sync pytest
```

Useful variations:

```bash
# Rust unit tests only (sets libpython loader path across Linux/macOS/Windows)
make test-rust

# Docs & examples parity/syntax checks only
make test-docs

# Run a single pytest file or specific test
uv run --no-sync pytest tests/test_channel.py
uv run --no-sync pytest tests/test_channel.py::test_send_recv

# Repeat a targeted concurrency test to shake out flaky races
uv run --no-sync pytest tests/test_pool.py -p no:cacheprovider --count=50
```

> Because `multiloop` is a concurrency library, `pytest-repeat` (`--count=N`) on
> a targeted test file is your friend for stress-testing channels, pools, and
> wait groups.

### Quick Start (5 minutes to first run)

```bash
make sync
make develop          # compile the Rust core in release mode
uv run --no-sync python examples/00_pool_basics.py
```

- Not sure which primitive to use? See the decision table in
  [docs/CHOOSING.md](docs/CHOOSING.md).
- About to touch concurrency code? Read
  [docs/CONCURRENCY.md](docs/CONCURRENCY.md) first — it lists the nine
  race-condition trap patterns and the mandatory pre-change checklist.
- More runnable examples: [`examples/README.md`](examples/README.md).

### Documentation (EN/ZH mirrors)

Every user-facing doc exists in an English original plus a Chinese mirror:
`README.md` ↔ `README_ZH.md`, `docs/API.md` ↔ `docs/API_ZH.md`,
`docs/CHOOSING.md` ↔ `docs/CHOOSING_ZH.md`,
`docs/CONCURRENCY.md` ↔ `docs/CONCURRENCY_ZH.md`, and
`examples/README.md` ↔ `examples/README_ZH.md`.

- Always edit the English original and the Chinese mirror **in the same PR**.
- The two versions must stay structurally identical (same sections, same
  headings, same code blocks) — `tests/test_docs.py` enforces this.
- The language-switch link at the top of each doc must keep pointing at the
  other version.

### Benchmarks

Performance-sensitive changes should be validated against the benchmark suite:

```bash
make bench
```

---

## Linting & Type Checking

The CI gates and `make lint` share the same checks: `ruff` (lint + format),
`mypy --strict`, `pyright`, `cargo clippy --locked`, `cargo fmt --check`,
`cargo doc`, and release manifest parity (`check_release.py`).

Run the full lint gate locally:

```bash
make lint
```

Auto-format Python and Rust code before committing:

```bash
make format
```

### Pre-submission checklist

Before opening a PR, run the single unified pre-submission target:

```bash
make all              # develop + lint + test
```

If your PR only edits Markdown docs or files under `examples/`, run:

```bash
uv run --no-sync ruff check tests/test_docs.py examples
uv run --no-sync ruff format --check tests/test_docs.py examples
make test-docs
```

---

## PR & Release Process

- All changes are submitted as pull requests **to `main`**.
- **Request before code**: open an issue/feature request first for any
  non-trivial change and wait for maintainer approval before writing code —
  it lets maintainers and other contributors weigh in on the design before
  effort is spent.
- **Green before merge**: a PR is merged only after **all** required CI
  checks pass (`Lint & Static Analysis`, `Docs Drift Check`, and the OS-grouped
  `Build & Test` runners on `ubuntu-latest`, `macos-latest`, and
  `windows-latest`, each verifying free-threaded `3.14t` and `3.15t`).
- **Docs/examples-only exception**: PRs touching only `*.md`, `docs/**`, or
  `examples/**` skip the compiled matrix and run `Docs Drift Check` (`ruff` +
  `tests/test_docs.py`) instead.
- Keep PRs focused: one logical change per PR, with a clear title and a
  description of *what* and *why*.
- **Label the PR** (`feature` / `fix` / `docs` / `chore` / `breaking`):
  Release Drafter (`release-drafter.yml`) aggregates merged PRs on `main` and
  derives the next version number from these labels.
- **Release & PyPI Publish**: publishing a GitHub Release (or manually
  running `Publish` via `workflow_dispatch` with `pypi` / `testpypi` /
  `dry-run`) runs pre-release validation, builds `cp314t` and `cp315t` wheels
  per OS/architecture plus `sdist`, verifies installed wheels and `sdist`
  across `3.14t` and `3.15t`, and publishes via Trusted Publishing (OIDC).

---

## License

By contributing, you agree that your contributions will be licensed under the
same [MIT License](LICENSE) as the rest of the project.
