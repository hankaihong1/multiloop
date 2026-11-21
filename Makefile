.PHONY: sync develop build sdist package check-release test test-rust test-docs test-slow bench lint format docs clean all

sync:
	uv sync --locked --group dev

develop:
	uv run --no-sync maturin develop --release --locked

build:
	uv run --no-sync maturin build --release --locked --out dist --compatibility pypi

sdist:
	uv run --no-sync maturin sdist --out dist

package: build sdist

check-release:
	uv run --no-sync python .github/scripts/check_release.py

test-rust:
	uv run --no-sync python .github/scripts/run_rust_tests.py

test-docs:
	PYTHONPATH=src uv run --no-sync pytest tests/test_docs.py -q

docs: test-docs

test: test-rust
	uv run --no-sync pytest

test-slow:
	uv run --no-sync pytest -m slow -o addopts=""

bench:
	for f in benchmarks/benchmark_pull_model.py benchmarks/bench_multithread_loops.py; do \
		uv run --no-sync python "$$f"; \
	done

lint: check-release
	uv run --no-sync ruff check .
	uv run --no-sync ruff format --check .
	MYPYPATH=src uv run --no-sync mypy --strict src/multiloop
	uv run --no-sync pyright src/multiloop
	cargo clippy --locked -- -D warnings
	cargo fmt --check
	RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --document-private-items --locked

format:
	uv run --no-sync ruff format .
	cargo fmt

clean:
	cargo clean
	rm -rf dist dist-check .venv-ci-* .venv-build .venv-dist-* .pytest_cache .ruff_cache .mypy_cache

all: develop lint test
