# AGENTS.md — AI Agent Onboarding Guide for multiloop

> **Repository**: [hankaihong1/multiloop](https://github.com/hankaihong1/multiloop)
> **License**: MIT

---

## 1. Project Identity

**multiloop** is a multi-event-loop engine and Go-style concurrency toolkit for
Python 3.14t (free-threaded / no-GIL). It wraps a Rust core (`_multiloop_core`)
built with PyO3, flume, and parking_lot, and exposes `EventLoopThreadPool`,
`Channel`, `select_channel`, `TaskGroup`, `CancelScope`, and async
synchronization primitives (`AsyncWaitGroup`, `AsyncOnce`, `AsyncRWMutex`,
`Lock`, `Semaphore`, `Condition`, `Barrier`, `CapacityLimiter`). The public API
is intentionally minimal, offering a clean top-level facade.

**Multi-Threaded Physical Parallelism & Concurrency Invariants**:
`multiloop` operates under Python 3.14t true multi-core physical parallelism across
multiple OS threads and isolated event loops. Correctness cannot rely on single-threaded
cooperative heuristics; it requires **formal state-machine invariants, token
conservation laws, OS-level mutex synchronization (`threading.Lock` / `parking_lot::Mutex`),
and cross-thread signal forwarding (`loop.call_soon_threadsafe`)**.

---

## 2. Directory Map

```
multiloop/
├── src/
│   ├── multiloop/              # Python package (14 modules)
│   │   ├── __init__.py       # Public API surface — start here
│   │   ├── pool.py           # EventLoopThreadPool engine
│   │   ├── primitives.py     # Channel, select_channel, AsyncWaitGroup, AsyncOnce
│   │   ├── context.py        # AsyncContext (O(1) set tracking)
│   │   ├── _cancel.py        # CancelScope, fail_after/at, move_on_after/at, sleep, wait_cancellable, checkpoint
│   │   ├── _sync.py          # Lock, Semaphore, Event, Condition, Barrier, CapacityLimiter (Rust-backed)
│   │   ├── _taskgroup.py     # TaskGroup (_TGState enum, pool & max_concurrency/limiter), TaskHandle, TaskState, TaskStatus
│   │   ├── rwlock.py         # AsyncRWMutex (async read-write lock, Rust-backed)
│   │   ├── _rust.py          # _try_import_rust_class helper
│   │   ├── _metrics.py       # MetricsCollector
│   │   ├── _options.py       # PoolOptions dataclass
│   │   ├── _logging.py       # get_logger, set_log_level
│   │   ├── exceptions.py     # MultiloopError hierarchy
│   │   └── testing/          # Test helpers
│   ├── lib.rs                # Rust pymodule registration (_multiloop_core)
│   ├── cancel.rs             # RawTaskCancelState (pure Rust task cancel stack & WaiterEntry parking)
│   ├── errors.rs             # Typed Python exception constructors (multiloop.exceptions)
│   ├── timeout.rs            # Panic-free timeout validation & monotonic Instant deadline helpers
│   ├── waiter.rs             # Unified WaiterId ticket & out-of-lock wakeup/cancel helpers
│   ├── metrics.rs            # AtomicMetrics & 64-byte PaddedAtomic
│   ├── pool.rs               # NativeWorkerPool, PollerGuard, >64-worker idle bitmap & drain_all
│   ├── channel.rs            # RawAsyncChannel & ChannelState (Anti-Barging FIFO, unified sync/async waiters)
│   ├── sync.rs               # RawAsyncLock, RawAsyncSemaphore, RawAsyncCapacityLimiter, RawAsyncEvent, RawAsyncCondition, RawAsyncBarrier, RawAsyncOnce
│   ├── waitgroup.rs          # RawAsyncWaitGroup & WaitGroupInner
│   ├── rwlock.rs             # RawAsyncRWMutex (writer-preference state machine)
│   └── tests.rs              # Rust unit tests
├── tests/                    # 14 test files + conftest.py (pytest + pytest-asyncio)
├── benchmarks/               # Multi-thread benchmarks
├── examples/                 # Runnable examples (python examples/00_*.py)
├── docs/
│   ├── API.md                # Complete API reference
│   ├── CHOOSING.md           # Primitive selection decision table (human entry point)
│   └── CONCURRENCY.md        # Concurrency correctness guide & pre-change checklist
├── Cargo.toml                # Rust crate config (edition 2021, thin release LTO)
├── pyproject.toml            # Python config (mypy strict, pyright strict, ruff, pytest)
├── Makefile                  # Convenience targets (sync, develop, test, lint, package, bench, all)
├── .github/
│   ├── actions/              # Shared composite actions (setup-env, run-lint, build-dist)
│   ├── scripts/              # Shared CI/Makefile helpers (_common.py, run_rust_tests.py, check_release.py, run_ci_versions.py, build_host_wheels.py, verify_dist.py)
│   └── workflows/            # CI (ci.yml), Publish (publish.yml), Release Drafter
└── CONTRIBUTING.md           # Human-oriented dev setup guide
```

## 2.1 Root-Directory Quick Reference (AI use)

One line per tracked root file — what it is and whether the AI should touch it.
Anything not listed here (`.omo/`, `.codegraph/`, `.benchmarks/`, `.ruff_cache/`,
`.mypy_cache/`, `.pytest_cache/`, `.hermes/`) is a tool cache: ignore in
searches, never modify.

| File | Purpose | AI should touch? |
|---|---|---|
| `pyproject.toml` / `Cargo.toml` / `uv.lock` / `Cargo.lock` | Build & dependency manifests | Yes (when changing deps) |
| `Makefile` | Command entry point (develop/test/lint/bench/all) | Yes — prefer it over raw commands |
| `.gitignore` / `.pre-commit-config.yaml` | VCS ignores / pre-commit hooks (ruff + cargo) | Yes (when changing hooks) |
| `AGENTS.md` / `README.md` / `CONTRIBUTING.md` / `docs/` (English files) | Docs | Yes (sync when behavior changes; docs `*_ZH.md` are the Chinese mirrors — same content, do not maintain separately) |
| `build.rs` | libpython link logic (extension-module detection) | Only for cargo-test link paths |
| `deny.toml` / `rust-toolchain.toml` / `.editorconfig` / `.python-version` | Toolchain pinning | No (leave alone) |
| `LICENSE` / `CODE_OF_CONDUCT.md` / `SECURITY.md` / `CODEOWNERS` / `CITATION.cff` / `CHANGELOG.md` | GitHub ecosystem files | No (unless asked) |
| `.github/` | CI + issue/PR templates | When changing CI |
| `test_build.sh` | (deleted — was a one-liner duplicating `make develop`) | — |
| `examples/` | Runnable demos | Yes (add example when adding a feature) |

---

## 3. Key Conventions

### Python

| Tool | Configuration | Gate |
|------|--------------|------|
| **mypy** | `strict = true`, `disallow_untyped_defs = true` | Zero errors |
| **pyright** | `typeCheckingMode = "strict"` | Zero errors |
| **ruff** | `target-version = "py313"`, `line-length = 100`, select `E, F, I, N, UP, W, B, C4, SIM, TCH, RUF` | Zero errors |
| **pytest** | `asyncio_mode = "strict"` | Zero failures |

### Rust

| Tool | Configuration | Gate |
|------|--------------|------|
| **cargo clippy** | `--locked -- -D warnings` | Zero warnings |
| **cargo fmt** | Edition 2021 | Clean diff |
| **release profile** | `opt-level = 3`, `lto = "thin"`, `codegen-units = 1`, `strip = false` | — |

### General

- **Python version**: `>= 3.14t` (free-threaded / no-GIL only; OS-grouped CI and release builds both target `3.14t` and `3.15t` across Linux, macOS, and Windows).
- **Package manager**: `uv` exclusively — do not use `pip`.
- **Rust toolchain**: stable (via rustup).
- **Rust extension required at runtime**: the compiled `_multiloop_core` extension
  is required at runtime (no pure-Python fallback exists; instantiating primitives
  or starting pools without a compiled extension raises `RuntimeError`).
  `_try_import_rust_class()` (`src/multiloop/_rust.py`) defers symbol lookup at import
  time only so docs, type checkers, and AST tools can inspect module metadata before compilation.
- **Commit style**: conventional, concise. PRs target `main`.
- **Request before code, green before merge**: for any non-trivial change,
  file an issue/request first and wait for approval before writing code.
  A PR is merged only after all required CI checks pass (lint, type checks,
  tests) — never merge a red PR, and never ask a maintainer to.
- **Docs/examples-only exception to green-before-merge**: changes touching only
  `*.md`, `docs/**`, or `examples/**` skip the heavy lint/build/test matrix —
  the `Docs Drift Check` job runs `ruff` on `tests/test_docs.py`/`examples` plus
  `tests/test_docs.py` (EN/ZH parity, code-block syntax, API coverage), and the
  project lead reviews the diff before merging or pushing. Anything else
  (code, config, CI files) runs the full suite; a mixed change runs both gates.
- **GitHub Actions is for CI, not debugging**: never use workflow runs as a
  debugging tool — no debug commits, no temporary diagnostic workflows, no
  bisecting platform-specific failures on remote runners (they pollute the
  Actions history and burn CI minutes). If code needs to be debugged on
  another platform's machine, ask the user to arrange access first; never
  push anything to trigger remote runs without explicit permission.
- **Docs: EN/ZH must stay in sync**: every doc has an English original and a
  `*_ZH.md` mirror (`README.md` ↔ `README_ZH.md`, `docs/*.md` ↔
  `docs/*_ZH.md`, `examples/README.md` ↔ `examples/README_ZH.md`). Any change
  to one must be mirrored to the other in the same commit — same sections,
  same headings, same code blocks; only the language differs.
  `tests/test_docs.py` enforces structural parity.

---

## 4. How to Navigate the Codebase

### Recommended reading order for AI agents

1. **`src/multiloop/__init__.py`** — Public API surface. Every exported symbol is
   re-exported here. Start here to understand the module layout.

2. **`src/multiloop/pool.py`** — Core engine. `EventLoopThreadPool` orchestrates
   OS threads running isolated asyncio event loops. The `submit()` method has
   two paths (global queue via `push_global`, local queue via `push_local`).
   `_worker_dispatcher` is the per-worker event loop that calls `pop_work()`
   and spawns tasks.

3. **`src/lib.rs`** — Rust backend (`_multiloop_core`). Registers thirteen `#[pyclass]`
   types across seven modules (plus shared `WaiterId` ticket / out-of-lock wakeup &
   cancellation helpers in `src/waiter.rs`, panic-free timeout helpers in `src/timeout.rs`,
   and typed exception constructors in `src/errors.rs`): `RawTaskCancelState` (`src/cancel.rs`),
   `NativeWorkerPool` (`src/pool.rs`), `RawAsyncChannel` (`src/channel.rs`), `AtomicMetrics`
   (`src/metrics.rs`), `RawAsyncWaitGroup` (`src/waitgroup.rs`),
   `RawAsyncRWMutex` (`src/rwlock.rs`), and the synchronization state machines in
   `src/sync.rs` (`RawAsyncLock`, `RawAsyncSemaphore`, `RawAsyncCapacityLimiter`,
   `RawAsyncEvent`, `RawAsyncCondition`, `RawAsyncBarrier`, `RawAsyncOnce`).

4. **`src/multiloop/primitives.py`** — `Channel` Python facade over native Rust
   `RawAsyncChannel`, `select_channel` (weakref-backed watchers), `AsyncWaitGroup`
   (over `RawAsyncWaitGroup`), and `AsyncOnce` (over `RawAsyncOnce`).

5. **`src/multiloop/_cancel.py`** — Pure Rust task cancel stack:
   `CancelScope` (with `shield` and `PLAIN` / `MOVE_ON` / `FAIL_AFTER` exit modes),
   `fail_after`/`fail_at`, `move_on_after`/`move_on_at`, `sleep`, `wait_cancellable`,
   and `checkpoint`, backed by `RawTaskCancelState` (stored directly on the task via
   `_multiloop_cancel_state`, with `_FALLBACK_STATES` `WeakKeyDictionary` only for
   slots-only task proxies) with zero mutation of CPython `task.cancel()` / `uncancel()` /
   `cancelling()`.

### For bug investigation

- Run the failing test: `uv run pytest tests/<file>.py::<test_name> -xvs`
- Stress-test for race conditions (**single target file only, never the whole suite**):
  `uv run pytest tests/<file>.py -p no:cacheprovider --count=50`
- Run last-failed tests: `uv run pytest --lf`

### Module dependency map (simplified)

```
__init__.py  ──┬── pool.py ──────────┬── _rust.py ── _multiloop_core (lib.rs)
               ├── primitives.py ────┤
               ├── _sync.py ─────────┤
               ├── rwlock.py ────────┤
               ├── exceptions.py ────┤
               ├── _metrics.py ──────┤
               ├── _cancel.py ───────┘
               ├── _taskgroup.py
               ├── context.py
               ├── _options.py
               └── _logging.py
```

## 4.1 Mandatory reading before touching concurrency code

Any change touching channels, locks, semaphores, events, conditions, barriers,
waitgroups, cancellation, or the pool **must** pass the checklist in
[`docs/CONCURRENCY.md`](docs/CONCURRENCY.md) (sections 1-3). Hard rules that
hold regardless:

1. **Read `docs/CONCURRENCY.md` first** — the nine trap patterns and the
   pre-change checklist are the operating manual; the seven design decisions
   below are the "why" summary.
2. New or modified waiter registration/wakeup paths need a fast
   path **plus an atomic re-check under the lock** (lost-wakeup is the #1 bug class
   here).
3. Cancellation paths re-acquiring locks must run under a cancellation
   shield or cancellation-absorbing retry loop; waiter removal must carry
   generation/`WaiterId` ticket checks.
4. New public APIs must update `docs/API.md`, `__init__.pyi`, and (for new
   features) an `examples/` script — `tests/test_docs.py` enforces the first
   two.
5. Concurrency behavior changes must be stress-tested with
   `pytest-repeat --count=50` on the **specific target test file or function**,
   never across the entire test suite or multiple socket-heavy files at once.
6. **Bugs require a minimal reproduction test first** (deterministic data,
   stable FAIL, `--count=50` on that target test to confirm reproduction rate);
   a fix is not done until that test passes stably.
7. **GitHub Actions is for CI, not debugging** — no debug commits, no
   temporary diagnostic workflows, no remote-runner bisecting. If code needs
   debugging on another platform's machine, ask the user to arrange access
   first; never push to trigger remote runs without explicit permission.
8. **Request before code, green before merge** — file an issue/request and
   wait for approval before writing non-trivial code; a PR merges only after
   all required CI checks pass. Exception: docs-only changes (`*.md`,
   `docs/**`) skip the heavy matrix — `Docs Drift Check` runs
   `tests/test_docs.py` and the project lead reviews the diff before merge.

---

## 5. Architecture Highlights

### Scheduler: round-robin with work-stealing

Unpinned tasks land on a single global unbounded flume queue (the "global
queue"). Every worker thread runs an independent asyncio event loop and pulls
work via `NativeWorkerPool::pop_work()` with this priority order:

1. **Private buffer** (per-worker `VecDeque` — cached from previous batch pulls)
2. **Global queue** (batch-pull under a soft poller gate)
3. **Local dedicated channel** (for loop-pinned tasks)
4. **Single-item fallback & cross-worker buffer work-stealing** (if skipped by the soft poller gate, or stealing up to half of a sibling worker's cached buffer)

The soft poller gate (`num_polling < max(num_workers / 2, 1)`) limits
concurrent global-queue pollers to ~half the workers, preventing contention
while allowing any idle worker to steal. Batch size is
`min(global_len / num_workers + 1, 128)` for fair-share distribution.

On the Python side, `_worker_dispatcher` calls `await asyncio.sleep(0)` between
pops so no single worker greedily drains the global queue.

### Rust core components

| Component | Source file | Crate | Purpose |
|---|---|---|---|
| `WaiterId` / `WaiterEntry` | `src/waiter.rs` | PyO3 | Monotonic `u64` waiter tickets and out-of-lock `call_soon_threadsafe` wakeup/cancel helpers (`wake_waiter_outside_lock`, `cancel_waiter_outside_lock`, `wake_pair_outside_lock`) with closed-loop rollback detection |
| `RawTaskCancelState` | `src/cancel.rs` | parking_lot + PyO3 | Per-task cancel stack (`push_scope`, `pop_scope_status`, `park_waiter`, `unpark_waiter`, `cancel_scope`, `set_shield`, `set_deadline`) with `AtomicBool` / `AtomicU64` fast-read caches — zero `task.cancelling()` mutation |
| `errors` | `src/errors.rs` | PyO3 | Typed exception builders mapping directly to `multiloop.exceptions` (`WouldBlock`, `ChannelClosedError`, `ThreadPoolClosedError`, `TimeoutError`) |
| `timeout` | `src/timeout.rs` | PyO3 | Panic-free floating-point timeout validation and monotonic `Instant` deadline calculation (`NaN`/`-inf` -> `ValueError`, `+inf` -> unbounded, finite capped at 10 years) |
| `NativeWorkerPool` | `src/pool.rs` | PyO3 + flume + parking_lot | Lock-free `flume` global queue + per-worker unbounded local channels (`pin_to`), `Box<[AtomicU64]>` >64-worker CAS idle bitmap, `outstanding_tasks` / `wait_drained` / `drain_all`, and 4-tier `pop_work` |
| `RawAsyncChannel` | `src/channel.rs` | parking_lot + PyO3 | Native async channel core with `WaiterId` tickets, anti-barging FIFO ordering protection, and out-of-lock wakeups |
| `RawAsyncLock`, `RawAsyncSemaphore`, `RawAsyncCapacityLimiter`, `RawAsyncEvent`, `RawAsyncCondition`, `RawAsyncBarrier`, `RawAsyncOnce` | `src/sync.rs` | parking_lot + PyO3 | Unified Rust synchronization state machines with `WaiterId` tickets, FIFO permit/notification conservation, and out-of-lock wakeups |
| `AtomicMetrics` | `src/metrics.rs` | `std::sync::atomic` + PyO3 | 64-byte-padded per-worker counters (`active`, `completed`, `global_pull_count`, `park_count`, `injection_queue_depth`, `remote_schedule_count`) — prevents false sharing |
| `RawAsyncWaitGroup` | `src/waitgroup.rs` | parking_lot + PyO3 | Go-style counter + `WaiterId` ticket waiter list under `Mutex<WaitGroupInner>` with out-of-lock wakeups |
| `RawAsyncRWMutex` | `src/rwlock.rs` | parking_lot + PyO3 | Writer-preference async read-write lock under `Mutex<RWMutexInner>` with `WaiterId` tickets, reader task reentrancy, and cancellation cleanup |

### Python-side patterns

- **`_try_import_rust_class(module, name)`** (`_rust.py`): Defers `_multiloop_core`
  symbol lookup at module import time so docs, type checkers, and AST tools can inspect
  package metadata without a compiled extension. At runtime, `_multiloop_core` is
  required (no pure-Python fallback exists; constructing primitives without it raises
  `RuntimeError`).

- **`CancelScope` with pure Rust task cancel stack** (`_cancel.py`, `src/cancel.rs`):
  Pushes scope entries into the task's `RawTaskCancelState` (stored directly on the task's
  `_multiloop_cancel_state` attribute, with `_FALLBACK_STATES` `WeakKeyDictionary` only for
  slots-only task proxies). `multiloop` wait points (`_await_cancellable`, `sleep`,
  `wait_cancellable`, `Channel`, `Lock`, `TaskHandle`, etc.) subscribe to `park_waiter`
  without ever mutating CPython `task.cancel()`, `task.uncancel()`, or `task.cancelling()`.
  `shield=True` blocks outer `CancelScope` cancellation/deadlines cleanly, and `__aexit__`
  enforces strict `PLAIN` / `MOVE_ON` / `FAIL_AFTER` exit rules (`submit_many` accepts
  `cancel_scope` only when `len(targets) <= 1`).

- **`Condition` latch & cancellation forwarding** (`_sync.py`, `src/sync.rs`): `notify()`
  does NOT require the underlying lock. Waiters register `WaiterId` tickets in
  `RawAsyncCondition` before releasing `Lock`, then re-acquire `Lock` via a
  cancellation-absorbing retry loop (`_reacquire_lock`), conserving notification
  tokens on cancellation via `_raw.cancel_or_forward()` and `_raw.forward_notify()`.

### Seven non-obvious design decisions

These are the places where a change that looks like a simplification usually
breaks a real correctness or performance property. **Read each before modifying
the related code.**

1. **Anti-barging FIFO token conservation in `Channel`** (`src/channel.rs:243-336, 615-786`, `src/multiloop/primitives.py:209-239`):
   `RawAsyncChannel` enforces strict FIFO ordering so external `try_send` cannot barge ahead
   of queued putters. Woken putters hold reservations via `in_flight_putters` until `claim_put`.
   If a woken putter is cancelled, `cancel_putter` automatically forwards
   the reservation to the next queued putter or removes it from `in_flight_putters`, conserving tokens and preventing deadlocks.

2. **Pure Rust task cancel stack & three-mode exit** (`src/multiloop/_cancel.py`, `src/cancel.rs`):
   `CancelScope` never mutates CPython `task.cancel()`/`uncancel()`/`cancelling()`, eliminating
   cross-ledger drift and shield count leaks. Waiters subscribe via `_await_cancellable` +
   `park_waiter`/`unpark_waiter`; foreign coroutines bridge via `wait_cancellable(...)` and
   cancellable waits use `multiloop.sleep(...)`. Exiting `__aexit__` strictly enforces `PLAIN`
   (raises `CancelledError` if cancelled), `MOVE_ON` (`cancelled_caught = True`), and `FAIL_AFTER`
   (raises `TimeoutError`) while propagating external `task.cancel()` (`task.cancelling() > enter_cancelling`)
   instead of swallowing or converting it. `EventLoopThreadPool.submit_many(..., cancel_scope=...)`
   accepts a scope only for single-target batches (`len <= 1`, raising `ValueError` otherwise).

3. **Condition notification forwarding on re-acquire cancellation** (`src/multiloop/_sync.py:463-499, 529-539`, `src/sync.rs:768-785`):
   When a `wait()` coroutine is cancelled while awaiting its future, `_raw.cancel_or_forward(ticket_id)`
   forwards the notification if `notify()` had already popped it. When awakened normally and cancelled
   while re-acquiring `Lock` in `_reacquire_lock()`, the retry loop finishes acquiring `Lock` before
   re-raising `CancelledError`, and `wait()` calls `_raw.forward_notify()` so subsequent waiters are not starved.

4. **Barrier automatic Broken state machine & generation tracking** (`src/multiloop/_sync.py`, `src/sync.rs`):
   A monotonic `generation` counter in Rust `RawAsyncBarrier` ties every waiter to the exact round it joined.
   On cancellation of any waiting party before round completion, `cancel_waiter(generation, ticket_id)`
   automatically marks the barrier broken, wakes all remaining waiting parties with an error outside the lock,
   and advances `generation`, preventing deadlocks without requiring manual abort.

5. **Condition latch** (`src/multiloop/_sync.py:463-539`, `src/sync.rs:711-800`):
   `notify()` does not require the lock, avoiding the notifier-blocks-while-sleeper-sleeps deadlock.
   Registering a `WaiterId` ticket in `RawAsyncCondition` (`_raw.register_waiter`) before releasing `Lock`
   — paired with `notified_tickets` tracking — closes the lost-notification window.

6. **Work-stealing with the `num_polling` gate** (`src/pool.rs:25-31, 275-285`):
   A soft atomic gate (`num_polling < max(num_workers/2, 1)`) limits
   concurrent global-queue pollers. The gate is momentary (acquired per
   `pop_work` call, released immediately via `PollerGuard`), so any worker can
   win it — no hard role assignments that drift with load.

7. **Rust global queue and 4-tier `pop_work` with multi-source drain** (`src/pool.rs:58, 156-171, 223-254, 256-391`):
   Four-tier consumption: private buffer → global batch pull → local channel → single-item fallback & cross-worker buffer work-stealing.
   Local channels are unbounded to guarantee 100% strict loop affinity for `pin_to`
   tasks without silent drift. Drain phase validates that the global queue, all worker
   buffers, and local channels are completely empty before worker loop termination.

---

## 6. Quick Commands

All commands assume you have run `make sync` (`uv sync --locked --group dev`) first.

### Build (required before first import)

```bash
make develop          # or: uv run maturin develop --release --locked
```

### Test

```bash
make test             # make test-rust + uv run --no-sync pytest
make test-rust        # cross-platform Rust unit tests via .github/scripts/run_rust_tests.py
make test-docs        # docs parity + code-block/example AST syntax guard
uv run --no-sync pytest -x -m "not slow"   # skip slow benchmarks
uv run --no-sync pytest --lf               # re-run last failures
```

### Lint & Type Check

```bash
make lint             # check-release + ruff (check/format) + mypy + pyright + cargo (clippy/fmt/doc)

# Individual:
uv run --no-sync ruff check .
uv run --no-sync ruff format --check .
MYPYPATH=src uv run --no-sync mypy --strict src/multiloop
uv run --no-sync pyright src/multiloop
cargo clippy --locked -- -D warnings
cargo fmt --check
```

### Format

```bash
make format           # ruff format . + cargo fmt
```

### Benchmarks

```bash
make bench
```

### Full pre-submission check (all gates)

```bash
make all              # develop + lint + test
```

---

## 7. Key Differences from CONTRIBUTING.md

`CONTRIBUTING.md` is the **human-oriented dev setup guide** (prerequisites,
step-by-step install, PR process, Code of Conduct). This `AGENTS.md` is the
**AI-oriented architecture and navigation guide** — it emphasizes module
layout, design decisions, tooling gates, and reading order. Both documents
reference the same commands and conventions, but serve different audiences.
