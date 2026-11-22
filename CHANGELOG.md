# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.5] - 2026-11-22

### Fixed, Hardened & Changed
- **FFI, GC & Timeout Memory Safety (`src/timeout.rs`, `src/channel.rs`, `src/pool.rs`, `src/sync.rs`, `src/cancel.rs`, `src/rwlock.rs`, `src/waitgroup.rs`, `src/multiloop/primitives.py`, `src/multiloop/context.py`)**:
  - Implemented PyO3 0.29 GC protocol (`__traverse__` using `try_lock()` and `__clear__` dropping extracted `Py<PyAny>` outside locks) across all 12 Python-object-holding `#[pyclass]` types.
  - Added `src/timeout.rs` for panic-free timeout validation and deadline computation (`NaN`/`-inf` -> `ValueError`, `+inf` -> unbounded wait, finite clamped to 10 years with `checked_add`), and protected `NativeWorkerPool::complete_task` against counter underflow.
  - Fixed `AsyncOnce.do` traceback reference cycle retention (`_clone_exception`, `__weakref__` slot, frame local cleanup) and custom multi-argument exception cloning; converted `AsyncContext._children` to `weakref.WeakSet`.
  - Tightened `CapacityLimiter` (`RawAsyncCapacityLimiter::new` and `set_total_tokens`) to reject non-finite `total_tokens` (`ValueError`).
- **Concurrency State-Machine & Lifecycle Fixes (`src/rwlock.rs`, `src/channel.rs`, `src/multiloop/pool.py`, `src/multiloop/_cancel.py`, `src/multiloop/_taskgroup.py`)**:
  - Unified `RawAsyncRWMutex` wakeup transitions (`prepare_wake_action` / `dispatch_wake_action`) so cancelling a queued writer while `ReadLocked` immediately admits and wakes queued `read_waiters`.
  - Enforced `occupied() < maxsize` capacity accounting across `RawAsyncChannel` send/claim paths.
  - Hardened `EventLoopThreadPool` lifecycle (`_PoolState` state machine, pre-transition `close(timeout=...)` validation, serialized concurrent `close()`/`abort()` callers, shared monotonic deadline in `_join_threads_sync`, two-stage stubborn task cancellation, unstarted `close()` blocking subsequent `start()`, and `move_on_after` completion handling).
  - Added monotonic `_deadline_gen` to `CancelScope._on_deadline_timer`, `RawTaskCancelState::pop_scope_status`, and `_enter_cancelling` baseline checks so `move_on_after` and `fail_after` never swallow or convert concurrent external `task.cancel()`.
  - Fixed `TaskGroup(pool=...)` external child `asyncio.Future` cancellation handling and cross-loop `TaskHandle.__await__` wakeup.
- **Hot-Path Optimization & API Alignment (`src/cancel.rs`, `src/sync.rs`, `src/multiloop/_cancel.py`, `src/multiloop/_sync.py`, `src/multiloop/_taskgroup.py`)**:
  - Replaced global `_STATE_LOCK` + `WeakKeyDictionary` contention on standard `asyncio.Task` instances with direct `_multiloop_cancel_state` task attributes and `AtomicBool`/`AtomicU64` fast-read caches on `RawTaskCancelState`.
  - Removed `LockState::AbandonedByDeadOwner` (transitioning dead owners directly to `Unlocked` when no waiters remain) and eliminated cross-thread `.done()` calls in `RawAsyncLock::owner`.
  - Aligned `BarrierWaitResult.index` / `is_leader` with `threading.Barrier` (`index = 0` for first arriver / `is_leader = True`, `parties - 1` for tripping last arriver) and added `RawAsyncBarrier::try_arrive` zero-Future fast path.
  - Updated `TaskHandle.exception` to raise `asyncio.CancelledError` on cancelled tasks, renamed and exported `TaskState` (`PENDING`, `STARTED`, `FINISHED`), removed internal shims (`_WaiterView`, `_LenShim`, `_waiters`, `_cancel_waiter`, `_getters`/`_putters`/`_notifiers`, `select_channel._deadline`, `_complete_waiter_future`, `skip_if_no_rust`), enabled `F821`/`RUF100` lint checks, and synchronized benchmarks and EN/ZH documentation.

## [0.1.4] - 2026-11-21

### Added & Changed
- **Rust Sync Primitives & Shared Waiter Infrastructure (`src/sync.rs`, `src/waiter.rs`, `src/errors.rs`)**:
  - Moved `Lock`, `Semaphore`, `CapacityLimiter`, `Event`, `Condition`, `Barrier`, and `AsyncOnce` state machines into Rust (`RawAsync*` pyclasses) with `WaiterId` generation tickets, out-of-lock wakeups/cancellations, FIFO token forwarding, and automatic broken-state propagation on `Barrier`.
  - Consolidated Python sync wrappers in `src/multiloop/_sync.py` and typed exception builders in `src/errors.rs`.
  - Removed legacy `src/multiloop/_channel_base.py` and streamlined Python `Channel`/`select_channel` over `RawAsyncChannel` with anti-barging FIFO ordering.
- **Route 3 Pure-Rust Cancellation Stack (`src/cancel.rs`, `src/multiloop/_cancel.py`)**:
  - Replaced CPython `task.cancel()`/`uncancel()`/`cancelling()` ledger mutation with `RawTaskCancelState` (`_TASK_CANCEL_STATES` weak-key tracking), strict `PLAIN` / `MOVE_ON` / `FAIL_AFTER` exit modes, `shield` isolation, `sleep`, `wait_cancellable`, and `checkpoint`.
  - Hardened `run_in_pool()`, `TaskGroup`, `TaskHandle._await_impl()`, `AsyncContext`, and all primitive wait points for Route 3 cancellation parity and FFI/GC safety.
- **Free-Threaded CI/CD & Developer Workflow Unification (`.github/`, `Makefile`)**:
  - Converged CI (`ci.yml`) and PyPI distribution (`publish.yml`) onto OS-grouped runners (`ubuntu-latest`, `macos-latest`, `windows-latest`) testing & packaging free-threaded Python (`3.14t`, `3.15t`) via shared composite actions (`setup-env`, `run-lint`, `build-dist`) and cross-platform scripts (`run_rust_tests.py`, `run_ci_versions.py`, `build_host_wheels.py`, `verify_dist.py`, `check_release.py`).
  - Added `test_sdist` build/install checks across target Python versions, fixed `test_wheels` `--no-sync` & `site-packages` smoke assertions, tightened `release-drafter.yml`, and aligned `Makefile`, `.pre-commit-config.yaml`, `CONTRIBUTING.md`, `AGENTS.md`, and `tests/test_docs.py`.

## [0.1.3] - 2026-09-17

### Added & Optimized
- **Cross-Loop Structured Concurrency (`TaskGroup` & `AsyncContext`)**:
  - `TaskGroup(pool=...)` multi-loop task distribution with isolated per-task child `CancelScope` instances.
  - Remote drain barrier on context exit, bidirectional cascading cancellation, and unified `ExceptionGroup` aggregation.
  - Unified `AsyncContext.task_group(pool=...)` lifecycle tracking and automatic cascading cancellation.
- **Native Synchronous Channel Communication (`Channel.send_sync` & `Channel.recv_sync`)**:
  - Replaced busy-spin polling with native `parking_lot::Condvar` wait/notify in `RawAsyncChannel`.
  - Releases the Python GIL via `py.detach()` during blocking waits, eliminating CPU waste.
  - Added `ReceiveChannel.recv_sync(timeout=None)` to segregation views.
- **Concurrency & Free-Threaded (Python 3.14t) Hardening**:
  - `AsyncRWMutex`: Protected reentrancy depth and writer task with `self._depth_lock = threading.Lock()`, with out-of-lock `_wake_all()` execution to eliminate deadlocks.
  - `EventLoopThreadPool`: Implemented condition-variable drain barrier `self._drain_barrier` and event-driven worker dispatcher.
  - `AtomicMetrics`: Added `add_global_pull` batch counter accumulation and upgraded pool metrics to `RwLock`.
  - `_channel_base.py`: Enforced $O(1)$ waiter unregistration via `OrderedDict.pop()`.
  - `AsyncWaitGroup`: Removed GC finalizer `self._wg.done()` invocation in `_TrackedCoroutine.__del__` to prevent cross-thread re-entrancy and deadlocks under multi-threaded GC.
  - Standardized cross-language `ThreadPoolClosedError` exception hierarchy.

## [0.1.2] - 2026-08-21

### Added & Optimized
- **wrk High-Concurrency Multi-Core Benchmark Suite (`benchmarks/bench_wrk_asgi.py`)**:
  - Production-grade wrk benchmark suite for physical multi-core scaling under Python 3.14t No-GIL.
  - Achieves **151,366+ QPS** (4-Worker) on `/api/plaintext` and **145,836+ QPS** on `/api/ping` with sub-millisecond latency (0.29ms avg).
- **Rust SIMD HTTP Sliced Streaming & Zero-Allocation Fast-Path (`src/http.rs`, `src/multiloop/_http11.py`)**:
  - `FastHttpConnection` streaming body sliced event dispatch (`ReceivingContentLength`) eliminating cursor rewind and $O(N^2)$ header re-parsing on split TCP packets.
  - $O(1)$ buffer compaction via `compact_buffer` clearing on cursor drain without memory copies.
  - Fast-path for zero-body GET/HEAD requests with module-level singleton `_EMPTY_BODY_MSG`, completely eliminating `asyncio.Queue` heap allocation and event loop dispatch overhead.
- **ASGI Transport Backpressure & Pipelining Concurrency Protection (`src/multiloop/_http11.py`, `src/multiloop/_websocket.py`)**:
  - Physical write backpressure flow control (`_drain_event`) in `Http11Protocol.send()` eliminating server OOM under slow clients.
  - Serial HTTP Pipelining request queueing (`_pending_pipeline_events`) guaranteeing strictly ordered, non-interleaved TCP responses.
  - RFC 6455 WebSocket thread-safe send frame lock synchronization (`_send_lock`), eliminating `RuntimeError: concurrent drain calls` on high-concurrency data/control frame writes.
- **WSGI True Streaming Channel & Client Disconnect Decoupling (`src/multiloop/wsgi.py`)**:
  - `SyncStreamReader` backed by lock-free cross-thread `Channel(maxsize=16)`, eliminating the 10MB pre-buffering requirement and enabling true streaming request body consumption.
  - Automatic channel closure and feeder cancellation upon client disconnection, preventing orphaned synchronous WSGI worker tasks from occupying the thread pool.
- **No-GIL Task Collection Thread Safety & Adaptive CLI Watcher (`src/multiloop/server.py`, `src/multiloop/cli.py`)**:
  - `ConnectionPinningServer` multi-worker task collection mutex protection (`_tasks_lock`), eliminating `RuntimeError: Set changed size during iteration` during `server.close()` under Python 3.14t physical multi-core execution.
  - CLI file watcher with extended ignored directories and adaptive smooth polling backoff (0.3s~0.8s), reducing idle CPU usage while maintaining fast hot-reload responsiveness.

## [0.1.1] - 2026-08-18

### Added & Optimized
- **Pure-Python Cross-Platform ASGI Benchmark Engine (`benchmarks/bench_asgi_throughput.py`)**:
  - Implemented high-performance multi-threaded HTTP/1.1 Keep-Alive raw socket load generator.
  - 100% native on Windows, macOS, and Linux with zero external command dependencies (eliminated `ab` requirement).
  - Achieves 70,000+ QPS with multi-core physical scaling across worker event loops.
- **Rust SIMD FastHttpParser & Zero-Allocation Assembly (`src/http.rs`)**:
  - Direct ASGI `PyList[(PyBytes, PyBytes)]` tuple assembly in Rust C-layer, eliminating 20+ intermediate small-object allocations per HTTP request.
  - Global `OnceLock<InternedMethods>` static interning for common HTTP verbs (`GET`, `POST`, `PUT`, `DELETE`, `PATCH`, `HEAD`, `OPTIONS`) and protocol versions (`1.0`, `1.1`, `2.0`), enabling zero-allocation method resolution across worker threads.
- **Rust SIMD RFC 6455 WebSocket Unmasking (`src/websocket.rs`, `src/multiloop/_websocket.py`)**:
  - 64-bit auto-vectorized chunk XOR unmasking with `PyBytes::new_with` single-pass memory safety under Python 3.14t No-GIL.
  - Zero-copy WebSocket frame header parser (`FastWebSocketParser::parse_frame_header`).
- **Rust `RawAsyncRWMutex` 64-Bit Atomic Core (`src/rwlock.rs`, `src/multiloop/rwlock.py`)**:
  - 64-bit atomic state machine with writer-preference fairness.
  - Cancellation token conservation and automatic waiter cleanup (`remove_waiter`), preventing cross-language GC reference leaks and writer-cancellation reader starvation.
- **Platform-Aware Multi-Core Connection Balancing (`src/multiloop/server.py`)**:
  - Linux: Symmetric kernel-level `SO_REUSEPORT` 4-tuple BPF hash balancing across worker event loops.
  - macOS / BSD: Multi-worker shared-socket racing accept (`loop.sock_accept(shared_sock)`) achieving perfectly balanced 25% traffic distribution per worker and 91,650+ QPS.
- **ASGI & FastAPI High-Throughput Benchmarks (`benchmarks/bench_asgi_throughput.py`)**:
  - Real-world POST payload throughput increased from 34.1k to **83,168 req/s (+143% speedup)**.
  - Ultra-lightweight JSON Ping stabilized at **91,125 ~ 91,653 req/s**.
  - Multi-core CPU task dispatch achieving **3.65x physical speedup** on 4 workers (91.3% of theoretical limit).

## [0.1.0] - 2026-08-18

### Added
- **Multi-Event-Loop Engine (`EventLoopThreadPool`)**:
  - Independent `asyncio` event loop running per worker OS thread under Python 3.14t free-threading (no GIL).
  - Native Rust work-stealing shared queue architecture (`NativeWorkerPool`) with lock-free global flume queue and per-worker local channels.
  - Soft poller gate (`num_polling < max(num_workers / 2, 1)`) and batch task pulling (`min(global_len / num_workers + 1, 128)`) to eliminate thundering herd contention.
  - ContextVars preservation across thread hops (`contextvars.copy_context()`).
- **Go-Style Concurrency Primitives**:
  - **`Channel`**: Lock-free channel backed by Rust core (`RawAsyncChannel`), supporting bounded/unbounded buffering, non-blocking `try_send`/`try_recv`, synchronous worker `send_sync`, and double-checked waiter lists.
  - **`select_channel`**: Non-blocking randomized probe and single-future multi-registration arbiter for $O(1)$ first-ready channel selection.
  - **`AsyncWaitGroup`**: Go-style atomic counter with generation tracking and leak-safe `track()`, `wrap()`, and `holding()` RAII context managers.
  - **`AsyncOnce`**: Thread-safe single initialization runner across multi-loop environments.
  - **`AsyncContext`**: Hierarchical cancellation context modeled after Go's `context.Context`.
- **Structured Concurrency**:
  - **`TaskGroup`**: Nursery-style structured concurrency manager with automatic sibling cancellation on failure and non-blocking `start_soon` / synchronized `start`.
  - **`CancelScope`**: Task-local cancellation scope with monotonic deadlines, shield snapshot/restore mechanics, and `fail_after`/`move_on_after` helpers.
- **Cross-Loop Synchronization Toolkit**:
  - **`Lock`**: Fair FIFO async mutex bound to owning tasks.
  - **`Semaphore`**: Fair FIFO async semaphore with token-conservative cancellation forwarding.
  - **`CapacityLimiter`**: Dynamically resizable token capacity limiter with single-lock atomic state transitions.
  - **`Event`**: One-shot cross-thread event with permanent set state.
  - **`Condition`**: Async condition variable atop `Lock` with lock-free `notify()` to prevent notifier-sleeper deadlocks.
  - **`Barrier`**: Synchronized N-party barrier with automatic round advancement and generation tracking.
  - **`AsyncRWMutex`**: Asynchronous read-write lock with writer-preference fairness.
- **High-Performance Web Server Adapters & CLI**:
  - **`ConnectionPinningServer`**: TCP server with Linux kernel-level `SO_REUSEPORT` 4-tuple hashing load balancing and connection affinity.
  - **`MultiloopASGIWorker`**: ASGI 3.0 server adapter with Lifespan protocol, HTTP/1.1 chunked streaming, RFC 6455 WebSockets, and HTTP/2 Tokio runtime bridge (`_multiloop_core`).
  - **`MultiloopWSGIWorker`**: WSGI 1.0.1 (PEP 3333) synchronous worker adapter for Flask and Django.
  - **CLI Runner**: `multiloop run <module:app>` command with auto-reload and configurable worker counts.
- **Telemetry**:
  - **`AtomicMetrics`**: 64-byte padded atomic counters preventing CPU cache-line false sharing.
