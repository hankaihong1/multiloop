# multiloop Concurrency Correctness Guide

**[中文版 (Chinese)](CONCURRENCY_ZH.md)**

> **Read before touching any concurrency-related code.** This guide serves
> two audiences: AI agents (walk through the Section 3 checklist before
> writing code) and human contributors (understand each primitive's
> concurrency model before editing). Every claim cites its source location —
> verify by jumping there.

---

## 1. Concurrency Model Overview (who locks what, who waits for what)

All multiloop synchronization primitives follow the same skeleton: **the Rust
core (`_multiloop_core`) owns 100% of cross-thread shared mutable state under
`parking_lot::Mutex`, while Python acts as a thin async facade**. Waiters are
registered with monotonic `WaiterId (u64)` tickets and opaque `(loop_obj, fut)`
handles (`src/waiter.rs`), and cross-thread wakeups always execute **outside**
the Rust mutex via `loop.call_soon_threadsafe(...)` with automatic token/wakeup
rollback if the target loop is already closed.

### 1.0 Physical Concurrency Axioms (Axiomatic Assumptions)

multiloop operates under **Python 3.14t (free-threaded / no-GIL) true multi-core physical parallelism** across multiple OS threads and isolated event loops. Correctness cannot rely on single-threaded heuristics; it rests on six foundational physical concurrency axioms:

1. **Python 3.14t Free-Threaded True Parallelism**: Bytecode and object operations execute concurrently across CPU cores without a GIL. Any composite cross-thread state transitions (e.g. counter updates + waiter notifications) are enclosed within a single Rust `parking_lot::Mutex` to prevent state tearing and split-brain locking.
2. **Cross-Thread EventLoop Isolation & Opaque Handles**: `asyncio.Task`, `Future`, `Event`, and `TimerHandle` are strictly bound to a single thread and its event loop. Rust never inspects `.done()` or invokes Python methods while holding a `parking_lot::Mutex`; all cross-thread wakeups go through `loop.call_soon_threadsafe(...)` outside the lock.
3. **ContextVar Thread/Task Locality**: `contextvars.ContextVar` is strictly local to the active OS thread and task. Cross-thread cancellation (`scope.cancel()`) never reads or modifies `ContextVar` state; it relies exclusively on thread-safe locks and `call_soon_threadsafe`.
4. **Pure Rust Task Cancel Stack (`RawTaskCancelState`)**: `CancelScope`, `fail_after`/`fail_at`, `move_on_after`/`move_on_at`, `sleep`, `wait_cancellable`, and `checkpoint` maintain a per-task Rust `RawTaskCancelState` cancel stack (stored directly on the task via `_multiloop_cancel_state`, with `_FALLBACK_STATES` `WeakKeyDictionary` only for slots-only task proxies) without ever calling `task.cancel()`, `task.uncancel()`, or reading/mutating `task.cancelling()`. Wait points subscribe via `_await_cancellable` (`park_waiter` / `unpark_waiter`), foreign coroutines bridge via `wait_cancellable()`, `shield=True` blocks outer `CancelScope` deadlines/cancellations, and `__aexit__` strictly enforces the three exit modes (`PLAIN` raises `CancelledError`, `MOVE_ON` absorbs with `cancelled_caught=True`, `FAIL_AFTER` converts to `TimeoutError`).
5. **Unified Ticket Waiter Protocol & Anti-Barging FIFO**: Every Rust primitive assigns a monotonic `WaiterId (u64)` ticket (`src/waiter.rs`) instead of raw pointer addresses (`fut.as_ptr()`), eliminating ABA pointer reuse hazards. Channels (`RawAsyncChannel`) enforce anti-barging FIFO ordering and token conservation (`in_flight_putters` reservations) across event loops.
6. **Structured Concurrency Physical Scope**: A `TaskGroup`'s host context and task-spawning calls (`start_soon` / `start`) are physically scoped to its host `asyncio.AbstractEventLoop` and linked to a host `CancelScope` so child failures or `cancel_all()` immediately interrupt a blocked `async with TaskGroup()` body. When constructed with `TaskGroup(pool=pool)`, child tasks are dispatched across `EventLoopThreadPool` worker loops with per-child `CancelScope`s and shielded remote drain on exit; cross-loop messaging and tree cancellation also coordinate via `Channel` and `AsyncContext`.

### 1.1 Channels (Channel)

| Component | Lock / primitive | Waiter structure | Key invariant |
|---|---|---|---|
| Rust `RawAsyncChannel` | `parking_lot::Mutex<ChannelState>` | `getters` / `putters` / `select_watchers` `VecDeque<WaiterEntry>` | Anti-barging FIFO ordering with `WaiterId (u64)` tickets: `try_send` rejects if putters are queued or buffer is full; woken putters hold reservations via `in_flight_putters` until `claim_put`. Out-of-lock wakeup via `call_soon_threadsafe` with rollback if target loop is closed. (`src/channel.rs`) |
| Python `Channel` | Thin facade over `RawAsyncChannel` | — | Direct forwarding to Rust core without split-brain dual tracks; supports `select_channel` arbiter registration. (`src/multiloop/primitives.py`) |
| Wakeup protocol | — | — | Waiters are popped FIFO under the lock and woken outside the lock via `wake_waiter_outside_lock`. Woken waiters cancelled before claim (`cancel_putter` / `cancel_getter`, or on a closed loop) forward tokens to preserve liveness. (`src/waiter.rs`, `src/channel.rs`) |

**Unified single-track architecture**: All data buffering and waiter queuing are unified in the Rust `RawAsyncChannel` core. `send`/`recv` follow
「try fast path → acquire/register future with `WaiterId` under lock (`send_or_register` / `register_getter_ticket`) → await → cancel by `WaiterId` on cancellation (`cancel_putter` / `cancel_getter`) → claim reservation (`claim_put` / `claim_get`)」.

### 1.2 Locks & Semaphores

| Primitive | Guarding lock | Waiter structure | Key invariant |
|---|---|---|---|
| `Lock` | Rust: `parking_lot::Mutex<LockInner>` (`RawAsyncLock`) | `VecDeque<LockWaiter>` (`WaiterId` tickets) | Strict FIFO; non-blocking `try_acquire()` + `acquire_or_register() -> Option<WaiterId>` + `cancel_waiter(task_id, ticket_id)` (forwarding ownership if already granted) + same-loop dead-owner reaping (`check_owner_same_loop` / `on_owner_task_done`); `release()` pops the next waiter under the lock and wakes outside the lock, rolling back and advancing if the target loop is closed (`src/sync.rs`, `_sync.py`) |
| `AsyncRWMutex` | Rust: `parking_lot::Mutex<RWMutexInner>` (`RawAsyncRWMutex`) | `read_waiters` / `write_waiters` `VecDeque<RWMutexWaiter>` (`WaiterId` tickets) + `woken_readers` | Writer-preference (`pending_writers` blocks new readers) with per-task reader reentrancy (`reader_depths`); `acquire_read_ticket()` / `cancel_read_ticket()` / `release_read_task()` and `acquire_write_ticket()` / `cancel_write_ticket()` / `release_write_task()`, with out-of-lock wakeups and closed-loop rollback (`src/rwlock.rs`, `rwlock.py`) |
| `Semaphore` | Rust: `parking_lot::Mutex<SemaphoreInner>` (`RawAsyncSemaphore`) | `VecDeque<WaiterEntry>` (`WaiterId` tickets) + `granted_tickets` | Strict FIFO; `acquire_or_register()` + `claim_acquire(ticket_id)` + `cancel_waiter(ticket_id)`; if a cancelled waiter was already granted a permit by `release()`, `cancel_waiter(ticket_id)` **forwards** its permit to the next waiter or returns it to the pool (`src/sync.rs`, `_sync.py`) |
| `CapacityLimiter` | Rust: `parking_lot::Mutex<CapacityLimiterInner>` (`RawAsyncCapacityLimiter`) | `VecDeque<WaiterEntry>` (`WaiterId` tickets) + `granted_tickets` | Single-lock atomic model in Rust: `acquire_or_register()`, `claim_acquire()`, `cancel_waiter()`, and `snapshot()` (`available == total_tokens - borrowed`, with dynamic shrink deficit absorption) under `parking_lot::Mutex`; out-of-lock wakeups with closed-loop rollback (`src/sync.rs`, `_sync.py`) |

### 1.3 Events & Conditions

| Primitive | Guarding lock | Waiter structure | Key invariant |
|---|---|---|---|
| `Event` | Rust: `parking_lot::Mutex<EventInner>` (`RawAsyncEvent`) | `Vec<WaiterEntry>` (`WaiterId` tickets) | **sticky** (set once, irreversible; no clear); `wait_or_register()` + `cancel_waiter()`; `set()` drains the waiter list under the lock, then wakes each waiter via `call_soon_threadsafe` outside the lock (`src/sync.rs`, `_sync.py`) |
| `Condition` | Rust: `parking_lot::Mutex<ConditionInner>` (`RawAsyncCondition`) + underlying `Lock` | `VecDeque<WaiterEntry>` (`WaiterId` tickets) + `notified_tickets` | `notify()` does **not** require the underlying lock; `wait()` registers a `WaiterId` ticket (`register_waiter()`) before releasing `Lock` → awaits notification (`cancel_or_forward()` on cancel, `claim_waiter()` on wake) → **re-acquires `Lock` via `_reacquire_lock()` cancellation-absorbing retry loop and forwards notification via `forward_notify()` if cancelled during re-acquisition** (`src/sync.rs`, `_sync.py`) |

### 1.4 Barriers & Group Sync

| Primitive | Guarding lock | Waiter structure | Key invariant |
|---|---|---|---|
| `Barrier` | Rust: `parking_lot::Mutex<BarrierInner>` (`RawAsyncBarrier`) | `VecDeque<BarrierWaiter>` (`WaiterId` tickets) | `generation` counter binds each waiter to its round via `arrive_or_register()`; on cancellation of any party before round completion, `cancel_waiter(generation, ticket_id)` automatically transitions the barrier into Broken state, wakes all remaining parties with an error outside the lock, and advances `generation` (`src/sync.rs`, `_sync.py`) |
| `AsyncWaitGroup` | Rust: `parking_lot::Mutex<WaitGroupInner>` (`RawAsyncWaitGroup`) | `Vec<WaiterEntry>` (`WaiterId` tickets) | Single mutex guards `next_waiter_id`, `counter`, and `waiters` (monotonic `WaiterId` tickets prevent ABA without a `generation` field); `add(-n)` and `done()` to zero atomically drain `waiters` under the lock and wake outside the lock via `wake_waiter_outside_lock` (`src/waitgroup.rs`, `src/waiter.rs`, `primitives.py`); `register_waiter_ticket()` returns `Option<WaiterId>` (`None` if already zero) and `cancel_waiter_ticket(ticket_id)` removes cancelled waiters |
| `AsyncOnce` | Rust: `parking_lot::Mutex<OnceInner>` (`RawAsyncOnce`) | `Vec<WaiterEntry>` (`WaiterId` tickets) | Single 4-status state machine (`done` / `executing` / `result` / `exc`) in Rust: `enter_or_register()` elects one leader (`status = 0`), registers followers with `WaiterId` (`status = 1`, cancelled via `cancel_waiter()`), or returns cached outcome (`status = 2` / `3`); `complete(result, exc)` transitions state and wakes followers outside the lock, who read the result via `outcome()` (`src/sync.rs`, `primitives.py`) |

### 1.5 Cancellation & Structured Concurrency

| Component | Mechanism | Key point |
|---|---|---|
| `CancelScope` | Pure Rust `RawTaskCancelState` stack (direct `_multiloop_cancel_state` task attribute, with `_FALLBACK_STATES` `WeakKeyDictionary` only for slots-only task proxies) + `_await_cancellable` (`park_waiter` / `unpark_waiter`) | zero mutation of CPython `task.cancel()` / `uncancel()` / `cancelling()`; `shield=True` blocks outer `CancelScope` entries while leaving external CPython `task.cancelling()` untouched; strict three-mode exit (`PLAIN` / `MOVE_ON` / `FAIL_AFTER`); `sleep()` & `wait_cancellable()` provide cancellable suspension (`_cancel.py`, `src/cancel.rs`) |
| `TaskGroup` | `_TGState` enum (`IDLE`..`FAILED`) + `_CancelReason` enum + `_children_lock` (`threading.Lock`) + per-child `CancelScope`s (local & pool) + `_wake_fut` (`_await_cancellable`) | host context and spawning calls are bound to `self._loop`; unified per-child `CancelScope` cancellation for both local and `pool=pool` children; shielded `_wait_children` drain with `CancelScope(shield=True)` + external CPython `host_task.cancelling()` preservation (`_taskgroup.py`) |
| `select_channel` | 2-phase arbiter (Phase 1 fast `try_recv` with round-robin start index, Phase 2 multi-channel registration with `WaiterId` tickets & `weakref.ref(ch)` via `register_select_watcher_ticket`) | readiness is reported via `_await_cancellable(loop, arbiter_fut)` without zero-cycle leaks; winner consumes via `try_recv()` and unregisters all `WaiterId` watcher tickets via `cancel_select_watcher` in `finally` (`primitives.py`, `src/channel.rs`) |

### 1.6 Multi-Core Scheduling & Pool Concurrency Model

| Component | Architecture Role | Threading / State Guard | Key Invariant |
|---|---|---|---|
| Rust `NativeWorkerPool` | Lock-Free Task Queue & Work Stealing | Flume global + per-worker queues + per-worker `Mutex<VecDeque>` buffers + `Box<[AtomicU64]>` idle bitmap + `outstanding_tasks` drain condvar | Soft poller gate (`num_polling < max(workers/2, 1)`) limits concurrent global queue pollers; >64-worker CAS idle bitmap; 4-tier consumption + `wait_drained` / `drain_all` guarantees zero task starvation and complete queue drain (`src/pool.rs`). |
| Python `EventLoopThreadPool` | Multi-Loop Coordinator | Immutable `_loops_snapshot` / `_notify_events_snapshot` + `threading.Lock` (`_lifecycle_lock`) + `_PoolWorkItem` (`weakref.ref(pool)`) | Lock-free submit hot path (`itertools.count()` + atomic metrics/native queues); worker-local `active_items` + `drain_all()` cleanup; `submit_many` rejects `len > 1` with `cancel_scope is not None` (`ValueError`) (`src/multiloop/pool.py`). |
| Python `AsyncContext` | Cross-Thread Tree Cancellation | `threading.Lock` + loop-safe `CancelScope` (`_scopes`) / child / `WeakSet[TaskGroup]` dispatch | Cascades cancellation across child `AsyncContext`s, linked `TaskGroup`s (`_cancel_from_context()`), and worker `CancelScope`s (`src/multiloop/context.py`). |

---

## 2. Known Trap Patterns (Race-Condition Trap Patterns)

For each pattern: symptom → root cause → correct approach → source example.
Check your change against these, then walk the Section 3 checklist.

### Pattern 1: check-then-act window

**Symptom**: the two steps of "check then act" are mutated concurrently, so
the operation fails or reads stale state.
**Root cause**: the check and the act are not atomic.
**Correct approach**: merge into a single locked operation; when that is
impossible, fall back with try/except.
**Examples**:
- `CapacityLimiter` (`RawAsyncCapacityLimiter`) holds all token, borrowed, and waiter state under a single `parking_lot::Mutex`; reading `snapshot()` atomically yields `(total, avail, borrowed)` (`src/sync.rs`, `_sync.py`).
- General lesson (fixed elsewhere): `contains_key → getitem` can lose the key
  to a concurrent delete → split the two steps + `try/except KeyError`.

### Pattern 2: double-checked lock lost-wakeup

**Symptom**: a sender enqueues and wakes a receiver that "no longer exists",
or a receiver registers its future after the data was already taken — the
future is never resolved → hang.
**Root cause**: the non-blocking fast path (`try_send`/`try_recv` or `is_set()`/`is_zero()`) and the
locked waiter registration are two separate steps; when a concurrent state transition wins
between the fast path and registration, the wakeup signal is lost.
**Correct approach**: after the fast-path attempt, **re-check under the
`parking_lot::Mutex`** before registering the future; registration and wakeup extraction both
happen under the same `parking_lot::Mutex`, and `call_soon_threadsafe` is invoked outside the lock.
**Examples**: `RawAsyncChannel::send_or_register` / `register_getter_ticket` (`src/channel.rs`),
`Channel.send` / `Channel.recv` (`primitives.py`).

### Pattern 3: lock re-acquisition on the cancellation path (missing shield → deadlock)

**Symptom**: after `Condition.wait()` is cancelled, re-acquiring the lock in
the cleanup path immediately re-raises `CancelledError`; the lock is never
taken back → the task is stuck in cleanup and the lock leaks.
**Root cause**: Python cancellation re-raises at every `await`, so cleanup
code cannot `await`.
**Correct approach**: isolate async cleanup from cancellation — either via
`CancelScope(shield=True)` (`_cancel.py`, `src/cancel.rs`), which masks outer
`CancelScope` deadlines and cancellations on the task's Rust `RawTaskCancelState`
stack across `multiloop` awaits (`sleep`, `wait_cancellable`, `Channel`, `Lock`,
etc.) without touching CPython `task.cancelling()`; or via a cancellation-absorbing
retry loop (the canonical `asyncio.Condition.wait` pattern, bpo-34094 family)
that catches `asyncio.CancelledError` in a `while True` loop until
`await self._lock.acquire()` succeeds and re-raises `CancelledError` once the lock is held.
**Examples**: `Condition._reacquire_lock` (`_sync.py`, paired with `_raw.forward_notify()` in `Condition.wait`); `CancelScope(shield=True)` (`_cancel.py`).

### Pattern 4: notify/wait lock mismatch

**Symptom**: the notifier must hold some lock to `notify()`, but the waiter
has **already released that lock** before waiting → the notifier can never
get the lock → deadlock.
**Root cause**: the lock required by `notify` differs from the lock released
by `wait`, or `notify` is wrongly required to hold the lock.
**Correct approach**: `notify()` must not require the underlying lock; the
waiter queue is guarded by a separate `RawAsyncCondition` mutex in Rust — `notify` only touches
the waiter queue, never the business lock.
**Examples**: `Condition.notify()` (`_sync.py`, `src/sync.rs`) — a lock-gated notify
would also let producers batch notifications and starve waiters.

### Pattern 5: round/generation mismatch & ABA pointer reuse (wrong-round removal)

**Symptom**: a cancellation/timeout handler from the previous round deletes a
**next-round** waiter entry, or a newly allocated `asyncio.Future` reuses the
memory address (`fut.as_ptr()`) of a recently completed Future and gets deleted
by a stale cancellation callback.
**Root cause**: waiter entries or memory addresses are reused, so pointer-based or round-blind removal deletes the wrong waiter.
**Correct approach**: assign every waiter registration a monotonic `WaiterId (u64)` ticket (`src/waiter.rs`), and for cyclic round-based primitives additionally record the `generation` on registration; cancellation handlers match by `ticket_id` (or `(generation, ticket_id)` for cyclic barriers) before removing or forwarding tokens.
**Examples**: `RawAsyncBarrier::cancel_waiter` (`src/sync.rs`), `RawAsyncWaitGroup::cancel_waiter_ticket` (`src/waitgroup.rs`),
`RawAsyncSemaphore::cancel_waiter` (`src/sync.rs`).

**Barrier Broken note**: the generation guard protects *next-round* entries, and a cancelled party automatically transitions the barrier into a Broken state, waking all current round parties with an error to eliminate deadlocks.

**Known limitation**: cancellation-handler removal in
`Channel`/`Lock`/`Semaphore`/`CapacityLimiter`/`Condition`/`RWMutex`/`WaitGroup`/`Once` scans the waiter queue by `WaiterId` (O(n) per cancelled waiter), so
a cancellation storm on N parked waiters costs O(n²) (`Barrier::cancel_waiter` breaks the round on the first cancel in O(n) and short-circuits subsequent same-round cancels in O(1) via `generation`). Acceptable for realistic waiter counts;
documented so nobody "optimises" it into a wrong-round or ABA bug.

### Pattern 6: register-after-done race

**Symptom**: `wait()` checks "counter is already 0" and is about to
register, when `done()` decrements to 0 and clears the waiter list → the new
waiter is never woken.
**Root cause**: the window between the check and the registration.
**Correct approach**: the registration call atomically checks completion under
the mutex and returns `None` when already done; if so, return immediately without
waiting (same semantics as Go `sync.WaitGroup.Wait`).
**Examples**: `AsyncWaitGroup.wait()` + `RawAsyncWaitGroup::register_waiter_ticket()` returning `Option<WaiterId>` (`src/multiloop/primitives.py:752-763`, `src/waitgroup.rs:112-125`), guarded under `parking_lot::Mutex<WaitGroupInner>`. Note: **all `add()` calls with a positive delta must happen-before `register_waiter_ticket()`** to ensure callers do not observe a stale counter of 0 before workers start.

### Pattern 7: sharing asyncio objects across threads

**Symptom**: an `asyncio.Event`/`Task` owned by one loop is set/cancelled
directly from another thread → `RuntimeError` or silent loss.
**Root cause**: asyncio objects are not thread-safe;
`call_soon_threadsafe` is the only safe cross-thread injection channel.
**Correct approach**: cross-thread, always use
`loop.call_soon_threadsafe(fut.set_result, ...)` or
`call_soon_threadsafe(event.set)`; communicate between threads with
`Channel`, never raw asyncio primitives.
**Examples**: `wake_waiter_outside_lock` / `wake_pair_outside_lock` (`src/waiter.rs`),
`RawAsyncChannel::dispatch_wake` (`src/channel.rs`), `RawTaskCancelState::cancel_scope` (`src/cancel.rs`),
`RawAsyncEvent::set` and `RawAsyncLock::release` (`src/sync.rs`).

### Pattern 8: the deadlock family (lock order / holding locks across await / recursive locking)

**Symptom**: two threads each hold the lock the other wants; or a lock is
held across `await`; or the same thread re-acquires a non-reentrant lock.
**Correct approach**:
- with multiple locks, fix a strict acquisition order (or unify composite state
  under a single Rust `parking_lot::Mutex`, as in `RawAsyncCapacityLimiter`);
- **never `await` while holding a lock** (`threading.Lock` cannot survive an
  await — equivalent to releasing the lock while believing you hold it;
  only `asyncio.Lock` has that semantics);
- `drop(guard)` before calling a function that may take locks again or invoke Python callbacks.
**Examples**: cross-worker buffer stealing in `NativeWorkerPool::pop_work` (`src/pool.rs:341-363`, dropping `victim_buf` before locking `self.buffers[index]`) and all out-of-lock
wakeup paths (`src/channel.rs`, `src/sync.rs`, `src/rwlock.rs`, `src/waitgroup.rs`, `src/waiter.rs`) drop the mutex guard before taking another lock or invoking Python callbacks.

### Pattern 9: component anti-patterns and misuse traps

**Symptom**: Runtime errors, unexpected deadlocks, starvation, or livelocks caused by breaking primitive-specific contracts.

**1. `AsyncWaitGroup.track` unawaited coroutine leak vs. `wrap` lazy increment**:
- **Trap**: `wg.track(coro)` calls `wg.add(1)` **eagerly** at the call site and returns a tracked coroutine wrapper that calls `wg.done()` in `try/finally` (or on `.close()`). Creating `wg.track(worker())` without awaiting it, passing it to `tg.start_soon(...)` / `pool.submit(...)`, or closing it leaks the counter permanently (emitting a `RuntimeWarning` on GC). Conversely, `wg.wrap(fn)` calls `wg.add(1)` lazily when the task starts executing, which can race with an immediate `await wg.wait()` if the child task has not stepped yet.
- **Negative example**:
```python
async def worker():
    pass


wg = AsyncWaitGroup()
tracked = wg.track(worker())  # add(1) ran eagerly; discarding `tracked` leaks the counter!
```
- **Correct approach**: Pass `wg.track(worker())` directly to `tg.start_soon(...)` or `pool.submit(...)` (both accept coroutine objects and close them on pre-step cancellation), or `await wg.track(worker())` directly:
```python
async with TaskGroup() as tg:
    wg = AsyncWaitGroup()
    tg.start_soon(wg.track(worker()))  # add(1) happens-before wait(); done() runs in finally
    await wg.wait()
```

**2. `select_channel` thread-local round-robin index assumptions (no positional priority & lockstep aliasing)**:
- **Trap**: Phase 1 rotates its `try_recv()` probe start via a thread-local counter (`_next_select_index`), so argument order does **not** grant priority to earlier channels; conversely, calling `N` `N`-channel `select_channel` sites in lockstep on the same thread aliases `counter % N`, pinning each site to a fixed starting index.
- **Negative example**:
```python
while True:
    # Assumes high_prio is always drained before low_prio (false: round-robin!)
    ch, val = await select_channel(high_prio, low_prio)
    process(val)
```
- **Correct approach**: Drain priority channels explicitly with `try_recv()` before falling back to `select_channel` (or randomize argument order when breaking lockstep cadence across multiple call sites).

**3. `Barrier` party timeout and automatic broken state**:
- **Trap**: When a waiting party is cancelled or times out, `Barrier` automatically transitions to the Broken state and wakes all remaining waiting parties with a `RuntimeError`. Subsequent `wait()` calls will continue to raise until `barrier.reset()` is called.
- **Negative example**:
```python
try:
    async with fail_after(1.0):
        await barrier.wait()
except TimeoutError:
    pass  # Leaves barrier broken; future waits will immediately fail!
```
- **Correct approach**:
```python
try:
    async with fail_after(1.0):
        await barrier.wait()
except TimeoutError:
    barrier.reset()  # Reset barrier generation to allow future rounds
    raise
```

**4. `CapacityLimiter` anonymous release and fractional capacity checks**:
- **Trap**: `CapacityLimiter` uses anonymous token counters (no borrower task validation), and fractional `total_tokens` (e.g. `2.5`) leaves effective integer slot capacity at `int(total_tokens) = 2`.
- **Negative example**:
```python
if limiter.available_tokens > 0:  # e.g. 0.5 > 0
    await limiter.acquire()  # Blocks if integer capacity (2) is already borrowed!
```
- **Correct approach**: Always use `async with limiter:`; require `limiter.available_capacity >= 1` (or `available_tokens >= 1.0`) for non-blocking expectations.

**5. Bare `asyncio.*` blocking calls inside `CancelScope` / `TaskGroup` / `AsyncContext`**:
- **Trap**: `CancelScope` does not mutate CPython `task.cancel()`, so bare `asyncio.sleep(...)`, `asyncio.Event().wait()`, `asyncio.Queue().get()`, `asyncio.wait_for(...)` / `asyncio.gather(...)` (which spawn detached child `Task`s), and unbridged `await pool.submit(...)` ignore scope cancellation and deadlines.
- **Negative example**:
```python
async with fail_after(0.1):
    await asyncio.sleep(10)  # Hangs 10s; CancelScope does not wake asyncio.sleep
```
- **Correct approach**: Use `CancelScope`-aware `multiloop` primitives (`multiloop.sleep(...)`, `fail_after(...)`, `TaskGroup`, `multiloop.Event`, `Channel`, `run_in_pool(...)`) directly, and wrap third-party/stdlib awaitables or raw futures in `wait_cancellable(...)`.

---

## 3. Pre-Change Checklist

**Before changing concurrency code (channels/locks/semaphores/events/
conditions/barriers/waitgroups/cancellation/pool), walk through each item;
if any item is not satisfied, think it through before touching anything.**

- [ ] Does the change add/modify **waiter registration or wakeup paths**? →
      Patterns 2/4: are registration and wakeup under the same lock? Is
      there a fast path plus an atomic re-check under the lock?
- [ ] Does the change touch the **cancellation/timeout path**? → Patterns
      3/5: is the cleanup-path `await` inside a shield or cancellation-absorbing retry loop? Does waiter removal
      carry a generation/`WaiterId` ticket check?
- [ ] Does the change move **asyncio objects across threads**? → Pattern 7:
      cross-thread only `call_soon_threadsafe`.
- [ ] Does the change introduce **multiple locks**? → Pattern 8: is the lock
      order consistent? Any `await` while holding a lock? Are locks released
      before calling functions that may take locks?
- [ ] Does the change touch **counter/token increments**? → Patterns 1/6:
      are check and act atomic? On cancellation/failure, are tokens
      forwarded or returned (never lost, never duplicated)?
- [ ] Rust-side changes touching **poller counts, batch pull, or atomics**? →
      Section 4: does the RAII guard keep the count safe? Is the memory
      ordering sufficient?
- [ ] Is the new behavior stress-tested with `pytest-repeat --count=50`
      on the **specific target test file/function** (never across the whole suite)?
      Is the test marked `@pytest.mark.free_threading` (3.14t-specific stress)?
- [ ] Found a bug? Did you write a **minimal reproduction test** (deterministic
      data, stable FAIL) before fixing? (mandatory process, Section 5.5)

---

## 4. Rust Core Notes (`src/*.rs`)

### 4.1 Core components and their thread boundaries

| Component | Thread boundary | Note |
|---|---|---|
| `NativeWorkerPool` | any thread may `push_global`/`push_local`; `pop_work` only on worker threads | close: drop senders first, then set the flag; unbounded local channels + sibling buffer stealing (`src/pool.rs`) |
| `RawAsyncChannel` | any thread may send/recv/register | `parking_lot::Mutex<ChannelState>` with anti-barging FIFO, `WaiterId (u64)` tickets, and out-of-lock wakeups (`src/channel.rs`) |
| `RawAsyncWaitGroup` | any thread may add/done/register | single `parking_lot::Mutex<WaitGroupInner>` state machine with `WaiterId` tickets; `done()` drains waiters under the lock and wakes outside via `wake_waiter_outside_lock` (`src/waitgroup.rs`, `src/waiter.rs`, `primitives.py`) |
| `RawAsyncRWMutex` | any thread may acquire/release reader/writer | writer-preference state machine under `parking_lot::Mutex<RWMutexInner>` with `WaiterId` tickets and out-of-lock wakeups (`src/rwlock.rs`) |
| `RawAsyncLock` / `Semaphore` / `Event` / `Condition` / `Barrier` / `CapacityLimiter` / `Once` | any thread may acquire/release/notify/wait | unified Rust state machines (`src/sync.rs`) under `parking_lot::Mutex` with `WaiterId (u64)` tickets and out-of-lock wakeups (`src/waiter.rs`) |
| `RawTaskCancelState` | any thread may call `cancel_scope` / inspect; owner task pushes/pops/parks | pure Rust task cancel stack under `parking_lot::Mutex<TaskCancelInner>` with `AtomicBool` / `AtomicU64` fast-path caches, waking parked `WaiterEntry` outside lock (`src/cancel.rs`, `_cancel.py`) |
| `AtomicMetrics` | any thread may record/read | 64-byte padded atomic counters per worker to avoid false sharing (`src/metrics.rs`) |

### 4.2 The soft poller gate (`num_polling`)

- `fetch_add(1, Relaxed)` then check `< max(num_workers/2, 1)` — a
  **momentary gate**: winning it only applies to this round's batch pull and
  expires immediately; there are no hard role assignments — any worker can
  steal from the global queue at any time (`src/pool.rs:275-285`).
- **Always use `PollerGuard` (RAII) to decrement**: the `fetch_sub` runs even
  on panic (`src/pool.rs:25-31`). Hand-rolled `fetch_add`/`fetch_sub` pairs
  leak the count on any `?`/panic path → always use the guard.

### 4.3 The four-tier consumption of `pop_work`

Priority: **private buffer → global batch pull → local channel → single-item fallback & cross-worker buffer work-stealing**
(`src/pool.rs:256-391`).
- batch size = `min(global_len / num_workers + 1, 128)` (or `64` while draining) amortizes the flume
  pop cost (`src/pool.rs:287-290`).
- When changing the priority/batch formula, also check: can workers starve
  the global queue? (the anti-greed `await asyncio.sleep(0)` mechanism on the
  Python side lives in `_worker_dispatcher` in `pool.py`).
- `push_local` uses **unbounded** local channels to preserve 100% strict
  affinity for `pin_to` tasks without silent drift to global queues or submitter stalling, and shutdown draining verifies that the global queue, all worker buffers, and local channels are completely empty (`src/pool.rs:58, 156-171, 223-254, 371-381`).

### 4.4 Atomics and memory ordering

- Every `AtomicMetrics` counter is a `#[repr(align(64))] PaddedAtomic` —
  prevents false sharing. **Keep 64-byte alignment when adding counters**,
  or throughput mysteriously drops on multi-core (`src/metrics.rs:6-7`).
- Established ordering conventions, follow them in new code:
  - `is_closed` and `RawTaskCancelState` fast-read caches (`effectively_cancelled`, `effective_deadline_bits`, `parked_id`): `Release` store / `Acquire` load (`src/pool.rs`, `src/cancel.rs`)
  - `idle_mask`: `Release` bit-ops (`fetch_or`/`fetch_and`) / `Acquire` load & `AcqRel` CAS (`src/pool.rs:78-114`)
  - `num_polling` and `AtomicMetrics` counters: `Relaxed` (`src/pool.rs`, `src/metrics.rs`)
- flume is lock-free but **not zero-cost**: cross-thread send/recv still has
  atomic ops and memory fences. Don't assume "lock-free = free" on
  performance-sensitive paths.

### 4.5 The ordering constraint on `register_waiter_ticket`

In `RawAsyncWaitGroup` (`src/waitgroup.rs:112-125`), the state machine is guarded by `parking_lot::Mutex<WaitGroupInner>`: **callers must ensure all positive-delta `add()` calls happen-before `register_waiter_ticket()`** (same convention as Go's WaitGroup). Otherwise, calling `wait()` when the counter is zero immediately returns ready and misses concurrent work started afterwards.

---

## 5. Testing Methodology

### 5.1 The concurrency regression triad

```bash
# Stress a single test file (race detection): repeat 50 times
uv run pytest tests/test_pool.py -p no:cacheprovider --count=50

# Deadlocks must fail, not hang: pytest-timeout 180s + thread mode
# auto-dumps all thread stacks on timeout (configured in pyproject.toml)
# when stuck, look for "Thread dump" in the output to locate the await point

# Re-run only the last failures
uv run pytest --lf
```

### 5.2 Marker semantics

| marker | Purpose | CI behavior |
|---|---|---|
| `slow` | performance tests (`tests/test_perf_regression.py`) | skipped by default (`addopts = -m "not slow"`); run via `pytest -m slow` |
| `free_threading` | 3.14t free-threaded stress | runs only in free-threaded build jobs |
| `repeat(n)` | declares stress intent | used with `--count` |

New concurrency tests: mark `free_threading` where possible; declare stress
intent with `pytest.mark.repeat` when deterministic.

### 5.3 Test helpers (`tests/conftest.py`)

- Compiled `_multiloop_core` required at runtime: all primitives are Rust-backed (no pure-Python fallback exists; run `make develop` before running tests; `multiloop.testing.rust_available()` checks whether `_multiloop_core` is compiled).
- `yielder` fixture → `wait_all_tasks_blocked()`: wait until all tasks are
  blocked before asserting — eliminates "asserted before it started"
  timing false-positives.

### 5.4 Local resource constraints (measured on M1 8GB)

- **Run function by function, not the whole suite**; always add a timeout
  (pytest-timeout is global; use `--timeout=60` for tighter single tests).
- Local free-threaded (3.14t) stress: **≤ 6 threads**, pool thread counts
  stay at 4-8.
- CI stress principle: **don't reduce load, extend timeouts** — what fails
  locally may pass in CI and vice versa.

### 5.5 Mandatory process when a bug is found (user rule, non-negotiable)

1. **Write the minimal reproduction test first**: deterministic data (fixed
   seed/fixed concurrency, no random timing), stable FAIL.
2. Confirm the reproduction rate with `--count=50`, and keep the test in the
   suite as a regression test.
3. After the fix, the same test must pass stably, then re-run `--count=50`
   to confirm no concurrency regression.
4. **Forbidden**: "fix while experimenting" or "fix without a test" — a fix
   without a reproduction test is not done.

---

## 6. Related Documents

- [API.md](API.md) — complete API reference
- [CHOOSING.md](CHOOSING.md) — primitive selection decision table ("which
  one to use")
- [AGENTS.md](../AGENTS.md) — AI navigation (quick reference for the seven
  non-obvious design decisions)
