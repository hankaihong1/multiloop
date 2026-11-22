# multiloop Primitive Selection Guide (CHOOSING.md)

**[中文版 (Chinese)](CHOOSING_ZH.md)**

> Find the right API in 30 seconds. Look up your scenario in the table below;
> the one-line "core idea" for each primitive follows. Full signatures and
> parameters: [API.md](API.md). Concurrency semantics:
> [CONCURRENCY.md](CONCURRENCY.md).
>
> **Architecture Note**: All multiloop primitives execute under **Python 3.14t multi-threaded multi-event-loop physical parallelism** with OS mutexes, formal token conservation, and cross-thread signal forwarding. See [CONCURRENCY.md](CONCURRENCY.md).

---

## Decision Table

| What you want to do | Use this | Why |
|---|---|---|
| Run async tasks in parallel across a multi-thread pool | `EventLoopThreadPool.submit()` / `run_in_pool()` | The only entry point that executes coroutines cross-thread; manage the pool with `async with` |
| Explicitly create a pool object (deferred start / custom options) | `create_pool(num_threads=..., **PoolOptions)` | More controllable than `run_in_pool`; returns a reusable pool |
| Submit a batch of tasks and collect results together | `pool.submit_many()` / `TaskGroup(pool=pool)` | Batch dispatch + structured concurrency; `TaskGroup` auto-cancels siblings on failure |
| Pin a task to a fixed worker (stateful connection affinity) | `pool.submit(..., pin_to=N)` | Dedicated local queue, deterministic routing |
| Communicate between tasks (Go channel style) | `Channel` | Native Rust `RawAsyncChannel` with anti-barging FIFO, fastest cross-thread |
| Race multiple channels and take the first ready one (Go select) | `select_channel(*channels)` | First ready channel wins |
| Expose only send / only receive halves | `SendChannel` / `ReceiveChannel` (`ch.split()`) | Interface-level misuse prevention |
| Iterate a data stream elegantly (until close) | `Channel` (`async for item in ch:`) | Iteration protocol + auto termination on close |
| Standalone channel without pool (zero-config) | `Channel()` | Direct construction, no pool needed |
| Mutually exclusive access to a shared resource | `Lock` | Fair FIFO, safe across loops/threads |
| Limit concurrency (N permits) | `Semaphore` | Fixed permit pool, FIFO waiting |
| Dynamically resize the concurrency cap | `CapacityLimiter` | `total_tokens` resizable at runtime |
| One-shot broadcast "state has happened" | `Event` | Sticky (irreversible set, no clear) |
| Wait for a condition (paired with a lock) | `Condition` | `wait()` releases the lock; `notify()` needs no lock |
| N tasks wait for each other (round sync) | `Barrier` | Auto-resets each round, reusable |
| Wait until a group of tasks all finish | `AsyncWaitGroup` | Go `sync.WaitGroup` semantics, cross-thread |
| Run exactly-once initialization (singleton) | `AsyncOnce` | Concurrency-safe, runs once, shares result |
| Read-mostly shared state | `AsyncRWMutex` | `async with rw.reader():` parallel readers, `async with rw.writer():` exclusive |
| Structured concurrency: children always reaped | `TaskGroup` (`tg.start_soon()` + `TaskHandle` / `TaskState`) | All children guaranteed finished when leaving the scope |
| Overall deadline, raise on timeout | `fail_after(sec)` / `fail_at(deadline)` | Raises `TimeoutError` on expiry |
| Overall deadline, exit silently on timeout | `move_on_after(sec)` / `move_on_at(deadline)` | Exits silently; check `scope.cancelled_caught` |
| Fine-grained cancellation (manual / shielded) | `CancelScope(deadline=..., shield=...)` | Pure Rust task cancel stack (three exit modes); `shield` blocks parent scope cancellation |
| Sleep honoring `CancelScope` cancellation | `sleep(delay)` | Zero-overhead cancellable sleep (`multiloop.sleep`) |
| Bridge foreign asyncio awaitables into `CancelScope` | `wait_cancellable(coro)` | Wraps `asyncio.sleep` / 3rd-party coroutines for immediate `CancelScope` cancellation |
| Periodically check cancellation in a long loop | `checkpoint()` | Level-triggered cancellation checkpoint without altering `task.cancelling()` |
| Query the tightest effective deadline | `current_effective_deadline()` | Shield truncates outer deadlines |
| Cross-thread cascading cancellation / timeout broadcast | `AsyncContext` | Tree-propagated cancellation |
| Logging / adjust log level | `get_logger()` / `set_log_level()` | Unified logging outlet |

---

## Primitive Quick Reference (one line each)

- **`EventLoopThreadPool`** — a pool = N isolated asyncio loops; `submit` goes
  through a global lock-free queue with work stealing; the `pin_to=` argument
  uses a dedicated local queue. The pool is an **async context manager**:
  `async with EventLoopThreadPool(num_threads=4) as pool:`.
- **`Channel`** — native Rust `RawAsyncChannel` core with anti-barging FIFO
  ordering and token reservation; Python waiter futures for cross-thread
  wakeups. Bounded (`maxsize=N`) or unbounded (default). Safe to send/recv
  cross-thread.
- **`select_channel`** — one reader per channel; the first to deliver wins
  and the other readers are cancelled; supports `timeout=` and `default=`
  (non-blocking poll mode).
- **`AsyncWaitGroup`** — `add()` counts, `done()` decrements, `wait()` blocks
  until zero. **Rule: all `add()` calls must happen-before `wait()`** (Go
  same).
- **`AsyncOnce`** — the first caller is the leader and executes; followers
  await the result; exceptions are cached and re-raised to every follower.
- **`TaskGroup`** — exits `async with` only after all children finish; a
  failing child cancels the rest and failures aggregate into
  `BaseExceptionGroup`.
- **`CancelScope`** — pure Rust task cancel stack (`RawTaskCancelState`)
  never mutating CPython `task.cancelling()`; supports `shield=True`, strict
  three-mode exit (`PLAIN` / `MOVE_ON` / `FAIL_AFTER`), `sleep()`, and
  `wait_cancellable()` (`fail_after`/`move_on_after` are convenience wrappers).
- **`Lock`/`Semaphore`/`Event`/`Condition`/`Barrier`** — cross-thread async
  synchronization: FIFO fairness, cross-loop/thread safety, cancellation-safe.
  `Event` has no `clear()`.
- **`AsyncContext`** — a global cascading cancellation tree: when a context's
  tasks/channels are cancelled, the whole subtree is cancelled together.
- **`AsyncRWMutex`** — `async with rw.reader():` shared read,
  `async with rw.writer():` exclusive write; see [API.md](API.md) for the
  corresponding entry.
- **Exceptions** — `ChannelClosedError` (channel closed),
  `ThreadPoolClosedError` (pool closed), `TimeoutError` (multiloop's own
  timeout, not the builtin), `WouldBlock` (non-blocking op has no data),
  base class `MultiloopError`.

---

## Minimal Quick-Start Code

```python
import asyncio
import multiloop


async def main():
    # Pool: submit + await result
    async with multiloop.EventLoopThreadPool(num_threads=4) as pool:
        fut = pool.submit(asyncio.sleep, 0.01)
        await fut

    # Channel: cross-task communication
    ch = multiloop.Channel()
    await ch.send(42)
    print(await ch.recv())  # 42

    # Wait for a group
    wg = multiloop.AsyncWaitGroup()
    wg.add(1)
    wg.done()
    await wg.wait()


asyncio.run(main())
```

Full runnable examples: [`examples/`](../examples/README.md).
