# multiloop API Reference

**[中文版 (Chinese)](API_ZH.md)**

Complete API documentation for `multiloop`: a multi-event-loop engine and concurrency toolkit for Python 3.14t (free-threaded / no-GIL).

---

## 📋 Table of Contents

- [Quick Examples](#quick-examples)
- [Core Engine](#core-engine)
  - [`EventLoopThreadPool`](#eventloopthreadpool)
  - [`PoolOptions`](#pooloptions)
- [Top-Level Functions](#top-level-functions)
  - [`create_pool`](#create_pool)
  - [`run_in_pool`](#run_in_pool)
  - [`checkpoint`](#checkpoint)
  - [`sleep`](#sleep)
  - [`wait_cancellable`](#wait_cancellable)
  - [`fail_after`](#fail_after)
  - [`move_on_after`](#move_on_after)
  - [`fail_at`](#fail_at)
  - [`move_on_at`](#move_on_at)
  - [`current_effective_deadline`](#current_effective_deadline)
  - [`get_logger`](#get_logger)
  - [`set_log_level`](#set_log_level)
- [Golang-Style Channels & Multiplexing](#golang-style-channels--multiplexing)
  - [`Channel`](#channel)
  - [`select_channel`](#select_channel)
- [Task Management](#task-management)
  - [`TaskGroup`](#taskgroup)
  - [`TaskHandle`](#taskhandle)
  - [`TaskStatus`](#taskstatus)
  - [`CancelScope`](#cancelscope)
- [Synchronization & Control Primitives](#synchronization--control-primitives)
  - [`Lock`](#lock)
  - [`Semaphore`](#semaphore)
  - [`CapacityLimiter`](#capacitylimiter)
  - [`Event`](#event)
  - [`Condition`](#condition)
  - [`Barrier`](#barrier)
  - [`AsyncContext`](#asynccontext)
  - [`AsyncWaitGroup`](#asyncwaitgroup)
  - [`AsyncOnce`](#asynconce)
  - [`AsyncRWMutex`](#asyncrwmutex)
- [Exceptions](#exceptions)
  - [`MultiloopError`](#multilooperror)
  - [`ChannelClosedError`](#channelclosederror)
  - [`ThreadPoolClosedError`](#threadpoolclosederror)
  - [`TimeoutError`](#timeouterror)
  - [`WouldBlock`](#wouldblock)

---

## Quick Examples

Copy-paste runnable snippets. Each example is self-contained: it imports `multiloop`, defines an `async def main()`, and runs it with `asyncio.run(main())`. Simulated work uses `asyncio.sleep` (or `multiloop.sleep` inside `CancelScope`, `TaskGroup`, and `AsyncContext` tasks).

### 1. `EventLoopThreadPool` + `submit`

```python
import asyncio
import multiloop


async def heavy_task(x: int) -> int:
    await asyncio.sleep(0.01)  # simulated work
    return x * 2


async def main():
    async with multiloop.EventLoopThreadPool(num_threads=2) as pool:
        fut1 = pool.submit(heavy_task, 21)
        fut2 = pool.submit(heavy_task, 21, pin_to=0)  # pinned to worker 0
        results = await asyncio.gather(fut1, fut2)
        print(results)  # [42, 42]


if __name__ == "__main__":
    asyncio.run(main())
```

### 2. `Channel` send/recv

```python
import asyncio
import multiloop


async def main():
    ch = multiloop.Channel()

    async def producer():
        for i in range(3):
            await ch.send(i)
        ch.close()  # signal end of stream

    asyncio.create_task(producer())
    async for item in ch:  # terminates when closed & empty
        print("got", item)


if __name__ == "__main__":
    asyncio.run(main())
```

### 3. `select_channel`

```python
import asyncio
import multiloop


async def main():
    ch1 = multiloop.Channel()
    ch2 = multiloop.Channel()

    async def feeder():
        await asyncio.sleep(0.01)
        await ch2.send("data from ch2")
        ch2.close()

    asyncio.create_task(feeder())
    selected, val = await multiloop.select_channel(ch1, ch2, timeout=2.0)
    print(f"selected: {val}")  # ("data from ch2") from ch2


if __name__ == "__main__":
    asyncio.run(main())
```

### 4. `AsyncContext` cancel

```python
import asyncio
import multiloop


async def main():
    async with multiloop.EventLoopThreadPool(num_threads=2) as pool:
        ctx = multiloop.AsyncContext()

        async def slow_work():
            try:
                await multiloop.sleep(10)
            except asyncio.CancelledError:
                # cleanup if needed, then let cancellation propagate
                raise

        fut = ctx.submit(pool, slow_work)
        await asyncio.sleep(0.01)  # let slow_work start on the pool
        ctx.cancel()  # cascades cancellation to the pool task
        try:
            await fut
        except asyncio.CancelledError:
            print("task cancelled")  # "task cancelled"
        print("ctx cancelled:", ctx.is_cancelled)  # True


if __name__ == "__main__":
    asyncio.run(main())
```

### 5. `AsyncWaitGroup`

```python
import asyncio
import multiloop


async def worker(wg: multiloop.AsyncWaitGroup, name: str):
    await asyncio.sleep(0.01)
    print(f"{name} done")
    wg.done()


async def main():
    wg = multiloop.AsyncWaitGroup()
    wg.add(2)
    asyncio.create_task(worker(wg, "a"))
    asyncio.create_task(worker(wg, "b"))
    await wg.wait()  # blocks until both workers call done()
    print("all workers finished")


if __name__ == "__main__":
    asyncio.run(main())
```

### 6. `AsyncOnce`

```python
import asyncio
import multiloop


async def init_db() -> str:
    await asyncio.sleep(0.01)  # simulated one-time setup
    return "db ready"


async def main():
    once = multiloop.AsyncOnce()
    r1 = await once.do(init_db)
    r2 = await once.do(init_db)  # skipped: runs once
    print(r1)  # "db ready"
    print(r2)  # "db ready" (cached result)


if __name__ == "__main__":
    asyncio.run(main())
```

### 7. `AsyncRWMutex`

```python
import asyncio
import multiloop


async def main():
    rw = multiloop.AsyncRWMutex()
    data = {"value": 0}

    async with rw.reader():  # concurrent readers allowed
        await asyncio.sleep(0.01)
        print("read:", data["value"])

    async with rw.writer():  # exclusive writer
        await asyncio.sleep(0.01)
        data["value"] = 1
        print("wrote:", data["value"])


if __name__ == "__main__":
    asyncio.run(main())
```

### 8. `Lock`

```python
import asyncio
import multiloop


async def main():
    lock = multiloop.Lock()
    counter = 0

    async def inc():
        nonlocal counter
        async with lock:
            await asyncio.sleep(0.01)
            counter += 1

    await asyncio.gather(*(inc() for _ in range(5)))
    print("counter:", counter)  # 5 (no lost updates)


if __name__ == "__main__":
    asyncio.run(main())
```

### 9. `TaskGroup`

```python
import asyncio
import multiloop


async def worker(name: str) -> str:
    await multiloop.sleep(0.01)
    return f"{name} done"


async def main():
    async with multiloop.TaskGroup() as tg:
        h1 = tg.start_soon(worker, "a")
        h2 = tg.start_soon(worker, "b")
    # Both tasks are guaranteed finished here.
    print(await h1, await h2)


if __name__ == "__main__":
    asyncio.run(main())
```

### 10. `CancelScope` with `fail_after` / `move_on_after`

```python
import asyncio
import multiloop


async def slow():
    await multiloop.sleep(10)


async def main():
    # fail_after raises TimeoutError when the deadline expires.
    try:
        async with multiloop.fail_after(0.05):
            await slow()
    except multiloop.TimeoutError:
        print("timed out with error")

    # move_on_after exits silently when the deadline expires.
    async with multiloop.move_on_after(0.05) as scope:
        await slow()
    if scope.cancelled_caught:
        print("timed out silently")


if __name__ == "__main__":
    asyncio.run(main())
```

---

## Core Engine

### `EventLoopThreadPool`

A multi-event-loop thread pool for CPython 3.14t. Each worker thread runs its own event loop. Tasks are pushed into a shared lock-free queue and pulled by idle workers (work-stealing model); wakeup notifications are dispatched round-robin across the worker loops.

#### Constructor

```text
EventLoopThreadPool(
    options: PoolOptions | None = None,
    num_threads: int | None = None,
    loop_factory: Callable[[], asyncio.AbstractEventLoop] | None = None,
)
```

- **`num_threads`** (*int | None*): Number of physical worker threads to spawn. Defaults to `os.cpu_count() or 4`. Must be $\ge 1$.
- **`loop_factory`** (*Callable | None*): Factory that creates each worker's event loop. Defaults to `asyncio.new_event_loop`.
- **`options`** (*PoolOptions | None*): Optional settings that supply defaults for `num_threads` and `loop_factory`.

#### Properties & Methods

##### `is_running` -> `bool`
Returns `True` if the pool is actively running worker loops.

##### `async start()` -> `None`
Starts all worker threads and their work-stealing queue dispatchers.

##### `async close(timeout: float | None = None)` -> `None`
Gracefully stops all worker loops, drains pending tasks, and joins worker threads.

Drain timeout is configurable: `close()` waits up to `timeout` seconds (defaults to 5.0 seconds when `None`) for in-flight tasks to finish, then force-stops every worker loop — tasks still running after the timeout are cancelled and are not guaranteed to complete.

##### `async abort()` -> `None`
Forcefully stops all worker loops without draining pending tasks. Queued but unexecuted work is discarded.

##### `async wait_closed()` -> `None`
Waits until the pool has been fully stopped. Returns immediately if the pool is not running.

##### `submit(target: Callable[..., Any], *args: Any, pin_to: asyncio.AbstractEventLoop | int | None = None, cancel_scope: CancelScope | None = None, **kwargs: Any)` -> `asyncio.Future`
Submits a coroutine function, coroutine object, or callable to the shared work queue. Workers pull tasks as they become idle. Returns an `asyncio.Future` that resolves with the task's result.

- **`pin_to`**: Optionally pins the task to a specific worker loop (an `int` worker index or an `AbstractEventLoop` managed by the pool). When `None`, the task goes to the global shared queue.
- **`cancel_scope`**: Optionally binds the task to a `CancelScope`; cancelled scopes cancel the returned future (`asyncio.CancelledError`) and interrupt Route 3 wait points (`multiloop.sleep`, `Channel`, `Lock`, `wait_cancellable`, etc.).
- **Raises**: `ThreadPoolClosedError` if the pool is closed, `ValueError` for an invalid `pin_to` index, `TypeError` if `target` is an `asyncio.Future` (raw futures are loop-bound) or if arguments are passed to a coroutine object.

##### `submit_many(targets: Iterable[Callable[..., Any] | Any], cancel_scope: CancelScope | None = None)` -> `list[asyncio.Future]`
Submits a batch of tasks to the shared global work queue in a single scheduling cycle, maximizing throughput across worker threads. Returns a list of `asyncio.Future` instances corresponding to each submitted task.

- **`targets`**: An iterable of coroutine objects, coroutine functions, or callables.
- **`cancel_scope`**: Optional `CancelScope` supported **only for single-target batches (`len <= 1`)**; because `CancelScope` is bound to a single task/stack, passing a non-`None` `cancel_scope` with more than one target closes all coroutine objects and raises `ValueError`.
- **Raises**: `ThreadPoolClosedError` if the pool is closed, `ValueError` if `len(targets) > 1` with `cancel_scope is not None`.

##### `get_metrics()` -> `dict[str, Any]`
Returns a JSON-serializable dictionary containing health metrics:
```json
{
  "is_running": true,
  "thread_count": 4,
  "completed_tasks": [120, 98, 131, 87],
  "active_tasks": [1, 0, 2, 0]
}
```



##### Context Manager
Supports `async with EventLoopThreadPool(...) as pool:` for automatic startup (`start()`) and shutdown (`close()`).

---

### `PoolOptions`

A dataclass carrying configuration defaults for `EventLoopThreadPool`.

```text
PoolOptions(
    num_threads: int = 0,
    loop_factory: Callable[[], asyncio.AbstractEventLoop] | None = None,
)
```

- **`num_threads`** (*int*): Number of worker threads. `0` means auto-detect via `os.cpu_count()`.
- **`loop_factory`** (*Callable | None*): Event loop factory for worker threads.

---

## Top-Level Functions

### `create_pool`

```text
async def create_pool(
    num_threads: int = 0,
    options: PoolOptions | None = None,
    **kwargs: Any,
) -> EventLoopThreadPool
```

Creates and **starts** a pool, returning it ready for use. `num_threads=0` auto-detects via `os.cpu_count()`. Use it as an async context manager, or call `close()` manually when finished.

### `run_in_pool`

```text
async def run_in_pool(coro: Any, *args: Any, num_threads: int = 0, **kwargs: Any) -> Any
```

Runs a coroutine in a freshly-created pool (one-shot convenience). Spawns a pool, submits the task, awaits its result, then closes the pool.

### `checkpoint`

```text
async def checkpoint() -> None
```

Checks the current task's Route 3 Rust cancel stack (`RawTaskCancelState`) and raises `asyncio.CancelledError` if the current scope, or any unshielded ancestor, has been cancelled. Call periodically inside long-running code that cannot `await` frequently.

Under Route 3, cancellation checks are **level-triggered** and never read or mutate CPython's `task.cancelling()` counter.

### `sleep`

```text
async def sleep(delay: float) -> None
```

Sleeps for `delay` seconds while subscribing to the current task's Route 3 `CancelScope` stack. Prefer `multiloop.sleep(...)` over `asyncio.sleep(...)` inside `CancelScope`, `fail_after`, `move_on_after`, and `TaskGroup` tasks so scope cancellation interrupts the sleep immediately.

- **`delay`** (*float*): Sleep duration in seconds (`delay <= 0` delegates to `checkpoint()`; `NaN` raises `ValueError`).

```python
async with fail_after(0.05):
    await multiloop.sleep(10)  # interrupted immediately on expiry
```

### `wait_cancellable`

```text
async def wait_cancellable(awaitable: Awaitable[_T]) -> _T
```

Bridges a foreign asyncio awaitable (e.g., `asyncio.sleep(...)` or third-party async I/O coroutines) so it honors active Route 3 `CancelScope` cancellations and deadlines immediately. If a `CancelScope` cancels while waiting, the underlying task is cancelled cleanly and awaited before `asyncio.CancelledError` propagates.

- **`awaitable`** (*Awaitable[_T]*): Foreign coroutine or awaitable to wrap.

```python
async with fail_after(0.05):
    await multiloop.wait_cancellable(asyncio.sleep(10))
```

### `fail_after`

```text
def fail_after(seconds: float) -> CancelScope
```

Returns a `CancelScope` that raises `TimeoutError` when the deadline expires.

```python
async with fail_after(5):
    await long_operation()
```

### `move_on_after`

```text
def move_on_after(seconds: float) -> CancelScope
```

Returns a `CancelScope` that silently exits when the deadline expires. `scope.cancelled_caught` is `True` after the deadline fires.

```python
async with move_on_after(5) as scope:
    await maybe_slow()
if scope.cancelled_caught:
    print("timed out silently")
```

### `fail_at`

```text
def fail_at(deadline: float) -> CancelScope
```

Returns a `CancelScope` with an **absolute monotonic deadline** that raises `TimeoutError` when the deadline expires. Use when a deadline is already known as a monotonic timestamp (e.g. derived from `current_effective_deadline()`); for relative seconds, use `fail_after`.

```python
deadline = asyncio.get_running_loop().time() + 5
async with fail_at(deadline):
    await long_operation()
```

### `move_on_at`

```text
def move_on_at(deadline: float) -> CancelScope
```

Returns a `CancelScope` with an **absolute monotonic deadline** that silently exits when the deadline expires. `scope.cancelled_caught` is `True` after the deadline fires. For relative seconds, use `move_on_after`.

```python
deadline = asyncio.get_running_loop().time() + 5
async with move_on_at(deadline) as scope:
    await maybe_slow()
if scope.cancelled_caught:
    print("timed out silently")
```

### `current_effective_deadline`

```text
def current_effective_deadline() -> float
```

Walks the task-local scope stack and returns the tightest (nearest) effective deadline among the active scopes. Shielded scopes act as a barrier: deadlines from scopes outside a shielded scope are invisible to code inside the shield. Returns `float("inf")` when no deadline is active.

```python
async with fail_after(10):
    async with CancelScope(shield=True):
        dl = current_effective_deadline()  # float("inf"): outer deadline is shielded
```

### `get_logger`

```text
def get_logger(name: str | None = None) -> logging.LoggerAdapter[Any]
```

Returns a structured logger adapter for the `multiloop` namespace. The adapter wraps the `"multiloop"` logger (or the `"multiloop.<name>"` sub-logger when `name` is given, which propagates to the root `"multiloop"` logger and inherits its level) and injects `task_id` and `span` structured fields into every emitted record.

- **`name`** (*str | None*): Optional sub-logger name (e.g. `"pool"`).

```python
log = get_logger("pool")
log.info("pool started", extra={"span": "boot"})
```

### `set_log_level`

```text
def set_log_level(level: int) -> None
```

Sets the multiloop logger's minimum log level. `level` is a `logging` level constant (e.g. `logging.INFO` or `logging.DEBUG`).

```python
import logging

set_log_level(logging.DEBUG)
```

---

## Golang-Style Channels & Multiplexing

### `Channel`

High-performance cross-thread channel backed by the Rust native `RawAsyncChannel` state machine with anti-barging FIFO ordering protection and zero lost-wakeup signal races.

#### Constructor

```text
Channel(maxsize: int = 0)
```

- **`maxsize`** (*int*): Channel capacity limit. `0` for unbounded channel.

#### Methods

##### `async send(item: Any, timeout: float | None = None)` -> `None`
Sends an item into the channel. Suspends if the channel is full until space is available.
- **`timeout`** (*float | None*): Optional timeout in seconds.
- **Raises**: `ChannelClosedError` if the channel is closed, `TimeoutError` if timeout expires.

##### `send_sync(item: Any, timeout: float | None = None)` -> `None`
Synchronous blocking send, designed for worker threads executing synchronous tasks.
- **`timeout`** (*float | None*): Maximum blocking time in seconds.
- **Raises**: `ChannelClosedError` if the channel is closed, `TimeoutError` if timeout expires.

##### `async recv(timeout: float | None = None)` -> `Any`
Receives an item from the channel. Suspends if empty until an item is available.
- **`timeout`** (*float | None*): Timeout in seconds.
- **Raises**: `ChannelClosedError` if closed and empty, `TimeoutError` if timed out.

##### `try_send(item: Any)` -> `bool`
Non-blocking send. Returns `True` if the item was enqueued, `False` if the channel is full.
- **Raises**: `ChannelClosedError` if the channel is closed.

##### `try_recv()` -> `Any`
Non-blocking receive.
- **Raises**: `WouldBlock` if the channel is empty, `ChannelClosedError` if closed and empty.

##### `qsize()` -> `int`
Returns the number of items currently buffered in the channel.

##### `close()` -> `None`
Closes the channel. All pending senders/receivers are woken up with `ChannelClosedError`.

##### `split()` -> `tuple[SendChannel, ReceiveChannel]`
Splits this channel into a `(SendChannel, ReceiveChannel)` pair for interface segregation.

##### `__aiter__()` & `__anext__()`
Supports `async for item in ch:` iteration. Automatically terminates when the channel is closed and empty.

---

### `SendChannel`

A send-only view of a `Channel` for interface segregation (Go `chan<- T` style).

- **`send(item, timeout=None)`**: Sends an item into the channel.
- **`send_sync(item, timeout=None)`**: Synchronous blocking send.
- **`try_send(item)` -> `bool`**: Non-blocking send.
- **`close()`**: Closes the underlying channel.
- **`is_closed`** -> `bool`: True if closed.
- **`maxsize`** -> `int`: Maximum capacity.
- **`full()`** -> `bool`: True if buffer is full.

---

### `ReceiveChannel`

A receive-only view of a `Channel` for interface segregation (Go `<-chan T` style).

- **`recv(timeout=None)`**: Receives an item from the channel.
- **`recv_sync(timeout=None)`**: Synchronous blocking receive from a worker thread.
- **`try_recv()`**: Non-blocking receive.
- **`close()`**: Closes the underlying channel.
- **`is_closed`** -> `bool`: True if closed.
- **`empty()`** -> `bool`: True if empty.
- **`qsize()`** -> `int`: Number of buffered items.
- **`__aiter__()` & `__anext__()`**: Async iteration support.

---

### `select_channel`

```text
async def select_channel(
    *channels: Any,
    timeout: float | None = None,
    default: Any = ...,
) -> Any
```

Selects the first ready channel from multiple `Channel` instances (Go `select`-style).

- **Returns**: `(selected_channel, value)`.
- **`default`**: When provided, `select_channel` is non-blocking: it tries each channel with `try_recv()` and returns `(channel, value)` for the first ready channel, or `default` if none is ready.
- **Raises**: `ValueError` if no channels are given, `TimeoutError` if `timeout` is reached before any channel becomes ready, `ChannelClosedError` if **every** channel is closed and empty (a closed-empty channel can never become ready, so waiting would hang forever).

**Closed-channel semantics**:
- A closed channel that still holds buffered items reports ready and yields its data — closing never destroys buffered values.
- A closed-and-empty channel is silently ignored while any other channel is still open (select keeps waiting for the open one).
- When **all** channels are closed and empty, `select_channel` raises `ChannelClosedError` instead of hanging.

**Example**:
```python
selected_ch, val = await select_channel(ch1, ch2, timeout=2.0)
```

---

## Task Management

### `TaskGroup`

An async context manager that spawns and manages child tasks, backed by `CancelScope` for cancellation propagation. Supports optional structured concurrency throttling via `max_concurrency` or a shared `CapacityLimiter`.

```python
async with TaskGroup(max_concurrency=10) as tg:
    for url in urls:
        tg.start_soon(fetch_url, url)
# All child tasks are guaranteed finished here, with at most 10 running concurrently.
```

#### Constructor
- **`name`** (*str | None*): Optional group identifier.
- **`max_concurrency`** (*int | None*): Optional maximum concurrent active child tasks. Automatically creates an internal `CapacityLimiter(max_concurrency)`.
- **`limiter`** (*CapacityLimiter | None*): Optional external `CapacityLimiter` to share concurrency budgets across task groups.
- **`pool`** (*EventLoopThreadPool | None*): Optional thread pool to schedule child tasks across worker event loops while retaining structured concurrency guarantees (cascading cancellation, barrier drain on exit, and exception group aggregation).

#### Methods
- **`start_soon(coro_fn, *args)` -> `TaskHandle`**: Spawns a child task and returns its handle immediately without blocking. Children spawned before the group is entered are tracked by the first entry.
  - **Raises**: `RuntimeError` if called after the group has exited or called from a foreign event loop/thread (TaskGroup is physically scoped to a single loop); the orphan is cancelled and its exception consumed.
- **`start(coro_fn, *args)` -> `TaskHandle`**: Spawns a child task, blocking until it calls `task_status.started()`.
  - **Raises**: `RuntimeError` if called after the group has exited or called from a foreign event loop/thread, or if the child exits without calling `task_status.started()` (a silent handle to a dead task would hide the protocol violation).
- **Raises**: Child exceptions are re-raised on exit; multiple failures surface as an `ExceptionGroup`. Cancelled children are not reported as errors, and when the host is cancelled while the group waits, all children are guaranteed finished before the block exits.

### `TaskHandle`

A handle to a child task spawned inside a `TaskGroup`, returned by `TaskGroup.start_soon()` and `TaskGroup.start()`. Awaiting the handle returns the task's result (or raises its exception).

#### Properties

- **`status`** -> `_TaskStatus`: Current lifecycle status — `_TaskStatus.PENDING`, `_TaskStatus.STARTED`, or `_TaskStatus.FINISHED`.
- **`result`** -> `Any`: The task's result once finished.
  - **Raises**: `RuntimeError` if the task is not finished yet.
- **`exception`** -> `BaseException | None`: The task's exception, or `None` if it succeeded.
  - **Raises**: `RuntimeError` if the task is not finished yet.

#### Awaitable
`await handle` returns the task's result and re-raises its exception.

```python
h = tg.start_soon(worker, "a")
result = await h
```

### `TaskStatus`

Status tracker used with `TaskGroup.start()`. The spawned coroutine receives a `TaskStatus` instance as its first argument and calls `started()` once initialised, unblocking `TaskGroup.start()` so it can return the handle.

```python
async def worker(task_status: TaskStatus):
    await setup()
    task_status.started()  # unblocks TaskGroup.start()
    await run()


async with TaskGroup() as tg:
    h = await tg.start(worker)
```

- **`started()`**: Marks the task as started, unblocking the pending `TaskGroup.start()`.

### `CancelScope`

A task-local, single-use cancellation scope backed by Route 3's pure Rust `RawTaskCancelState` cancel stack (never modifying CPython `task.cancel()`, `task.uncancel()`, or `task.cancelling()`).

```text
CancelScope(deadline: float = float("inf"), shield: bool = False)
```

- **`deadline`** (*float*): Absolute monotonic deadline. `float("inf")` means no deadline. Mutating `.deadline` reschedules dynamically.
- **`shield`** (*bool*): Route 3 scope barrier: masks enclosing `CancelScope` cancellations and deadlines on the Rust cancel stack while `shield` is `True` (supporting full async cleanup with `multiloop.sleep`, `Channel`, `Lock`, `wait_cancellable`, etc.), without ever touching CPython's `task.cancelling()`. Note that external CPython `task.cancel()` calls are unaffected.

#### Properties & Methods

- **`cancel_called`** -> `bool`: `True` after `cancel()` has been called or deadline expired.
- **`cancelled_caught`** -> `bool`: `True` if a move-on scope (`move_on_after` / `move_on_at`) absorbed its own cancellation.
- **`deadline`** -> `float`: The absolute monotonic deadline (read/write).
- **`shield`** -> `bool`: Whether the scope blocks parent `CancelScope` cancellation (read/write).
- **`cancel()`**: Marks the scope cancelled in Rust and wakes any parked Route 3 waiter via `call_soon_threadsafe`. Idempotent and thread-safe.

#### Three-Mode Exit Contract

When `async with scope:` exits `__aexit__`:
1. **Plain `CancelScope` (`PLAIN`)**: If cancelled and the body exited normally (`exc_type is None`), raises `asyncio.CancelledError()` on exit; non-cancellation body exceptions pass through untouched.
2. **`move_on_after` / `move_on_at` (`MOVE_ON`)**: Silently absorbs its own cancellation, sets `scope.cancelled_caught = True`, and returns `True` without swallowing external CPython `task.cancel()` or body errors.
3. **`fail_after` / `fail_at` (`FAIL_AFTER`)**: Converts its own cancellation/expiry (even if inner `CancelledError` was caught in the body) into `multiloop.TimeoutError`.

#### Context Manager
`async with CancelScope(...) as scope:` pushes the scope onto the current task's Rust cancel stack (`RuntimeError` if shared across tasks or re-entered).

---

## Synchronization & Control Primitives

### `Lock`

A cross-thread async mutual-exclusion lock.

```python
lock = Lock()

async with lock:
    critical_section()
```

- **`locked`** -> `bool`: `True` if the lock is held.
- **`owner`** -> `asyncio.Task | None`: The task currently holding the lock, if any.

> **Diagnostic properties** (`locked`/`owner`/`Semaphore.value`/`qsize`/`Event.is_set`/`TaskHandle.status`) are consistent snapshots taken under the relevant internal lock — they are **not** atomic across each other and not a substitute for the actual acquire/wait operations under concurrency.
- **`acquire()`**: Acquires the lock, suspending until it is free.
- **`try_acquire()` -> `bool`**: Non-blocking atomic attempt to acquire the lock immediately. Returns `True` if acquired, `False` otherwise.
- **`release()`**: Releases the lock. Must be called by the owner.

### `Semaphore`

An async counting semaphore.

```text
Semaphore(max_value: int)
```

- **`value`** -> `int`: Number of tokens currently available.
- **`max_value`** -> `int`: Maximum number of tokens.
- **`acquire()`**: Acquires a token, suspending until one is available.
- **`try_acquire()` -> `bool`**: Non-blocking atomic attempt to acquire a permit immediately. Returns `True` if acquired, `False` otherwise.
- **`release()`**: Releases a token back to the semaphore.

### `CapacityLimiter`

A token-bucket style limiter supporting fractional tokens.

```text
CapacityLimiter(total_tokens: float)
```

- **`total_tokens`** -> `float`: Total capacity (readable and writable).
- **`total_capacity`** -> `int`: Integer floor capacity (`int(total_tokens)`).
- **`available_tokens`** -> `float`: Tokens currently available (`total_tokens - borrowed_tokens`).
- **`available_capacity`** -> `int`: Discrete integer token capacity currently available (`max(0, int(total_tokens) - borrowed)`).
- **`borrowed_tokens`** -> `float`: Tokens currently borrowed.
- **`snapshot()`** -> `(total, available, borrowed)`: Atomically consistent
  read of all three counters under one lock acquisition. Use this instead of
  reading the three properties separately when another thread may be resizing
  `total_tokens` concurrently — separate reads can mix values computed
  against different totals.
- **`acquire()`**: Acquires one token, suspending until available.
- **`try_acquire()` -> `bool`**: Non-blocking atomic attempt to borrow one token immediately. Returns `True` if acquired, `False` otherwise.
- **`release()`**: Returns one token.

### `Event`

A cross-thread async event flag.

```text
Event()
```

- **`is_set`** -> `bool`: `True` once `set()` has been called.
- **`set()`**: Sets the event and wakes all waiters.
- **`wait()`**: Suspends until the event is set.

### `Condition`

A cross-thread async condition variable, typically paired with a `Lock`.

```text
Condition(lock: Lock | None = None)
```

- **`acquire()`** / **`release()`**: Manage the underlying lock.
- **`wait()`**: Atomically releases the lock and suspends until notified.
- **`wait_for(predicate)`**: Repeatedly waits until `predicate` returns a truthy value. The underlying lock must be held.
- **`notify(n=1)`**: Wakes `n` waiting tasks.
- **`notify_all()`**: Wakes all waiting tasks.

### `Barrier`

A cross-thread async barrier that blocks until a fixed number of parties arrive.

```text
Barrier(parties: int)
```

- **`parties`** -> `int`: Number of parties required to trip the barrier.
- **`n_waiting`** -> `int`: Number of parties currently waiting.
- **`wait()`** -> `BarrierWaitResult`: Blocks until all parties arrive.
- **`abort()`**: Aborts the barrier, waking all waiters.

### `BarrierWaitResult`

Result object returned by `Barrier.wait()`.

- **`parties`** -> `int`: Total number of parties in this barrier round.
- **`index`** -> `int`: Zero-based arrival index (`0` to `parties - 1`) of the calling task.
- **`is_leader`** -> `bool`: `True` if this task arrived first in the round (`index == 0`).

### `AsyncContext`

Go `context.Context`-style cancellation context supporting cross-thread cascading task cancellation.

```text
ctx = AsyncContext(parent: AsyncContext | None = None)
```

- **`ctx.cancel()`**: Cancels this context and cascades cancellation thread-safely to all child contexts and submitted futures.
- **`ctx.submit(pool, target, *args, **kwargs)`**: Submits a task to the pool bound to this context.
- **`ctx.task_group(pool=None, name=None, max_concurrency=None, limiter=None)`**: Creates a `TaskGroup` linked to this context whose tasks cascade cancellation when the context is cancelled.
- **`ctx.parent`**: The parent context, or `None` for a root context (read-only, fixed at construction).
- **`ctx.is_cancelled`**: Returns `True` if cancelled.

---

### `AsyncWaitGroup`

Go `sync.WaitGroup`-style cross-thread synchronization primitive.

```python
wg = AsyncWaitGroup()
wg.add(2)
# In worker tasks: wg.done()
await wg.wait()
```

All `add()` calls with a positive delta must happen-before the first `wait()` on the counter (matches Go `sync.WaitGroup`): a concurrent `add()` racing with `wait()` can lose its wakeup and hang forever. Negative deltas are allowed (Go semantics) but must never drive the counter below zero — doing so raises `RuntimeError`.

Structured RAII tracking is supported via `async with wg.holding():` and `tg.start_soon(wg.wrap(fn))` to guarantee counter balance under exceptions and cancellations.

---

### `AsyncOnce`

Go `sync.Once`-style single execution primitive across multiple event loop threads.

```python
once = AsyncOnce()
res = await once.do(init_function, arg1)
```

If the execution raises, subsequent callers re-raise the same exception. If the leader task is **cancelled**, the leader re-raises `CancelledError` while later callers receive `RuntimeError("AsyncOnce execution was cancelled")` — a `CancelledError` raised in an unrelated task would silently mark that task as cancelled.

---

### `AsyncRWMutex`

Go `sync.RWMutex`-style reader-writer lock allowing concurrent readers or single exclusive writer.

```python
rw = AsyncRWMutex()

# Concurrent read access
async with rw.reader():
    read_data()

# Exclusive write access
async with rw.writer():
    write_data()
```

---

## Exceptions

- **`MultiloopError`**: Base exception for all `multiloop` errors.

### `MultiloopError`

Base exception for all `multiloop` errors.

### `ChannelClosedError`

`ChannelClosedError(MultiloopError)` — raised when operating on a closed channel.

### `ThreadPoolClosedError`

`ThreadPoolClosedError(MultiloopError, RuntimeError)` — raised when submitting tasks to a closed thread pool.

### `TimeoutError`

`TimeoutError(MultiloopError)` — raised when a concurrency operation times out.

### `WouldBlock`

`WouldBlock(MultiloopError)` — raised when a non-blocking operation (e.g. `try_recv()`) cannot proceed immediately.
