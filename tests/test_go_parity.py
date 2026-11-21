import asyncio
import threading

import pytest

from multiloop import (
    AsyncOnce,
    AsyncRWMutex,
    AsyncWaitGroup,
    EventLoopThreadPool,
)


@pytest.mark.asyncio
async def test_async_rw_mutex_concurrent_read_exclusive_write():
    """4. Go parity sync.RWMutex: Validate AsyncRWMutex read sharing and write mutual exclusion"""
    rwlock = AsyncRWMutex()
    read_count = 0
    max_concurrent_readers = 0
    writing = False

    async def reader():
        nonlocal read_count, max_concurrent_readers
        async with rwlock.reader():
            assert not writing  # No writes during reads
            read_count += 1
            max_concurrent_readers = max(max_concurrent_readers, read_count)
            await asyncio.sleep(0.02)
            read_count -= 1

    async def writer():
        nonlocal writing
        async with rwlock.writer():
            writing = True
            await asyncio.sleep(0.03)
            writing = False

    # Start 5 reader coroutines and 1 writer coroutine
    readers = [asyncio.create_task(reader()) for _ in range(5)]
    await asyncio.sleep(0.005)
    writer_task = asyncio.create_task(writer())

    await asyncio.gather(*readers, writer_task)
    assert (
        max_concurrent_readers > 1
    )  # Validate shared read lock (multiple readers enter concurrently)


@pytest.mark.asyncio
async def test_async_wait_group_basic():
    """Test AsyncWaitGroup cooperative waiting"""
    wg = AsyncWaitGroup()
    wg.add(3)

    results = []

    async def worker(i):
        await asyncio.sleep(0.01)
        results.append(i)
        wg.done()

    for i in range(3):
        asyncio.create_task(worker(i))

    await wg.wait()
    assert len(results) == 3


@pytest.mark.asyncio
async def test_async_once_single_execution():
    """Test AsyncOnce ensures logic executes exactly once"""
    once = AsyncOnce()
    counter = 0

    async def init_func():
        nonlocal counter
        counter += 1
        await asyncio.sleep(0.01)
        return "init_result"

    async def runner():
        return await once.do(init_func)

    # 10 concurrent tasks call once.do simultaneously
    results = await asyncio.gather(*[runner() for _ in range(10)])

    assert counter == 1
    assert all(r == "init_result" for r in results)


@pytest.mark.asyncio
async def test_async_wait_group_wait_zero():
    """AsyncWaitGroup.wait() returns immediately when counter is already zero (line 273)."""
    wg = AsyncWaitGroup()
    # Counter starts at 0 — wait() must return without blocking.
    await wg.wait()


@pytest.mark.asyncio
async def test_async_once_with_coroutine():
    """AsyncOnce.do() with a coroutine function — covers primitives.py
    branch 342->344 (iscoroutine/isfuture check)."""
    once = AsyncOnce()

    async def async_fn():
        return 42

    result = await once.do(async_fn)
    assert result == 42

    # Second call returns cached result
    result2 = await once.do(async_fn)
    assert result2 == 42


@pytest.mark.asyncio
async def test_async_once_exception_rethrow():
    """Test AsyncOnce exception capture and re-throw"""
    once = AsyncOnce()

    def fail_fn():
        raise ValueError("Once failed")

    with pytest.raises(ValueError, match="Once failed"):
        await once.do(fail_fn)

    # Second do still raises the same exception
    with pytest.raises(ValueError, match="Once failed"):
        await once.do(fail_fn)


@pytest.mark.asyncio
@pytest.mark.repeat(10)
async def test_race_async_once_100_threads_blitz():
    """Race 3: Validate that 100 coroutines racing AsyncOnce under multi-threaded loops execute exactly once"""
    async with EventLoopThreadPool(num_threads=4) as pool:
        once = AsyncOnce()
        counter = 0

        async def init_func():
            nonlocal counter
            counter += 1
            await asyncio.sleep(0.01)
            return counter

        futs = [pool.submit(once.do, init_func) for _ in range(100)]
        results = await asyncio.gather(*futs)

        assert counter == 1
        assert all(r == 1 for r in results)


@pytest.mark.asyncio
async def test_race_wait_group_high_concurrency():
    """Race 4: Validate that AsyncWaitGroup add/done/wait under high-concurrency multi-threading has no race-condition missed wakeups"""
    async with EventLoopThreadPool(num_threads=4) as pool:
        wg = AsyncWaitGroup()
        total_tasks = 100
        wg.add(total_tasks)
        completed = 0
        lock = threading.Lock()

        async def worker():
            nonlocal completed
            await asyncio.sleep(0.005)
            with lock:
                completed += 1
            wg.done()

        for _ in range(total_tasks):
            pool.submit(worker)

        await wg.wait()
        assert completed == total_tasks


# ---------------------------------------------------------------------------
# Regression test: cancelled AsyncWaitGroup.wait() must unregister
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_waitgroup_cancelled_wait_unregisters() -> None:
    """a cancelled ``wait()`` must remove its entry from the Rust
    waiter list — pre-fix every cancelled wait left a stale entry that only
    a later done()-to-zero drained (unbounded growth on long-lived groups)."""
    wg = AsyncWaitGroup()
    wg.add(1)
    tasks = [asyncio.create_task(wg.wait()) for _ in range(5)]
    await asyncio.sleep(0)
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

    waiters = wg._inner.done()  # returns the handed-over waiter list
    n = len(waiters) if waiters else 0
    assert n == 0, f"{n} stale waiter entries left after cancellation"


# ---------------------------------------------------------------------------
# R7-D: cancelled AsyncOnce leader must not poison later callers with CE
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_once_cancelled_leader_does_not_poison_later_callers() -> None:
    """R7-D: after the leader is cancelled, a later caller's do() gets a
    RuntimeError instead of a CancelledError.

    Pre-fix: _exc stored the CancelledError, so an unrelated later caller's
    do() raised it — a user-level CE marks its task as cancelled.
    """
    once = AsyncOnce()
    started = asyncio.Event()

    async def fn() -> None:
        started.set()
        await asyncio.sleep(60)

    lt = asyncio.create_task(once.do(fn))
    await started.wait()
    await asyncio.sleep(0.01)
    lt.cancel()
    with pytest.raises(asyncio.CancelledError):
        await lt  # the leader itself re-raises the original CE (correct)

    ct = asyncio.create_task(once.do(fn))
    with pytest.raises(RuntimeError, match="cancelled"):
        await ct
    assert not ct.cancelled()  # pre-fix: True (CE poisoning)


@pytest.mark.asyncio
async def test_once_cancelled_leader_follower_gets_runtime_error() -> None:
    """R7-D: a follower waiting alongside the leader gets a RuntimeError
    instead of a CancelledError."""
    once = AsyncOnce()
    started = asyncio.Event()

    async def fn() -> None:
        started.set()
        await asyncio.sleep(60)

    lt = asyncio.create_task(once.do(fn))
    await started.wait()
    ft = asyncio.create_task(once.do(fn))
    await asyncio.sleep(0.01)
    lt.cancel()
    with pytest.raises(asyncio.CancelledError):
        await lt  # the leader itself re-raises the original CE (correct)
    with pytest.raises(RuntimeError, match="cancelled"):
        await ft
    assert not ft.cancelled()  # pre-fix: True (CE poisoning)


@pytest.mark.asyncio
async def test_waitgroup_track_happens_before() -> None:
    """AsyncWaitGroup.track wraps tasks with automatic add(1) and done()."""
    wg = AsyncWaitGroup()
    completed: list[int] = []

    async def worker(idx: int) -> int:
        await asyncio.sleep(0.01)
        completed.append(idx)
        return idx * 2

    # Spawn 5 workers using wg.track
    tasks = [asyncio.create_task(wg.track(worker(i))) for i in range(5)]

    # wg.wait() should block until all 5 tasks finish
    await wg.wait()
    assert len(completed) == 5
    results = await asyncio.gather(*tasks)
    assert results == [0, 2, 4, 6, 8]


# ---------------------------------------------------------------------------
# Consolidated AsyncWaitGroup & AsyncRWMutex lifecycle / concurrency tests
# ---------------------------------------------------------------------------


def test_waitgroup_underflow_raises() -> None:
    """AsyncWaitGroup.done() raises RuntimeError/ValueError on counter underflow."""
    wg = AsyncWaitGroup()
    with pytest.raises((RuntimeError, ValueError), match="counter"):
        wg.done()


@pytest.mark.asyncio
async def test_waitgroup_add_negative_wake() -> None:
    """AsyncWaitGroup.add(-n) to 0 wakes registered waiters."""
    wg = AsyncWaitGroup()
    wg.add(2)
    waiter_task = asyncio.create_task(wg.wait())
    await asyncio.sleep(0.01)
    assert not waiter_task.done()
    wg.add(-2)
    await asyncio.wait_for(waiter_task, timeout=1.0)


@pytest.mark.asyncio
async def test_waitgroup_cross_generation_race() -> None:
    """Fast done() to 0 followed by new generation does not prematurely wake gen 2 waiters."""
    wg = AsyncWaitGroup()
    gen1_results: list[int] = []
    gen2_results: list[int] = []

    wg.add(1)

    async def gen1_waiter() -> None:
        await wg.wait()
        gen1_results.append(1)

    t1 = asyncio.create_task(gen1_waiter())
    await asyncio.sleep(0.01)
    wg.done()
    await t1
    assert gen1_results == [1]

    wg.add(1)

    async def gen2_waiter() -> None:
        await wg.wait()
        gen2_results.append(2)

    t2 = asyncio.create_task(gen2_waiter())
    await asyncio.sleep(0.02)
    assert not t2.done()
    wg.done()
    await t2
    assert gen2_results == [2]


@pytest.mark.asyncio
async def test_async_wait_group_holding() -> None:
    """AsyncWaitGroup.holding() increments on enter and decrements on exit/exception."""
    wg = AsyncWaitGroup()

    async def worker() -> None:
        async with wg.holding():
            await asyncio.sleep(0.01)

    t1 = asyncio.create_task(worker())
    t2 = asyncio.create_task(worker())
    await asyncio.sleep(0.002)
    await asyncio.wait_for(wg.wait(), timeout=1.0)
    await t1
    await t2

    async def failing_worker() -> None:
        async with wg.holding():
            raise ValueError("boom")

    with pytest.raises(ValueError, match="boom"):
        await failing_worker()
    await asyncio.wait_for(wg.wait(), timeout=0.1)


@pytest.mark.asyncio
async def test_async_wait_group_wrap() -> None:
    """AsyncWaitGroup.wrap wraps callables and tracks execution."""
    wg = AsyncWaitGroup()

    async def async_worker(x: int) -> int:
        await asyncio.sleep(0.01)
        return x * 2

    res = await wg.wrap(async_worker)(21)
    assert res == 42
    await asyncio.wait_for(wg.wait(), timeout=0.1)


@pytest.mark.asyncio
async def test_async_wait_group_track_unawaited_and_invalid() -> None:
    """AsyncWaitGroup.track/wrap reject invalid targets and protect against leaked/closed coroutines."""
    import gc
    import warnings

    wg = AsyncWaitGroup()
    with pytest.raises(TypeError):
        wg.track(12345)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        wg.wrap(12345)  # type: ignore[arg-type]
    await asyncio.wait_for(wg.wait(), timeout=0.1)

    async def sample_coro() -> int:
        await asyncio.sleep(0)
        return 123

    # Explicit close before awaiting keeps counter 0
    tracked = wg.track(sample_coro())
    tracked.close()
    await asyncio.wait_for(wg.wait(), timeout=0.1)

    # Discarded without awaiting emits warning and preserves counter
    wg2 = AsyncWaitGroup()
    with pytest.warns(
        RuntimeWarning, match="Tracked coroutine was never awaited; AsyncWaitGroup counter leaked"
    ):

        def discard_tracked() -> None:
            _ = wg2.track(sample_coro())

        discard_tracked()
        gc.collect()
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(wg2.wait(), timeout=0.05)
    wg2.done()
    await asyncio.wait_for(wg2.wait(), timeout=0.05)

    # Awaiting via __await__ marks _stepped=True so subsequent close()/__del__ is safe no-op
    wg.add(1)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        tracked_normal = wg.track(sample_coro())
        assert await tracked_normal == 123
        tracked_normal.close()
        del tracked_normal
        gc.collect()
    assert not caught
    wg.done()
    await asyncio.wait_for(wg.wait(), timeout=0.2)


@pytest.mark.asyncio
async def test_rwmutex_cancel_cleanup_leak() -> None:
    """Cancelled reader holder completes release path so queued writer is admitted."""
    rw = AsyncRWMutex()
    entered = asyncio.Event()

    async def holder() -> None:
        async with rw.reader():
            entered.set()
            await asyncio.Event().wait()

    h = asyncio.create_task(holder())
    await entered.wait()
    w_cm = rw.writer()
    w = asyncio.create_task(w_cm.__aenter__())
    await asyncio.sleep(0.01)

    h.cancel()
    with pytest.raises(asyncio.CancelledError):
        await h

    async with asyncio.timeout(1.0):
        await w
    await w_cm.__aexit__(None, None, None)
    assert rw._readers == 0  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_rwmutex_nesting_and_reader_depth_rejected() -> None:
    """reader->writer / writer->reader / writer->writer nesting raises RuntimeError; reader->reader stays legal."""
    rw = AsyncRWMutex()

    with pytest.raises(RuntimeError):
        async with rw.reader(), asyncio.timeout(0.5), rw.writer():
            pass

    with pytest.raises(RuntimeError):
        async with rw.writer(), asyncio.timeout(0.5), rw.reader():
            pass

    with pytest.raises(RuntimeError):
        async with rw.writer(), asyncio.timeout(0.5), rw.writer():
            pass

    async with rw.reader():
        async with rw.reader():
            pass
        with pytest.raises(RuntimeError):
            async with asyncio.timeout(0.5), rw.writer():
                pass


@pytest.mark.asyncio
async def test_rwmutex_cancelled_waiter_preserves_holder_and_grants() -> None:
    """Cancelling queued/granted writers or readers cleanly transfers ownership without leaking state."""
    rw = AsyncRWMutex()

    # 1. Cancelled queued writer does not corrupt active writer state
    async with rw.writer():

        async def queued_writer() -> None:
            async with rw.writer():
                await asyncio.Event().wait()

        w2 = asyncio.create_task(queued_writer())
        await asyncio.sleep(0.01)
        w2.cancel()
        with pytest.raises(asyncio.CancelledError):
            await w2

        r = asyncio.create_task(rw.reader().__aenter__())
        await asyncio.sleep(0.01)
        assert not r.done()
        r.cancel()
        with pytest.raises(asyncio.CancelledError):
            await r

    # 2. Writer cancelled immediately after release_write() grants ownership
    w1_cm = rw.writer()
    await w1_cm.__aenter__()

    async def next_writer() -> None:
        async with rw.writer():
            pass

    t_w = asyncio.create_task(next_writer())
    await asyncio.sleep(0.01)
    t_w.cancel()
    await w1_cm.__aexit__(None, None, None)
    with pytest.raises(asyncio.CancelledError):
        await t_w
    assert rw._writer is False

    # 3. Reader cancelled immediately after release_write() grants read permit
    await w1_cm.__aenter__()

    async def next_reader() -> None:
        async with rw.reader():
            pass

    t_r = asyncio.create_task(next_reader())
    await asyncio.sleep(0.01)
    t_r.cancel()
    await w1_cm.__aexit__(None, None, None)
    with pytest.raises(asyncio.CancelledError):
        await t_r
    assert rw._readers == 0

    async with asyncio.timeout(0.5), rw.writer():
        pass


@pytest.mark.asyncio
async def test_writer_cancel_wakes_blocked_readers() -> None:
    """Cancelling a pending writer immediately wakes readers queued behind it."""
    rw = AsyncRWMutex()
    reader_done = asyncio.Event()

    async def reader(done: asyncio.Event) -> None:
        async with rw.reader():
            done.set()

    async def writer_stuck() -> None:
        try:
            async with rw.writer():
                await asyncio.Event().wait()
        except asyncio.CancelledError:
            pass

    t1 = asyncio.create_task(reader(asyncio.Event()))
    await asyncio.sleep(0.05)
    t2 = asyncio.create_task(writer_stuck())
    await asyncio.sleep(0.05)
    t3 = asyncio.create_task(reader(reader_done))
    await asyncio.sleep(0.05)

    t2.cancel()
    await asyncio.wait_for(reader_done.wait(), timeout=1.0)
    await asyncio.gather(t1, t2, t3, return_exceptions=True)


def test_rwmutex_foreign_task_release_raises() -> None:
    """Releasing AsyncRWMutex read/write lock from a non-holder task token raises RuntimeError."""
    rw = AsyncRWMutex()
    with pytest.raises(RuntimeError):
        rw._raw.release_read_task(999_999)
    with pytest.raises(RuntimeError):
        rw._raw.release_write_task(999_999)


@pytest.mark.asyncio
async def test_rwmutex_child_task_no_inherited_read_depth() -> None:
    """Spawned child task does not inherit parent task's read depth and queues behind pending writer."""
    rw = AsyncRWMutex()
    log: list[str] = []

    async def writer_coro() -> None:
        async with rw.writer():
            log.append("writer_entered")

    async def child_coro() -> None:
        async with rw.reader():
            log.append("child_entered")

    async with rw.reader():
        log.append("parent_reader_entered")
        w_task = asyncio.create_task(writer_coro())
        await asyncio.sleep(0)
        c_task = asyncio.create_task(child_coro())
        await asyncio.sleep(0)
        log.append("parent_reader_exiting")

    await asyncio.gather(w_task, c_task)
    assert log == [
        "parent_reader_entered",
        "parent_reader_exiting",
        "writer_entered",
        "child_entered",
    ]


@pytest.mark.asyncio
async def test_rwmutex_woken_tickets_cleared_on_claim() -> None:
    """Readers batch-woken by writer release clear woken_tickets upon claim."""
    rw = AsyncRWMutex()
    w_entered = asyncio.Event()
    w_release = asyncio.Event()

    async def writer_task() -> None:
        async with rw.writer():
            w_entered.set()
            await w_release.wait()

    w_fut = asyncio.create_task(writer_task())
    await w_entered.wait()

    r1_in = asyncio.Event()
    r2_in = asyncio.Event()

    async def reader1() -> None:
        async with rw.reader():
            r1_in.set()
            await asyncio.sleep(0.05)

    async def reader2() -> None:
        async with rw.reader():
            r2_in.set()
            await asyncio.sleep(0.05)

    r1_fut = asyncio.create_task(reader1())
    r2_fut = asyncio.create_task(reader2())
    await asyncio.sleep(0)

    w_release.set()
    await r1_in.wait()
    await r2_in.wait()

    woken_len = getattr(rw._raw, "woken_tickets_len", None)
    assert woken_len is not None and woken_len() == 0
    await asyncio.gather(w_fut, r1_fut, r2_fut)


@pytest.mark.asyncio
async def test_rwmutex_closed_loop_writer_wakeup_rollback() -> None:
    """Closed-loop queued writer rolls back writer ownership so live tasks do not deadlock."""
    rw = AsyncRWMutex()
    dead_loop = asyncio.new_event_loop()
    dead_fut = dead_loop.create_future()

    async with rw.reader():
        ticket_id = rw._raw.acquire_write_ticket(987654321, dead_loop, dead_fut)
        assert ticket_id is not None
        dead_loop.close()

    assert not rw._writer
    async with rw.writer():
        assert rw._writer


@pytest.mark.asyncio
async def test_rwmutex_stale_ticket_cancel_idempotency() -> None:
    """Stale ticket_id cancellation does not release AsyncRWMutex held by the same task."""
    task = asyncio.current_task()
    assert task is not None
    task_id = id(task)

    rw = AsyncRWMutex()
    async with rw.writer():
        assert rw._writer
        rw._raw.cancel_write_ticket(task_id, 999_999)
        assert rw._writer
    assert not rw._writer

    async with rw.reader():
        assert rw._readers == 1
        rw._raw.cancel_read_ticket(task_id, 999_999)
        assert rw._readers == 1
    assert rw._readers == 0
