import asyncio
import threading
from contextlib import suppress

import pytest

from multiloop import (
    AsyncOnce,
    AsyncRWMutex,
    AsyncWaitGroup,
    EventLoopThreadPool,
)
from multiloop._cancel import _task_token
from multiloop.testing import wait_all_tasks_blocked


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
    await wait_all_tasks_blocked()
    assert wg._inner.waiter_count() == 5
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    assert all(t.cancelled() for t in tasks)

    n = wg._inner.waiter_count()
    assert n == 0, f"{n} stale waiter entries left after cancellation"
    wg.done()
    assert wg._inner.is_zero()


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
    t1_acquired = asyncio.Event()
    t1_release = asyncio.Event()
    t3_acquired = asyncio.Event()

    async def holder_reader() -> None:
        async with rw.reader():
            t1_acquired.set()
            await t1_release.wait()

    async def pending_writer() -> None:
        async with rw.writer():
            pass

    async def blocked_reader() -> None:
        async with rw.reader():
            t3_acquired.set()

    t1 = asyncio.create_task(holder_reader())
    t2: asyncio.Task[None] | None = None
    t3: asyncio.Task[None] | None = None
    try:
        await t1_acquired.wait()

        t2 = asyncio.create_task(pending_writer())
        await wait_all_tasks_blocked()
        assert rw._pending_writers == 1

        t3 = asyncio.create_task(blocked_reader())
        await wait_all_tasks_blocked()
        assert not t3.done()
        assert not t3_acquired.is_set()

        t2.cancel()
        with suppress(asyncio.CancelledError):
            await t2
        assert t2.cancelled()
        assert rw._pending_writers == 0
        assert rw._readers == 2
        assert not t1.done()

        await asyncio.wait_for(t3, timeout=0.5)
        assert t3_acquired.is_set()
        assert rw._readers == 1
    finally:
        t1_release.set()
        for task in (t2, t3):
            if task is not None and not task.done():
                task.cancel()
        tasks = [task for task in (t1, t2, t3) if task is not None]
        try:
            await asyncio.wait_for(
                asyncio.gather(*tasks, return_exceptions=True),
                timeout=1.0,
            )
        except TimeoutError:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
    assert rw._readers == 0


@pytest.mark.asyncio
async def test_writer_cancel_multiple_writers_and_blocked_readers() -> None:
    """Cancelling multiple queued writers keeps readers blocked until the last writer cancels, then admits all blocked readers and blocks subsequent writers until all release."""
    rw = AsyncRWMutex()
    holder_acquired = asyncio.Event()
    holder_release = asyncio.Event()
    promoted_release = asyncio.Event()
    r_acquired = [asyncio.Event() for _ in range(3)]
    w3_acquired = asyncio.Event()

    async def holder_reader() -> None:
        async with rw.reader():
            holder_acquired.set()
            await holder_release.wait()

    async def pending_writer() -> None:
        async with rw.writer():
            pass

    async def blocked_reader(idx: int) -> None:
        async with rw.reader():
            r_acquired[idx].set()
            await promoted_release.wait()

    async def subsequent_writer() -> None:
        async with rw.writer():
            w3_acquired.set()

    holder = asyncio.create_task(holder_reader())
    w1: asyncio.Task[None] | None = None
    w2: asyncio.Task[None] | None = None
    w3: asyncio.Task[None] | None = None
    readers: list[asyncio.Task[None]] = []
    try:
        await holder_acquired.wait()

        w1 = asyncio.create_task(pending_writer())
        w2 = asyncio.create_task(pending_writer())
        await wait_all_tasks_blocked()
        assert rw._pending_writers == 2

        readers = [asyncio.create_task(blocked_reader(i)) for i in range(3)]
        await wait_all_tasks_blocked()
        assert all(not ev.is_set() for ev in r_acquired)
        assert rw._readers == 1

        # Cancelling w1 while w2 remains queued preserves writer preference.
        w1.cancel()
        with suppress(asyncio.CancelledError):
            await w1
        assert w1.cancelled()
        assert rw._pending_writers == 1
        assert rw._readers == 1
        await wait_all_tasks_blocked()
        assert all(not ev.is_set() for ev in r_acquired)

        # Cancelling the last queued writer promotes all 3 blocked readers concurrently.
        w2.cancel()
        with suppress(asyncio.CancelledError):
            await w2
        assert w2.cancelled()
        assert rw._pending_writers == 0
        assert rw._readers == 4

        await asyncio.wait_for(
            asyncio.gather(*(ev.wait() for ev in r_acquired)),
            timeout=0.5,
        )
        assert rw._readers == 4
        assert not rw._writer
        assert rw._raw.woken_tickets_len() == 0

        # Subsequent writer must wait until holder and all promoted readers release.
        w3 = asyncio.create_task(subsequent_writer())
        await wait_all_tasks_blocked()
        assert rw._pending_writers == 1
        assert not w3_acquired.is_set()

        holder_release.set()
        await asyncio.wait_for(holder, timeout=0.5)
        assert rw._readers == 3
        await wait_all_tasks_blocked()
        assert not w3_acquired.is_set()
        assert not w3.done()

        promoted_release.set()
        await asyncio.wait_for(asyncio.gather(*readers, w3), timeout=0.5)
        assert w3_acquired.is_set()
        assert rw._readers == 0
        assert not rw._writer
        assert rw._pending_writers == 0
    finally:
        holder_release.set()
        promoted_release.set()
        all_tasks = [t for t in (holder, w1, w2, w3, *readers) if t is not None]
        for task in all_tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*all_tasks, return_exceptions=True)


@pytest.mark.asyncio
async def test_rwmutex_writer_cancel_closed_loop_reader_rollback() -> None:
    """Closed-loop blocked reader promoted by writer cancellation rolls back cleanly."""
    rw = AsyncRWMutex()
    loop = asyncio.get_running_loop()
    dead_loop = asyncio.new_event_loop()
    dead_fut = dead_loop.create_future()
    live_acquired = asyncio.Event()
    live_release = asyncio.Event()

    async def live_reader() -> None:
        async with rw.reader():
            live_acquired.set()
            await live_release.wait()

    live_task: asyncio.Task[None] | None = None
    try:
        async with rw.reader():
            w_fut: asyncio.Future[None] = loop.create_future()
            w_ticket = rw._raw.acquire_write_ticket(777_001, loop, w_fut)
            assert w_ticket is not None

            dead_r_ticket = rw._raw.acquire_read_ticket(777_002, dead_loop, dead_fut)
            assert dead_r_ticket is not None

            live_task = asyncio.create_task(live_reader())
            await wait_all_tasks_blocked()
            assert not live_acquired.is_set()
            assert rw._readers == 1

            dead_loop.close()
            rw._raw.cancel_write_ticket(777_001, w_ticket)

            await asyncio.wait_for(live_acquired.wait(), timeout=0.5)
            assert rw._readers == 2

        assert rw._readers == 1
        live_release.set()
        await asyncio.wait_for(live_task, timeout=0.5)
        assert rw._readers == 0

        async with asyncio.timeout(0.5), rw.writer():
            assert rw._writer
    finally:
        live_release.set()
        if not dead_loop.is_closed():
            dead_loop.close()
        if live_task is not None and not live_task.done():
            live_task.cancel()
        if live_task is not None:
            await asyncio.gather(live_task, return_exceptions=True)


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
    task_token = _task_token(task)

    rw = AsyncRWMutex()
    async with rw.writer():
        assert rw._writer
        rw._raw.cancel_write_ticket(task_token, 999_999)
        assert rw._writer
    assert not rw._writer

    async with rw.reader():
        assert rw._readers == 1
        rw._raw.cancel_read_ticket(task_token, 999_999)
        assert rw._readers == 1
    assert rw._readers == 0

    loop = asyncio.get_running_loop()

    # Real stale write ticket from a prior contended acquisition must not release current write lock.
    r_held = asyncio.Event()
    r_release = asyncio.Event()

    async def holder_reader() -> None:
        async with rw.reader():
            r_held.set()
            await r_release.wait()

    r_task = asyncio.create_task(holder_reader())
    try:
        await r_held.wait()
        w_fut: asyncio.Future[None] = loop.create_future()
        stale_write_ticket = rw._raw.acquire_write_ticket(task_token, loop, w_fut)
        assert stale_write_ticket is not None
        r_release.set()
        await w_fut
        assert rw._writer
        rw._raw.release_write_task(task_token)
        assert not rw._writer

        async with rw.writer():
            assert rw._writer
            rw._raw.cancel_write_ticket(task_token, stale_write_ticket)
            assert rw._writer
        assert not rw._writer
    finally:
        r_release.set()
        if not r_task.done():
            r_task.cancel()
        await asyncio.gather(r_task, return_exceptions=True)

    # Already-claimed read ticket cancellation must not drop current reader ownership.
    w_held = asyncio.Event()
    w_release = asyncio.Event()

    async def holder_writer() -> None:
        async with rw.writer():
            w_held.set()
            await w_release.wait()

    w_task = asyncio.create_task(holder_writer())
    try:
        await w_held.wait()
        r_fut: asyncio.Future[None] = loop.create_future()
        read_ticket = rw._raw.acquire_read_ticket(task_token, loop, r_fut)
        assert read_ticket is not None
        w_release.set()
        await r_fut
        rw._raw.claim_read_ticket(read_ticket)
        assert rw._readers == 1

        rw._raw.cancel_read_ticket(task_token, read_ticket)
        assert rw._readers == 1
        rw._raw.release_read_task(task_token)
        assert rw._readers == 0
    finally:
        w_release.set()
        if not w_task.done():
            w_task.cancel()
        await asyncio.gather(w_task, return_exceptions=True)
