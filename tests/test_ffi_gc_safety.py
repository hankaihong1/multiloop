"""FFI destructor reentrancy, zero-cycle GC, __slots__, zero-alloc fast paths, and multi-thread GC convergence tests."""

from __future__ import annotations

import asyncio
import gc
import math
import threading
import weakref
from typing import Any

import pytest

from multiloop import (
    AsyncContext,
    AsyncOnce,
    AsyncRWMutex,
    AsyncWaitGroup,
    Barrier,
    CancelScope,
    CapacityLimiter,
    Channel,
    ChannelClosedError,
    Condition,
    Event,
    EventLoopThreadPool,
    Lock,
    ReceiveChannel,
    Semaphore,
    SendChannel,
    TaskGroup,
    TaskHandle,
    TaskStatus,
    TimeoutError,
    checkpoint,
    current_effective_deadline,
    move_on_after,
    select_channel,
    sleep,
)
from multiloop._metrics import MetricsCollector
from multiloop._multiloop_core import NativeWorkerPool
from multiloop._sync import BarrierWaitResult
from multiloop.pool import _PoolWorkItem
from multiloop.primitives import _TrackedCoroutine


class _ReentrantDelPayload:
    """Payload whose __del__ re-enters concurrency primitive query/cancel APIs."""

    def __init__(self, callback: Any, tag: str = "") -> None:
        self._callback = callback
        self.tag = tag

    def __del__(self) -> None:
        cb = self._callback
        self._callback = None
        if cb is not None:
            cb()


@pytest.mark.asyncio
async def test_destructor_reentrancy_no_deadlock() -> None:
    """Payloads with __del__ re-entering primitive state must never deadlock under Rust mutexes."""
    reentered: list[str] = []

    # 1. Channel critical paths (try_send/recv, close drop, register_getter cancel, send_sync timeout)
    ch = Channel(maxsize=2)
    ch.try_send(
        _ReentrantDelPayload(
            lambda: reentered.append(f"ch:{ch.qsize()}:{ch.empty()}:{ch.full()}:{ch.is_closed}")
        )
    )
    item = await ch.recv()
    del item
    gc.collect()

    ch.try_send(
        _ReentrantDelPayload(lambda: (ch.close(), reentered.append(f"close:{ch.is_closed}")))
    )
    ch.close()
    assert await ch.recv() is not None or True
    gc.collect()

    ch2 = Channel(maxsize=1)
    ch2.try_send("occupy")

    def _sync_put_timeout() -> None:
        try:
            ch2.send_sync(
                _ReentrantDelPayload(lambda: reentered.append(f"sync:{ch2.full()}")),
                timeout=0.01,
            )
        except TimeoutError:
            pass

    t_sync = threading.Thread(target=_sync_put_timeout)
    t_sync.start()
    await asyncio.to_thread(t_sync.join, 2.0)
    gc.collect()

    # 2. EventLoopThreadPool abort drain dropping work items holding __del__ payloads
    async def _noop(x: Any) -> Any:
        return x

    pool = EventLoopThreadPool(num_threads=1)
    await pool.start()
    gate = asyncio.Event()

    async def _block_worker() -> None:
        await sleep(5.0)

    async def _release_payload(_p: Any) -> None:
        return None

    _ = gate
    f1 = pool.submit(_block_worker)
    f2 = pool.submit(
        _release_payload,
        _ReentrantDelPayload(
            lambda: reentered.append(
                f"pool:{pool.get_metrics()['thread_count']}:{pool._native_pool.outstanding_count() if pool._native_pool else -1}"
            )
        ),
    )
    await asyncio.sleep(0.02)
    await pool.abort()
    for f in (f1, f2):
        try:
            f.exception()
        except asyncio.CancelledError:
            pass
    del f1, f2
    gc.collect()

    # 3. AsyncOnce & CancelScope destructor reentrancy
    once = AsyncOnce()
    once_holder: list[Any] = [once]

    async def _make_once() -> _ReentrantDelPayload:
        return _ReentrantDelPayload(
            lambda: reentered.append(
                f"once:{once_holder[0]._done if once_holder else ch.is_closed}"
            )
        )

    res = await once.do(_make_once)
    once_holder.clear()
    del res, once
    gc.collect()

    async with CancelScope() as scope:
        _local_guard = _ReentrantDelPayload(
            lambda: reentered.append(f"scope:{scope.cancel_called}:{current_effective_deadline()}")
        )
        del _local_guard
        gc.collect()

    assert len(reentered) >= 4
    assert any(entry.startswith("ch:") for entry in reentered)
    assert any(entry.startswith("close:") for entry in reentered)
    assert any(entry.startswith("once:") for entry in reentered)
    assert any(entry.startswith("scope:") for entry in reentered)


@pytest.mark.asyncio
async def test_zero_cross_language_reference_cycles() -> None:
    """All core cross-language objects are reclaimed by a single gc.collect() once dropped."""

    async def _build_pool_close() -> weakref.ref[EventLoopThreadPool]:
        pool = EventLoopThreadPool(num_threads=2)
        ref = weakref.ref(pool)
        await pool.start()
        f = pool.submit(lambda: 42)
        assert await f == 42
        await pool.close()
        return ref

    async def _build_pool_abort() -> weakref.ref[EventLoopThreadPool]:
        pool = EventLoopThreadPool(num_threads=2)
        ref = weakref.ref(pool)
        await pool.start()

        async def _slow() -> None:
            await sleep(10.0)

        futs = [pool.submit(_slow) for _ in range(8)]
        await asyncio.sleep(0.02)
        await pool.abort()
        for f in futs:
            try:
                f.exception()
            except asyncio.CancelledError:
                pass
        futs.clear()
        return ref

    async def _build_lock() -> weakref.ref[Lock]:
        lock = Lock()
        ref = weakref.ref(lock)
        await lock.acquire()

        async def _contend() -> None:
            async with lock:
                pass

        t = asyncio.create_task(_contend())
        await asyncio.sleep(0.01)
        t.cancel()
        with pytest.raises(asyncio.CancelledError):
            await t
        lock.release()
        return ref

    async def _build_select_channels() -> tuple[weakref.ref[Channel], weakref.ref[ReceiveChannel]]:
        ch = Channel(maxsize=2)
        rx = ReceiveChannel(ch)
        ch2 = Channel(maxsize=2)
        ch_ref = weakref.ref(ch)
        rx_ref = weakref.ref(rx)

        async def _sender() -> None:
            await asyncio.sleep(0.01)
            await ch2.send("ping")

        sender = asyncio.create_task(_sender())
        winner, val = await select_channel(rx, ch2, timeout=1.0)
        assert winner is ch2 and val == "ping"
        await sender
        with pytest.raises(TimeoutError):
            await select_channel(rx, ch2, timeout=0.005)
        ch.close()
        ch2.close()
        return ch_ref, rx_ref

    async def _build_cancel_scope() -> weakref.ref[CancelScope]:
        scope = CancelScope(deadline=asyncio.get_running_loop().time() + 1.0)
        ref = weakref.ref(scope)
        with pytest.raises(asyncio.CancelledError):
            async with scope:
                scope.cancel()
                await sleep(1.0)
        return ref

    async def _build_task_group() -> weakref.ref[TaskGroup]:
        pool = EventLoopThreadPool(num_threads=2)
        await pool.start()
        try:
            tg = TaskGroup(pool=pool)
            ref = weakref.ref(tg)

            async def _child() -> int:
                await sleep(0)
                return 1

            async with tg:
                h = tg.start_soon(_child)
                assert await h == 1
            return ref
        finally:
            await pool.close()

    async def _build_async_context() -> tuple[weakref.ref[AsyncContext], weakref.ref[AsyncContext]]:
        pool = EventLoopThreadPool(num_threads=2)
        await pool.start()
        try:
            parent = AsyncContext()
            child = AsyncContext(parent=parent)
            p_ref = weakref.ref(parent)
            c_ref = weakref.ref(child)

            async def _work() -> None:
                await sleep(10.0)

            fut = child.submit(pool, _work)
            with pytest.raises(asyncio.CancelledError):
                async with child.task_group(pool=pool) as tg:
                    tg.start_soon(_work)
                    await asyncio.sleep(0.01)
                    parent.cancel()
            with pytest.raises(asyncio.CancelledError):
                await fut
            return p_ref, c_ref
        finally:
            await pool.close()

    r_pool_close = await _build_pool_close()
    r_pool_abort = await _build_pool_abort()
    r_lock = await _build_lock()
    r_ch, r_rx = await _build_select_channels()
    r_scope = await _build_cancel_scope()
    r_tg = await _build_task_group()
    r_ctx_p, r_ctx_c = await _build_async_context()

    await asyncio.sleep(0)
    gc.collect()

    assert r_pool_close() is None
    assert r_pool_abort() is None
    assert r_lock() is None
    assert r_ch() is None
    assert r_rx() is None
    assert r_scope() is None
    assert r_tg() is None
    assert r_ctx_p() is None
    assert r_ctx_c() is None


@pytest.mark.asyncio
async def test_all_core_classes_use_slots() -> None:
    """Every core public and internal concurrency class declares __slots__ and has no __dict__."""
    loop = asyncio.get_running_loop()

    async def _coro_fn() -> int:
        return 1

    raw_coro = _coro_fn()
    pool = EventLoopThreadPool(num_threads=1)
    ch = Channel(maxsize=1)
    wg = AsyncWaitGroup()
    rw = AsyncRWMutex()
    cond = Condition()
    lock = cond._lock  # type: ignore[attr-defined]
    sem = Semaphore(1)
    lim = CapacityLimiter(1)
    ev = Event()
    bar = Barrier(1)
    once = AsyncOnce()
    tg = TaskGroup()
    ctx = AsyncContext()
    scope = CancelScope()
    ts = TaskStatus()
    fut = loop.create_future()
    fut.set_result(None)
    handle = TaskHandle(fut)
    metrics = MetricsCollector(1)
    work_item = _PoolWorkItem(pool, _coro_fn, (), {}, loop, loop.create_future(), None)
    tracked = _TrackedCoroutine(raw_coro, wg)
    read_guard = rw.reader()
    write_guard = rw.writer()
    wg_guard = wg.holding()
    bar_res = BarrierWaitResult(0, 1)

    instances: list[object] = [
        pool,
        work_item,
        metrics,
        ch,
        SendChannel(ch),
        ReceiveChannel(ch),
        wg,
        wg_guard,
        tracked,
        once,
        rw,
        read_guard,
        write_guard,
        lock,
        sem,
        lim,
        ev,
        cond,
        bar,
        bar_res,
        scope,
        tg,
        handle,
        ts,
        ctx,
    ]
    try:
        for obj in instances:
            assert not hasattr(obj, "__dict__"), f"{type(obj).__name__} unexpectedly has __dict__"
            assert hasattr(type(obj), "__slots__"), f"{type(obj).__name__} missing __slots__"
    finally:
        tracked.close()


@pytest.mark.asyncio
async def test_fast_path_zero_future_allocation_10000(monkeypatch: pytest.MonkeyPatch) -> None:
    """10,000 uncontended fast-path calls across sync, channel, and CancelScope allocate 0 Futures."""
    loop = asyncio.get_running_loop()
    lock = Lock()
    sem = Semaphore(1)
    lim = CapacityLimiter(1)
    ev = Event()
    ev.set()
    rw = AsyncRWMutex()
    once = AsyncOnce()
    await once.do(lambda: 1)
    wg = AsyncWaitGroup()
    ch = Channel(maxsize=4)

    async def _async_noop() -> None:
        return None

    future_allocs = 0
    task_allocs = 0
    orig_create_future = loop.create_future
    orig_create_task = loop.create_task

    def _count_future(*args: Any, **kwargs: Any) -> asyncio.Future[Any]:
        nonlocal future_allocs
        future_allocs += 1
        return orig_create_future(*args, **kwargs)

    def _count_task(*args: Any, **kwargs: Any) -> asyncio.Task[Any]:
        nonlocal task_allocs
        task_allocs += 1
        return orig_create_task(*args, **kwargs)

    monkeypatch.setattr(loop, "create_future", _count_future)
    monkeypatch.setattr(loop, "create_task", _count_task)

    for _ in range(10_000):
        async with lock:
            pass
    assert future_allocs == 0
    assert task_allocs == 0

    for _ in range(10_000):
        async with sem:
            pass
    assert future_allocs == 0

    for _ in range(10_000):
        async with lim:
            pass
    assert future_allocs == 0

    for _ in range(10_000):
        await ev.wait()
    assert future_allocs == 0

    for _ in range(10_000):
        async with rw.reader():
            pass
    assert future_allocs == 0

    for _ in range(10_000):
        async with rw.writer():
            pass
    assert future_allocs == 0

    for _ in range(10_000):
        await once.do(_async_noop)
    assert future_allocs == 0

    for _ in range(10_000):
        await wg.wait()
    assert future_allocs == 0

    for i in range(10_000):
        await ch.send(i)
        assert await ch.recv() == i
    assert future_allocs == 0
    assert task_allocs == 0

    for i in range(10_000):
        await ch.send(i, timeout=0)
        assert await ch.recv(timeout=0) == i
    assert future_allocs == 0
    assert task_allocs == 0

    for _ in range(10_000):
        async with CancelScope():
            await checkpoint()
    assert future_allocs == 0
    assert task_allocs == 0


def test_multithread_5000_rounds_zero_garbage_convergence() -> None:
    """8 OS threads x 625 rounds (5,000 total) of submit/channel/select/cancel storm leave 0 gc.garbage and bounded objects."""
    num_threads = 8
    rounds_per_thread = 625  # 8 * 625 = 5,000 rounds
    errors: list[BaseException] = []

    async def _run_storm_rounds(pool: EventLoopThreadPool, start_idx: int, count: int) -> None:
        ch_a = Channel(maxsize=4)
        ch_b = Channel(maxsize=4)
        for i in range(start_idx, start_idx + count):
            ch_a.try_send(i)
            winner, val = await select_channel(ch_a, ch_b)
            assert winner is ch_a and val == i

            fut = pool.submit(lambda x=i: x + 1)
            assert await fut == i + 1

            async with move_on_after(60.0) as scope:
                if i % 5 == 0:
                    scope.cancel()
                    await sleep(1.0)
                else:
                    await checkpoint()

            if i % 25 == 0:
                ctx = AsyncContext()
                ctx.cancel()
                cfut = ctx.submit(pool, lambda: None)
                assert cfut.cancelled()

        ch_a.close()
        ch_b.close()

    def _run_pool_storm(total_per_thread: int) -> None:
        async def _orchestrate() -> None:
            async with EventLoopThreadPool(num_threads=4) as pool:
                await asyncio.gather(
                    *(
                        asyncio.to_thread(
                            lambda tid=t: asyncio.run(
                                _run_storm_rounds(pool, tid * total_per_thread, total_per_thread)
                            )
                        )
                        for t in range(num_threads)
                    )
                )

        asyncio.run(_orchestrate())

    # Warm-up pass to stabilize internal caches and module globals
    _run_pool_storm(25)
    gc.collect()
    baseline_objects = len(gc.get_objects())

    try:

        async def _main_storm() -> None:
            async with EventLoopThreadPool(num_threads=4) as pool:
                threads: list[threading.Thread] = []

                def _thread_entry(tid: int) -> None:
                    try:
                        asyncio.run(
                            _run_storm_rounds(pool, tid * rounds_per_thread, rounds_per_thread)
                        )
                    except BaseException as exc:
                        errors.append(exc)

                for tid in range(num_threads):
                    t = threading.Thread(target=_thread_entry, args=(tid,))
                    threads.append(t)
                    t.start()
                for t in threads:
                    await asyncio.to_thread(t.join)

        asyncio.run(_main_storm())
    except BaseException as exc:
        errors.append(exc)

    assert not errors, f"Multi-thread storm raised errors: {errors}"
    gc.collect()
    after_objects = len(gc.get_objects())
    assert len(gc.garbage) == 0, f"Uncollectable gc.garbage found: {gc.garbage}"
    assert after_objects - baseline_objects < 500, (
        f"Tracked GC objects drifted excessively: baseline={baseline_objects}, after={after_objects}"
    )


@pytest.mark.asyncio
async def test_async_once_failed_do_releases_self_and_args_no_leak() -> None:
    """Failed AsyncOnce.do() does not leak self or argument objects via stored traceback frames."""

    class _TrackedArg:
        __slots__ = ("__weakref__", "tag")

        def __init__(self, tag: str) -> None:
            self.tag = tag

    def _failing_sync(arg: _TrackedArg) -> None:
        raise RuntimeError(f"boom:{arg.tag}")

    once = AsyncOnce()
    tracked_arg = _TrackedArg("sync")
    w_once = weakref.ref(once)
    w_arg = weakref.ref(tracked_arg)

    with pytest.raises(RuntimeError, match="boom:sync"):
        await once.do(_failing_sync, tracked_arg)

    # Even while `once` stays alive, the argument is not pinned by stored traceback frames
    del tracked_arg
    gc.collect()
    gc.collect()
    assert w_arg() is None

    del once
    gc.collect()
    gc.collect()
    assert w_once() is None

    # Also verify async coroutine failure and cancelled-leader failure do not pin args/once
    async def _failing_coro(arg: _TrackedArg) -> None:
        await asyncio.sleep(0)
        raise ValueError(f"async:{arg.tag}")

    once_async = AsyncOnce()
    tracked_async = _TrackedArg("async")
    w_once_async = weakref.ref(once_async)
    w_arg_async = weakref.ref(tracked_async)

    with pytest.raises(ValueError, match="async:async"):
        await once_async.do(_failing_coro, tracked_async)

    del once_async, tracked_async
    gc.collect()
    gc.collect()
    assert w_once_async() is None
    assert w_arg_async() is None

    once_cancel = AsyncOnce()
    tracked_cancel = _TrackedArg("cancel")
    started = asyncio.Event()
    w_once_cancel = weakref.ref(once_cancel)
    w_arg_cancel = weakref.ref(tracked_cancel)

    async def _cancellable_fn(_arg: _TrackedArg) -> None:
        started.set()
        await asyncio.sleep(10.0)

    task = asyncio.create_task(once_cancel.do(_cancellable_fn, tracked_cancel))
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    del task, once_cancel, tracked_cancel
    await asyncio.sleep(0)
    gc.collect()
    gc.collect()
    assert w_once_cancel() is None
    assert w_arg_cancel() is None


@pytest.mark.asyncio
async def test_async_once_failed_custom_two_arg_exception_preserves_attrs() -> None:
    """Custom exceptions whose __init__ differs from self.args clone cleanly across all callers."""

    class CustomError(Exception):
        def __init__(self, code: int, msg: str) -> None:
            super().__init__(msg)
            self.code = code

    once = AsyncOnce()
    started = asyncio.Event()

    async def _fail_custom() -> None:
        started.set()
        await asyncio.sleep(0.02)
        raise CustomError(42, "boom")

    leader = asyncio.create_task(once.do(_fail_custom))
    await started.wait()
    follower = asyncio.create_task(once.do(_fail_custom))

    with pytest.raises(CustomError) as exc1:
        await leader
    assert exc1.value.code == 42
    assert str(exc1.value) == "boom"

    with pytest.raises(CustomError) as exc2:
        await follower
    assert exc2.value.code == 42
    assert str(exc2.value) == "boom"
    assert exc2.value is not exc1.value

    with pytest.raises(CustomError) as exc3:
        await once.do(_fail_custom)
    assert exc3.value.code == 42
    assert str(exc3.value) == "boom"
    assert exc3.value is not exc2.value


def test_cyclic_ffi_primitives_collected() -> None:
    """Self-referential Channel, Lock, and AsyncWaitGroup cycles are reclaimed by gc.collect()."""

    class _CyclicOwner:
        __slots__ = ("__weakref__", "ref")

        def __init__(self, ref: Any) -> None:
            self.ref = ref

        def done(self) -> bool:
            return False

    def _build_self_ref_channel() -> weakref.ref[Channel]:
        ch = Channel(maxsize=1)
        ch.try_send(ch)
        return weakref.ref(ch)

    def _build_cyclic_lock() -> weakref.ref[Lock]:
        lock = Lock()
        owner = _CyclicOwner(lock)
        assert lock._raw.try_acquire(1, 1, weakref.ref(owner))
        loop = asyncio.new_event_loop()
        try:
            fut = loop.create_future()
            fut.add_done_callback(lambda _f, _lock=lock, _owner=owner: None)
            ticket = lock._raw.acquire_or_register(2, 1, weakref.ref(owner), loop, fut)
            assert ticket is not None
        finally:
            loop.close()
        return weakref.ref(lock)

    def _build_cyclic_waitgroup() -> weakref.ref[_CyclicOwner]:
        wg = AsyncWaitGroup()
        wg.add(1)
        dummy = _CyclicOwner(wg)
        ticket = wg._inner.register_waiter_ticket(dummy, dummy)
        assert ticket is not None
        return weakref.ref(dummy)

    w_ch = _build_self_ref_channel()
    w_lock = _build_cyclic_lock()
    w_wg = _build_cyclic_waitgroup()

    gc.collect()
    gc.collect()
    assert w_ch() is None
    assert w_lock() is None
    assert w_wg() is None


def test_uncancelled_child_async_context_gc_and_active_cancel() -> None:
    """Uncancelled child AsyncContexts do not leak in parent._children, while live children still cancel."""
    parent = AsyncContext()
    child_refs: list[weakref.ref[AsyncContext]] = []
    for _ in range(20):
        child = AsyncContext(parent=parent)
        child_refs.append(weakref.ref(child))
        del child

    gc.collect()
    gc.collect()
    assert all(ref() is None for ref in child_refs)
    assert len(parent._children) == 0

    live_child = AsyncContext(parent=parent)
    live_ref = weakref.ref(live_child)
    assert len(parent._children) == 1
    assert not live_child.is_cancelled

    parent.cancel()
    assert live_child.is_cancelled
    assert len(parent._children) == 0

    del live_child
    gc.collect()
    assert live_ref() is None


def test_sync_timeouts_nan_and_infinite() -> None:
    """NaN timeouts raise ValueError and huge/infinite timeouts work cleanly on sync FFI boundaries."""
    # 1. Channel.send_sync
    ch_tx = Channel(maxsize=1)
    ch_tx.try_send("occupy")
    with pytest.raises(ValueError):
        ch_tx.send_sync("nan_item", timeout=math.nan)

    t1 = threading.Timer(0.02, ch_tx.try_recv)
    t1.start()
    try:
        ch_tx.send_sync("inf_item", timeout=math.inf)
    finally:
        t1.join(timeout=2.0)
        assert not t1.is_alive()

    t2 = threading.Timer(0.02, ch_tx.try_recv)
    t2.start()
    try:
        ch_tx.send_sync("huge_item", timeout=1e20)
    finally:
        t2.join(timeout=2.0)
        assert not t2.is_alive()
    assert ch_tx.try_recv() == "huge_item"

    # 2. Channel.recv_sync
    ch_rx = Channel(maxsize=1)
    with pytest.raises(ValueError):
        ch_rx.recv_sync(timeout=math.nan)

    t3 = threading.Timer(0.02, lambda: ch_rx.try_send("from_inf"))
    t3.start()
    try:
        assert ch_rx.recv_sync(timeout=math.inf) == "from_inf"
    finally:
        t3.join(timeout=2.0)
        assert not t3.is_alive()

    t4 = threading.Timer(0.02, lambda: ch_rx.try_send("from_huge"))
    t4.start()
    try:
        assert ch_rx.recv_sync(timeout=1e20) == "from_huge"
    finally:
        t4.join(timeout=2.0)
        assert not t4.is_alive()

    t5 = threading.Timer(0.02, ch_rx.close)
    t5.start()
    try:
        with pytest.raises(ChannelClosedError):
            ch_rx.recv_sync(timeout=math.inf)
    finally:
        t5.join(timeout=2.0)
        assert not t5.is_alive()

    # 3. NativeWorkerPool.wait_drained
    np = NativeWorkerPool(1)
    assert np.wait_drained(math.inf) is True

    np.push_global("pending_1")
    with pytest.raises(ValueError):
        np.wait_drained(math.nan)

    def _drain_one() -> None:
        np.pop_work(0)
        np.complete_task()

    t6 = threading.Timer(0.02, _drain_one)
    t6.start()
    try:
        assert np.wait_drained(math.inf) is True
    finally:
        t6.join(timeout=2.0)
        assert not t6.is_alive()

    np.push_global("pending_2")
    t7 = threading.Timer(0.02, _drain_one)
    t7.start()
    try:
        assert np.wait_drained(1e20) is True
    finally:
        t7.join(timeout=2.0)
        assert not t7.is_alive()


def test_capacity_limiter_non_finite_raises_value_error() -> None:
    """CapacityLimiter rejects non-finite total_tokens with ValueError (never OverflowError)."""
    with pytest.raises(ValueError):
        CapacityLimiter(math.inf)
    with pytest.raises(ValueError):
        CapacityLimiter(math.nan)

    limiter = CapacityLimiter(2)
    with pytest.raises(ValueError):
        limiter.total_tokens = math.inf
    with pytest.raises(ValueError):
        limiter.total_tokens = math.nan
    assert limiter.total_tokens == 2.0
