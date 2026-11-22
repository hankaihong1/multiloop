"""Tests for multiloop TaskGroup (structured concurrency)."""

import asyncio
import threading
from typing import Any

import pytest

from multiloop import Event, TaskState, current_effective_deadline, sleep
from multiloop._taskgroup import TaskGroup, TaskHandle, TaskStatus, _TGState


async def _worker(value: Any) -> Any:
    await asyncio.sleep(0)
    return value


async def _worker_slow(value: Any, delay: float = 0.1) -> Any:
    await sleep(delay)
    return value


async def _worker_fail(msg: str) -> None:
    await asyncio.sleep(0)
    raise ValueError(msg)


async def _worker_fail_slow(msg: str, delay: float = 0.1) -> None:
    await sleep(delay)
    raise ValueError(msg)


# ---------------------------------------------------------------------------
# Test 1: all children succeed
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_all_succeed() -> None:
    """Spawn 3 tasks — every one returns cleanly, TaskGroup exits without error."""
    async with TaskGroup() as tg:
        h1 = tg.start_soon(_worker, 1)
        h2 = tg.start_soon(_worker, 2)
        h3 = tg.start_soon(_worker, 3)

    assert await h1 == 1
    assert await h2 == 2
    assert await h3 == 3
    assert h1.status == TaskState.FINISHED
    assert h2.status == TaskState.FINISHED
    assert h3.status == TaskState.FINISHED


# ---------------------------------------------------------------------------
# Test 2: one failure cancels siblings, single exception re-raised
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_one_fails_cancels_others() -> None:
    """Task 0 raises ValueError; tasks 1 & 2 are cancelled by the scope.

    Only the single ValueError is re-raised (not wrapped in a group).
    """
    h1: TaskHandle | None = None
    h2: TaskHandle | None = None

    with pytest.raises(ValueError, match="boom"):
        async with TaskGroup() as tg:
            h1 = tg.start_soon(_worker_fail, "boom")  # fails fast
            h2 = tg.start_soon(_worker_slow, "slow")  # gets cancelled

    # h1 has the ValueError.
    assert h1 is not None
    assert h1.status == TaskState.FINISHED
    assert isinstance(h1.exception, ValueError)

    # h2 should have been cancelled by the sibling-cancel.
    assert h2 is not None
    assert h2.status == TaskState.FINISHED
    with pytest.raises(asyncio.CancelledError):
        _ = h2.exception


# ---------------------------------------------------------------------------
# Test 3: multiple independent failures → BaseExceptionGroup
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_multiple_fail_aggregate() -> None:
    """Two tasks raise different exceptions concurrently.

    Both exceptions appear in a BaseExceptionGroup.
    """
    with pytest.raises(BaseExceptionGroup, match="taskgroup crashed") as exc_info:
        async with TaskGroup() as tg:
            tg.start_soon(_worker_fail, "e1")  # both fire ~simultaneously
            tg.start_soon(_worker_fail, "e2")

    eg = exc_info.value
    exceptions = eg.exceptions
    assert len(exceptions) == 2
    msgs = {str(e) for e in exceptions}
    assert msgs == {"e1", "e2"}


# ---------------------------------------------------------------------------
# Test 4: TaskGroup.start() blocks until task_status.started()
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_start_with_task_status() -> None:
    """``tg.start()`` does not return before the spawned task calls
    ``task_status.started()``."""

    started_flag: bool = False

    async def child(ts: TaskStatus) -> str:
        nonlocal started_flag
        # Simulate expensive initialisation.
        await asyncio.sleep(0.05)
        ts.started()
        started_flag = True
        await asyncio.sleep(0.02)
        return "done"

    async with TaskGroup() as tg:
        handle = await tg.start(child)
        # By the time start() returns, started() has been called.
        assert started_flag is True
        assert handle.status == TaskState.STARTED

    assert handle.status == TaskState.FINISHED
    assert handle.result == "done"


# ---------------------------------------------------------------------------
# Test 5: CancelledError from sibling-cancel is filtered
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cancelled_children_filtered() -> None:
    """When the scope cancels siblings, their CancelledError does NOT appear
    in the exception group (only the originating error does)."""

    h_slow: TaskHandle | None = None

    with pytest.raises(ValueError, match="original"):
        async with TaskGroup() as tg:
            h_slow = tg.start_soon(_worker_slow, "ok", 0.1)  # will get cancelled
            tg.start_soon(_worker_fail, "original")  # triggers sibling-cancel

    # The slow task was cancelled by sibling-cancel — that CancelledError
    # is filtered and should NOT be in the raised exception.
    assert h_slow is not None
    with pytest.raises(asyncio.CancelledError):
        _ = h_slow.exception


# ---------------------------------------------------------------------------
# Test 6: TaskHandle.result / .exception raise RuntimeError when unfinished
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_handle_properties_raise_before_done() -> None:
    """Accessing .result or .exception before the task finishes raises
    RuntimeError."""
    handle: TaskHandle | None = None

    async with TaskGroup() as tg:
        handle = tg.start_soon(_worker_slow, 42, 0.05)
        with pytest.raises(RuntimeError, match="not finished"):
            _ = handle.result
        with pytest.raises(RuntimeError, match="not finished"):
            _ = handle.exception

    assert handle.result == 42
    assert handle.exception is None


# ---------------------------------------------------------------------------
# TaskGroup shield + start crash (Wave 2 regression test)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_taskgroup_start_child_crash_cleans_up():
    """TaskGroup.start() cancels siblings when the spawned child crashes after init.

    Post-fix: when a child started via tg.start() crashes after calling
    task_status.started(), the sibling-cancel mechanism in _wait_children
    cancels remaining children and propagates the exception.
    """
    cleanup_called = False

    async def child_crashes_after_init(ts: TaskStatus) -> None:
        ts.started()
        await asyncio.sleep(0.02)
        raise ValueError("post-init crash")

    async def innocent_child() -> int:
        nonlocal cleanup_called
        try:
            await sleep(10)
        except asyncio.CancelledError:
            cleanup_called = True
            raise
        return 0

    with pytest.raises(ValueError, match="post-init crash"):
        async with TaskGroup() as tg:
            tg.start_soon(innocent_child)
            await tg.start(child_crashes_after_init)

    assert cleanup_called, "sibling child was not cancelled after start child crash"


# ---------------------------------------------------------------------------
# Regression tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_body_and_child_exceptions_both_visible() -> None:
    """a body exception must not be silently dropped when children fail."""

    async def child_fail() -> None:
        raise ValueError("child")

    with pytest.raises(BaseExceptionGroup) as ei:
        async with TaskGroup() as tg:
            tg.start_soon(child_fail)
            # Yield once so the child actually runs and fails before the
            # body raises — both exceptions must surface ().
            await asyncio.sleep(0)
            raise KeyError("body")

    names = {type(e).__name__ for e in ei.value.exceptions}
    assert "KeyError" in names, f"body exception lost: {names}"
    assert "ValueError" in names, f"child exception lost: {names}"


@pytest.mark.asyncio
async def test_body_cancel_propagates_without_merge() -> None:
    """Cancelling a TaskGroup body cancels stuck children promptly and propagates CancelledError without merging."""

    async def stuck() -> None:
        await Event().wait()

    async def run() -> None:
        async with TaskGroup() as tg:
            tg.start_soon(stuck)
            await Event().wait()

    task = asyncio.create_task(run())
    await asyncio.sleep(0.05)  # let the group body park
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=2.0)


@pytest.mark.asyncio
async def test_taskgroup_not_reusable() -> None:
    """TS-3: a crashed TaskGroup must raise RuntimeError on re-entry, not a
    confusing CancelledError."""

    async def boom() -> None:
        raise ValueError("child")

    with pytest.raises(ValueError):
        async with TaskGroup() as tg:
            tg.start_soon(boom)

    with pytest.raises(RuntimeError, match="reus"):
        async with tg:
            pass


@pytest.mark.asyncio
async def test_cancel_all_concurrent_start_soon() -> None:
    """B2: cancel_all from another thread must not race with start_soon
    (free-threaded: an unlocked iteration raises mid-loop)."""

    async def noop() -> None:
        await asyncio.sleep(0)

    tg = TaskGroup()
    stop = threading.Event()
    errors: list[BaseException] = []

    def hammer() -> None:
        try:
            for _ in range(500):
                tg.cancel_all()
        except BaseException as e:
            errors.append(e)
        finally:
            stop.set()

    t = threading.Thread(target=hammer)
    t.start()
    try:
        for _ in range(500):
            tg.start_soon(noop)
            await asyncio.sleep(0)
    finally:
        t.join(timeout=10)

    assert not errors, f"cancel_all raced with start_soon: {errors}"

    # Cleanup: cancel everything and let the children finish.
    tg.cancel_all()
    handles = list(tg._children)
    await asyncio.gather(*(h._task for h in handles), return_exceptions=True)


# ───────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_start_soon_after_exit_raises() -> None:
    """start_soon() after the group exited must raise
    instead of silently spawning an orphan task that nobody waits for —
    and the orphan must be cancelled, not left running."""
    tg = TaskGroup()
    async with tg:
        pass
    with pytest.raises(RuntimeError, match="not active"):
        tg.start_soon(_worker, "x")
    await asyncio.sleep(0.01)
    assert not tg._children


@pytest.mark.asyncio
async def test_reenter_after_body_exception_clean() -> None:
    """R5 revision C: re-entering after a body-exception exit must start a fresh
    lifecycle — the old children (pre-cancelled by __aexit__) must not be
    re-collected, or their stale CancelledError resurfaces on the second
    exit."""
    tg = TaskGroup()

    async def boom() -> None:
        await asyncio.sleep(0.01)
        raise KeyError("child")

    with pytest.raises(KeyError, match="body"):
        async with tg:
            tg.start_soon(boom)
            raise KeyError("body")  # body fails first → pre-cancel path

    # Pre-fix: the second exit re-collected the cancelled child and raised
    # a stale CancelledError here.  Fixed: fresh lifecycle, clean exit.
    async with tg:
        pass


@pytest.mark.asyncio
async def test_reenter_after_normal_exit_ok() -> None:
    """normal exit → re-enter is a fresh lifecycle (children cleared,
    start_soon works again)."""
    tg = TaskGroup()
    async with tg:
        h = tg.start_soon(_worker, "a")
        assert await h == "a"
    async with tg:
        h = tg.start_soon(_worker, "b")
        assert await h == "b"


# ---------------------------------------------------------------------------
# Regression test: start() must not hang when the child fails before started()
# ---------------------------------------------------------------------------


async def _child_fails_before_started(_ts: TaskStatus) -> None:
    raise ValueError("boom before started")


@pytest.mark.asyncio
async def test_start_child_failure_raises_not_hangs() -> None:
    """A child raising before calling started() surfaces the single exception from start() without hanging."""
    with pytest.raises(ValueError, match="boom before started") as excinfo:
        async with TaskGroup() as tg:
            await asyncio.wait_for(tg.start(_child_fails_before_started), timeout=2.0)
    assert not isinstance(excinfo.value, BaseExceptionGroup)


@pytest.mark.asyncio
async def test_start_child_exits_without_started_raises() -> None:
    """A child that exits normally without calling started() raises
    RuntimeError (trio/anyio parity)."""

    async def no_started(task_status: TaskStatus) -> None:
        return None

    with pytest.raises(RuntimeError, match="started"):
        async with TaskGroup() as tg:
            await tg.start(no_started)


# ---------------------------------------------------------------------------
# R8 Unit 1: cancelled children are not errors (trio/anyio parity)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cancel_all_then_normal_exit_is_silent() -> None:
    """R8 Unit 1: cancel_all() victims must not be reported at group exit.

    Pre-fix the soft-exit branch raised the victims' CancelledErrors into
    the host task: a public-API cancel_all() followed by a
    normal body exit failed the whole task with a spurious CE group.
    """

    async def child() -> None:
        await sleep(30)

    async with TaskGroup() as tg:
        tg.start_soon(child)
        tg.start_soon(child)
        await asyncio.sleep(0.05)
        tg.cancel_all()
    # No raise: the group exits cleanly.


@pytest.mark.asyncio
async def test_start_failure_siblings_not_reported() -> None:
    """R8 Unit 1: siblings cancelled by start()'s failure path are silent.

    Pre-fix the sibling's CancelledError was collected (not in
    cancelled_by_scope) and the group exit raised it via the soft-exit
    branch while the real ValueError had already been delivered by
    start()."""

    async def failing_child(task_status: TaskStatus) -> None:
        task_status.started()
        raise ValueError("boom after started")

    async def sibling() -> None:
        await sleep(30)

    async with TaskGroup() as tg:
        tg.start_soon(sibling)
        with pytest.raises(ValueError, match="boom after started"):
            await tg.start(failing_child)
    # No raise at exit.


@pytest.mark.asyncio
async def test_externally_cancelled_child_is_silent() -> None:
    """R8 Unit 1: a child cancelled by task.cancel() is not an error.

    trio/anyio both absorb externally cancelled children (anyio filters
    every child CancelledError in task_done); multiloop previously raised
    the CE out of the group and spuriously cancelled the host task."""

    async def child() -> None:
        await asyncio.sleep(30)

    async with TaskGroup() as tg:
        h = tg.start_soon(child)
        await asyncio.sleep(0.05)
        h._task.cancel()
    # No raise: external child cancellation is absorbed.


# ---------------------------------------------------------------------------
# R8 Unit 2: host cancellation waits for children before the group exits
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_host_cancel_waits_for_children_to_finish() -> None:
    """R8 Unit 2: when the host is cancelled while the group waits, all
    children must be FINISHED before the block exits (trio/anyio parity).

    Pre-fix the host's except handler ran while the child's finally had
    not — the structured-
    concurrency guarantee was broken on the cancellation path."""

    child_finally_ran = asyncio.Event()

    async def slow_child() -> None:
        try:
            await sleep(30)
        finally:
            child_finally_ran.set()

    saw_flag_inside_except: dict[str, bool] = {}

    async def host() -> None:
        try:
            async with TaskGroup() as tg:
                tg.start_soon(slow_child)
                await asyncio.sleep(0.05)  # let the child start
        except BaseException:
            saw_flag_inside_except["flag"] = child_finally_ran.is_set()
            raise

    host_task = asyncio.create_task(host())
    await asyncio.sleep(0.1)  # host is now blocked waiting on the child
    host_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await host_task
    assert saw_flag_inside_except["flag"] is True
    assert child_finally_ran.is_set()


# ---------------------------------------------------------------------------
# R8 Unit 3: children spawned before the first entry stay tracked
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_preexisting_children_tracked_through_first_entry() -> None:
    """R8 Unit 3: children spawned BEFORE the first entry stay tracked —
    the group waits for them at exit."""

    finished = asyncio.Event()

    async def child() -> None:
        await asyncio.sleep(0.1)
        finished.set()

    tg = TaskGroup()
    h = tg.start_soon(child)
    async with tg:
        await asyncio.sleep(0.02)
    assert h._task.done()
    assert finished.is_set()


# ---------------------------------------------------------------------------
# R10 P2: start() cancellation semantics (trio parity)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_start_child_self_cancelled_after_started_returns_handle() -> None:
    """R10 P2: a child that raises CancelledError right after started() must
    not make start() itself raise — trio parity: start() completes once
    started() is called; the failure surfaces via the group's soft exit.
    Pre-fix: task.exception() raised CancelledError from start()'s finally."""
    started_flag = asyncio.Event()

    async def worker(task_status: TaskStatus) -> None:
        task_status.started()
        started_flag.set()
        raise asyncio.CancelledError()

    handle_result: TaskHandle | None = None
    with pytest.raises(asyncio.CancelledError):
        async with TaskGroup() as tg:
            handle_result = await tg.start(worker)
            await started_flag.wait()
    # Pre-fix the start() call raised and handle_result stayed None.
    assert handle_result is not None


@pytest.mark.asyncio
async def test_start_child_cancelled_before_started_cancels_siblings() -> None:
    """R10 P2: when the child is cancelled before started(), start()
    propagates the cancellation AND cancels siblings.  Pre-fix: the
    finally's task.exception() raised before the consume/sibling-cancel
    code ran, orphaning the siblings (they were never cancelled)."""
    child_task: asyncio.Task[Any] | None = None
    sibling_cancelled = asyncio.Event()

    async def worker(task_status: TaskStatus) -> None:
        nonlocal child_task
        child_task = asyncio.current_task()
        await asyncio.sleep(10)  # never calls started()

    async def sibling() -> None:
        try:
            await sleep(10)
        except asyncio.CancelledError:
            sibling_cancelled.set()
            raise

    async with TaskGroup() as tg:
        # Fire-and-forget: awaiting the returned handle would block until
        # the 10s sibling finishes (TaskHandle.__await__ delegates to the
        # underlying task), which would defeat the cancellation check below.
        tg.start_soon(sibling)
        start_task = asyncio.create_task(tg.start(worker))
        while child_task is None:
            await asyncio.sleep(0)
        child_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await start_task
        await asyncio.wait_for(sibling_cancelled.wait(), timeout=1.0)


@pytest.mark.asyncio
async def test_child_failure_preserves_external_cross_thread_cancel() -> None:
    """R10 P5: a synchronous cross-thread task.cancel() landing during
    child-failure handling must still be delivered after the group exits.
    Pre-fix: cancel_siblings' uncancel plus the __aexit__ compensation
    consumed the count, and the host never saw its cancellation."""
    import threading
    import time

    host_task: asyncio.Task[Any] | None = None
    child_parked = asyncio.Event()
    release_child = asyncio.Event()
    cancel_done = threading.Event()

    async def failing_child() -> None:
        child_parked.set()
        await release_child.wait()
        raise ValueError("boom")

    async def run_group() -> None:
        nonlocal host_task
        host_task = asyncio.current_task()
        try:
            async with TaskGroup() as tg:
                await tg.start_soon(failing_child)
        except BaseExceptionGroup:
            pass
        await asyncio.sleep(5)  # the external cancel must fire here

    def _cancel_from_thread() -> None:
        # Task.cancel() from a foreign thread is synchronous: it can land
        # in the middle of the failure-handling sync section.
        while host_task is None:
            time.sleep(0.001)
        host_task.cancel()
        cancel_done.set()

    t = asyncio.create_task(run_group())
    threading.Thread(target=_cancel_from_thread, daemon=True).start()
    await child_parked.wait()
    await asyncio.to_thread(cancel_done.wait)
    release_child.set()
    # Either interleaving must end with the host's cancellation delivered:
    # (a) the cancel landed while the host was waiting on the child — the
    #     CancelledError path cancels and drains children, then propagates;
    # (b) the cancel landed during failure handling — the R10 P5 fix keeps
    #     the count alive so the CE fires at the next await after the
    #     group exits.  Pre-fix (b) swallowed the count and the task kept
    #     sleeping past the timeout.
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(asyncio.shield(t), timeout=2.0)


@pytest.mark.asyncio
async def test_taskgroup_cross_loop_rejection() -> None:
    """TaskGroup must reject start_soon and start when called from a foreign loop or thread."""
    async with TaskGroup() as tg:
        # Create a separate thread with a new event loop
        def foreign_worker(errs: list[Exception]) -> None:
            new_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(new_loop)

            async def attempt():
                async def dummy():
                    pass

                # start_soon from foreign loop must raise RuntimeError
                try:
                    tg.start_soon(dummy)
                except RuntimeError as exc:
                    errs.append(exc)

            try:
                new_loop.run_until_complete(attempt())
            finally:
                new_loop.close()

        errors: list[Exception] = []
        t = threading.Thread(target=foreign_worker, args=(errors,))
        t.start()
        t.join(timeout=2.0)

        assert len(errors) == 1
        assert "physically scoped to a single event loop" in str(errors[0])


@pytest.mark.asyncio
async def test_taskgroup_max_concurrency_throttling() -> None:
    """TaskGroup(max_concurrency=N) strictly caps concurrent active child coroutines."""
    active = 0
    max_active = 0

    async def worker(idx: int) -> int:
        nonlocal active, max_active
        active += 1
        max_active = max(max_active, active)
        await asyncio.sleep(0.01)
        active -= 1
        return idx

    async with TaskGroup(max_concurrency=3) as tg:
        handles = [tg.start_soon(worker, i) for i in range(15)]

    assert max_active == 3
    results = [await h for h in handles]
    assert results == list(range(15))


@pytest.mark.asyncio
async def test_taskgroup_shared_limiter() -> None:
    """Multiple TaskGroups can share an external CapacityLimiter."""
    from multiloop import CapacityLimiter

    shared_limiter = CapacityLimiter(2)
    active = 0
    max_active = 0

    async def worker():
        nonlocal active, max_active
        active += 1
        max_active = max(max_active, active)
        await asyncio.sleep(0.01)
        active -= 1

    async def run_group():
        async with TaskGroup(limiter=shared_limiter) as tg:
            for _ in range(5):
                tg.start_soon(worker)

    await asyncio.gather(run_group(), run_group())
    assert max_active == 2


@pytest.mark.asyncio
async def test_taskgroup_cross_loop_pool_dispatch() -> None:
    """TaskGroup(pool=pool) dispatches tasks across pool worker loops while enforcing structured concurrency."""
    from multiloop.pool import EventLoopThreadPool

    async def get_worker_tid(x: int) -> tuple[int, int]:
        tid = threading.get_ident()
        await asyncio.sleep(0.01)
        return x, tid

    async with EventLoopThreadPool(num_threads=4) as pool:
        async with TaskGroup(pool=pool) as tg:
            handles = [tg.start_soon(get_worker_tid, i) for i in range(8)]

        results = [await h for h in handles]
        values = [r[0] for r in results]
        tids = {r[1] for r in results}

        assert values == list(range(8))
        assert threading.get_ident() not in tids


@pytest.mark.asyncio
async def test_taskgroup_cross_loop_pool_failure_cascading_cancel() -> None:
    """TaskGroup(pool=pool) cancels cross-loop sibling tasks when one task fails."""
    from multiloop.pool import EventLoopThreadPool

    async def fast_fail() -> None:
        await asyncio.sleep(0.01)
        raise RuntimeError("cross-loop failure")

    cancelled_slow = False

    async def slow_worker() -> None:
        nonlocal cancelled_slow
        try:
            await sleep(1.0)
        except asyncio.CancelledError:
            cancelled_slow = True
            raise

    async with EventLoopThreadPool(num_threads=2) as pool:
        with pytest.raises(RuntimeError, match="cross-loop failure"):
            async with TaskGroup(pool=pool) as tg:
                tg.start_soon(fast_fail)
                tg.start_soon(slow_worker)

        assert cancelled_slow is True


@pytest.mark.asyncio
async def test_taskgroup_cross_loop_start_status() -> None:
    """TaskGroup(pool=pool).start properly syncs task_status across event loops."""
    from multiloop.pool import EventLoopThreadPool

    async def init_worker(task_status: TaskStatus, value: int) -> int:
        await asyncio.sleep(0.01)
        task_status.started()
        await asyncio.sleep(0.02)
        return value * 10

    async with (
        EventLoopThreadPool(num_threads=2) as pool,
        TaskGroup(pool=pool) as tg,
    ):
        handle = await tg.start(init_worker, 5)
        assert handle.status in (TaskState.STARTED, TaskState.FINISHED)
        res = await handle
        assert res == 50


@pytest.mark.asyncio
async def test_async_context_task_group_integration() -> None:
    """AsyncContext.task_group creates a TaskGroup tied to the context's cancellation."""
    from multiloop.context import AsyncContext
    from multiloop.pool import EventLoopThreadPool

    ctx = AsyncContext()
    cancelled_task = False

    async def worker() -> None:
        nonlocal cancelled_task
        try:
            await sleep(1.0)
        except asyncio.CancelledError:
            cancelled_task = True
            raise

    async with EventLoopThreadPool(num_threads=2) as pool:
        with pytest.raises(asyncio.CancelledError):
            async with ctx.task_group(pool=pool) as tg:
                tg.start_soon(worker)
                await asyncio.sleep(0.05)
                ctx.cancel()

        assert cancelled_task is True


@pytest.mark.asyncio
async def test_taskgroup_cross_loop_concurrent_start_safety() -> None:
    """Concurrent TaskGroup(pool=pool).start across multiple OS threads with started() values."""
    from multiloop.pool import EventLoopThreadPool

    async def worker(task_status: TaskStatus, item_id: int) -> int:
        await asyncio.sleep(0.005)
        task_status.started()
        await asyncio.sleep(0.01)
        return item_id * 2

    async with (
        EventLoopThreadPool(num_threads=4) as pool,
        TaskGroup(pool=pool) as tg,
    ):
        handles = await asyncio.gather(*(tg.start(worker, i) for i in range(10)))
        for handle in handles:
            assert handle.status in (TaskState.STARTED, TaskState.FINISHED)
        results = await asyncio.gather(*handles)
        assert results == [i * 2 for i in range(10)]


@pytest.mark.asyncio
async def test_async_context_taskgroup_weakref_gc_reclaim() -> None:
    """TaskGroup instances registered in AsyncContext must be reclaimed by GC via WeakSet when dropped."""
    import gc
    import weakref

    from multiloop.context import AsyncContext

    ctx = AsyncContext()

    def create_tg() -> weakref.ref[TaskGroup]:
        tg = ctx.task_group()
        ref = weakref.ref(tg)
        assert len(ctx._task_groups) == 1
        return ref

    tg_ref = create_tg()
    gc.collect()

    assert tg_ref() is None
    assert len(ctx._task_groups) == 0


# ---------------------------------------------------------------------------
# Phase 4 TDD: TaskGroup._cancel_scope cleanup (P0-2) & AsyncContext.task_group(limiter=...)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("limit_val", "total_tasks"),
    [
        (1, 4),
        (2, 6),
        (3, 6),
    ],
)
async def test_taskgroup_context_limiter_valid(limit_val: int, total_tasks: int) -> None:
    """Valid parameterized tests verifying AsyncContext.task_group(limiter=...) enforces concurrency budget."""
    from multiloop._sync import CapacityLimiter
    from multiloop.context import AsyncContext

    ctx = AsyncContext()
    limiter = CapacityLimiter(limit_val)
    active = 0
    peak = 0

    async def worker(idx: int) -> int:
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0.01)
        active -= 1
        return idx

    async with ctx.task_group(limiter=limiter) as tg:
        handles = [tg.start_soon(worker, i) for i in range(total_tasks)]

    assert [await h for h in handles] == list(range(total_tasks))
    assert 1 <= peak <= limit_val


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("exc_cls", "num_children"),
    [
        (asyncio.CancelledError, 1),
        (asyncio.CancelledError, 3),
        (RuntimeError, 1),
        (RuntimeError, 2),
    ],
)
async def test_taskgroup_wait_children_abort_cleans_scope_boundary(
    exc_cls: type[BaseException], num_children: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Boundary parameterized tests verifying TaskGroup._cancel_scope.__aexit__ runs if _wait_children raises."""
    tg = TaskGroup()
    orig_wait = TaskGroup._wait_children

    async def raising_wait(self_tg: TaskGroup, pre_cancelled: Any = None) -> list[BaseException]:
        await orig_wait(self_tg, pre_cancelled)
        raise exc_cls("abort inside _wait_children")

    monkeypatch.setattr(TaskGroup, "_wait_children", raising_wait)

    deadline_before = current_effective_deadline()
    with pytest.raises(exc_cls):
        async with tg:
            for i in range(num_children):
                tg.start_soon(_worker, i)

    assert current_effective_deadline() == deadline_before
    assert tg._state == _TGState.EXITED_CLEAN
    assert tg._cancel_scope._state is None
    assert tg._cancel_scope._entry_id is None
    assert tg._wake_fut is None
    await sleep(0)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("max_concurrency", "use_limiter", "expected_exc"),
    [
        (2, True, ValueError),
        (0, False, ValueError),
        (-1, False, ValueError),
    ],
)
async def test_taskgroup_context_limiter_error(
    max_concurrency: int | None, use_limiter: bool, expected_exc: type[BaseException]
) -> None:
    """Error parameterized tests verifying AsyncContext.task_group rejects invalid limiter configurations."""
    from multiloop._sync import CapacityLimiter
    from multiloop.context import AsyncContext

    ctx = AsyncContext()
    limiter = CapacityLimiter(2) if use_limiter else None
    with pytest.raises(expected_exc):
        ctx.task_group(max_concurrency=max_concurrency, limiter=limiter)


@pytest.mark.asyncio
async def test_repro_child_crash_immediately_cancels_blocked_host_body() -> None:
    """A child task raising a non-cancellation exception must immediately interrupt a blocked host body."""

    async def crashing_child() -> None:
        await asyncio.sleep(0.01)
        raise ValueError("child boom")

    async def run_group() -> None:
        async with TaskGroup() as tg:
            tg.start_soon(crashing_child)
            await sleep(10.0)

    with pytest.raises(ValueError, match="child boom"):
        await asyncio.wait_for(run_group(), timeout=0.5)


@pytest.mark.asyncio
async def test_repro_cancel_all_cancels_host_scope() -> None:
    """Calling tg.cancel_all() must also cancel the host task's _cancel_scope."""

    async def trigger_cancel(tg: TaskGroup) -> None:
        await asyncio.sleep(0.01)
        tg.cancel_all()

    async def run_group() -> None:
        async with TaskGroup() as tg:
            tg.start_soon(trigger_cancel, tg)
            await sleep(10.0)

    await asyncio.wait_for(run_group(), timeout=0.5)


@pytest.mark.asyncio
async def test_repro_host_cancel_during_wait_children_preserves_child_exception() -> None:
    """External host_task.cancel() while draining siblings in _wait_children must not discard child failures."""
    in_sibling_cleanup = asyncio.Event()
    sibling_cleanup_done = asyncio.Event()

    async def crashing_child() -> None:
        await asyncio.sleep(0.005)
        raise ValueError("critical child failure")

    async def slow_cleanup_sibling() -> None:
        try:
            await sleep(10.0)
        finally:
            in_sibling_cleanup.set()
            await asyncio.sleep(0.03)
            sibling_cleanup_done.set()

    async def host() -> None:
        async with TaskGroup() as tg:
            tg.start_soon(crashing_child)
            tg.start_soon(slow_cleanup_sibling)

    host_task = asyncio.create_task(host())
    await in_sibling_cleanup.wait()
    host_task.cancel()

    with pytest.raises(ValueError, match="critical child failure"):
        await asyncio.wait_for(host_task, timeout=1.0)
    assert sibling_cleanup_done.is_set()


@pytest.mark.asyncio
async def test_taskgroup_limiter_cancel_closes_unstepped_coro() -> None:
    """Cancelling TaskGroup while a coroutine object awaits CapacityLimiter closes it without RuntimeWarning."""
    import gc
    import warnings

    async def blocker() -> None:
        await sleep(10.0)

    async def unstepped_target() -> None:
        await sleep(10.0)

    coro2 = unstepped_target()

    with warnings.catch_warnings(record=True) as recorded:
        warnings.simplefilter("always")
        async with TaskGroup(max_concurrency=1) as tg:
            tg.start_soon(blocker)
            await asyncio.sleep(0)

            tg.start_soon(coro2)
            await asyncio.sleep(0)

            tg.cancel_all()

        assert coro2.cr_frame is None
        del coro2
        gc.collect()

        coro_warnings = [w for w in recorded if "never awaited" in str(w.message)]
        assert len(coro_warnings) == 0


@pytest.mark.asyncio
async def test_taskgroup_pool_externally_cancelled_child_is_silent() -> None:
    """External fut.cancel() on a TaskGroup(pool=pool) child must not fail the group or cancel siblings."""
    from multiloop.pool import EventLoopThreadPool

    sibling_finished = False
    child_started = threading.Event()
    child_cancelled = threading.Event()

    async def slow_child() -> None:
        child_started.set()
        try:
            await sleep(10.0)
        except asyncio.CancelledError:
            child_cancelled.set()
            raise

    async def ok_sibling() -> int:
        nonlocal sibling_finished
        await sleep(0.05)
        sibling_finished = True
        return 42

    async with EventLoopThreadPool(num_threads=2) as pool:
        async with TaskGroup(pool=pool) as tg:
            h_cancel = tg.start_soon(slow_child)
            h_ok = tg.start_soon(ok_sibling)
            await asyncio.to_thread(child_started.wait, 2.0)
            assert child_started.is_set()
            h_cancel._task.cancel()

        assert sibling_finished is True
        assert await h_ok == 42
        assert h_cancel._task.cancelled() is True
        await asyncio.to_thread(child_cancelled.wait, 2.0)
        assert child_cancelled.is_set()


@pytest.mark.asyncio
async def test_taskgroup_pool_child_manual_cancelled_error_propagates() -> None:
    """A TaskGroup(pool=pool) child that manually raises CancelledError triggers sibling cancel and propagates."""
    from multiloop.pool import EventLoopThreadPool

    sibling_cancelled = threading.Event()

    async def manual_cancel_child() -> None:
        await asyncio.sleep(0.01)
        raise asyncio.CancelledError()

    async def slow_sibling() -> None:
        try:
            await sleep(10.0)
        except asyncio.CancelledError:
            sibling_cancelled.set()
            raise

    async with EventLoopThreadPool(num_threads=2) as pool:
        with pytest.raises(asyncio.CancelledError):
            async with TaskGroup(pool=pool) as tg:
                tg.start_soon(manual_cancel_child)
                tg.start_soon(slow_sibling)

        await asyncio.to_thread(sibling_cancelled.wait, 2.0)
        assert sibling_cancelled.is_set()
