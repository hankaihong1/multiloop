"""Stress tests for multiloop running on Python 3.14t (free-threaded, no GIL).

These tests exercise cross-thread safety paths that are unique to the
free-threaded build where multiple OS threads can truly execute Python
bytecode concurrently.

All tests are skipped on non-t builds via ``@pytest.mark.free_threading``
which gate-keeps on ``sys._is_gil_enabled() is False``.
"""

import asyncio
import sys
import threading
from typing import Any

import pytest

# ── gate: only run on free-threaded builds ──────────────────────────────────

free_threaded = hasattr(sys, "_is_gil_enabled") and sys._is_gil_enabled() is False

# Note: pyproject.toml's markers define the free_threading marker; it is
# attached to pytestmark together with skipif so that `-m "not free_threading"`
# deselects this whole file (under a GIL build the file is skipped anyway via
# skipif; the deselection only matters for CI matrices that distinguish
# free-threading scenarios).
pytestmark = [
    pytest.mark.skipif(not free_threaded, reason="requires Python 3.14t (free-threaded) build"),
    pytest.mark.free_threading,
]

pytest.importorskip("multiloop")

from multiloop import (  # noqa: E402
    Barrier,
    CancelScope,
    CapacityLimiter,
    EventLoopThreadPool,
    ThreadPoolClosedError,
    sleep,
)
from multiloop.testing import wait_all_tasks_blocked  # noqa: E402

# ---------------------------------------------------------------------------
# CancelScope — cross-thread cancel + read under pressure
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cancel_scope_cross_thread_stress():
    """CancelScope.cancel() from multiple threads while reading cancel_called.

    On free-threaded builds, cancel() may interleave with cancel_called
    reads — the internal _lock keeps the flag consistent alongside RawTaskCancelState.
    """
    scope = CancelScope()
    read_values: list[bool] = []
    mutex = threading.Lock()

    async def worker():
        # WHY: do NOT catch CancelledError here.  The cancellation injected by
        # scope.cancel() must penetrate to the task itself; otherwise the
        # worker completes normally, `await task` never raises, and the
        # pytest.raises(CancelledError) assertion below contradicts itself.
        async with scope:
            await sleep(5)

    task = asyncio.create_task(worker())
    await asyncio.sleep(0)

    def hammer_cancel():
        for _ in range(100):
            scope.cancel()

    def hammer_read():
        for _ in range(100):
            with mutex:
                read_values.append(scope.cancel_called)

    threads = [threading.Thread(target=hammer_cancel) for _ in range(3)] + [
        threading.Thread(target=hammer_read) for _ in range(3)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    await asyncio.sleep(0.05)

    # All reads must be consistent (no torn reads of the boolean).
    # After hammering, cancel_called must be True.
    assert scope.cancel_called is True
    # Every read should return a valid bool.
    assert all(isinstance(v, bool) for v in read_values)

    with pytest.raises(asyncio.CancelledError):
        await task


# ---------------------------------------------------------------------------
# CapacityLimiter — concurrent mutations under free-threading
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_capacity_limiter_concurrent_mutations():
    """CapacityLimiter.total_tokens reads/writes are consistent under
    concurrent access from multiple threads.

    On free-threaded builds, Rust RawAsyncCapacityLimiter mutex serialises mutations.
    """
    limiter = CapacityLimiter(100.0)
    errors: list[Exception] = []
    snapshots: list[tuple[float, float, float]] = []

    def mutate_and_snapshot(seed: int):
        try:
            for i in range(200):
                new_total = float(50 + ((i + seed) % 50))
                limiter.total_tokens = new_total
                # snapshot() reads all three values under a single Rust
                # mutex acquisition — reading the three properties
                # separately lets another thread change total_tokens between
                # two reads, mixing values computed against different totals
                # (cross-lock-region snapshot inconsistency, not a data race).
                total, avail, borrowed = limiter.snapshot()
                # Invariant: available + borrowed ≈ total
                if not (abs((avail + borrowed) - total) < 1.0):
                    errors.append(
                        ValueError(
                            f"invariant broken: total={total} avail={avail} borrowed={borrowed}"
                        )
                    )
                snapshots.append((total, avail, borrowed))
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=mutate_and_snapshot, args=(i,)) for i in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(errors) == 0, f"errors: {errors}"
    # Final state must be a valid total_tokens.
    assert limiter.total_tokens > 0


# ---------------------------------------------------------------------------
# Barrier — cross-loop abort under concurrency
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_barrier_cross_loop_abort_stress():
    """Barrier.abort() from a non-asyncio thread while parties are waiting.

    On free-threaded builds, abort() must use call_soon_threadsafe
    to wake waiters, and the Rust RawAsyncBarrier mutex prevents torn state.
    """
    barrier = Barrier(parties=4)
    errors_in_waiters: list[type[Exception]] = []

    async def waiter():
        try:
            await barrier.wait()
        except RuntimeError:
            errors_in_waiters.append(RuntimeError)
        except BaseException as exc:
            errors_in_waiters.append(type(exc))

    tasks = [asyncio.create_task(waiter()) for _ in range(3)]
    await wait_all_tasks_blocked()
    assert barrier.n_waiting == 3

    def abort_from_thread():
        barrier.abort()

    thread = threading.Thread(target=abort_from_thread)
    thread.start()
    thread.join()

    await asyncio.wait_for(asyncio.gather(*tasks), timeout=5)
    assert len(errors_in_waiters) == 3
    assert all(e is RuntimeError for e in errors_in_waiters)

    # Subsequent wait must also raise
    with pytest.raises(RuntimeError, match="aborted"):
        await barrier.wait()


# ---------------------------------------------------------------------------
# EventLoopThreadPool — concurrent submit + close under free-threading
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pool_concurrent_submit_close():
    """Pool.submit() concurrent with close() must not crash.

    On free-threaded builds, _lifecycle_lock, immutable loop snapshots, and
    NativeWorkerPool synchronize concurrent submit() vs close() safely.
    """
    pool = EventLoopThreadPool(num_threads=2)
    await pool.start()

    num_submitters = 4
    submissions_per_thread = 20
    barrier = threading.Barrier(num_submitters + 1, timeout=5.0)
    submit_errors: list[Exception] = []
    future_errors: list[Exception] = []
    unexpected_errors: list[BaseException] = []
    accepted_counts: list[int] = []
    lock = threading.Lock()

    def hammer_submit() -> None:
        async def _submitter() -> None:
            assert asyncio.get_running_loop().is_running()
            accepted: list[asyncio.Future[Any]] = []
            local_submit_errors: list[Exception] = []
            local_future_errors: list[Exception] = []

            barrier.wait()
            for i in range(submissions_per_thread):
                try:
                    fut = pool.submit(asyncio.sleep, 0.001)
                except Exception as exc:
                    local_submit_errors.append(exc)
                else:
                    assert asyncio.isfuture(fut)
                    accepted.append(fut)
                if i % 4 == 3:
                    await asyncio.sleep(0.001)
                else:
                    await asyncio.sleep(0)

            if accepted:
                results = await asyncio.wait_for(
                    asyncio.gather(*accepted, return_exceptions=True),
                    timeout=5.0,
                )
                for res in results:
                    if isinstance(res, BaseException):
                        if isinstance(res, Exception):
                            local_future_errors.append(res)
                        else:
                            with lock:
                                unexpected_errors.append(res)

            with lock:
                submit_errors.extend(local_submit_errors)
                future_errors.extend(local_future_errors)
                accepted_counts.append(len(accepted))

        try:
            asyncio.run(_submitter())
        except BaseException as exc:
            with lock:
                unexpected_errors.append(exc)

    threads = [threading.Thread(target=hammer_submit) for _ in range(num_submitters)]
    for t in threads:
        t.start()

    try:
        await asyncio.to_thread(barrier.wait)
        # Close while submitter threads are actively racing submit() from their own running loops.
        await pool.close()
    finally:
        for t in threads:
            t.join(timeout=10.0)
        await pool.close()

    assert not any(t.is_alive() for t in threads), "Submitter thread failed to exit"
    assert not unexpected_errors, f"Unexpected thread/future exceptions: {unexpected_errors}"
    assert sum(accepted_counts) + len(submit_errors) == num_submitters * submissions_per_thread

    # Errors from submit() or unexecuted accepted futures during close() must be
    # specifically ThreadPoolClosedError — never the missing-loop RuntimeError
    # or internal Python errors.
    for exc in [*submit_errors, *future_errors]:
        assert "running asyncio event loop" not in str(exc), (
            f"submit() unexpectedly saw missing running loop: {exc}"
        )
        assert isinstance(exc, ThreadPoolClosedError), (
            f"Unexpected error type: {type(exc).__name__}: {exc}"
        )
