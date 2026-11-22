"""Performance regression tests.

Marked @pytest.mark.slow — skipped by default in CI.
Run with: pytest -m slow tests/test_perf_regression.py
"""

import asyncio
import time

import pytest

from multiloop.pool import EventLoopThreadPool


@pytest.mark.slow
@pytest.mark.asyncio
async def test_pool_throughput_baseline() -> None:
    """Single-thread pool submits N coroutines and verifies QPS > loose threshold.

    Uses a simple sleep-based task to simulate I/O-bound work.  The threshold
    (10 tasks / sec) is deliberately loose so the test only catches severe
    performance regressions, not normal CI variance.
    """
    n = 100

    async def quick_task() -> int:
        await asyncio.sleep(0.001)
        return 42

    async with EventLoopThreadPool(num_threads=1) as pool:
        start = time.perf_counter()
        futs = [pool.submit(quick_task) for _ in range(n)]
        results = await asyncio.gather(*futs)
        elapsed = time.perf_counter() - start

    qps = n / elapsed
    assert all(r == 42 for r in results)
    assert qps > 10, (
        f"Single-thread QPS {qps:.2f} below loose threshold of 10 tasks/sec — "
        f"possible regression in task dispatch path"
    )


@pytest.mark.slow
@pytest.mark.asyncio
async def test_pool_worker_local_acceleration() -> None:
    """Verify multi-thread worker-local dispatch achieves >1.5x single-thread throughput.

    Worker-local means each worker processes a pinned partition of CPU + async
    yield tasks entirely within its own event loop, avoiding cross-thread dispatch
    overhead (matching benchmarks/bench_multithread_loops.py).
    """
    n_tasks = 80
    num_workers = 4
    cpu_iters = 40_000

    async def mixed_task() -> int:
        acc = 0
        for i in range(cpu_iters):
            acc += i * i
        await asyncio.sleep(0)
        return acc

    async def worker_batch(n: int) -> list[int]:
        """Run a partition of tasks locally inside a single worker loop."""
        tasks = [mixed_task() for _ in range(n)]
        return list(await asyncio.gather(*tasks))

    expected = sum(i * i for i in range(cpu_iters))
    single_times: list[float] = []
    multi_times: list[float] = []

    # ── Single-thread baseline (per-task dispatch) ──────────────────────
    async with EventLoopThreadPool(num_threads=1) as pool:
        await pool.submit(mixed_task)  # warmup
        for _ in range(3):
            start = time.perf_counter()
            futs = [pool.submit(mixed_task) for _ in range(n_tasks)]
            results = await asyncio.gather(*futs)
            single_times.append(time.perf_counter() - start)
            assert all(r == expected for r in results)

    # ── Multi-thread worker-local (pinned batch dispatch, zero cross-thread IPC) ─
    async with EventLoopThreadPool(num_threads=num_workers) as pool:
        await asyncio.gather(
            *[pool.submit(mixed_task, pin_to=i) for i in range(num_workers)]
        )  # warmup
        per_worker = n_tasks // num_workers
        for _ in range(3):
            start = time.perf_counter()
            batch_futs = [
                pool.submit(worker_batch, per_worker, pin_to=i) for i in range(num_workers)
            ]
            batch_results = await asyncio.gather(*batch_futs)
            multi_times.append(time.perf_counter() - start)
            assert sum(len(b) for b in batch_results) == n_tasks
            assert all(r == expected for b in batch_results for r in b)

    t_single = min(single_times)
    t_multi = min(multi_times)
    speedup = t_single / t_multi
    assert speedup > 1.5, (
        f"Multi-thread worker-local speedup {speedup:.2f}x is below 1.5x threshold — "
        f"possible regression in multi-thread parallelism"
    )
