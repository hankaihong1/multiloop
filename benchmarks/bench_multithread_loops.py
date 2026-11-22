"""Multi-event-loop thread pool benchmark for Python 3.14t free-threaded builds.

Compares three execution models on an identical mixed I/O + CPU workload:
1. Single-thread asyncio event loop (baseline)
2. Multi-thread EventLoopThreadPool per-task cross-thread dispatch
3. Multi-thread EventLoopThreadPool worker-local batch execution
"""

from __future__ import annotations

import argparse
import asyncio
import os
import statistics
import time
from dataclasses import dataclass

from multiloop.pool import EventLoopThreadPool

DEFAULT_TOTAL_TASKS = 400
DEFAULT_ROUNDS = 3
DEFAULT_IO_SLEEP = 0.002
DEFAULT_CPU_ITERATIONS = 40_000
_MODULUS = 9_999_991


@dataclass(frozen=True, slots=True)
class BenchmarkStats:
    """Aggregated timing and throughput statistics across benchmark rounds."""

    samples: tuple[float, ...]
    median_s: float
    min_s: float
    stdev_s: float
    median_qps: float
    peak_qps: float


def compute_stats(samples: list[float], total_tasks: int) -> BenchmarkStats:
    """Compute median, min, standard deviation, and QPS from raw timing samples."""
    if not samples:
        raise ValueError("samples must not be empty")
    median_s = statistics.median(samples)
    min_s = min(samples)
    stdev_s = statistics.stdev(samples) if len(samples) > 1 else 0.0
    median_qps = total_tasks / median_s if median_s > 0.0 else 0.0
    peak_qps = total_tasks / min_s if min_s > 0.0 else 0.0
    return BenchmarkStats(
        samples=tuple(samples),
        median_s=median_s,
        min_s=min_s,
        stdev_s=stdev_s,
        median_qps=median_qps,
        peak_qps=peak_qps,
    )


def cpu_intensive_work(iterations: int = 150_000) -> int:
    """Pure Python CPU-bound workload (integer math loop) for Python 3.14t free-threading."""
    if iterations < 0:
        raise ValueError("iterations must be non-negative")
    acc = 0
    for i in range(iterations):
        acc = (acc + i * i) % _MODULUS
    return acc


def expected_workload_checksum(
    total_tasks: int,
    cpu_iterations: int = DEFAULT_CPU_ITERATIONS,
) -> int:
    """Compute the exact deterministic sum of all task results for verification."""
    per_task_cpu = cpu_intensive_work(iterations=cpu_iterations)
    task_id_sum = (total_tasks * (total_tasks - 1)) // 2
    return total_tasks * per_task_cpu + task_id_sum


async def mixed_workload_task(
    task_id: int,
    io_sleep: float = DEFAULT_IO_SLEEP,
    cpu_iterations: int = DEFAULT_CPU_ITERATIONS,
) -> int:
    """Simulate a mixed workload combining asynchronous I/O wait and CPU calculation."""
    if io_sleep > 0.0:
        await asyncio.sleep(io_sleep)
    res = cpu_intensive_work(iterations=cpu_iterations)
    return task_id + res


async def run_single_thread_benchmark(
    total_tasks: int,
    *,
    io_sleep: float = DEFAULT_IO_SLEEP,
    cpu_iterations: int = DEFAULT_CPU_ITERATIONS,
    expected_checksum: int | None = None,
) -> float:
    """Run the mixed workload on a single asyncio event loop."""
    start_time = time.perf_counter()
    tasks = [
        mixed_workload_task(i, io_sleep=io_sleep, cpu_iterations=cpu_iterations)
        for i in range(total_tasks)
    ]
    results = await asyncio.gather(*tasks)
    elapsed = time.perf_counter() - start_time
    if expected_checksum is not None and sum(results) != expected_checksum:
        raise RuntimeError(
            f"Single-thread checksum mismatch: got {sum(results)}, expected {expected_checksum}"
        )
    return elapsed


async def _run_pool_dispatch_once(
    pool: EventLoopThreadPool,
    total_tasks: int,
    *,
    io_sleep: float,
    cpu_iterations: int,
    expected_checksum: int | None,
) -> float:
    start_time = time.perf_counter()
    futs = [
        pool.submit(mixed_workload_task, i, io_sleep, cpu_iterations) for i in range(total_tasks)
    ]
    results = await asyncio.gather(*futs)
    elapsed = time.perf_counter() - start_time
    if expected_checksum is not None and sum(results) != expected_checksum:
        raise RuntimeError(
            f"Per-task pool checksum mismatch: got {sum(results)}, expected {expected_checksum}"
        )
    return elapsed


async def run_multithread_pool_benchmark(
    total_tasks: int,
    num_threads: int,
    *,
    pool: EventLoopThreadPool | None = None,
    io_sleep: float = DEFAULT_IO_SLEEP,
    cpu_iterations: int = DEFAULT_CPU_ITERATIONS,
    expected_checksum: int | None = None,
) -> float:
    """Run the workload distributed across a multi-threaded EventLoopThreadPool.

    Timing is measured strictly while the pool is running so OS thread creation
    and shutdown overhead do not skew steady-state task dispatch measurements.
    """
    if pool is not None:
        return await _run_pool_dispatch_once(
            pool,
            total_tasks,
            io_sleep=io_sleep,
            cpu_iterations=cpu_iterations,
            expected_checksum=expected_checksum,
        )

    async with EventLoopThreadPool(num_threads=num_threads) as managed_pool:
        warmup_futs = [
            managed_pool.submit(mixed_workload_task, i, 0.0, min(cpu_iterations, 2_000), pin_to=i)
            for i in range(num_threads)
        ]
        await asyncio.gather(*warmup_futs)
        return await _run_pool_dispatch_once(
            managed_pool,
            total_tasks,
            io_sleep=io_sleep,
            cpu_iterations=cpu_iterations,
            expected_checksum=expected_checksum,
        )


async def worker_local_batch(
    tasks_per_worker: int,
    start_id: int = 0,
    io_sleep: float = DEFAULT_IO_SLEEP,
    cpu_iterations: int = DEFAULT_CPU_ITERATIONS,
) -> int:
    """Run a partition of tasks directly inside a worker loop (minimal cross-thread IPC)."""
    if tasks_per_worker <= 0:
        return 0
    tasks = [
        mixed_workload_task(
            start_id + offset,
            io_sleep=io_sleep,
            cpu_iterations=cpu_iterations,
        )
        for offset in range(tasks_per_worker)
    ]
    results = await asyncio.gather(*tasks)
    return sum(results)


async def _run_worker_local_once(
    pool: EventLoopThreadPool,
    total_tasks: int,
    num_threads: int,
    *,
    io_sleep: float,
    cpu_iterations: int,
    expected_checksum: int | None,
) -> float:
    base, rem = divmod(total_tasks, num_threads)
    partitions: list[tuple[int, int, int]] = []
    cursor = 0
    for worker_idx in range(num_threads):
        count = base + (1 if worker_idx < rem else 0)
        if count > 0:
            partitions.append((worker_idx, count, cursor))
            cursor += count

    start_time = time.perf_counter()
    futs = [
        pool.submit(
            worker_local_batch,
            count,
            start_id,
            io_sleep,
            cpu_iterations,
            pin_to=worker_idx,
        )
        for worker_idx, count, start_id in partitions
    ]
    batch_sums = await asyncio.gather(*futs)
    elapsed = time.perf_counter() - start_time
    if expected_checksum is not None and sum(batch_sums) != expected_checksum:
        raise RuntimeError(
            f"Worker-local checksum mismatch: got {sum(batch_sums)}, expected {expected_checksum}"
        )
    return elapsed


async def run_worker_local_benchmark(
    total_tasks: int,
    num_threads: int,
    *,
    pool: EventLoopThreadPool | None = None,
    io_sleep: float = DEFAULT_IO_SLEEP,
    cpu_iterations: int = DEFAULT_CPU_ITERATIONS,
    expected_checksum: int | None = None,
) -> float:
    """Run tasks partitioned across worker loops with remainder-safe distribution."""
    if pool is not None:
        return await _run_worker_local_once(
            pool,
            total_tasks,
            num_threads,
            io_sleep=io_sleep,
            cpu_iterations=cpu_iterations,
            expected_checksum=expected_checksum,
        )

    async with EventLoopThreadPool(num_threads=num_threads) as managed_pool:
        warmup_futs = [
            managed_pool.submit(worker_local_batch, 1, i, 0.0, min(cpu_iterations, 2_000), pin_to=i)
            for i in range(num_threads)
        ]
        await asyncio.gather(*warmup_futs)
        return await _run_worker_local_once(
            managed_pool,
            total_tasks,
            num_threads,
            io_sleep=io_sleep,
            cpu_iterations=cpu_iterations,
            expected_checksum=expected_checksum,
        )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line benchmark parameters."""
    cpu_count = os.cpu_count() or 4
    default_threads = max(1, min(cpu_count, 8))
    parser = argparse.ArgumentParser(
        description="multiloop Python 3.14t multi-event-loop scaling benchmark"
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=default_threads,
        help=f"Worker event-loop threads (default: {default_threads})",
    )
    parser.add_argument(
        "--tasks",
        type=int,
        default=DEFAULT_TOTAL_TASKS,
        help=f"Total tasks per round (default: {DEFAULT_TOTAL_TASKS})",
    )
    parser.add_argument(
        "--rounds",
        type=int,
        default=DEFAULT_ROUNDS,
        help=f"Timed benchmark rounds per scenario (default: {DEFAULT_ROUNDS})",
    )
    parser.add_argument(
        "--io-sleep",
        type=float,
        default=DEFAULT_IO_SLEEP,
        help=f"Simulated async I/O sleep seconds per task (default: {DEFAULT_IO_SLEEP})",
    )
    parser.add_argument(
        "--cpu-iterations",
        type=int,
        default=DEFAULT_CPU_ITERATIONS,
        help=f"CPU loop iterations per task (default: {DEFAULT_CPU_ITERATIONS})",
    )
    args = parser.parse_args(argv)
    if args.threads <= 0:
        parser.error("--threads must be >= 1")
    if args.tasks <= 0:
        parser.error("--tasks must be >= 1")
    if args.rounds <= 0:
        parser.error("--rounds must be >= 1")
    if args.io_sleep < 0.0:
        parser.error("--io-sleep must be >= 0.0")
    if args.cpu_iterations < 0:
        parser.error("--cpu-iterations must be >= 0")
    return args


async def main(argv: list[str] | None = None) -> None:
    """Execute the multi-thread event-loop benchmark suite."""
    args = parse_args(argv)
    cpu_count = os.cpu_count() or 4
    num_threads: int = args.threads
    total_tasks: int = args.tasks
    rounds: int = args.rounds
    io_sleep: float = args.io_sleep
    cpu_iterations: int = args.cpu_iterations
    checksum = expected_workload_checksum(total_tasks, cpu_iterations=cpu_iterations)

    print("=" * 74)
    print("🚀 multiloop Python 3.14t (Free-Threaded) Multi-Loop Benchmark")
    print(
        f"CPU Cores: {cpu_count} | Worker Threads: {num_threads} | "
        f"Tasks/Round: {total_tasks} | Rounds: {rounds}"
    )
    print("=" * 74)

    # Warmup single-thread loop
    warmup_tasks = min(total_tasks, max(num_threads * 2, 16))
    await run_single_thread_benchmark(
        warmup_tasks,
        io_sleep=io_sleep,
        cpu_iterations=min(cpu_iterations, 10_000),
    )

    # 1. Single-Thread Event Loop Benchmark
    print("\n[1/3] Benchmarking Single-Thread Event Loop...")
    single_samples = [
        await run_single_thread_benchmark(
            total_tasks,
            io_sleep=io_sleep,
            cpu_iterations=cpu_iterations,
            expected_checksum=checksum,
        )
        for _ in range(rounds)
    ]
    single_stats = compute_stats(single_samples, total_tasks)
    print(
        f"  ➜ Median: {single_stats.median_s:.4f} s (min: {single_stats.min_s:.4f} s, "
        f"stdev: {single_stats.stdev_s:.4f} s) | Median QPS: {single_stats.median_qps:.2f} req/s"
    )

    async with EventLoopThreadPool(num_threads=num_threads) as pool:
        # Warmup worker threads & event loops before timed rounds
        await run_multithread_pool_benchmark(
            warmup_tasks,
            num_threads,
            pool=pool,
            io_sleep=0.0,
            cpu_iterations=min(cpu_iterations, 10_000),
        )
        await run_worker_local_benchmark(
            warmup_tasks,
            num_threads,
            pool=pool,
            io_sleep=0.0,
            cpu_iterations=min(cpu_iterations, 10_000),
        )

        # 2. Per-Task Dispatch Multi-Thread Event Loop Pool Benchmark
        print("\n[2/3] Benchmarking Multi-Thread Pool (Per-Task Cross-Thread Dispatch)...")
        dispatch_samples = [
            await run_multithread_pool_benchmark(
                total_tasks,
                num_threads,
                pool=pool,
                io_sleep=io_sleep,
                cpu_iterations=cpu_iterations,
                expected_checksum=checksum,
            )
            for _ in range(rounds)
        ]
        dispatch_stats = compute_stats(dispatch_samples, total_tasks)
        print(
            f"  ➜ Median: {dispatch_stats.median_s:.4f} s (min: {dispatch_stats.min_s:.4f} s, "
            f"stdev: {dispatch_stats.stdev_s:.4f} s) | "
            f"Median QPS: {dispatch_stats.median_qps:.2f} req/s"
        )

        # 3. Worker-Local Event Loop Benchmark (Partitioned Batch Execution)
        print("\n[3/3] Benchmarking Multi-Thread Pool (Worker-Local Execution)...")
        local_samples = [
            await run_worker_local_benchmark(
                total_tasks,
                num_threads,
                pool=pool,
                io_sleep=io_sleep,
                cpu_iterations=cpu_iterations,
                expected_checksum=checksum,
            )
            for _ in range(rounds)
        ]
        local_stats = compute_stats(local_samples, total_tasks)
        print(
            f"  ➜ Median: {local_stats.median_s:.4f} s (min: {local_stats.min_s:.4f} s, "
            f"stdev: {local_stats.stdev_s:.4f} s) | "
            f"Median QPS: {local_stats.median_qps:.2f} req/s"
        )

    baseline_qps = single_stats.median_qps if single_stats.median_qps > 0.0 else 1.0
    dispatch_ratio = dispatch_stats.median_qps / baseline_qps
    local_ratio = local_stats.median_qps / baseline_qps

    print("\n" + "=" * 74)
    print("📊 Performance & Overhead Summary (Median QPS):")
    print(
        f"  1. Single-Thread Loop QPS          : {single_stats.median_qps:8.2f} req/s "
        "(1.00x Baseline)"
    )
    print(
        f"  2. Per-Task Cross-Thread Dispatch  : {dispatch_stats.median_qps:8.2f} req/s "
        f"({dispatch_ratio:.2f}x vs Single-Thread)"
    )
    print(
        f"  3. Worker-Local Execution (No IPC) : {local_stats.median_qps:8.2f} req/s "
        f"({local_ratio:.2f}x Multi-Core Speedup)"
    )
    print("=" * 74)


if __name__ == "__main__":
    asyncio.run(main())
