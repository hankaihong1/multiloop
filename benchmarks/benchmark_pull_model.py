"""Work-stealing pull-model and batched task dispatch benchmark for multiloop.

Compares CPU-bound coroutine execution across:
1. Single asyncio event loop baseline
2. Multi-thread pool fine-grained per-task pull dispatch (submit & submit_many)
3. Multi-thread pool worker-chunked batch dispatch (amortized cross-thread IPC)
"""

from __future__ import annotations

import argparse
import asyncio
import os
import statistics
import time
from dataclasses import dataclass

from multiloop import EventLoopThreadPool

DEFAULT_NUM_TASKS = 40
DEFAULT_CALC_ITERATIONS = 500_000
DEFAULT_ROUNDS = 3


@dataclass(frozen=True, slots=True)
class PullBenchmarkStats:
    """Timing and speedup statistics across benchmark rounds."""

    samples: tuple[float, ...]
    median_s: float
    min_s: float
    stdev_s: float


def compute_pull_stats(samples: list[float]) -> PullBenchmarkStats:
    """Compute median, minimum, and standard deviation from raw elapsed samples."""
    if not samples:
        raise ValueError("samples must not be empty")
    return PullBenchmarkStats(
        samples=tuple(samples),
        median_s=statistics.median(samples),
        min_s=min(samples),
        stdev_s=statistics.stdev(samples) if len(samples) > 1 else 0.0,
    )


def cpu_heavy_work(n: int) -> int:
    """Simulate a pure Python CPU-bound computation task."""
    if n < 0:
        raise ValueError("n must be non-negative")
    s = 0
    for i in range(n):
        s += i * i
    return s


async def cpu_coro(n: int) -> int:
    """Coroutine wrapper around a CPU-bound computation."""
    return cpu_heavy_work(n)


async def batched_cpu_coro(batch_size: int, iterations_per_item: int) -> list[int]:
    """Execute a chunk of CPU tasks in one coroutine to amortize cross-thread IPC."""
    if batch_size < 0:
        raise ValueError("batch_size must be non-negative")
    return [cpu_heavy_work(iterations_per_item) for _ in range(batch_size)]


async def _run_single_loop_round(
    num_tasks: int,
    calc_iterations: int,
    expected_total: int,
) -> float:
    t0 = time.perf_counter()
    tasks = [cpu_coro(calc_iterations) for _ in range(num_tasks)]
    results = await asyncio.gather(*tasks)
    elapsed = time.perf_counter() - t0
    if sum(results) != expected_total:
        raise RuntimeError(
            f"Single-loop result mismatch: got {sum(results)}, expected {expected_total}"
        )
    return elapsed


async def _run_fine_submit_round(
    pool: EventLoopThreadPool,
    num_tasks: int,
    calc_iterations: int,
    expected_total: int,
) -> float:
    t0 = time.perf_counter()
    futs = [pool.submit(cpu_coro, calc_iterations) for _ in range(num_tasks)]
    results = await asyncio.gather(*futs)
    elapsed = time.perf_counter() - t0
    if sum(results) != expected_total:
        raise RuntimeError(
            f"Fine-grained submit result mismatch: got {sum(results)}, expected {expected_total}"
        )
    return elapsed


async def _run_submit_many_round(
    pool: EventLoopThreadPool,
    num_tasks: int,
    calc_iterations: int,
    expected_total: int,
) -> float:
    t0 = time.perf_counter()
    futs = pool.submit_many(cpu_coro(calc_iterations) for _ in range(num_tasks))
    results = await asyncio.gather(*futs)
    elapsed = time.perf_counter() - t0
    if sum(results) != expected_total:
        raise RuntimeError(
            f"submit_many result mismatch: got {sum(results)}, expected {expected_total}"
        )
    return elapsed


async def _run_chunked_batch_round(
    pool: EventLoopThreadPool,
    num_tasks: int,
    num_threads: int,
    calc_iterations: int,
    expected_total: int,
) -> float:
    base, rem = divmod(num_tasks, num_threads)
    partitions = [
        (worker_idx, base + (1 if worker_idx < rem else 0))
        for worker_idx in range(num_threads)
        if base + (1 if worker_idx < rem else 0) > 0
    ]

    t0 = time.perf_counter()
    futs = [
        pool.submit(batched_cpu_coro, batch_size, calc_iterations, pin_to=worker_idx)
        for worker_idx, batch_size in partitions
    ]
    batches = await asyncio.gather(*futs)
    elapsed = time.perf_counter() - t0
    actual_total = sum(sum(batch) for batch in batches)
    if actual_total != expected_total:
        raise RuntimeError(
            f"Chunked batch result mismatch: got {actual_total}, expected {expected_total}"
        )
    return elapsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments for the pull-model benchmark."""
    cpu_count = os.cpu_count() or 4
    default_threads = max(1, min(cpu_count, 4))
    parser = argparse.ArgumentParser(
        description="multiloop multi-thread pull-model and batch dispatch benchmark"
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=default_threads,
        help=f"Number of worker threads (default: {default_threads})",
    )
    parser.add_argument(
        "--tasks",
        type=int,
        default=DEFAULT_NUM_TASKS,
        help=f"Number of CPU tasks per round (default: {DEFAULT_NUM_TASKS})",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=DEFAULT_CALC_ITERATIONS,
        help=f"Loop iterations per CPU task (default: {DEFAULT_CALC_ITERATIONS})",
    )
    parser.add_argument(
        "--rounds",
        type=int,
        default=DEFAULT_ROUNDS,
        help=f"Timed rounds per scenario (default: {DEFAULT_ROUNDS})",
    )
    args = parser.parse_args(argv)
    if args.threads <= 0:
        parser.error("--threads must be >= 1")
    if args.tasks <= 0:
        parser.error("--tasks must be >= 1")
    if args.iterations < 0:
        parser.error("--iterations must be >= 0")
    if args.rounds <= 0:
        parser.error("--rounds must be >= 1")
    return args


async def run_benchmark(argv: list[str] | None = None) -> None:
    """Run the single-loop vs multi-thread pull-model and chunked dispatch benchmark."""
    args = parse_args(argv)
    num_threads: int = args.threads
    num_tasks: int = args.tasks
    calc_iterations: int = args.iterations
    rounds: int = args.rounds

    per_item_expected = cpu_heavy_work(calc_iterations)
    expected_total = num_tasks * per_item_expected

    print("=" * 72)
    print("🚀 multiloop Multi-Thread Architecture Benchmark")
    print(
        f"Workers: {num_threads} | Tasks: {num_tasks} | "
        f"Iterations/Task: {calc_iterations:,} | Rounds: {rounds}"
    )
    print("=" * 72)

    # Warmup single-thread loop
    warmup_iters = min(calc_iterations, 25_000)
    await asyncio.gather(*(cpu_coro(warmup_iters) for _ in range(min(num_tasks, 8))))

    # -------------------------------------------------------------
    # Scenario 1: Fine-grained single-task dispatch
    # -------------------------------------------------------------
    print(
        f"\n📊 Scenario 1: Fine-Grained Task Dispatch "
        f"({num_tasks} Tasks x {calc_iterations:,} Iterations)"
    )
    single_samples = [
        await _run_single_loop_round(num_tasks, calc_iterations, expected_total)
        for _ in range(rounds)
    ]
    single_stats = compute_pull_stats(single_samples)
    print(
        f"   1️⃣ Single EventLoop Baseline    : {single_stats.median_s:.4f} s "
        f"(min: {single_stats.min_s:.4f} s, stdev: {single_stats.stdev_s:.4f} s)"
    )

    async with EventLoopThreadPool(num_threads=num_threads) as pool:
        # Warmup every worker thread and event loop before timed rounds
        warmup_futs = [pool.submit(cpu_coro, warmup_iters, pin_to=i) for i in range(num_threads)]
        await asyncio.gather(*warmup_futs)

        fine_samples = [
            await _run_fine_submit_round(pool, num_tasks, calc_iterations, expected_total)
            for _ in range(rounds)
        ]
        fine_stats = compute_pull_stats(fine_samples)
        speedup_fine = (
            single_stats.median_s / fine_stats.median_s if fine_stats.median_s > 0.0 else 1.0
        )
        print(
            f"   2️⃣ multiloop {num_threads}-Worker submit()  : {fine_stats.median_s:.4f} s "
            f"(min: {fine_stats.min_s:.4f} s, Speedup: {speedup_fine:.2f}x)"
        )

        many_samples = [
            await _run_submit_many_round(pool, num_tasks, calc_iterations, expected_total)
            for _ in range(rounds)
        ]
        many_stats = compute_pull_stats(many_samples)
        speedup_many = (
            single_stats.median_s / many_stats.median_s if many_stats.median_s > 0.0 else 1.0
        )
        print(
            f"   3️⃣ multiloop {num_threads}-Worker submit_many: {many_stats.median_s:.4f} s "
            f"(min: {many_stats.min_s:.4f} s, Speedup: {speedup_many:.2f}x)"
        )

        # -------------------------------------------------------------
        # Scenario 2: Batched dispatch (chunked) — amortizes cross-thread
        # communication overhead across worker partitions
        # -------------------------------------------------------------
        print(
            f"\n📊 Scenario 2: Batched Task Dispatch "
            f"({num_threads} Worker Batches, {num_tasks} Total Items - Reduced IPC Overhead)"
        )
        batch_samples = [
            await _run_chunked_batch_round(
                pool,
                num_tasks,
                num_threads,
                calc_iterations,
                expected_total,
            )
            for _ in range(rounds)
        ]
        batch_stats = compute_pull_stats(batch_samples)
        speedup_batch = (
            single_stats.median_s / batch_stats.median_s if batch_stats.median_s > 0.0 else 1.0
        )

        print(f"   1️⃣ Single EventLoop Baseline    : {single_stats.median_s:.4f} s")
        print(
            f"   2️⃣ multiloop {num_threads}-Worker Batched   : {batch_stats.median_s:.4f} s "
            f"(min: {batch_stats.min_s:.4f} s, stdev: {batch_stats.stdev_s:.4f} s)"
        )
        print(
            f"   🔥 Batched Multi-Core Speedup   : {speedup_batch:.2f}x Faster "
            f"({num_threads} Worker Threads)"
        )

    print("=" * 72)


if __name__ == "__main__":
    asyncio.run(run_benchmark())
