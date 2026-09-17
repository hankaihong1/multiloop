"""Example 05: Multi-Core Pipeline Processing.

Demonstrates building a high-throughput multi-core data processing pipeline
using EventLoopThreadPool, Channel, and AsyncWaitGroup across Python 3.14t.
"""

import asyncio

from multiloop import AsyncWaitGroup, Channel, EventLoopThreadPool


async def producer(ch: Channel, num_items: int) -> None:
    """Produce work items into the channel."""
    for i in range(num_items):
        await ch.send(f"item-{i}")
    ch.close()


async def worker(worker_id: int, in_ch: Channel, out_ch: Channel, wg: AsyncWaitGroup) -> None:
    """Consume from in_ch, process on a dedicated worker loop, and send to out_ch."""
    try:
        async for item in in_ch:
            # Simulate CPU/IO processing
            result = f"worker-{worker_id}-processed-{item}"
            await out_ch.send(result)
    finally:
        wg.done()


async def collector(out_ch: Channel, results: list[str]) -> None:
    """Collect processed results from out_ch."""
    async for res in out_ch:
        results.append(res)


async def main() -> None:
    print("=== Example 05: Multi-Core Pipeline Processing ===")
    num_items = 20
    num_workers = 4

    in_ch = Channel(maxsize=10)
    out_ch = Channel(maxsize=10)
    wg = AsyncWaitGroup()
    results: list[str] = []

    async with EventLoopThreadPool(num_threads=num_workers) as pool:
        # Start producer
        producer_task = asyncio.create_task(producer(in_ch, num_items))

        # Start workers pinned to distinct OS thread event loops
        wg.add(num_workers)
        for i in range(num_workers):
            pool.submit(worker, i, in_ch, out_ch, wg, pin_to=i)

        # Start collector
        collector_task = asyncio.create_task(collector(out_ch, results))

        # Wait for producer to finish
        await producer_task

        # Wait for all workers to finish processing
        await wg.wait()
        out_ch.close()

        # Wait for collector to finish
        await collector_task

    print(f"Successfully processed {len(results)} items across {num_workers} multi-core workers:")
    for r in results[:5]:
        print(f"  {r}")
    print("  ...")
    print("=== Completed cleanly ===")


if __name__ == "__main__":
    asyncio.run(main())
