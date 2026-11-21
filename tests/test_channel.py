import asyncio
import threading
import time

import pytest

from multiloop import (
    EventLoopThreadPool,
    TimeoutError,
    select_channel,
)
from multiloop.exceptions import ChannelClosedError, MultiloopError, WouldBlock
from multiloop.primitives import Channel
from multiloop.testing import wait_all_tasks_blocked


@pytest.mark.asyncio
async def test_channel_valid_basic_send_recv():
    """Valid 1: Basic send and receive"""
    ch = Channel()
    await ch.send("hello")
    res = await ch.recv()
    assert res == "hello"


@pytest.mark.asyncio
async def test_channel_valid_multiple_items():
    """Valid 2: Sequential send and receive of multiple items"""
    ch = Channel()
    items = [1, 2, 3, "test", {"key": "val"}]
    for item in items:
        await ch.send(item)

    received = []
    for _ in range(len(items)):
        received.append(await ch.recv())
    assert received == items


@pytest.mark.asyncio
async def test_channel_valid_concurrent_producers_consumers():
    """Valid 3: Multi-coroutine concurrent send/receive data integrity"""
    ch = Channel()
    count = 50

    async def producer(n):
        await ch.send(n)

    async def consumer():
        return await ch.recv()

    producers = [producer(i) for i in range(count)]
    consumers = [consumer() for _ in range(count)]

    _, results = await asyncio.gather(asyncio.gather(*producers), asyncio.gather(*consumers))
    assert sorted(results) == list(range(count))


@pytest.mark.asyncio
async def test_channel_boundary_none_and_empty_payloads():
    """Boundary 1: None / empty bytes / empty data structure passing"""
    ch = Channel()
    payloads = [None, b"", {}, [], 0, False]
    for p in payloads:
        await ch.send(p)
        res = await ch.recv()
        assert res is p


@pytest.mark.asyncio
async def test_channel_boundary_capacity_limit():
    """Boundary 2: Capacity-limited (capacity=1) send and blocking unlock"""
    ch = Channel(maxsize=1)
    await ch.send("first")

    # The second send should suspend, waiting for consumption
    send_task = asyncio.create_task(ch.send("second"))
    await wait_all_tasks_blocked()
    assert not send_task.done()

    val1 = await ch.recv()
    assert val1 == "first"

    # After receiving, send_task should be able to complete
    await asyncio.wait_for(send_task, timeout=1.0)
    val2 = await ch.recv()
    assert val2 == "second"


@pytest.mark.asyncio
async def test_channel_boundary_recv_blocks_until_send():
    """Boundary 3: recv suspends on empty channel, unblocks after send"""
    ch = Channel()

    recv_task = asyncio.create_task(ch.recv())
    await asyncio.sleep(0.01)
    assert not recv_task.done()

    await ch.send("unblock")
    val = await asyncio.wait_for(recv_task, timeout=1.0)
    assert val == "unblock"


@pytest.mark.asyncio
async def test_channel_error_send_to_closed_channel():
    """Error 1: Sending data to a closed channel raises ChannelClosedError"""
    ch = Channel()
    ch.close()
    with pytest.raises(ChannelClosedError):
        await ch.send("fail")


@pytest.mark.asyncio
async def test_channel_error_recv_from_closed_empty_channel():
    """Error 2: Receiving from a closed and empty channel raises ChannelClosedError"""
    ch = Channel()
    ch.close()
    with pytest.raises(ChannelClosedError):
        await ch.recv()


@pytest.mark.asyncio
async def test_channel_error_recv_timeout():
    """Timed recv raises TimeoutError without leaking getter waiters."""
    ch = Channel()
    with pytest.raises((TimeoutError, asyncio.TimeoutError)):
        await ch.recv(timeout=0.02)
    assert len(ch._getters) == 0


def test_wouldblock_is_multiloop_error():
    """WouldBlock should be a subclass of MultiloopError."""
    assert isinstance(WouldBlock(), MultiloopError)


def test_channel_try_send_recv_roundtrip():
    """try_send + try_recv on Channel should round-trip a value."""
    ch = Channel()
    assert ch.try_send("hello")
    assert ch.try_recv() == "hello"


def test_channel_try_recv_empty_raises_wouldblock():
    """try_recv on an empty Channel should raise WouldBlock."""
    ch = Channel()
    with pytest.raises(WouldBlock):
        ch.try_recv()


def test_channel_qsize():
    """qsize() should reflect the number of buffered items."""
    ch = Channel()
    assert ch.qsize() == 0
    ch.try_send(1)
    assert ch.qsize() == 1
    ch.try_send(2)
    assert ch.qsize() == 2
    ch.try_recv()
    assert ch.qsize() == 1
    ch.try_recv()
    assert ch.qsize() == 0


def test_channel_try_send_full_bounded():
    """try_send on a full bounded Channel should return False."""
    ch = Channel(maxsize=1)
    assert ch.try_send("first")
    assert not ch.try_send("second")


def test_channel_try_recv_closed_raises():
    """try_recv on a closed empty Channel should raise ChannelClosedError."""
    ch = Channel()
    ch.close()
    with pytest.raises(ChannelClosedError):
        ch.try_recv()


def test_channel_try_send_closed_raises():
    """try_send on a closed Channel should raise ChannelClosedError."""
    ch = Channel()
    ch.close()
    with pytest.raises(ChannelClosedError):
        ch.try_send("nope")


@pytest.mark.asyncio
async def test_channel_close_with_blocked_waiters():
    """Close Channel while sender and receiver are blocked — covers
    RawAsyncChannel.close() waking blocked putters/getters and BaseException cleanup."""
    # Blocked sender: fill bounded channel, start sender, close
    send_ch = Channel(maxsize=1)
    await send_ch.send("fill")

    async def blocked_sender():
        await send_ch.send("blocked")

    sender_task = asyncio.create_task(blocked_sender())
    await wait_all_tasks_blocked()
    send_ch.close()
    with pytest.raises(ChannelClosedError):
        await sender_task

    # Blocked receiver: start receiver on empty channel, close
    recv_ch = Channel()

    async def blocked_receiver():
        return await recv_ch.recv()

    recv_task = asyncio.create_task(blocked_receiver())
    await wait_all_tasks_blocked()
    recv_ch.close()
    with pytest.raises(ChannelClosedError):
        await recv_task


@pytest.mark.asyncio
async def test_channel_timeout_and_close_idempotent():
    """Timed send cleans up putters, and repeated close() is idempotent while draining."""
    ch = Channel(maxsize=1)
    await ch.send("item1")

    with pytest.raises((TimeoutError, asyncio.TimeoutError)):
        await ch.send("item2", timeout=0.02)
    assert len(ch._putters) == 0

    ch.close()
    ch.close()  # Second close is idempotent
    assert ch.is_closed

    with pytest.raises(ChannelClosedError):
        await ch.send("item3")

    # Retrieve existing data
    val = await ch.recv()
    assert val == "item1"

    with pytest.raises(ChannelClosedError):
        await ch.recv()


@pytest.mark.asyncio
async def test_vulnerability_fast_channel_no_busy_wait():
    """Vulnerability 1: Channel should not busy-wait with sleep when no data is available, and should be properly woken up"""
    ch = Channel()

    async def consumer():
        return await ch.recv()

    task = asyncio.create_task(consumer())
    await asyncio.sleep(0.02)
    assert not task.done()

    await ch.send("unblocked_event")
    result = await asyncio.wait_for(task, timeout=1.0)
    assert result == "unblocked_event"


@pytest.mark.asyncio
async def test_race_high_concurrency_channel_send_recv():
    """Race 2: Validate data integrity and zero race-condition data loss for 100 coroutines concurrently sending/receiving on Channel under a multi-threaded event loop pool"""
    async with EventLoopThreadPool(num_threads=4) as pool:
        ch = Channel(maxsize=10)
        total_items = 200
        received_items = []
        lock = threading.Lock()

        async def producer(val):
            await ch.send(val)

        async def consumer():
            val = await ch.recv()
            with lock:
                received_items.append(val)

        # Dispatch 200 producers and 200 consumers running and waiting simultaneously across threads
        producers = [pool.submit(producer, i) for i in range(total_items)]
        consumers = [pool.submit(consumer) for _ in range(total_items)]

        await asyncio.gather(*producers, *consumers)

        assert len(received_items) == total_items
        assert sorted(received_items) == list(range(total_items))


@pytest.mark.asyncio
async def test_channel_async_for_iteration():
    """1. Go parity range ch: Validate async for item in ch loop consumption and graceful exit on close"""
    ch = Channel()

    async def producer():
        for i in range(3):
            await ch.send(i)
            await asyncio.sleep(0.01)
        ch.close()

    asyncio.create_task(producer())

    received = []
    async for item in ch:
        received.append(item)

    assert received == [0, 1, 2]


@pytest.mark.asyncio
async def test_select_channel_multi_branch():
    """2. Go parity select: Validate select_channel listening on multiple channels simultaneously"""
    ch1 = Channel()
    ch2 = Channel()

    async def send_to_ch2():
        await asyncio.sleep(0.02)
        await ch2.send("from_ch2")

    asyncio.create_task(send_to_ch2())

    selected_ch, val = await select_channel(ch1, ch2, timeout=1.0)
    assert selected_ch is ch2
    assert val == "from_ch2"


# ---------------------------------------------------------------------------
# Regression tests
# payloads) — 2026-08-10 audit
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fastchannel_none_payload() -> None:
    """Channel must transport None payloads end to end.

    Before the fix `send(None)` consumed the item but `recv()` saw the
    channel as empty and registered a waiter that nobody would wake —
    a permanent hang.
    """
    ch = Channel()
    await ch.send(None)
    # wait_for guards against the historical permanent hang.
    assert await asyncio.wait_for(ch.recv(), timeout=1.0) is None

    # try_send/try_recv must distinguish "None payload" from "empty".
    assert ch.try_send(None) is True
    assert ch.try_recv() is None  # None payload, NOT WouldBlock

    # The channel is now truly empty: WouldBlock must be raised.
    with pytest.raises(WouldBlock):
        ch.try_recv()


@pytest.mark.asyncio
async def test_fastchannel_none_after_regular_items() -> None:
    """None must not disturb the FIFO order of surrounding items."""
    ch = Channel()
    await ch.send(1)
    await ch.send(None)
    await ch.send(2)
    assert await ch.recv() == 1
    assert await ch.recv() is None
    assert await ch.recv() == 2


# ---------------------------------------------------------------------------
# select_channel contracts
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_select_no_item_loss_simultaneous() -> None:
    """both channels ready at once — the unselected item stays buffered."""
    ch1 = Channel()
    ch2 = Channel()
    await ch1.send("a")
    await ch2.send("b")

    selected_ch, val = await select_channel(ch1, ch2, timeout=1.0)

    # The loser's item must still be receivable; the winner's was consumed.
    if selected_ch is ch1:
        assert val == "a"
        assert await asyncio.wait_for(ch2.recv(), timeout=1.0) == "b"
    else:
        assert val == "b"
        assert await asyncio.wait_for(ch1.recv(), timeout=1.0) == "a"


@pytest.mark.asyncio
async def test_select_timeout_no_loss() -> None:
    """W4: an item sent at the timeout boundary must never vanish."""

    async def delayed_send(ch: Channel, delay: float, value: str) -> None:
        await asyncio.sleep(delay)
        await ch.send(value)

    for i in range(100):
        ch = Channel()
        sender = asyncio.create_task(delayed_send(ch, 0.005, f"v{i}"))
        consumed = 0
        try:
            await select_channel(ch, timeout=0.01)
            consumed = 1
        except TimeoutError:
            pass
        await sender
        # Accounting: exactly one item was sent — it was either consumed by
        # the select winner or is still buffered.  Zero means it was dropped.
        assert ch.qsize() + consumed == 1, (
            f"round {i}: item lost (qsize={ch.qsize()}, consumed={consumed})"
        )


# ───────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_select_all_closed_raises() -> None:
    """selecting on channels that are closed AND empty
    must raise ChannelClosedError instead of hanging forever (pre-fix: the
    notifier registered successfully and nobody ever woke it)."""
    ch1 = Channel()
    ch2 = Channel()
    ch1.close()
    ch2.close()
    with pytest.raises(ChannelClosedError):
        await asyncio.wait_for(select_channel(ch1, ch2), timeout=0.5)


@pytest.mark.asyncio
async def test_select_closed_with_data_returns_data() -> None:
    """a closed channel that still holds buffered items reports ready
    and yields its data — closing must not destroy buffered values."""
    ch1 = Channel()
    ch2 = Channel()
    await ch1.send("value")
    ch1.close()
    ch2.close()
    ch, val = await asyncio.wait_for(select_channel(ch1, ch2), timeout=1.0)
    assert (ch, val) == (ch1, "value")


@pytest.mark.asyncio
async def test_select_one_closed_one_open_waits() -> None:
    """with one closed-empty channel and one open channel, select must
    ignore the closed one and wait for the open channel (no spurious raise)."""
    ch1 = Channel()
    ch2 = Channel()
    ch1.close()
    task = asyncio.create_task(select_channel(ch1, ch2))
    await asyncio.sleep(0.05)
    assert not task.done()
    await ch2.send("data")
    ch, val = await asyncio.wait_for(task, timeout=1.0)
    assert (ch, val) == (ch2, "data")


@pytest.mark.asyncio
async def test_select_close_race() -> None:
    """close racing with select must never hang — either the buffered
    item wins or the select raises ChannelClosedError."""
    for _ in range(20):
        ch1 = Channel()
        ch2 = Channel()
        await ch1.send("x")
        task = asyncio.create_task(select_channel(ch1, ch2))
        await asyncio.sleep(0)
        ch1.close()
        ch2.close()
        try:
            ch, val = await asyncio.wait_for(task, timeout=1.0)
            assert ch in (ch1, ch2)
            assert val == "x"
        except ChannelClosedError:
            pass


@pytest.mark.asyncio
async def test_putter_cancel_forwards_wakeup_to_next_putter() -> None:
    """R10 P1: a cancelled putter whose entry was already popped must forward
    the wakeup — the freed slot would otherwise idle while later putters
    sleep forever (pre-fix: the second putter hangs)."""
    ch = Channel(1)
    await ch.send("a")  # fill the buffer

    puts_done: list[str] = []
    a_cancelled = asyncio.Event()

    async def put_b() -> None:
        try:
            await ch.send("b")
            puts_done.append("b")
        except asyncio.CancelledError:
            a_cancelled.set()
            raise

    async def put_c() -> None:
        await ch.send("c")
        puts_done.append("c")

    a = asyncio.create_task(put_b())
    await asyncio.sleep(0)  # A registers in _putters
    c = asyncio.create_task(put_c())
    await asyncio.sleep(0)  # C registers in _putters
    assert len(ch._putters) == 2

    # Consumer takes the buffered item; this pops putter A's entry and
    # queues fut.set_result for A.
    assert ch.try_recv() == "a"

    # Cancel A synchronously, before the set_result callback runs — the
    # wakeup is then consumed by a cancelled putter (lost-wakeup window).
    a.cancel()
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    assert a_cancelled.is_set()

    # The freed slot must reach C: it eventually sends "c".
    await asyncio.wait_for(c, timeout=1.0)
    assert puts_done == ["c"]


@pytest.mark.asyncio
async def test_getter_cancel_forwards_wakeup_to_next_getter() -> None:
    """R10 P1: a cancelled getter whose entry was already popped must forward
    the wakeup — the buffered item would otherwise sit unconsumed while
    later getters sleep forever (pre-fix: the second getter hangs)."""
    ch = Channel(0)  # unbounded
    recv_done: list[str] = []

    async def recv_a() -> None:
        try:
            recv_done.append(await ch.recv())
        except asyncio.CancelledError:
            raise

    async def recv_b() -> None:
        recv_done.append(await ch.recv())

    a = asyncio.create_task(recv_a())
    await asyncio.sleep(0)  # A registers in _getters
    b = asyncio.create_task(recv_b())
    await asyncio.sleep(0)  # B registers in _getters
    assert len(ch._getters) == 2

    # Sender puts an item; this pops getter A's entry and queues
    # set_result for A.
    assert ch.try_send("x") is True

    # Cancel A synchronously — the wakeup is consumed by a cancelled getter.
    a.cancel()
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    # The buffered item must still reach B.
    await asyncio.wait_for(b, timeout=1.0)
    assert recv_done == ["x"]


def test_channel_empty_full_maxsize() -> None:
    """Channel should correctly report repr(), empty(), full(), and maxsize."""
    ch_unbounded = Channel(0)
    assert repr(ch_unbounded) == "<Channel is_closed=False>"
    assert ch_unbounded.maxsize == 0
    assert ch_unbounded.empty() is True
    assert ch_unbounded.full() is False

    ch_unbounded.try_send(1)
    assert ch_unbounded.empty() is False
    assert ch_unbounded.full() is False
    ch_unbounded.close()
    assert ch_unbounded.is_closed

    ch_bounded = Channel(2)
    assert ch_bounded.maxsize == 2
    assert ch_bounded.empty() is True
    assert ch_bounded.full() is False

    ch_bounded.try_send("a")
    assert ch_bounded.empty() is False
    assert ch_bounded.full() is False

    ch_bounded.try_send("b")
    assert ch_bounded.empty() is False
    assert ch_bounded.full() is True


# ---------------------------------------------------------------------------
# Concurrency audit & semantic refactoring tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_select_channel_timeout_zero_no_false_timeout() -> None:
    """select_channel with ready data and timeout=0 must return ready channel immediately."""
    ch1 = Channel()
    ch2 = Channel()

    ch1.try_send("hello")

    # timeout=0 must immediately probe ch1 without false TimeoutError
    ch, val = await select_channel(ch1, ch2, timeout=0)
    assert ch is ch1
    assert val == "hello"


@pytest.mark.asyncio
async def test_select_channel_empty_timeout_zero() -> None:
    """select_channel on empty channels with timeout=0 raises TimeoutError."""
    ch1 = Channel()
    ch2 = Channel()

    with pytest.raises(TimeoutError):
        await select_channel(ch1, ch2, timeout=0)


@pytest.mark.asyncio
async def test_select_channel_deadline_contention_retry() -> None:
    """select_channel contention retry must deduct elapsed time against deadline."""
    ch1 = Channel()
    ch2 = Channel()

    start = time.monotonic()
    with pytest.raises(TimeoutError, match="select_channel timed out"):
        await select_channel(ch1, ch2, timeout=0.05)
    elapsed = time.monotonic() - start
    assert 0.04 <= elapsed < 0.5, f"Timeout took {elapsed}s, expected ~0.05s"
    assert len(ch1._getters) == 0 and len(ch1._notifiers) == 0
    assert len(ch2._getters) == 0 and len(ch2._notifiers) == 0


@pytest.mark.asyncio
async def test_select_channel_contention_iterative() -> None:
    """select_channel with contended consumers must not raise RecursionError."""
    ch1 = Channel()
    ch2 = Channel()

    async def producer() -> None:
        for i in range(50):
            await ch1.send(i)
            await ch2.send(i)

    async def consumer() -> list[int]:
        consumed: list[int] = []
        for _ in range(50):
            try:
                _, val = await select_channel(ch1, ch2, timeout=0.5)
                consumed.append(val)
            except TimeoutError:
                break
        return consumed

    prod_task = asyncio.create_task(producer())
    c1 = asyncio.create_task(consumer())
    c2 = asyncio.create_task(consumer())

    await prod_task
    r1, r2 = await asyncio.gather(c1, c2)
    assert len(r1) + len(r2) == 100


@pytest.mark.asyncio
async def test_select_channel_ghost_receiver_token_forwarding() -> None:
    """R10: When multiple select watchers wait on a shared channel, and the first
    watcher is fulfilled by another channel (becoming a ghost receiver), the shared
    channel's wakeup must be forwarded to subsequent watchers without lost wakeups."""
    ch_shared = Channel()
    ch_other = Channel()

    # Waiter 1 waits on (ch_shared, ch_other)
    w1_task = asyncio.create_task(select_channel(ch_shared, ch_other, timeout=2.0))
    # Waiter 2 waits on (ch_shared,)
    w2_task = asyncio.create_task(select_channel(ch_shared, timeout=2.0))

    await wait_all_tasks_blocked()

    # Fulfill waiter 1 via ch_other
    await ch_other.send("other_val")
    res1_ch, res1_val = await w1_task
    assert res1_ch is ch_other
    assert res1_val == "other_val"

    # Now send to ch_shared -> waiter 2 must reliably receive it
    await ch_shared.send("shared_val")
    res2_ch, res2_val = await w2_task
    assert res2_ch is ch_shared
    assert res2_val == "shared_val"


@pytest.mark.asyncio
async def test_select_channel_fast_prng_distribution() -> None:
    """select_channel with lock-free PRNG must select ready channels fairly across multiple rounds."""
    ch1 = Channel()
    ch2 = Channel()
    ch3 = Channel()

    counts = {ch1: 0, ch2: 0, ch3: 0}
    rounds = 150
    for i in range(rounds):
        await ch1.send(f"val1_{i}")
        await ch2.send(f"val2_{i}")
        await ch3.send(f"val3_{i}")

        selected, _ = await select_channel(ch1, ch2, ch3)
        counts[selected] += 1

        # Drain unselected items
        for ch in (ch1, ch2, ch3):
            if ch is not selected:
                await ch.recv()

    # All channels should be selected at least once with uniform pseudo-random start
    assert counts[ch1] > 0
    assert counts[ch2] > 0
    assert counts[ch3] > 0
    assert sum(counts.values()) == rounds


@pytest.mark.asyncio
async def test_fastchannel_cancellation_storm() -> None:
    """Cancellation storm on Channel: 500 queued getters simultaneously cancelled."""
    ch = Channel()
    cancelled_count = 0

    async def getter() -> None:
        nonlocal cancelled_count
        try:
            await ch.recv()
        except asyncio.CancelledError:
            cancelled_count += 1
            raise

    tasks = [asyncio.create_task(getter()) for _ in range(500)]
    await asyncio.sleep(0.01)
    assert len(ch._getters) == 500

    for t in tasks:
        t.cancel()

    await asyncio.gather(*tasks, return_exceptions=True)
    assert cancelled_count == 500
    assert len(ch._getters) == 0

    # Normal recv/send works without leftover corruption
    await ch.send("post_storm")
    assert await ch.recv() == "post_storm"


@pytest.mark.asyncio
async def test_select_channel_cancellation_storm() -> None:
    """Cancellation storm on select_channel: 200 select watchers simultaneously cancelled."""
    ch1 = Channel()
    ch2 = Channel()
    cancelled_count = 0

    async def select_watcher() -> None:
        nonlocal cancelled_count
        try:
            await select_channel(ch1, ch2)
        except asyncio.CancelledError:
            cancelled_count += 1
            raise

    tasks = [asyncio.create_task(select_watcher()) for _ in range(200)]
    await asyncio.sleep(0.01)
    assert len(ch1._notifiers) == 200
    assert len(ch2._notifiers) == 200

    for t in tasks:
        t.cancel()

    await asyncio.gather(*tasks, return_exceptions=True)
    assert cancelled_count == 200
    assert len(ch1._notifiers) == 0
    assert len(ch2._notifiers) == 0

    # Send an item and select immediately works
    await ch1.send("healthy")
    chosen, val = await select_channel(ch1, ch2)
    assert chosen is ch1
    assert val == "healthy"


@pytest.mark.asyncio
async def test_fastchannel_anti_barging_fifo() -> None:
    """Anti-barging: Queued getters must receive items in FIFO order even if non-blocking
    try_recv callers attempt to barge in while getters are waking up."""
    ch = Channel()
    results: list[int] = []

    async def queued_receiver(idx: int) -> None:
        val = await ch.recv()
        results.append(val)

    # Queue 3 receivers
    t1 = asyncio.create_task(queued_receiver(1))
    t2 = asyncio.create_task(queued_receiver(2))
    t3 = asyncio.create_task(queued_receiver(3))
    await wait_all_tasks_blocked()
    assert len(ch._getters) == 3

    # Send 3 items in sequence
    for i in range(1, 4):
        ch.try_send(i)

    await asyncio.gather(t1, t2, t3)
    assert results == [1, 2, 3]


@pytest.mark.asyncio
async def test_fastchannel_anti_barging_try_recv_rejection_when_getters_queued() -> None:
    """Non-blocking try_recv must be rejected if there are queued getters waiting."""
    ch = Channel()

    # Queue an async getter
    recv_task = asyncio.create_task(ch.recv())
    await wait_all_tasks_blocked()
    assert len(ch._getters) == 1

    # Send item into channel
    ch.try_send("queued_item")

    # A concurrent try_recv must NOT barge ahead of the waiting getter
    with pytest.raises(WouldBlock):
        ch.try_recv()

    # The original getter must receive the item
    res = await asyncio.wait_for(recv_task, timeout=1.0)
    assert res == "queued_item"


@pytest.mark.asyncio
async def test_fastchannel_anti_barging_try_send_rejection_when_putters_queued() -> None:
    """Non-blocking try_send must be rejected if there are queued putters waiting."""
    ch = Channel(maxsize=1)
    await ch.send("item1")

    # Queue an async putter
    put_task = asyncio.create_task(ch.send("item2"))
    await wait_all_tasks_blocked()
    assert len(ch._putters) == 1

    # Concurrent try_send must be rejected due to anti-barging gate
    assert ch.try_send("barge_item") is False

    # Consume item1 to let queued putter proceed
    assert await ch.recv() == "item1"
    await asyncio.wait_for(put_task, timeout=1.0)
    assert await ch.recv() == "item2"


@pytest.mark.asyncio
async def test_channel_none_payload_cancellation_preservation() -> None:
    """Ensure None payload is not lost when receiver is cancelled after wakeup dispatch."""
    ch = Channel()

    # 1. Receiver 1 starts waiting
    recv_task1 = asyncio.create_task(ch.recv())
    await wait_all_tasks_blocked()
    assert len(ch._getters) == 1

    # 2. Cancel receiver 1 and immediately send None
    recv_task1.cancel()
    ch.try_send(None)

    # 3. Allow cancellation callback to run
    with pytest.raises(asyncio.CancelledError):
        await recv_task1

    # 4. Receiver 2 must reliably receive the None payload
    recv_task2 = asyncio.create_task(ch.recv())
    result = await asyncio.wait_for(recv_task2, timeout=1.0)
    assert result is None


@pytest.mark.asyncio
async def test_channel_send_sync_from_thread() -> None:
    """Ensure send_sync reliably delivers items from synchronous worker threads."""
    ch = Channel(maxsize=4)
    items_to_send = list(range(20))

    def producer_thread() -> None:
        for item in items_to_send:
            ch.send_sync(item)

    thread = threading.Thread(target=producer_thread)
    thread.start()

    received = []
    for _ in range(len(items_to_send)):
        received.append(await ch.recv())

    thread.join(timeout=2.0)
    assert received == items_to_send


@pytest.mark.asyncio
async def test_channel_split_send_receive_views() -> None:
    """Ensure Channel.split() produces isolated SendChannel and ReceiveChannel views."""
    ch = Channel(maxsize=3)
    tx, rx = ch.split()

    assert repr(tx) == "<SendChannel is_closed=False>"
    assert repr(rx) == "<ReceiveChannel is_closed=False>"

    assert tx.maxsize == 3
    assert rx.empty()
    assert not tx.full()

    # Send through tx
    await tx.send("item1")
    assert tx.try_send("item2") is True
    tx.send_sync("item3")

    assert tx.full()
    assert rx.qsize() == 3

    # Receive through rx
    assert rx.try_recv() == "item1"
    assert await rx.recv() == "item2"
    assert await rx.recv() == "item3"

    # Test context manager on tx and rx
    async with tx:
        await tx.send("item4")

    assert rx.try_recv() == "item4"
    assert tx.is_closed
    assert rx.is_closed


@pytest.mark.asyncio
async def test_channel_recv_sync_from_thread() -> None:
    """Ensure recv_sync reliably receives items on a worker thread with Condvar blocking."""
    ch = Channel(maxsize=4)
    received = []

    def consumer_thread() -> None:
        for _ in range(5):
            received.append(ch.recv_sync(timeout=2.0))

    thread = threading.Thread(target=consumer_thread)
    thread.start()

    for i in range(5):
        await asyncio.sleep(0.01)
        await ch.send(f"msg_{i}")

    thread.join(timeout=3.0)
    assert received == [f"msg_{i}" for i in range(5)]


def test_channel_sync_timeout_and_closed() -> None:
    """Ensure send_sync and recv_sync timeout and handle channel close correctly."""
    ch = Channel(maxsize=1)
    ch.send_sync("first", timeout=1.0)

    # Channel full, send_sync times out
    with pytest.raises(TimeoutError):
        ch.send_sync("second", timeout=0.05)

    assert ch.recv_sync(timeout=1.0) == "first"

    # Channel empty, recv_sync times out
    with pytest.raises(TimeoutError):
        ch.recv_sync(timeout=0.05)

    ch.close()
    with pytest.raises(ChannelClosedError):
        ch.send_sync("blocked")

    with pytest.raises(ChannelClosedError):
        ch.recv_sync()


# ---------------------------------------------------------------------------
# Phase 3 TDD Suite: Cancelled getter zero-loss & FIFO preservation + select
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("scenario", "maxsize", "extra"),
    [
        ("bounded_full_fifo", 1, 1),
        ("bounded_full_fifo", 2, 2),
        ("bounded_full_fifo", 3, 3),
        ("bounded_full_fifo", 4, 4),
        ("bounded_full_fifo", 5, 5),
        ("closed_after_send", 0, 1),
        ("closed_after_send", 1, 1),
        ("closed_after_send", 2, 2),
        ("post_set_result_cancel", 0, 1),
        ("post_set_result_cancel", 1, 1),
        ("post_set_result_cancel", 2, 2),
        ("forward_to_second_getter", 0, 2),
        ("forward_to_second_getter", 1, 2),
        ("select_round_robin", 2, 2),
        ("select_round_robin", 2, 4),
    ],
)
async def test_channel_cancel_conservation_valid(scenario: str, maxsize: int, extra: int) -> None:
    """Valid parameterized tests for zero-loss cancelled getter recovery and FIFO order."""
    if scenario == "bounded_full_fifo":
        ch = Channel(maxsize=maxsize)
        recv_task = asyncio.create_task(ch.recv())
        await wait_all_tasks_blocked()

        # First item wakes recv_task (via call_soon_threadsafe, holding 1 slot in-flight)
        assert ch.try_send(0) is True
        # Fill the remaining maxsize - 1 bounded slots; capacity conservation blocks overflow
        for i in range(1, maxsize):
            assert ch.try_send(i) is True
        assert ch.try_send(maxsize) is False
        # Cancel recv_task before it can consume item 0
        recv_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await recv_task
        await asyncio.sleep(0)

        # Every single item [0, ..., maxsize - 1] must be preserved in strict FIFO order without overflow!
        assert ch.qsize() == maxsize
        received = [await ch.recv(timeout=0.5) for _ in range(maxsize)]
        assert received == list(range(maxsize))

    elif scenario == "closed_after_send":
        ch = Channel(maxsize=maxsize)
        recv_task = asyncio.create_task(ch.recv())
        await wait_all_tasks_blocked()

        assert ch.try_send("before_close") is True
        ch.close()
        recv_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await recv_task
        await asyncio.sleep(0)

        # Even though channel was closed, "before_close" was sent prior to close() and must drain!
        assert await ch.recv(timeout=0.5) == "before_close"

    elif scenario == "post_set_result_cancel":
        ch = Channel(maxsize=maxsize)
        recv_task = asyncio.create_task(ch.recv())
        await wait_all_tasks_blocked()

        assert ch.try_send("first") is True
        if maxsize == 0 or maxsize >= 2:
            assert ch.try_send("second") is True
        else:
            assert ch.try_send("second") is False
        # Let _wake_fut run and call fut.set_result("first"), then cancel recv_task in same turn!
        loop = asyncio.get_running_loop()
        loop.call_soon(recv_task.cancel)
        with pytest.raises(asyncio.CancelledError):
            await recv_task

        assert await ch.recv(timeout=0.5) == "first"
        if maxsize == 0 or maxsize >= 2:
            assert await ch.recv(timeout=0.5) == "second"

    elif scenario == "forward_to_second_getter":
        ch = Channel(maxsize=maxsize)
        t1 = asyncio.create_task(ch.recv())
        t2 = asyncio.create_task(ch.recv())
        await wait_all_tasks_blocked()

        assert ch.try_send("handoff_val") is True
        t1.cancel()
        with pytest.raises(asyncio.CancelledError):
            await t1
        assert await asyncio.wait_for(t2, timeout=0.5) == "handoff_val"

    elif scenario == "select_round_robin":
        chans = [Channel(maxsize=maxsize) for _ in range(extra)]
        for idx, c in enumerate(chans):
            c.try_send(idx)
        seen = set()
        for _ in range(extra):
            _chosen, val = await select_channel(*chans)
            seen.add(val)
        assert seen == set(range(extra))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("boundary_case", "maxsize", "payload"),
    [
        ("sync_try_recv_after_cancel", 1, "imm_1"),
        ("sync_try_recv_after_cancel", 2, "imm_2"),
        ("sync_try_recv_after_cancel", 0, "imm_0"),
        ("falsy_payload", 1, None),
        ("falsy_payload", 1, False),
        ("falsy_payload", 1, 0),
        ("falsy_payload", 1, ""),
        ("falsy_payload", 1, {}),
        ("consecutive_cancelled_getters", 1, 2),
        ("consecutive_cancelled_getters", 1, 3),
        ("consecutive_cancelled_getters", 2, 5),
        ("cancelled_putter_forwarding", 1, 0),
        ("cancelled_putter_forwarding", 1, 1),
        ("cancelled_putter_forwarding", 1, 2),
        ("select_default_boundary", 1, "default_sentinel"),
    ],
)
async def test_channel_cancel_conservation_boundary(
    boundary_case: str, maxsize: int, payload: Any
) -> None:
    """Boundary parameterized tests for immediate sync try_recv, falsy payloads, and chained cancels."""
    if boundary_case == "sync_try_recv_after_cancel":
        ch = Channel(maxsize=maxsize)
        t = asyncio.create_task(ch.recv())
        await wait_all_tasks_blocked()
        t.cancel()
        ch.try_send(payload)
        with pytest.raises(asyncio.CancelledError):
            await t
        # Immediately call synchronous try_recv() without an extra asyncio.sleep(0)!
        assert ch.try_recv() == payload

    elif boundary_case == "falsy_payload":
        ch = Channel(maxsize=maxsize)
        t = asyncio.create_task(ch.recv())
        await wait_all_tasks_blocked()
        assert ch.try_send(payload) is True
        # In-flight getter occupies the single slot on maxsize=1
        assert ch.try_send("after_falsy") is False
        t.cancel()
        with pytest.raises(asyncio.CancelledError):
            await t
        assert await ch.recv(timeout=0.5) == payload
        assert ch.try_send("after_falsy") is True
        assert await ch.recv(timeout=0.5) == "after_falsy"

    elif boundary_case == "consecutive_cancelled_getters":
        ch = Channel(maxsize=maxsize)
        n_cancel = int(payload)
        tasks = [asyncio.create_task(ch.recv()) for _ in range(n_cancel)]
        live_task = asyncio.create_task(ch.recv())
        await wait_all_tasks_blocked()

        for t in tasks:
            t.cancel()
        assert ch.try_send("survivor") is True
        for t in tasks:
            with pytest.raises(asyncio.CancelledError):
                await t
        assert await asyncio.wait_for(live_task, timeout=0.5) == "survivor"

    elif boundary_case == "cancelled_putter_forwarding":
        ch = Channel(maxsize=maxsize)
        await ch.send("initial")
        p1 = asyncio.create_task(ch.send("from_p1"))
        extra_putters = [
            asyncio.create_task(ch.send(f"from_extra_{i}")) for i in range(int(payload))
        ]
        await wait_all_tasks_blocked()

        # Free space to wake p1, then immediately cancel p1
        assert ch.try_recv() == "initial"
        p1.cancel()
        with pytest.raises(asyncio.CancelledError):
            await p1

        for i, ep in enumerate(extra_putters):
            await asyncio.wait_for(ep, timeout=0.5)
            assert await ch.recv(timeout=0.5) == f"from_extra_{i}"

    elif boundary_case == "select_default_boundary":
        ch = Channel(maxsize=maxsize)
        assert await select_channel(ch, default=payload) == payload


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("error_op", "maxsize", "expected_exc"),
    [
        ("try_send_closed", 0, ChannelClosedError),
        ("try_send_closed", 1, ChannelClosedError),
        ("try_send_closed", 2, ChannelClosedError),
        ("send_closed", 0, ChannelClosedError),
        ("send_closed", 1, ChannelClosedError),
        ("send_closed", 2, ChannelClosedError),
        ("try_recv_closed_empty", 0, ChannelClosedError),
        ("try_recv_closed_empty", 1, ChannelClosedError),
        ("try_recv_closed_empty", 2, ChannelClosedError),
        ("recv_closed_empty", 0, ChannelClosedError),
        ("recv_closed_empty", 1, ChannelClosedError),
        ("try_recv_open_empty", 1, WouldBlock),
        ("select_no_channels", 0, ValueError),
        ("select_all_closed", 1, ChannelClosedError),
        ("select_zero_timeout", 1, TimeoutError),
    ],
)
async def test_channel_cancel_conservation_error(
    error_op: str, maxsize: int, expected_exc: type[BaseException]
) -> None:
    """Error parameterized tests asserting exact exception types for invalid/closed channel ops."""
    ch = Channel(maxsize=maxsize)
    if error_op == "try_send_closed":
        ch.close()
        with pytest.raises(expected_exc):
            ch.try_send("x")
    elif error_op == "send_closed":
        ch.close()
        with pytest.raises(expected_exc):
            await ch.send("x")
    elif error_op == "try_recv_closed_empty":
        ch.close()
        with pytest.raises(expected_exc):
            ch.try_recv()
    elif error_op == "recv_closed_empty":
        ch.close()
        with pytest.raises(expected_exc):
            await ch.recv()
    elif error_op == "try_recv_open_empty":
        with pytest.raises(expected_exc):
            ch.try_recv()
    elif error_op == "select_no_channels":
        with pytest.raises(expected_exc):
            await select_channel()
    elif error_op == "select_all_closed":
        ch2 = Channel(maxsize=maxsize)
        ch.close()
        ch2.close()
        with pytest.raises(expected_exc):
            await select_channel(ch, ch2)
    elif error_op == "select_zero_timeout":
        with pytest.raises(expected_exc):
            await select_channel(ch, timeout=0.0)


@pytest.mark.asyncio
async def test_repro_try_send_select_watcher_maxsize_overflow() -> None:
    """try_send must not overflow maxsize or barge ahead of putters when select_watchers are present."""
    ch = Channel(maxsize=1)
    sel1 = asyncio.create_task(select_channel(ch))
    sel2 = asyncio.create_task(select_channel(ch))
    await wait_all_tasks_blocked()

    assert ch.try_send("first") is True
    assert ch.qsize() == 1
    # Second try_send must return False because maxsize=1 is already full!
    assert ch.try_send("overflow") is False
    assert ch.qsize() == 1

    sel1.cancel()
    sel2.cancel()
    await asyncio.gather(sel1, sel2, return_exceptions=True)


@pytest.mark.asyncio
async def test_repro_send_sync_woken_when_in_flight_putter_cancelled() -> None:
    """register_getter and in-flight putter cancel must notify condvar so blocked send_sync threads wake up."""
    ch = Channel(maxsize=1)
    assert ch.try_send("initial") is True

    send_sync_err: list[BaseException] = []

    def sync_sender() -> None:
        try:
            ch.send_sync("sync_item", timeout=0.25)
        except BaseException as exc:
            send_sync_err.append(exc)

    t = threading.Thread(target=sync_sender)
    t.start()
    await asyncio.sleep(0.03)

    # Consume "initial" via register_getter_ticket fast-path -> must notify condvar to wake sync_sender!
    loop = asyncio.get_running_loop()
    res = ch._inner.register_getter_ticket(loop, loop.create_future())
    assert res == (0, "initial")

    await asyncio.to_thread(t.join, 1.0)
    assert not send_sync_err, (
        f"send_sync failed to wake after register_getter drained buffer: {send_sync_err}"
    )
    assert ch.try_recv() == "sync_item"


@pytest.mark.asyncio
async def test_repro_select_channel_receive_channel_and_unicast_forwarding() -> None:
    """select_channel must support ReceiveChannel and forward unicast wakeup if cancelled after arbiter_fut resolves."""
    ch = Channel(maxsize=2)
    tx, rx = ch.split()

    sel1 = asyncio.create_task(select_channel(rx))
    sel2 = asyncio.create_task(select_channel(rx))
    await wait_all_tasks_blocked()

    assert tx.try_send("only_item") is True
    # Let _select_wake fulfill sel1's arbiter_fut, and cancel sel1 in the same turn before it steps
    loop = asyncio.get_running_loop()
    loop.call_soon(sel1.cancel)
    with pytest.raises(asyncio.CancelledError):
        await sel1

    # sel2 must receive the forwarded wakeup and consume "only_item"
    chosen, val = await asyncio.wait_for(sel2, timeout=0.3)
    assert chosen is rx
    assert val == "only_item"


@pytest.mark.asyncio
async def test_select_channel_caller_cancel_cleans_up() -> None:
    """Cancelling a select_channel caller cleans up channel notifiers immediately."""
    ch = Channel()
    sel = asyncio.create_task(select_channel(ch))
    await wait_all_tasks_blocked()
    await asyncio.sleep(0)
    assert len(ch._notifiers) == 1

    sel.cancel()
    with pytest.raises(asyncio.CancelledError):
        await sel
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    assert len(ch._notifiers) == 0


@pytest.mark.asyncio
async def test_bounded_channel_getter_cancel_respects_maxsize() -> None:
    """In-flight getter cancel on full Channel(maxsize=1) preserves capacity without exceeding maxsize."""
    ch = Channel(maxsize=1)

    getter_task = asyncio.create_task(ch.recv())
    await asyncio.sleep(0)

    p1_task = asyncio.create_task(ch.send("a"))
    await asyncio.sleep(0)
    await p1_task

    assert ch.full() is True
    assert ch.try_send("b") is False

    p2_task = asyncio.create_task(ch.send("b"))
    await asyncio.sleep(0)
    assert not p2_task.done()

    getter_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await getter_task
    await asyncio.sleep(0)

    assert ch.qsize() == 1
    assert not p2_task.done()
    assert await ch.recv() == "a"
    await p2_task
    assert await ch.recv() == "b"


@pytest.mark.asyncio
async def test_closed_channel_inflight_getter_cancel_retains_item() -> None:
    """Cancelling an in-flight getter after close() returns item to buffer for subsequent recv()."""
    ch = Channel()
    g1 = asyncio.create_task(ch.recv())
    await asyncio.sleep(0)

    assert ch.try_send("payload") is True
    ch.close()

    g1.cancel()
    with pytest.raises(asyncio.CancelledError):
        await g1

    assert await ch.recv() == "payload"


@pytest.mark.asyncio
async def test_try_send_no_barge_over_inflight_putter() -> None:
    """try_send() must return False while a woken putter is still in-flight before claim_put."""
    ch = Channel(maxsize=1)
    assert ch.try_send("first") is True

    p2_task = asyncio.create_task(ch.send("second"))
    await asyncio.sleep(0)

    assert ch.try_recv() == "first"
    assert ch.try_send("x") is False

    await p2_task
    assert ch.qsize() == 1
    assert ch.try_recv() == "second"


@pytest.mark.asyncio
async def test_try_recv_no_barge_over_queued_getters() -> None:
    """try_recv() raises WouldBlock while queued getters exist during in-flight rollback."""
    ch = Channel()
    g1 = asyncio.create_task(ch.recv())
    await asyncio.sleep(0)

    assert ch.try_send("item1") is True
    g2 = asyncio.create_task(ch.recv())
    await asyncio.sleep(0)

    g1.cancel()
    with pytest.raises(asyncio.CancelledError):
        await g1

    with pytest.raises(WouldBlock):
        ch.try_recv()
    assert await g2 == "item1"


def test_cancel_getter_stale_ticket_idempotent() -> None:
    """Calling RawAsyncChannel.cancel_getter with a stale waiter_id is a safe no-op."""
    ch = Channel(maxsize=10)
    raw = ch._inner
    assert raw.try_send("buffered_item") is True
    assert raw.cancel_getter(999_999) is False
    assert raw.qsize() == 1
    assert ch.try_recv() == "buffered_item"


@pytest.mark.asyncio
async def test_send_sync_fifo_with_async_putters() -> None:
    """send_sync() from a worker thread preserves strict FIFO order with async putters."""
    ch = Channel(maxsize=1)
    assert ch.try_send("item0") is True

    t1 = asyncio.create_task(ch.send("async_1"))
    await asyncio.sleep(0)

    def sync_worker() -> None:
        ch.send_sync("sync_2")

    th = threading.Thread(target=sync_worker, daemon=True)
    th.start()
    time.sleep(0.04)

    t3 = asyncio.create_task(ch.send("async_3"))
    await asyncio.sleep(0)

    assert await ch.recv() == "item0"
    assert await ch.recv() == "async_1"
    assert await ch.recv() == "sync_2"
    assert await ch.recv() == "async_3"

    th.join(timeout=1.0)
    await t1
    await t3
