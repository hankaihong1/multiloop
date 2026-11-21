"""Tests for multiloop.Condition and multiloop.Barrier primitives."""

import asyncio
import threading
from typing import Any

import pytest

from multiloop import Barrier, Condition, Lock
from multiloop.testing import wait_all_tasks_blocked

# ---------------------------------------------------------------------------
# Condition tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_condition_wait_notify():
    """A waiter wakes after notify and holds the lock on resume."""
    cond = Condition()
    shared: list[int] = []

    async def consumer() -> None:
        async with cond:
            await cond.wait()
            shared.append(1)

    task = asyncio.create_task(consumer())
    await wait_all_tasks_blocked()

    # The consumer should be waiting; lock is free.
    cond.notify()
    await asyncio.wait_for(task, timeout=5)
    assert shared == [1]


@pytest.mark.asyncio
async def test_condition_wait_predicate_loop():
    """Classic wait-while-not-ready pattern with predicate loop."""
    cond = Condition()
    ready = False
    shared_value = 0

    async def consumer() -> int:
        async with cond:
            while not ready:
                await cond.wait()
            return shared_value

    task = asyncio.create_task(consumer())
    await asyncio.sleep(0.05)

    async with cond:
        ready = True
        shared_value = 42
        cond.notify()

    result = await asyncio.wait_for(task, timeout=5)
    assert result == 42


@pytest.mark.asyncio
async def test_condition_notify_all():
    """3 waiters all resume after notify_all."""
    cond = Condition()
    results: list[int] = []

    async def waiter(i: int) -> None:
        async with cond:
            await cond.wait()
            results.append(i)

    tasks = [asyncio.create_task(waiter(i)) for i in range(3)]
    await asyncio.sleep(0.05)

    cond.notify_all()
    await asyncio.wait_for(asyncio.gather(*tasks), timeout=5)
    assert len(results) == 3
    assert sorted(results) == [0, 1, 2]


@pytest.mark.asyncio
async def test_condition_cancel_while_waiting():
    """Cancelled waiter is removed from queue; lock is re-acquired."""
    cond = Condition()
    order: list[str] = []

    async def waiter() -> None:
        async with cond:
            await cond.wait()
            order.append("resumed")

    async def holder() -> None:
        async with cond:
            await asyncio.sleep(0.1)
            cond.notify()
            order.append("notified")

    # First, have one task enter wait
    async with cond:
        t1 = asyncio.create_task(waiter())
        await asyncio.sleep(0.05)

        # Cancel the first waiter while it's waiting
        t1.cancel()
        with pytest.raises(asyncio.CancelledError):
            await t1

        # Now the lock should be held again (re-acquired after cancel)
        # Hold and release, then a new waiter should work fine
        t2 = asyncio.create_task(waiter())

    # Release lock — t2 should acquire and wait
    await asyncio.sleep(0.05)

    # Notify — only t2 should resume
    cond.notify()
    await asyncio.wait_for(t2, timeout=5)
    assert "resumed" in order


@pytest.mark.asyncio
async def test_condition_custom_lock():
    """Condition accepts an external Lock."""
    lock = Lock()
    cond = Condition(lock=lock)
    assert cond._lock is lock

    acquired = False

    async def worker() -> None:
        nonlocal acquired
        await cond.acquire()
        acquired = True
        cond.release()

    await worker()
    assert acquired


@pytest.mark.asyncio
async def test_condition_context_manager():
    """Condition.__aenter__ / __aexit__ work correctly."""
    cond = Condition()
    async with cond:
        assert cond._lock.locked
    assert not cond._lock.locked


@pytest.mark.asyncio
async def test_condition_cancel_during_reacquire_forwards_notification():
    """If a notified waiter is cancelled while re-acquiring the lock,
    the notification must be forwarded to the next live waiter.
    """
    cond = Condition()
    woken: list[int] = []

    async def waiter(idx: int) -> None:
        async with cond:
            await cond.wait()
            woken.append(idx)

    t1 = asyncio.create_task(waiter(1))
    t2 = asyncio.create_task(waiter(2))
    await wait_all_tasks_blocked()
    assert len(cond._waiters) == 2

    # Acquire lock in main task after waiters have released it inside wait()
    async with cond:
        # Notify 1 waiter while holding the lock (t1's event is set, t1 wakes up from event.wait()
        # and starts trying to reacquire cond._lock, but cond._lock is held by main task)
        cond.notify(1)
        await asyncio.sleep(0.02)

        # Cancel t1 while t1 is blocked waiting inside _reacquire_lock()
        t1.cancel()
        await asyncio.sleep(0.02)

    # Main task has released cond lock
    with pytest.raises(asyncio.CancelledError):
        await t1

    # t2 MUST receive the forwarded notification and complete
    await asyncio.wait_for(t2, timeout=2.0)
    assert woken == [2]


# ---------------------------------------------------------------------------
# Barrier cross-loop wake (Wave 1 regression test)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_barrier_cross_loop_wake():
    """Barrier wakes waiters when the last party arrives from a different event loop.

    Post-fix: Barrier.wait() uses call_soon_threadsafe to set the waiter
    events, which works correctly when the last waiter is in a different
    event loop than the waiting parties.
    """
    barrier = Barrier(parties=2)
    results: list[int] = []

    def waiter_in_thread():
        loop = asyncio.new_event_loop()

        async def _wait():
            r = await barrier.wait()
            results.append(r.parties)

        loop.run_until_complete(_wait())
        loop.close()

    thread = threading.Thread(target=waiter_in_thread)
    thread.start()

    # Allow the cross-loop waiter to register
    await asyncio.sleep(0.1)
    assert barrier.n_waiting == 1

    # Last party arrives from our loop — triggers cross-loop wake
    await barrier.wait()
    thread.join(timeout=5)
    assert not thread.is_alive()
    assert results == [2]
    assert barrier.n_waiting == 0


# ---------------------------------------------------------------------------
# Barrier tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_barrier_wait_two_parties():
    """Two parties wait and both unblock."""
    barrier = Barrier(parties=2)
    results: list[int] = []

    async def party(i: int) -> None:
        r = await barrier.wait()
        assert r.parties == 2
        results.append(i)

    await asyncio.wait_for(asyncio.gather(party(1), party(2)), timeout=5)
    assert sorted(results) == [1, 2]


@pytest.mark.asyncio
async def test_barrier_multiple_rounds():
    """Barrier can be reused across multiple rounds."""
    barrier = Barrier(parties=2)
    round_results: list[list[int]] = []

    async def party(i: int) -> None:
        for round_num in range(3):
            r = await barrier.wait()
            assert r.parties == 2
            round_results.append(round_num)

    await asyncio.wait_for(asyncio.gather(party(1), party(2)), timeout=5)
    assert len(round_results) == 6


@pytest.mark.asyncio
async def test_barrier_n_waiting():
    """n_waiting reflects current waiters."""
    barrier = Barrier(parties=3)
    assert barrier.n_waiting == 0
    assert barrier.parties == 3

    done = asyncio.Event()

    async def waiter() -> None:
        await barrier.wait()
        done.set()

    t1 = asyncio.create_task(waiter())
    await asyncio.sleep(0.05)
    assert barrier.n_waiting == 1

    t2 = asyncio.create_task(waiter())
    await asyncio.sleep(0.05)
    assert barrier.n_waiting == 2

    # Third party triggers release
    t3 = asyncio.create_task(waiter())
    await asyncio.wait_for(done.wait(), timeout=5)

    await asyncio.gather(t1, t2, t3)
    assert barrier.n_waiting == 0


@pytest.mark.asyncio
async def test_barrier_abort():
    """Abort raises RuntimeError in waiting tasks."""
    barrier = Barrier(parties=3)

    errors: list[type[Exception]] = []

    async def waiter() -> None:
        try:
            await barrier.wait()
        except BaseException as exc:
            errors.append(type(exc))

    t1 = asyncio.create_task(waiter())
    t2 = asyncio.create_task(waiter())
    await wait_all_tasks_blocked()

    barrier.abort()
    await asyncio.wait_for(asyncio.gather(t1, t2), timeout=5)
    assert len(errors) == 2
    assert all(e is RuntimeError for e in errors)

    # Subsequent wait() calls also raise
    with pytest.raises(RuntimeError, match="aborted"):
        await barrier.wait()


@pytest.mark.asyncio
async def test_barrier_invalid_parties():
    """parties <= 0 raises ValueError."""
    with pytest.raises(ValueError):
        Barrier(parties=0)
    with pytest.raises(ValueError):
        Barrier(parties=-1)


@pytest.mark.asyncio
async def test_condition_barrier_combined():
    """Condition + Barrier used together: producer/consumer with barrier sync."""
    barrier = Barrier(parties=2)
    cond = Condition()
    consumer_ready = asyncio.Event()
    shared: list[int] = []

    async def producer() -> None:
        # Wait for consumer to be ready before proceeding.
        await consumer_ready.wait()
        async with cond:
            shared.append(42)
            cond.notify()
        await barrier.wait()

    async def consumer() -> None:
        async with cond:
            consumer_ready.set()
            await cond.wait()
            val = shared[-1]
        assert val == 42
        await barrier.wait()

    t1 = asyncio.create_task(producer())
    t2 = asyncio.create_task(consumer())
    await asyncio.wait_for(asyncio.gather(t1, t2), timeout=5)
    assert shared == [42]


@pytest.mark.asyncio
async def test_condition_wait_reacquires_lock_through_cancellation():
    """Cancel-retry-loop contract: a cancellation arriving while re-acquiring
    the lock must not abort the re-acquisition.

    Pre-fix behavior (F-0 penetration): the second cancel pierced the
    snapshot/restore shield, wait() raised CE without re-acquiring the lock,
    and the caller's __aexit__ release() raised RuntimeError masking the CE —
    asserting the task ends with CancelledError would fail.
    """
    lock = Lock()
    cond = Condition(lock)
    holder_gate = asyncio.Event()
    holder_done = asyncio.Event()

    async def holder() -> None:
        await cond.acquire()
        holder_gate.set()
        await holder_done.wait()
        cond.release()

    async def waiter() -> None:
        await cond.acquire()
        try:
            await cond.wait()
        finally:
            cond.release()

    w = asyncio.create_task(waiter())
    await wait_all_tasks_blocked()  # waiter acquired and parked in cond.wait() (lock released)
    h = asyncio.create_task(holder())
    await holder_gate.wait()  # holder holds the lock
    w.cancel()  # cancel #1: the waiter enters the re-acquire path
    await wait_all_tasks_blocked()  # re-acquire blocked on the Lock queue (holder holds it)
    assert len(lock._waiters) == 1  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType, reportUnknownArgumentType]
    w.cancel()  # cancel #2: arrives inside the blocked re-acquire window
    holder_done.set()
    await h
    with pytest.raises(asyncio.CancelledError):
        await w
    assert not lock.locked  # the waiter's finally released the lock properly


@pytest.mark.asyncio
async def test_condition_wait_for_sync_and_async_predicate() -> None:
    """Condition.wait_for should support both sync and async predicates."""
    cond = Condition()
    val = 0

    async def producer() -> None:
        nonlocal val
        await asyncio.sleep(0.02)
        async with cond:
            val = 42
            cond.notify_all()

    async def consumer_sync() -> int:
        async with cond:
            res = await cond.wait_for(lambda: val == 42)
            assert res is True
            return val

    async def consumer_async() -> int:
        async def check() -> bool:
            await asyncio.sleep(0.001)
            return val == 42

        async with cond:
            res = await cond.wait_for(check)
            assert res is True
            return val

    p = asyncio.create_task(producer())
    c1 = asyncio.create_task(consumer_sync())
    c2 = asyncio.create_task(consumer_async())

    r1, r2, _ = await asyncio.gather(c1, c2, p)
    assert r1 == 42
    assert r2 == 42


@pytest.mark.asyncio
async def test_barrier_index_leader_election_and_unique_indices() -> None:
    """Barrier.wait() returns BarrierWaitResult with unique indices and leader."""
    barrier = Barrier(parties=4)
    results: list[tuple[int, bool]] = []

    async def party(idx: int) -> None:
        await asyncio.sleep(idx * 0.01)
        res = await barrier.wait()
        results.append((res.index, res.is_leader))

    tasks = [asyncio.create_task(party(i)) for i in range(4)]
    await asyncio.gather(*tasks)

    assert len(results) == 4
    indices = [r[0] for r in results]
    assert sorted(indices) == [0, 1, 2, 3]
    leaders = [r[1] for r in results if r[1] is True]
    assert len(leaders) == 1


@pytest.mark.asyncio
async def test_barrier_reset_wakes_waiters_and_resets_aborted_state() -> None:
    """Barrier.reset() wakes current waiters and allows the barrier to be reused."""
    barrier = Barrier(parties=3)
    barrier_errors: list[str] = []

    async def waiter() -> None:
        try:
            await barrier.wait()
        except RuntimeError as e:
            barrier_errors.append(str(e))

    w1 = asyncio.create_task(waiter())
    w2 = asyncio.create_task(waiter())
    await asyncio.sleep(0.02)
    assert barrier.n_waiting == 2

    # Reset while waiting
    barrier.reset()
    await asyncio.gather(w1, w2)
    assert len(barrier_errors) == 2
    assert all("reset" in err for err in barrier_errors)

    # Test abort then reset
    barrier.abort()
    with pytest.raises(RuntimeError, match="aborted"):
        await barrier.wait()

    # Reset clears aborted state
    barrier.reset()
    completed = []

    async def worker(i: int) -> None:
        res = await barrier.wait()
        completed.append((i, res.parties, res.index))

    tasks = [asyncio.create_task(worker(i)) for i in range(3)]
    await asyncio.gather(*tasks)
    assert len(completed) == 3


# ---------------------------------------------------------------------------
# Concurrency audit & semantic refactoring tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_condition_wait_unacquired_lock_no_ghost_waiter() -> None:
    """Condition.wait without holding lock raises and leaves no ghost waiter."""
    lock = Lock()
    cond = Condition(lock)

    # Calling wait() without acquiring lock must fail
    with pytest.raises(RuntimeError):
        await cond.wait()

    # Now verify that a real waiter receives notify(1) properly
    woken = False

    async def real_waiter() -> None:
        nonlocal woken
        async with cond:
            await cond.wait()
            woken = True

    t = asyncio.create_task(real_waiter())
    await asyncio.sleep(0.02)

    async with cond:
        cond.notify(1)

    await asyncio.wait_for(t, timeout=1.0)
    assert woken, "Real waiter must be woken by notify(1) — not swallowed by ghost waiter"


@pytest.mark.asyncio
async def test_barrier_abort_after_round_graduates_preserved() -> None:
    """Barrier.abort called after a round completes must not fail already-graduated waiters."""
    barrier = Barrier(2)
    results: list[object] = []

    async def party() -> None:
        res = await barrier.wait()
        results.append(res)

    t1 = asyncio.create_task(party())
    t2 = asyncio.create_task(party())

    await asyncio.gather(t1, t2)
    assert len(results) == 2

    # Now abort barrier for subsequent rounds
    barrier.abort()

    # Verify future waits fail
    with pytest.raises(RuntimeError, match="barrier has been aborted"):
        await barrier.wait()


@pytest.mark.asyncio
async def test_barrier_broken_on_cancellation() -> None:
    """Barrier automatically enters broken state when a waiting party is cancelled."""
    barrier = Barrier(3)
    assert not barrier.broken

    async def party_cancelled() -> None:
        await barrier.wait()

    party2_exc: list[Exception] = []

    async def party_survivor() -> None:
        try:
            await barrier.wait()
        except RuntimeError as e:
            party2_exc.append(e)

    t1 = asyncio.create_task(party_cancelled())
    t2 = asyncio.create_task(party_survivor())
    await asyncio.sleep(0.02)

    assert barrier.n_waiting == 2
    assert not barrier.broken

    # Cancel party 1
    t1.cancel()
    with pytest.raises(asyncio.CancelledError):
        await t1

    # Party 2 must be woken and receive RuntimeError (broken barrier)
    await asyncio.wait_for(t2, timeout=2.0)
    assert len(party2_exc) == 1
    assert "broken" in str(party2_exc[0])
    assert barrier.broken is True

    # Subsequent waits fail immediately while broken
    with pytest.raises(RuntimeError, match="broken"):
        await barrier.wait()

    # Reset clears broken state
    barrier.reset()
    assert not barrier.broken
    assert barrier.n_waiting == 0


@pytest.mark.asyncio
async def test_barrier_subsequent_round_broken_does_not_affect_graduated_party() -> None:
    """A party that graduated from round G must not fail if round G+1 is broken concurrently."""
    barrier = Barrier(2)
    t1_results: list[Any] = []
    t1_errors: list[Exception] = []

    async def party_1() -> None:
        try:
            res = await barrier.wait()
            t1_results.append(res)
        except Exception as e:  # noqa: BLE001
            t1_errors.append(e)

    # Round 0: party 1 enters and blocks
    t1 = asyncio.create_task(party_1())
    await asyncio.sleep(0.01)
    assert barrier.n_waiting == 1

    # Party 2 completes Round 0, then Round 1 breaks BEFORE party 1 resumes from Round 0 wakeup
    res2_r0 = await barrier.wait()
    r1_waiter = asyncio.create_task(barrier.wait())
    barrier.abort()

    await asyncio.wait_for(t1, timeout=2.0)

    # Party 1 graduated from Round 0 before Round 1 broke
    assert len(t1_errors) == 0, f"Party 1 erroneously failed with: {t1_errors}"
    assert len(t1_results) == 1
    assert {t1_results[0].index, res2_r0.index} == {0, 1}
    assert barrier.broken is True

    # Round 1 waiter must observe the broken barrier error
    with pytest.raises(RuntimeError, match=r"broken|aborted"):
        await asyncio.wait_for(r1_waiter, timeout=2.0)

    # Round 2 after reset() must work cleanly
    barrier.reset()
    assert barrier.broken is False
    r2_a = asyncio.create_task(barrier.wait())
    r2_b = asyncio.create_task(barrier.wait())
    r2_res_a, r2_res_b = await asyncio.wait_for(asyncio.gather(r2_a, r2_b), timeout=2.0)
    assert {r2_res_a.index, r2_res_b.index} == {0, 1}


@pytest.mark.asyncio
async def test_condition_notify_consumed_by_cancelled_waiter_forwards() -> None:
    """A notify() consumed by a cancelled waiter forwards wakeup to the next waiter."""
    cond = Condition()
    w3_got = asyncio.Event()
    results: list[str] = []

    async def waiter(name: str, got: asyncio.Event | None = None) -> None:
        async with cond:
            await cond.wait()
            results.append(name)
            if got is not None:
                got.set()

    t2 = asyncio.create_task(waiter("w2"))
    await asyncio.sleep(0.05)
    t3 = asyncio.create_task(waiter("w3", w3_got))
    await asyncio.sleep(0.05)

    cond.notify()
    t2.cancel()

    await asyncio.wait_for(w3_got.wait(), timeout=1.0)
    await asyncio.gather(t2, t3, return_exceptions=True)
    assert results == ["w3"]


@pytest.mark.asyncio
async def test_condition_wait_preserves_body_exc_on_reacquire_cancel() -> None:
    """Non-cancellation exception from wait future is preserved when cancelled during _reacquire_lock()."""
    lock = Lock()
    cond = Condition(lock)
    loop = asyncio.get_running_loop()

    intercepted_fut: asyncio.Future[Any] | None = None
    orig_create_future = loop.create_future

    def patched_create_future() -> asyncio.Future[Any]:
        nonlocal intercepted_fut
        fut = orig_create_future()
        intercepted_fut = fut
        return fut

    loop.create_future = patched_create_future  # type: ignore[method-assign]

    async def worker() -> None:
        async with lock:
            await cond.wait()

    worker_task = asyncio.create_task(worker())
    await asyncio.sleep(0)

    await lock.acquire()
    assert intercepted_fut is not None
    intercepted_fut.set_exception(ValueError("original_body_exc"))
    await asyncio.sleep(0)

    worker_task.cancel()
    await asyncio.sleep(0)
    lock.release()

    with pytest.raises(ValueError, match="original_body_exc"):
        await worker_task


@pytest.mark.asyncio
async def test_repro_condition_wait_cancel_does_not_rearm_spurious_cancel() -> None:
    """When a task waiting in cond.wait() is cancelled once, re-acquiring the lock
    under CancelScope(shield=True) inside except BaseException must NOT re-arm a
    second spurious CancelledError after the first CancelledError is caught.
    """
    cond = Condition()
    cleanup_completed = False

    async def waiter() -> None:
        nonlocal cleanup_completed
        async with cond:
            try:
                await cond.wait()
            except asyncio.CancelledError:
                # Perform async cleanup while still holding cond's re-acquired lock
                await asyncio.sleep(0.01)
                cleanup_completed = True
                raise

    t = asyncio.create_task(waiter())
    await asyncio.sleep(0.01)
    t.cancel()
    with pytest.raises(asyncio.CancelledError):
        await t
    assert cleanup_completed is True


@pytest.mark.asyncio
async def test_repro_barrier_abort_records_waiter_outcome() -> None:
    """When barrier.abort() is called while a party is waiting, and barrier.reset()
    is called before the party's loop processes the wakeup, the party must still
    raise 'barrier has been aborted' rather than 'barrier was reset while waiting'.
    """
    barrier = Barrier(2)
    errors: list[Exception] = []

    async def party() -> None:
        try:
            await barrier.wait()
        except Exception as e:
            errors.append(e)

    t = asyncio.create_task(party())
    await asyncio.sleep(0.01)
    assert barrier.n_waiting == 1

    # Abort and immediately reset in the same synchronous tick before t runs
    barrier.abort()
    barrier.reset()

    await t
    assert len(errors) == 1
    assert "aborted" in str(errors[0])
