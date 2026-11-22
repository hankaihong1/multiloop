"""Tests for multiloop.Lock and multiloop.Event cross-event-loop-safe primitives."""

import asyncio
import gc
import threading
import weakref
from typing import Any

import pytest

from multiloop import Event, Lock
from multiloop._cancel import _task_token
from multiloop.testing import wait_all_tasks_blocked

# ---------------------------------------------------------------------------
# Lock tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_lock_acquire_release():
    """Basic acquire/release cycle — lock becomes free after release."""
    lock = Lock()
    assert not lock.locked
    assert lock.owner is None

    await lock.acquire()
    assert lock.locked
    assert lock.owner is asyncio.current_task()

    lock.release()
    assert not lock.locked
    assert lock.owner is None


@pytest.mark.asyncio
async def test_lock_try_acquire():
    """try_acquire() succeeds when unheld and fails when held."""
    lock = Lock()
    assert lock.try_acquire() is True
    assert lock.locked
    assert lock.owner is asyncio.current_task()

    # Reentrant attempt raises RuntimeError
    with pytest.raises(RuntimeError, match="Lock is not reentrant"):
        lock.try_acquire()

    # Another task cannot acquire
    async def other_task():
        return lock.try_acquire()

    assert await asyncio.create_task(other_task()) is False

    lock.release()
    assert not lock.locked
    assert lock.try_acquire() is True
    lock.release()


@pytest.mark.asyncio
async def test_lock_async_context_manager():
    """Lock works as an async context manager."""
    lock = Lock()
    assert not lock.locked

    async with lock:
        assert lock.locked
        assert lock.owner is asyncio.current_task()

    assert not lock.locked


@pytest.mark.asyncio
async def test_lock_release_unowned():
    """Releasing a lock from a non-owning task raises RuntimeError."""
    lock = Lock()

    async def holder():
        async with lock:
            pass

    await holder()

    async def thief():
        with pytest.raises(RuntimeError, match="does not own"):
            lock.release()

    await thief()


@pytest.mark.asyncio
async def test_lock_fair_fifo():
    """Three tasks acquire in FIFO order."""
    lock = Lock()
    order: list[int] = []

    async def worker(n: int) -> None:
        await lock.acquire()
        order.append(n)
        lock.release()

    # Hold the lock so all workers queue up.
    await lock.acquire()
    t1 = asyncio.create_task(worker(1))
    t2 = asyncio.create_task(worker(2))
    t3 = asyncio.create_task(worker(3))

    # Let them all reach the acquire point.
    await wait_all_tasks_blocked()

    lock.release()  # hand off to first waiter

    await asyncio.gather(t1, t2, t3)

    assert order == [1, 2, 3]


@pytest.mark.asyncio
async def test_lock_cancel_while_waiting():
    """Cancelling a waiting task removes it from the waiter queue."""
    lock = Lock()

    await lock.acquire()

    async def waiter():
        await lock.acquire()

    task = asyncio.create_task(waiter())
    await asyncio.sleep(0.02)  # let waiter queue up
    assert lock.locked

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    # Release — should not hand off to the cancelled task.
    lock.release()
    assert not lock.locked


@pytest.mark.asyncio
async def test_lock_owner_death_breaker():
    """If owner task crashes, next acquirer seizes the lock."""
    lock = Lock()

    async def crash_owner():
        await lock.acquire()
        # Simulate a crash: the coroutine raises, task becomes done.
        raise RuntimeError("boom")

    task = asyncio.create_task(crash_owner())
    with pytest.raises(RuntimeError, match="boom"):
        await task

    # The lock's _owner still points to the crashed (done) task.
    # Next acquire should trigger the owner-death breaker.
    await lock.acquire()
    assert lock.locked
    assert lock.owner is asyncio.current_task()
    lock.release()


@pytest.mark.asyncio
async def test_lock_cross_loop():
    """Release from main loop wakes a waiter running in a different event loop."""
    lock = Lock()
    got_lock: list[str] = []

    await lock.acquire()
    assert lock.locked

    def waiter_in_other_loop():
        loop = asyncio.new_event_loop()

        async def waiter():
            await lock.acquire()
            got_lock.append("acquired")
            lock.release()

        loop.run_until_complete(waiter())
        loop.close()

    thread = threading.Thread(target=waiter_in_other_loop)
    thread.start()

    # Let the waiter queue up.
    await asyncio.sleep(0.1)

    # Release from main loop — must wake the waiter in the other thread.
    lock.release()
    thread.join(timeout=5)
    assert not thread.is_alive()
    assert got_lock == ["acquired"]
    assert not lock.locked


# ---------------------------------------------------------------------------
# Event tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_event_set_before_wait():
    """set() before wait() — wait() returns immediately."""
    event = Event()
    assert not event.is_set()

    event.set()
    assert event.is_set()

    # Should return without blocking.
    await event.wait()


@pytest.mark.asyncio
async def test_event_wait_then_set():
    """A waiter wakes up after set() is called."""
    event = Event()

    async def waiter():
        await event.wait()
        return "woken"

    task = asyncio.create_task(waiter())
    await asyncio.sleep(0.02)

    event.set()
    result = await task
    assert result == "woken"
    assert event.is_set()


@pytest.mark.asyncio
async def test_event_multiple_waiters():
    """All waiters wake when set() is called."""
    event = Event()
    results: list[str] = []

    async def waiter(i: int):
        await event.wait()
        results.append(f"w{i}")

    tasks = [asyncio.create_task(waiter(i)) for i in range(5)]
    await asyncio.sleep(0.02)

    event.set()
    await asyncio.gather(*tasks)

    assert len(results) == 5
    assert event.is_set()


@pytest.mark.asyncio
async def test_event_idempotent_set():
    """Calling set() multiple times is harmless."""
    event = Event()
    event.set()
    event.set()
    event.set()
    assert event.is_set()
    await event.wait()


@pytest.mark.asyncio
async def test_event_cross_thread():
    """set() from a non-asyncio thread wakes the waiter."""
    event = Event()

    async def waiter():
        await event.wait()
        return "done"

    task = asyncio.create_task(waiter())
    await asyncio.sleep(0.02)

    def set_from_thread():
        event.set()

    thread = threading.Thread(target=set_from_thread)
    thread.start()
    thread.join()

    result = await asyncio.wait_for(task, timeout=5)
    assert result == "done"


# ---------------------------------------------------------------------------
# Event.wait cancel cleanup (Wave 0 regression test)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_event_wait_cancel_cleanup():
    """Cancelling an Event.wait() caller removes its entry from _waiters.

    Post-fix: the internal _waiters list must be cleaned up so subsequent
    set() does not try to wake a dead waiter loop/event pair.
    """
    event = Event()

    async def waiter():
        await event.wait()

    task = asyncio.create_task(waiter())
    await wait_all_tasks_blocked()
    assert event._raw.waiter_count() == 1

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    # Waiter must be removed after cancellation.
    assert event._raw.waiter_count() == 0

    # Subsequent set() and wait() must still work correctly.
    event.set()
    await event.wait()


# ---------------------------------------------------------------------------
# Combined / stress test
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_lock_event_combined():
    """Use Lock + Event together in a producer/consumer pattern."""
    lock = Lock()
    ready = Event()
    shared: list[int] = []

    async def producer():
        async with lock:
            shared.append(42)
        ready.set()

    async def consumer():
        await ready.wait()
        async with lock:
            val = shared[0]
        return val

    prod = asyncio.create_task(producer())
    cons = asyncio.create_task(consumer())

    results = await asyncio.gather(prod, cons)
    assert results[-1] == 42


# ---------------------------------------------------------------------------
# Regression tests
# forward) — 2026-08-10 audit
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_lock_handoff_to_cancelled_waiter_forwards() -> None:
    """releasing the lock onto a waiter that is then cancelled before
    it wakes must forward the lock to the next FIFO waiter instead of
    stranding ownership on a dead task."""
    lock = Lock()
    go = asyncio.Event()
    w3_got = asyncio.Event()
    results: list[str] = []

    async def w1() -> None:
        async with lock:
            await go.wait()

    async def w2() -> None:
        try:
            async with lock:
                results.append("w2")
        except asyncio.CancelledError:
            pass

    async def w3() -> None:
        async with lock:
            results.append("w3")
            w3_got.set()

    t1 = asyncio.create_task(w1())
    await asyncio.sleep(0.05)  # W1 holds the lock
    t2 = asyncio.create_task(w2())
    await asyncio.sleep(0.05)  # W2 queued
    t3 = asyncio.create_task(w3())
    await asyncio.sleep(0.05)  # W3 queued behind W2

    go.set()  # W1 releases: ownership handed to W2
    t2.cancel()  # ...but W2 is cancelled before its wakeup runs

    # Pre-fix this times out: W3 waits forever on a lock owned by a dead task.
    await asyncio.wait_for(w3_got.wait(), timeout=1.0)
    await asyncio.gather(t1, t2, t3, return_exceptions=True)
    assert results == ["w3"]


@pytest.mark.asyncio
async def test_wake_race_no_callback_noise() -> None:
    """W21: cancelling a waiter after it was popped must not spam the loop
    exception handler with InvalidStateError."""
    loop = asyncio.get_running_loop()
    errors: list[BaseException | None] = []
    old_handler = loop.get_exception_handler()

    def handler(_loop: asyncio.AbstractEventLoop, context: dict[str, Any]) -> None:
        errors.append(context.get("exception"))

    loop.set_exception_handler(handler)
    try:

        async def grab(lock: Lock, ev: asyncio.Event) -> None:
            try:
                async with lock:
                    ev.set()
            except asyncio.CancelledError:
                pass

        for _ in range(200):
            lock = Lock()
            held = asyncio.Event()
            t1 = asyncio.create_task(grab(lock, held))
            await asyncio.wait_for(held.wait(), timeout=1.0)  # t1 holds
            t2 = asyncio.create_task(grab(lock, asyncio.Event()))
            await asyncio.sleep(0)  # t2 queued
            rel = asyncio.create_task(_release_after(lock))
            await asyncio.sleep(0)  # release pops t2 and wakes it
            t2.cancel()  # race: woken entry cancelled mid-delivery
            await asyncio.gather(t1, t2, rel, return_exceptions=True)
    finally:
        loop.set_exception_handler(old_handler)

    assert not errors, f"wake race produced {len(errors)} callback error(s)"


async def _release_after(lock: Lock) -> None:
    await asyncio.sleep(0)
    lock.release()


# ---------------------------------------------------------------------------
# Regression test: wakeup loops must tolerate waiters whose loop was closed
# ---------------------------------------------------------------------------


def _register_on_abandoned_loop(coro_factory: Any) -> None:
    """Register a waiter on a fresh loop, then stop and close that loop
    WITHOUT cancelling the waiter — simulating an abandoned loop whose
    waiter entry is still registered."""
    loop = asyncio.new_event_loop()

    def _run() -> None:
        asyncio.set_event_loop(loop)
        loop.run_forever()

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    asyncio.run_coroutine_threadsafe(coro_factory(), loop)
    # WHY: block until the coroutine's first step ran on loop_a (waiter
    # registered) — a bare call_soon sync point can race the loop's
    # iteration boundary and close the loop before the waiter registers,
    # silently turning the test into a no-op.
    asyncio.run_coroutine_threadsafe(asyncio.sleep(0.1), loop).result()
    loop.call_soon_threadsafe(loop.stop)
    t.join(2.0)
    loop.close()


@pytest.mark.asyncio
async def test_event_set_tolerates_closed_loop_waiter() -> None:
    """``Event.set()`` must wake live-loop waiters even when a stale
    waiter's loop was closed — pre-fix the first ``call_soon_threadsafe``
    raised RuntimeError and aborted the wakeup loop."""
    ev = Event()
    _register_on_abandoned_loop(lambda: ev.wait())

    t2 = asyncio.create_task(ev.wait())
    await asyncio.sleep(0)
    ev.set()  # must not raise
    await asyncio.wait_for(t2, timeout=1.0)


@pytest.mark.asyncio
async def test_lock_release_tolerates_closed_loop_waiter() -> None:
    """releasing a lock whose FIFO head lives on a closed loop must
    hand ownership to the next live waiter — pre-fix ``release()`` raised
    RuntimeError after transferring ownership to the dead task and the lock
    was permanently broken (every later acquire hung)."""
    lock = Lock()
    await lock.acquire()
    _register_on_abandoned_loop(lock.acquire)

    t2 = asyncio.create_task(lock.acquire())
    await asyncio.sleep(0)
    lock.release()  # must not raise; ownership must reach t2
    await asyncio.wait_for(t2, timeout=1.0)


@pytest.mark.asyncio
async def test_lock_dead_owner_fifo_recovery() -> None:
    """Lock dead-owner recovery wakes FIFO waiters in order instead of barging."""
    lock = Lock()
    order: list[str] = []
    acquired = asyncio.Event()
    exit_without_release = asyncio.Event()

    async def doomed_owner() -> None:
        await lock.acquire()
        acquired.set()
        await exit_without_release.wait()

    doomed = asyncio.create_task(doomed_owner())
    await acquired.wait()
    assert lock.locked

    async def waiter_1() -> None:
        await lock.acquire()
        order.append("waiter_1")
        lock.release()

    t_waiter = asyncio.create_task(waiter_1())
    await wait_all_tasks_blocked()

    async def new_caller() -> None:
        await lock.acquire()
        order.append("new_caller")
        lock.release()

    t_caller = asyncio.create_task(new_caller())
    await wait_all_tasks_blocked()

    exit_without_release.set()
    await doomed
    await asyncio.gather(t_waiter, t_caller)

    assert order == ["waiter_1", "new_caller"], f"Expected FIFO order, got {order}"
    assert not lock.locked


@pytest.mark.asyncio
async def test_lock_reentrant_acquire_raises() -> None:
    """Same-task re-acquire raises RuntimeError (asyncio.Lock parity) instead of self-deadlock."""
    lock = Lock()
    async with lock:
        with pytest.raises(RuntimeError):
            async with lock:
                pass


@pytest.mark.asyncio
async def test_lock_reentrant_cancel_no_ownership_theft() -> None:
    """A cancelled re-entrant acquire must NOT hand the lock to a queued waiter while outer holds it."""
    lock = Lock()
    t1_in_cs = {"v": False}

    async def t1() -> None:
        try:
            async with lock:
                t1_in_cs["v"] = True
                try:
                    async with asyncio.timeout(0.2):
                        async with lock:
                            pass
                except (TimeoutError, RuntimeError):
                    pass
                await asyncio.sleep(0.3)
                t1_in_cs["v"] = False
        except RuntimeError:
            pass

    async def t2() -> None:
        async with lock:
            assert t1_in_cs["v"] is False

    t1_task = asyncio.create_task(t1())
    await asyncio.sleep(0.05)
    t2_task = asyncio.create_task(t2())
    results = await asyncio.wait_for(
        asyncio.gather(t1_task, t2_task, return_exceptions=True), timeout=5
    )
    assert not any(isinstance(r, BaseException) for r in results), results


@pytest.mark.asyncio
async def test_lock_abandoned_owner_gc_and_locked_state() -> None:
    """D3a: exiting task holding Lock unlocks when waiters are empty and allows GC/re-acquire."""
    lock = Lock()

    async def abandoned_worker() -> None:
        await lock.acquire()

    task = asyncio.create_task(abandoned_worker())
    task_ref = weakref.ref(task)
    await task
    await asyncio.sleep(0)
    del task
    gc.collect()

    assert lock.locked is False
    assert lock.owner is None
    assert task_ref() is None
    assert lock.try_acquire() is True
    lock.release()
    assert lock.locked is False

    # Also verify dead-weakref reaping via locked() and owner() without calling .done()
    class _WeakOwner:
        __slots__ = ("__weakref__",)

        def done(self) -> bool:
            raise AssertionError("dead-weakref reap must not invoke .done()")

    raw_owner_1: _WeakOwner | None = _WeakOwner()
    assert raw_owner_1 is not None
    raw_ref_1 = weakref.ref(raw_owner_1)
    assert lock._raw.try_acquire(1 << 60, 1, raw_ref_1) is True
    assert lock.locked is True
    del raw_owner_1
    gc.collect()
    assert raw_ref_1() is None
    assert lock.locked is False
    assert lock.owner is None
    assert lock.try_acquire() is True
    lock.release()

    raw_owner_2: _WeakOwner | None = _WeakOwner()
    assert raw_owner_2 is not None
    raw_ref_2 = weakref.ref(raw_owner_2)
    assert lock._raw.try_acquire((1 << 60) + 1, 1, raw_ref_2) is True
    del raw_owner_2
    gc.collect()
    assert raw_ref_2() is None
    assert lock.owner is None
    assert lock.locked is False
    assert lock.try_acquire() is True
    lock.release()


@pytest.mark.asyncio
async def test_lock_stale_ticket_cancel_idempotency() -> None:
    """Stale ticket_id cancellation must not release a Lock held by the same task."""
    task = asyncio.current_task()
    assert task is not None
    task_token = _task_token(task)

    lock = Lock()
    async with lock:
        assert lock.locked
        assert lock.owner is task
        lock._raw.cancel_waiter(task_token, 999_999)
        assert lock.locked
        assert lock.owner is task
    assert not lock.locked
    assert lock.owner is None

    # Also verify cancelling a real previously-issued ticket_id after release/re-acquire.
    held = asyncio.Event()
    release = asyncio.Event()

    async def holder() -> None:
        async with lock:
            held.set()
            await release.wait()

    h_task = asyncio.create_task(holder())
    try:
        await held.wait()
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[None] = loop.create_future()
        stale_ticket = lock._raw.acquire_or_register(
            task_token, id(loop), weakref.ref(task), loop, fut
        )
        assert stale_ticket is not None
        release.set()
        await fut
        assert lock.locked
        assert lock.owner is task
        lock._raw.release(task_token)
        assert not lock.locked

        async with lock:
            assert lock.locked
            assert lock.owner is task
            lock._raw.cancel_waiter(task_token, stale_ticket)
            assert lock.locked
            assert lock.owner is task
        assert not lock.locked
        assert lock.owner is None
    finally:
        release.set()
        if not h_task.done():
            h_task.cancel()
        await asyncio.gather(h_task, return_exceptions=True)


@pytest.mark.asyncio
async def test_cancellation_storm_lock() -> None:
    """Cancellation storm on Lock: 500 queued waiters simultaneously cancelled."""
    lock = Lock()
    await lock.acquire()

    cancelled_count = 0

    async def waiter() -> None:
        nonlocal cancelled_count
        try:
            await lock.acquire()
            lock.release()
        except asyncio.CancelledError:
            cancelled_count += 1
            raise

    tasks = [asyncio.create_task(waiter()) for _ in range(500)]
    await asyncio.sleep(0.01)
    assert lock._raw.waiter_count() == 500

    for t in tasks:
        t.cancel()

    await asyncio.gather(*tasks, return_exceptions=True)
    assert cancelled_count == 500
    assert lock._raw.waiter_count() == 0

    lock.release()
    assert not lock.locked
    await lock.acquire()
    assert lock.locked
    lock.release()


@pytest.mark.asyncio
async def test_lock_owner_cross_thread_never_calls_done() -> None:
    """lock.owner from a foreign loop or non-loop thread must never invoke .done()."""
    lock = Lock()
    main_tid = threading.get_ident()
    acquired_ev = threading.Event()
    holder_task_box: list[Any] = []
    foreign_loop_box: list[asyncio.AbstractEventLoop] = []
    foreign_release_box: list[asyncio.Event] = []
    foreign_errors: list[BaseException] = []
    done_call_threads: list[int] = []

    class _TrackingTask(asyncio.Task[Any]):
        def done(self) -> bool:
            done_call_threads.append(threading.get_ident())
            return super().done()

    def _foreign_runner() -> None:
        foreign_loop = asyncio.new_event_loop()
        foreign_loop.set_task_factory(lambda lp, coro, **kw: _TrackingTask(coro, loop=lp, **kw))
        asyncio.set_event_loop(foreign_loop)

        async def _hold_lock() -> None:
            release_ev = asyncio.Event()
            foreign_loop_box.append(foreign_loop)
            foreign_release_box.append(release_ev)
            holder_task_box.append(asyncio.current_task())
            await lock.acquire()
            acquired_ev.set()
            await release_ev.wait()
            # Same-loop query is permitted to inspect .done()
            assert lock.owner is asyncio.current_task()
            lock.release()

        try:
            foreign_loop.run_until_complete(_hold_lock())
        except BaseException as exc:
            foreign_errors.append(exc)
        finally:
            foreign_loop.close()

    foreign_thread = threading.Thread(target=_foreign_runner)
    foreign_thread.start()
    try:
        assert await asyncio.to_thread(acquired_ev.wait, 5.0)
        holder_task = holder_task_box[0]
        foreign_tid = foreign_thread.ident
        assert foreign_tid is not None

        # Foreign event-loop thread calling lock.owner / lock.locked
        assert lock.owner is holder_task
        assert lock.locked is True
        assert main_tid not in done_call_threads

        # Non-event-loop thread (caller_loop_id is None) calling lock.owner / lock.locked
        sync_results: list[tuple[Any, bool]] = []
        sync_tid_box: list[int] = []

        def _sync_observer() -> None:
            sync_tid_box.append(threading.get_ident())
            sync_results.append((lock.owner, lock.locked))

        sync_thread = threading.Thread(target=_sync_observer)
        sync_thread.start()
        await asyncio.to_thread(sync_thread.join, 5.0)
        assert not sync_thread.is_alive()
        assert sync_results == [(holder_task, True)]
        assert sync_tid_box[0] not in done_call_threads
        assert done_call_threads == []
    finally:
        if foreign_loop_box and foreign_release_box:
            try:
                foreign_loop_box[0].call_soon_threadsafe(foreign_release_box[0].set)
            except RuntimeError:
                pass
        await asyncio.to_thread(foreign_thread.join, 5.0)

    assert not foreign_thread.is_alive()
    assert not foreign_errors
    assert all(tid == foreign_thread.ident for tid in done_call_threads)
    assert not lock.locked
