import asyncio
import threading
import time

import pytest

from multiloop import (
    AsyncContext,
    CancelScope,
    EventLoopThreadPool,
    TaskGroup,
    TimeoutError,
    checkpoint,
    current_effective_deadline,
    fail_after,
    fail_at,
    move_on_after,
    move_on_at,
    run_in_pool,
    sleep,
    wait_cancellable,
)
from multiloop.testing import wait_all_tasks_blocked


@pytest.mark.asyncio
async def test_deadline_expired() -> None:
    """CancelScope(deadline=0) enters, any await inside raises CancelledError."""
    loop = asyncio.get_running_loop()
    with pytest.raises(asyncio.CancelledError):
        async with CancelScope(deadline=loop.time() + 0.001) as scope:
            await sleep(0.1)
    assert scope.cancel_called


@pytest.mark.asyncio
async def test_shield_blocks_parent() -> None:
    """Outer scope cancelled (before or during shield) does not interrupt inner scope(shield=True)."""
    shield_before_ran = False
    try:
        async with CancelScope() as outer:
            outer.cancel()
            async with CancelScope(shield=True) as inner:
                await sleep(0)
                shield_before_ran = True
                assert not inner._effectively_cancelled()
        await sleep(0)
    except asyncio.CancelledError:
        pass
    assert shield_before_ran

    shield_during_ran = False
    with pytest.raises(asyncio.CancelledError):
        async with CancelScope() as outer:
            async with CancelScope(shield=True):
                outer.cancel()
                await sleep(0.01)
                shield_during_ran = True
            await sleep(0.01)
    assert shield_during_ran


@pytest.mark.asyncio
async def test_nested_respects_parent() -> None:
    """Parent cancel propagates to child (child without shield)."""
    try:
        async with CancelScope() as parent, CancelScope() as child:
            parent.cancel()
            assert child._effectively_cancelled()
        await sleep(0)
    except asyncio.CancelledError:
        pass


@pytest.mark.asyncio
async def test_fail_after_timeout() -> None:
    """fail_after(0.001) raises TimeoutError (multiloop.TimeoutError)."""
    scope = fail_after(0.001)
    with pytest.raises(TimeoutError):
        async with scope:
            await sleep(0.1)
    assert scope.cancel_called


@pytest.mark.asyncio
async def test_move_on_after_silent() -> None:
    """move_on_after(0.001) exits silently, scope.cancelled_caught is True."""
    async with move_on_after(0.001) as scope:
        await sleep(0.1)
    assert scope.cancelled_caught
    assert scope.cancel_called


@pytest.mark.asyncio
async def test_cancel_called_inspectable() -> None:
    """After scope.cancel(), scope.cancel_called is True."""
    scope = CancelScope()
    assert not scope.cancel_called
    scope.cancel()
    assert scope.cancel_called
    scope.cancel()  # idempotent — no-op
    assert scope.cancel_called


@pytest.mark.asyncio
async def test_checkpoint_noop_no_scope() -> None:
    """checkpoint() is a no-op when no CancelScope is active."""
    await checkpoint()


@pytest.mark.asyncio
async def test_checkpoint_raises_when_cancelled() -> None:
    """checkpoint() raises CancelledError under Route 3 without touching task.cancelling()."""
    task = asyncio.current_task()
    assert task is not None
    with pytest.raises(asyncio.CancelledError):
        async with CancelScope() as scope:
            scope.cancel()
            assert task.cancelling() == 0
            with pytest.raises(asyncio.CancelledError):
                await checkpoint()
            assert task.cancelling() == 0
    assert task.cancelling() == 0


@pytest.mark.asyncio
async def test_current_effective_deadline_no_scope() -> None:
    """current_effective_deadline() returns inf when no scope is active."""
    assert current_effective_deadline() == float("inf")


@pytest.mark.asyncio
async def test_current_effective_deadline_tightest() -> None:
    """current_effective_deadline() returns the tightest deadline from nested scopes."""
    loop = asyncio.get_running_loop()
    t = loop.time()

    async with CancelScope(deadline=t + 10.0):
        assert current_effective_deadline() == pytest.approx(t + 10.0)

        async with CancelScope(deadline=t + 5.0):
            assert current_effective_deadline() == pytest.approx(t + 5.0)

        # Outer scope deadline again
        assert current_effective_deadline() == pytest.approx(t + 10.0)


@pytest.mark.asyncio
async def test_fail_at_expired_deadline() -> None:
    """fail_at raises TimeoutError when the absolute deadline expires."""
    loop = asyncio.get_running_loop()
    with pytest.raises(TimeoutError):
        async with fail_at(loop.time() + 0.001) as scope:
            await sleep(0.1)
    assert scope.cancel_called


@pytest.mark.asyncio
async def test_move_on_at_silent_exit() -> None:
    """move_on_at exits silently when the absolute deadline expires."""
    loop = asyncio.get_running_loop()
    async with move_on_at(loop.time() + 0.001) as scope:
        await sleep(0.1)
    assert scope.cancel_called
    assert scope.cancelled_caught


@pytest.mark.asyncio
async def test_cancel_scope_cross_thread_cancel():
    """CancelScope.cancel() from a non-asyncio thread wakes the hosting task.

    Post-fix: cancel() signals the Rust RawTaskCancelState and wakes parked
    waiters via call_soon_threadsafe.
    """
    inner_cancelled = False
    scope = CancelScope()

    async def cancellable_work():
        nonlocal inner_cancelled
        try:
            async with scope:
                await sleep(5)
        except asyncio.CancelledError:
            inner_cancelled = True
            raise

    task = asyncio.create_task(cancellable_work())
    await asyncio.sleep(0)  # let the scope enter

    def cancel_from_thread():
        scope.cancel()

    thread = threading.Thread(target=cancel_from_thread)
    thread.start()
    thread.join()

    await asyncio.sleep(0.05)
    assert inner_cancelled, "CancelScope.cancel() from thread did not fire"

    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_already_cancelled_skips_submit():
    """Cancel before submit → future is pre-cancelled, task never reaches pool."""
    async with EventLoopThreadPool(num_threads=2) as pool:
        ctx = AsyncContext()
        ctx.cancel()

        metrics_before = pool.get_metrics()
        fut = ctx.submit(pool, sleep, 0.1)

        # Future must be cancelled immediately — no task was submitted.
        assert fut.cancelled()
        with pytest.raises(asyncio.CancelledError):
            await fut

        # Pool completed-tasks count must NOT have changed.
        metrics_after = pool.get_metrics()
        assert metrics_after["completed_tasks"] == metrics_before["completed_tasks"]


@pytest.mark.asyncio
async def test_cancel_during_execution():
    """Submit first, then cancel → task runs but result is never delivered."""
    async with EventLoopThreadPool(num_threads=1) as pool:
        ctx = AsyncContext()
        delivered_count = 0

        async def long_task() -> str:
            await sleep(0.15)
            return "done"

        fut = ctx.submit(pool, long_task)
        # Cancel immediately after submission, before the task finishes.
        ctx.cancel()

        # Awaiting the future must raise CancelledError.
        with pytest.raises(asyncio.CancelledError):
            await fut

        # Result was never delivered to the caller.
        assert delivered_count == 0

        # Allow worker thread to drain the task (it runs but result is dropped).
        await asyncio.sleep(0.3)


@pytest.mark.asyncio
async def test_async_context_nested_cancellation():
    """Test AsyncContext nested cancellation logic"""
    parent = AsyncContext()
    child = AsyncContext(parent=parent)

    assert not parent.is_cancelled
    assert not child.is_cancelled

    parent.cancel()
    parent.cancel()  # Second cancel is idempotent

    assert parent.is_cancelled
    assert child.is_cancelled

    # child created after cancel is also immediately cancelled
    child2 = AsyncContext(parent=parent)
    assert child2.is_cancelled


@pytest.mark.asyncio
@pytest.mark.repeat(10)
async def test_race_async_context_concurrent_cancel_and_submit():
    """Race 1: Validate that AsyncContext is absolutely safe during high-concurrency interleaving of submit and cancel, with no missed cancellations"""
    async with EventLoopThreadPool(num_threads=4) as pool:
        ctx = AsyncContext()
        go_event = asyncio.Event()

        async def dummy_work():
            await sleep(0.1)
            return "ok"

        # Launch 50 coroutines to concurrently submit tasks — first block all on the go_event barrier
        async def submitter():
            await go_event.wait()
            try:
                fut = ctx.submit(pool, dummy_work)
                return await fut
            except asyncio.CancelledError:
                return "cancelled"

        sub_tasks = [asyncio.create_task(submitter()) for _ in range(50)]

        # Yield to let all submitters reach the Event.wait() barrier
        await wait_all_tasks_blocked()

        # Release all submitters simultaneously and immediately trigger cancel —
        # Maximize the interleaving race between submit and cancel
        go_event.set()
        await wait_all_tasks_blocked()  # Let some submits begin first
        ctx.cancel()

        results = await asyncio.gather(*sub_tasks)
        # All submitted tasks after cancel should either return "cancelled" or complete normally; absolutely no stuck tasks
        assert all(r in ("cancelled", "ok") for r in results)


@pytest.mark.asyncio
async def test_async_context_cross_thread_cancellation():
    """Validate AsyncContext cross-thread cascading cancellation while task is running."""
    async with EventLoopThreadPool(num_threads=2) as pool:
        ctx = AsyncContext()
        started_event = threading.Event()

        async def long_task():
            started_event.set()
            await sleep(5.0)
            return "done"

        fut = ctx.submit(pool, long_task)
        await asyncio.to_thread(started_event.wait, 2.0)
        assert started_event.is_set()

        ctx.cancel()

        with pytest.raises(asyncio.CancelledError):
            await fut


@pytest.mark.asyncio
async def test_async_context_cancel_survives_dead_loop() -> None:
    """A dead loop in the cancel() cascade must be skipped without aborting remaining scopes."""
    from multiloop._cancel import _RawTaskCancelState

    ctx = AsyncContext()
    loop = asyncio.get_running_loop()
    scope1 = CancelScope()
    scope2 = CancelScope()
    key1 = loop.create_future()
    key2 = loop.create_future()

    dead_loop = asyncio.new_event_loop()
    fut_dead = dead_loop.create_future()
    raw_dead = _RawTaskCancelState()
    entry_dead = raw_dead.push_scope(float("inf"), 0, False, False)
    raw_dead.park_waiter(dead_loop, fut_dead)
    scope1._loop = dead_loop
    scope1._state = raw_dead
    scope1._entry_id = entry_dead
    scope1._entered = True
    scope1._deadline_handle = dead_loop.call_later(10.0, lambda: None)
    dead_loop.close()

    live_entered = asyncio.Event()
    live_cancelled = False

    async def live_worker() -> None:
        nonlocal live_cancelled
        try:
            async with scope2:
                live_entered.set()
                await sleep(10.0)
        except asyncio.CancelledError:
            live_cancelled = True
            raise

    live_task = asyncio.create_task(live_worker())
    await live_entered.wait()

    with ctx._lock:
        ctx._scopes[key1] = scope1
        ctx._scopes[key2] = scope2

    ctx.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(live_task, timeout=2.0)
    assert live_cancelled is True
    assert scope1.cancel_called is True
    assert scope2.cancel_called is True


# ---------------------------------------------------------------------------
# Route 3 contracts & regression checks
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_timeout_leaves_cancelling_zero() -> None:
    """fail_after/move_on_after timeouts leave task.cancelling() untouched."""
    task = asyncio.current_task()
    assert task is not None

    try:
        async with fail_after(0.01):
            await sleep(0.1)
    except TimeoutError:
        pass
    assert task.cancelling() == 0

    async with move_on_after(0.01):
        await sleep(0.1)
    assert task.cancelling() == 0

    await sleep(0.01)
    assert task.cancelling() == 0


@pytest.mark.asyncio
async def test_move_on_after_does_not_swallow_external_cancel() -> None:
    """move_on_after must absorb only its own deadline cancel."""

    async def worker() -> None:
        async with move_on_after(5.0):
            await sleep(1.0)

    task = asyncio.create_task(worker())
    await asyncio.sleep(0.05)  # let the worker enter the scope
    task.cancel()
    try:
        await asyncio.wait_for(task, timeout=2.0)
        pytest.fail("external task.cancel() was swallowed by move_on_after")
    except asyncio.CancelledError:
        pass


@pytest.mark.asyncio
async def test_cancel_scope_cannot_share_across_tasks() -> None:
    """TS-2: a CancelScope entered from a second task must raise RuntimeError."""
    scope = CancelScope()
    results: dict[str, str] = {}

    async def enter_a() -> None:
        try:
            async with scope:
                await sleep(0.3)
        except BaseException as e:  # noqa: BLE001
            results["a"] = type(e).__name__

    async def enter_b() -> None:
        await asyncio.sleep(0.05)  # let A enter first
        try:
            async with scope:
                await sleep(0.3)
        except BaseException as e:  # noqa: BLE001
            results["b"] = type(e).__name__

    ta = asyncio.create_task(enter_a())
    tb = asyncio.create_task(enter_b())
    await asyncio.wait_for(asyncio.gather(ta, tb, return_exceptions=True), timeout=3.0)
    assert results.get("b") == "RuntimeError"


# ============================================================================
# Regression tests
# ============================================================================


@pytest.mark.asyncio
async def test_aenter_raise_does_not_leak_stack() -> None:
    """Abnormally exiting an inner deadline scope must pop Rust stack & clear timer."""
    loop = asyncio.get_running_loop()
    outer_deadline = loop.time() + 120
    inner_deadline = loop.time() + 60
    outer = CancelScope(deadline=outer_deadline)
    inner = CancelScope(deadline=inner_deadline)

    with pytest.raises(asyncio.CancelledError):
        async with outer:
            assert current_effective_deadline() == pytest.approx(outer_deadline)
            async with inner:
                assert current_effective_deadline() == pytest.approx(inner_deadline)
                outer.cancel()
                await checkpoint()

    assert inner._state is None  # type: ignore[reportAttributeAccessIssue]
    assert inner._deadline_handle is None  # type: ignore[reportAttributeAccessIssue]
    assert outer._state is None  # type: ignore[reportAttributeAccessIssue]
    assert current_effective_deadline() == float("inf")
    async with CancelScope():
        await sleep(0)


@pytest.mark.asyncio
async def test_fail_after_zero_raises_timeout() -> None:
    """fail_after(0) raises TimeoutError and leaks no cancel count."""
    task = asyncio.current_task()
    assert task is not None
    with pytest.raises(TimeoutError):
        async with fail_after(0):
            await sleep(0.1)
    assert task.cancelling() == 0


@pytest.mark.asyncio
async def test_move_on_zero_silent_with_await() -> None:
    """move_on_after(0) is silent; the body's first await is cancelled cleanly."""
    task = asyncio.current_task()
    assert task is not None
    body_ran = False
    async with move_on_after(0) as scope:
        await sleep(0.1)
        body_ran = True
    assert not body_ran
    assert scope.cancelled_caught
    assert task.cancelling() == 0


@pytest.mark.asyncio
async def test_move_on_zero_silent_no_await() -> None:
    """move_on_after(0) with an await-free body completes cleanly."""
    task = asyncio.current_task()
    assert task is not None
    async with move_on_after(0) as scope:
        pass
    assert scope.cancelled_caught
    assert task.cancelling() == 0


@pytest.mark.asyncio
async def test_deadline_nan_rejected() -> None:
    """NaN deadlines raise ValueError at construction — previously
    call_later(NaN) corrupted the loop's selector timeout and crashed it with
    TypeError.
    """
    with pytest.raises(ValueError):
        CancelScope(deadline=float("nan"))
    with pytest.raises(ValueError):
        fail_after(float("nan"))
    with pytest.raises(ValueError):
        fail_at(float("nan"))
    # -inf is rejected too: express "already expired" with fail_after(0).
    with pytest.raises(ValueError):
        CancelScope(deadline=float("-inf"))


@pytest.mark.asyncio
async def test_no_scope_poisoning_after_expiry() -> None:
    """After an expired-deadline entry or failing TaskGroup, fresh scopes/groups still run cleanly."""

    async def noop() -> None:
        pass

    async def boom() -> None:
        raise ValueError("child")

    with pytest.raises(TimeoutError):
        async with fail_after(0):
            await sleep(0.1)

    with pytest.raises(ValueError):
        async with TaskGroup() as tg:
            tg.start_soon(boom)

    async with CancelScope():
        await sleep(0.001)

    async with TaskGroup() as tg:
        tg.start_soon(noop)

    task = asyncio.current_task()
    assert task is not None
    assert task.cancelling() == 0


@pytest.mark.asyncio
async def test_precancelled_fail_after_raises_timeout() -> None:
    """A pre-cancelled fail_after must convert to TimeoutError (anyio parity)."""
    scope = fail_after(60)
    scope.cancel()
    with pytest.raises(TimeoutError):
        async with scope:
            await sleep(0)


@pytest.mark.asyncio
async def test_precancelled_move_on_swallows() -> None:
    """A pre-cancelled move_on must swallow silently (anyio parity), cancelled_caught=True."""
    scope = move_on_after(60)
    scope.cancel()
    async with scope:
        await sleep(0)
    assert scope.cancelled_caught


@pytest.mark.asyncio
async def test_precancelled_move_on_await_free_body() -> None:
    """Pre-cancelled move_on with an await-free body exits silently with zero count."""
    scope = move_on_after(60)
    scope.cancel()
    async with scope:
        pass
    task = asyncio.current_task()
    assert task is not None
    assert task.cancelling() == 0


@pytest.mark.asyncio
async def test_precancelled_plain_scope_raises_cancelled() -> None:
    """A pre-cancelled plain scope raises CancelledError cleanly."""
    scope = CancelScope()
    scope.cancel()
    with pytest.raises(asyncio.CancelledError):
        async with scope:
            await sleep(0)


@pytest.mark.asyncio
async def test_inherited_cancel_not_converted() -> None:
    """Ancestor cancellation is not converted by fail_after (only its own
    cancel is handled — trio semantics)."""
    outer = CancelScope()
    with pytest.raises(asyncio.CancelledError):
        async with outer:
            outer.cancel()
            scope = fail_after(60)
            with pytest.raises(asyncio.CancelledError):
                async with scope:
                    await sleep(0)


@pytest.mark.asyncio
async def test_precancelled_move_on_body_raises_no_count_leak() -> None:
    """Pre-cancelled move_on + body raising ValueError propagates ValueError with zero count."""
    scope = move_on_after(60)
    scope.cancel()
    try:
        async with scope:
            raise ValueError("boom")  # sync raise, no await
    except ValueError:
        pass
    task = asyncio.current_task()
    assert task is not None
    assert task.cancelling() == 0
    await sleep(0)  # the next await must not raise a spurious CE


@pytest.mark.asyncio
async def test_checkpoint_consumes_pending_cancel() -> None:
    """Route 3 level-triggered checkpoint(): raises CancelledError repeatedly while cancelled without touching task.cancelling()."""
    task = asyncio.current_task()
    assert task is not None
    with pytest.raises(asyncio.CancelledError):
        async with CancelScope() as scope:
            scope.cancel()
            assert task.cancelling() == 0
            try:
                await checkpoint()
                pytest.fail("checkpoint() did not raise CancelledError")
            except asyncio.CancelledError:
                pass
            assert task.cancelling() == 0
            # Level-triggered: still cancelled inside the active scope
            await checkpoint()
    assert task.cancelling() == 0


@pytest.mark.asyncio
async def test_cancel_scope_reentry_raises() -> None:
    """CancelScope is single-use: re-entering an already-entered scope raises RuntimeError."""
    scope = CancelScope()
    async with scope:
        await sleep(0)
    with pytest.raises(RuntimeError):
        async with scope:
            pass


@pytest.mark.asyncio
async def test_cancel_scope_dynamic_deadline_extension() -> None:
    """Dynamically updating scope.deadline extends or shortens the timeout."""
    loop = asyncio.get_running_loop()
    task = asyncio.current_task()
    assert task is not None

    async with CancelScope(deadline=loop.time() + 0.05) as scope:
        # Extend deadline before it fires
        scope.deadline = loop.time() + 0.5
        await sleep(0.1)
        assert not scope.cancel_called

    # Shorten deadline to past triggers immediate cancellation
    with pytest.raises(TimeoutError):
        async with fail_after(10.0) as scope:
            scope.deadline = loop.time() - 1.0
            await sleep(0.5)


@pytest.mark.asyncio
async def test_cancel_scope_dynamic_shield_toggle() -> None:
    """Dynamically toggling shield isolates outer CancelScope without touching task.cancelling()."""
    task = asyncio.current_task()
    assert task is not None

    with pytest.raises(asyncio.CancelledError):
        async with CancelScope() as outer:
            async with CancelScope() as inner:
                outer.cancel()
                assert task.cancelling() == 0

                # Turn on shield: blocks outer scope cancellation
                inner.shield = True
                assert not inner._effectively_cancelled()
                await sleep(0.01)  # must not raise CancelledError

                # Turn off shield: exposes outer cancellation
                inner.shield = False
                assert inner._effectively_cancelled()
                assert task.cancelling() == 0

                with pytest.raises(asyncio.CancelledError):
                    await sleep(0.01)
    assert task.cancelling() == 0


# ---------------------------------------------------------------------------
# Concurrency audit & semantic refactoring tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cancel_scope_task_isolation() -> None:
    """CancelScope state is per-task so a child task's cancellation never affects its parent."""
    async with CancelScope() as parent_scope:
        started = asyncio.Event()

        async def worker() -> None:
            async with CancelScope() as child_scope:
                child_scope.cancel()
                started.set()
                try:
                    await sleep(1)
                except asyncio.CancelledError:
                    await sleep(1)

        t = asyncio.create_task(worker())
        await started.wait()

        # Parent task must NOT be cancelled by child scope
        await checkpoint()
        assert not parent_scope.cancel_called

        t.cancel()
        try:
            await t
        except BaseException:
            pass


@pytest.mark.asyncio
async def test_cancel_scope_cross_thread_post_scope_delivery_guard() -> None:
    """Cross-thread cancel delivered after scope exit must not cancel subsequent task work."""
    scope_ref: list[CancelScope] = []
    scope_exited = asyncio.Event()

    async def worker() -> str:
        async with CancelScope() as scope:
            scope_ref.append(scope)
        # Exited scope
        scope_exited.set()
        await asyncio.sleep(0.05)
        return "completed"

    task = asyncio.create_task(worker())
    await scope_exited.wait()

    # Now cancel the scope from another logical thread/loop perspective
    scope_ref[0].cancel()

    # Worker must complete successfully without being cancelled post-scope
    result = await asyncio.wait_for(task, timeout=1.0)
    assert result == "completed"


@pytest.mark.asyncio
async def test_shield_restores_outer_cancellation_on_exit() -> None:
    """Route 3: shield blocks outer CancelScope while leaving task.cancelling() untouched; its own expired deadline still raises."""
    task = asyncio.current_task()
    assert task is not None

    task.cancel()
    assert task.cancelling() == 1, "Pre-condition: task should have 1 pending cancel"
    try:
        shield_body_ran = False
        with pytest.raises(asyncio.CancelledError):
            async with CancelScope() as outer:
                outer.cancel()
                async with CancelScope(shield=True) as inner:
                    assert task.cancelling() == 1
                    assert not inner._effectively_cancelled()
                    shield_body_ran = True
                assert outer._effectively_cancelled()
                assert task.cancelling() == 1
        assert shield_body_ran
        assert task.cancelling() == 1

        # An expired deadline on the shield scope itself still raises CancelledError on exit
        with pytest.raises(asyncio.CancelledError):
            async with CancelScope(deadline=asyncio.get_running_loop().time() - 1, shield=True):
                assert task.cancelling() == 1
        assert task.cancelling() == 1
    finally:
        while task.cancelling() > 0:
            task.uncancel()


# ---------------------------------------------------------------------------
# Route 3: pure Rust stack contract tests (PLAIN / MOVE_ON / FAIL_AFTER)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("trigger_mode", "external_cancels"),
    [
        ("checkpoint_in_move_on", 0),
        ("checkpoint_in_move_on", 2),
        ("nested_aenter_in_move_on", 0),
        ("nested_aenter_in_move_on", 2),
        ("plain_scope_escaped_cancel", 0),
        ("plain_scope_escaped_cancel", 2),
        ("plain_scope_caught_raises_on_exit", 0),
        ("plain_scope_caught_raises_on_exit", 2),
        ("fail_after_escaped_timeout", 0),
        ("fail_after_escaped_timeout", 2),
        ("fail_after_caught_raises_timeout_on_exit", 0),
        ("fail_after_caught_raises_timeout_on_exit", 2),
    ],
)
async def test_cancel_scope_route3_exit_contracts(trigger_mode: str, external_cancels: int) -> None:
    """Verify Route 3 CancelScope/shield/checkpoint never mutate task.cancelling() and follow PLAIN/MOVE_ON/FAIL_AFTER exit rules."""
    task = asyncio.current_task()
    assert task is not None

    for _ in range(external_cancels):
        task.cancel()
    assert task.cancelling() == external_cancels

    try:
        if trigger_mode == "checkpoint_in_move_on":
            async with move_on_after(60) as scope:
                scope.cancel()
                assert task.cancelling() == external_cancels
                await checkpoint()
            assert scope.cancelled_caught is True
            assert task.cancelling() == external_cancels

        elif trigger_mode == "nested_aenter_in_move_on":
            async with move_on_after(60) as scope:
                scope.cancel()
                assert task.cancelling() == external_cancels
                async with CancelScope():
                    pass
            assert scope.cancelled_caught is True
            assert task.cancelling() == external_cancels

        elif trigger_mode == "plain_scope_escaped_cancel":
            with pytest.raises(asyncio.CancelledError):
                async with CancelScope() as scope:
                    scope.cancel()
                    assert task.cancelling() == external_cancels
                    await sleep(10)
            assert task.cancelling() == external_cancels

        elif trigger_mode == "plain_scope_caught_raises_on_exit":
            with pytest.raises(asyncio.CancelledError):
                async with CancelScope() as scope:
                    scope.cancel()
                    try:
                        await checkpoint()
                    except asyncio.CancelledError:
                        pass
                    assert task.cancelling() == external_cancels
            assert task.cancelling() == external_cancels

        elif trigger_mode == "fail_after_escaped_timeout":
            with pytest.raises(TimeoutError):
                async with fail_after(60) as scope:
                    scope.cancel()
                    assert task.cancelling() == external_cancels
                    await checkpoint()
            assert task.cancelling() == external_cancels

        elif trigger_mode == "fail_after_caught_raises_timeout_on_exit":
            with pytest.raises(TimeoutError):
                async with fail_after(60) as scope:
                    scope.cancel()
                    try:
                        await checkpoint()
                    except asyncio.CancelledError:
                        pass
                    assert task.cancelling() == external_cancels
            assert task.cancelling() == external_cancels
    finally:
        while task.cancelling() > 0:
            task.uncancel()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("boundary_mode", "outer_cancels"),
    [
        ("shield_isolates_checkpoint", 0),
        ("shield_isolates_checkpoint", 2),
        ("shield_isolates_nested_aenter", 0),
        ("shield_isolates_nested_aenter", 2),
        ("shield_blocks_outer_plain_leaves_count", 0),
        ("shield_blocks_outer_plain_leaves_count", 2),
    ],
)
async def test_cancel_scope_route3_shield_contracts(boundary_mode: str, outer_cancels: int) -> None:
    """Boundary parameterized tests for shielded scopes under Route 3."""
    task = asyncio.current_task()
    assert task is not None

    for _ in range(outer_cancels):
        task.cancel()
    assert task.cancelling() == outer_cancels

    try:
        if boundary_mode == "shield_isolates_checkpoint":
            async with CancelScope(shield=True):
                assert task.cancelling() == outer_cancels
                async with move_on_after(60) as inner:
                    inner.cancel()
                    await checkpoint()
                assert inner.cancelled_caught is True
                assert task.cancelling() == outer_cancels
            assert task.cancelling() == outer_cancels

        elif boundary_mode == "shield_isolates_nested_aenter":
            async with CancelScope(shield=True):
                assert task.cancelling() == outer_cancels
                async with move_on_after(60) as inner:
                    inner.cancel()
                    async with CancelScope():
                        pass
                assert inner.cancelled_caught is True
                assert task.cancelling() == outer_cancels
            assert task.cancelling() == outer_cancels

        elif boundary_mode == "shield_blocks_outer_plain_leaves_count":
            completed_shield = False
            with pytest.raises(asyncio.CancelledError):
                async with CancelScope() as outer:
                    outer.cancel()
                    async with CancelScope(shield=True) as inner:
                        assert not inner._effectively_cancelled()
                        if outer_cancels == 0:
                            await sleep(0.01)
                        completed_shield = True
                        assert task.cancelling() == outer_cancels
            assert completed_shield is True
            assert task.cancelling() == outer_cancels
    finally:
        while task.cancelling() > 0:
            task.uncancel()


@pytest.mark.asyncio
async def test_shield_inside_except_cancelled_does_not_rearm_second_cancel() -> None:
    """Entering and exiting CancelScope(shield=True) inside an except CancelledError handler must not re-arm a second CancelledError."""
    post_shield_completed = False

    async def victim() -> None:
        nonlocal post_shield_completed
        try:
            await asyncio.sleep(10.0)
        except asyncio.CancelledError:
            async with CancelScope(shield=True):
                await sleep(0.005)
            await sleep(0.005)
            post_shield_completed = True
            raise

    t = asyncio.create_task(victim())
    await asyncio.sleep(0.01)
    t.cancel()
    with pytest.raises(asyncio.CancelledError):
        await t
    assert post_shield_completed is True


@pytest.mark.asyncio
async def test_wait_cancellable_honors_cancel_scope() -> None:
    """wait_cancellable(asyncio.sleep(...)) interrupts foreign coroutines on scope cancel."""
    task = asyncio.current_task()
    assert task is not None
    child_finally_ran = False

    async def foreign_sleep() -> None:
        nonlocal child_finally_ran
        try:
            await asyncio.sleep(10.0)
        finally:
            child_finally_ran = True

    scope = CancelScope()

    async def runner() -> None:
        async with scope:
            await wait_cancellable(foreign_sleep())

    t = asyncio.create_task(runner())
    await asyncio.sleep(0.02)
    scope.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(t, timeout=1.0)
    assert child_finally_ran is True
    assert task.cancelling() == 0


@pytest.mark.asyncio
async def test_wait_cancellable_honors_fail_after_and_move_on_after() -> None:
    """wait_cancellable(asyncio.sleep(...)) respects fail_after and move_on_after deadlines."""
    task = asyncio.current_task()
    assert task is not None

    with pytest.raises(TimeoutError):
        async with fail_after(0.02):
            await wait_cancellable(asyncio.sleep(10.0))
    assert task.cancelling() == 0

    async with move_on_after(0.02) as scope:
        await wait_cancellable(asyncio.sleep(10.0))
    assert scope.cancelled_caught is True
    assert task.cancelling() == 0


@pytest.mark.asyncio
async def test_wait_cancellable_passthrough_value_and_exception() -> None:
    """wait_cancellable forwards normal return values and foreign exceptions."""

    async def returns_value() -> int:
        await asyncio.sleep(0.005)
        return 99

    async def raises_err() -> None:
        await asyncio.sleep(0.005)
        raise ValueError("foreign failure")

    async with CancelScope():
        assert await wait_cancellable(returns_value()) == 99
        with pytest.raises(ValueError, match="foreign failure"):
            await wait_cancellable(raises_err())


@pytest.mark.asyncio
async def test_run_in_pool_honors_route3_cancel_scope_and_fail_after() -> None:
    """run_in_pool must propagate active Route 3 CancelScope/fail_after cancellation and shield cleanup."""
    task = asyncio.current_task()
    assert task is not None
    shielded_cleanup_ran = False

    async def slow_worker() -> None:
        nonlocal shielded_cleanup_ran
        try:
            await sleep(5.0)
        finally:
            async with CancelScope(shield=True):
                await sleep(0.01)
                shielded_cleanup_ran = True

    async def invoke() -> None:
        async with fail_after(0.03):
            await run_in_pool(slow_worker, num_threads=1)

    t0 = time.monotonic()
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(invoke(), timeout=1.0)
    elapsed = time.monotonic() - t0
    assert elapsed < 0.5, f"fail_after took {elapsed:.2f}s (did not interrupt run_in_pool)"
    assert shielded_cleanup_ran is True
    assert task.cancelling() == 0


@pytest.mark.asyncio
async def test_run_in_pool_pre_cancelled_closes_coro_object() -> None:
    """Calling run_in_pool with a coroutine object inside a pre-cancelled scope closes it immediately."""

    async def work() -> int:
        return 1

    coro = work()
    scope = CancelScope()
    with pytest.raises(asyncio.CancelledError):
        async with scope:
            scope.cancel()
            await run_in_pool(coro, num_threads=1)
    assert coro.cr_frame is None


@pytest.mark.asyncio
async def test_task_handle_await_checks_pre_cancelled_scope() -> None:
    """Awaiting an already-done TaskHandle inside a cancelled CancelScope raises CancelledError immediately."""
    async with TaskGroup() as tg:
        handle = tg.start_soon(sleep, 0)
    assert handle._task.done()

    reached_after_await = False
    scope = CancelScope()
    with pytest.raises(asyncio.CancelledError):
        async with scope:
            scope.cancel()
            await handle
            reached_after_await = True
    assert reached_after_await is False


# ---------------------------------------------------------------------------
# Consolidated AsyncContext lifecycle & Route 3 edge-case tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_async_context_manager_and_parent_detachment() -> None:
    """AsyncContext supports async with and auto-detaches from parent upon exit/cancel."""
    root = AsyncContext()
    assert len(root._children) == 0

    async with EventLoopThreadPool(num_threads=2) as pool:
        started_event = threading.Event()

        async def worker() -> str:
            started_event.set()
            await sleep(5.0)
            return "ok"

        async with AsyncContext(parent=root) as child:
            assert len(root._children) == 1
            fut = child.submit(pool, worker)
            await asyncio.to_thread(started_event.wait, 2.0)

        assert child.is_cancelled is True
        assert len(root._children) == 0
        with pytest.raises(asyncio.CancelledError):
            await fut


@pytest.mark.asyncio
async def test_async_context_child_unregistration_no_leak() -> None:
    """AsyncContext must unregister child context from parent on cancel to prevent memory leaks."""
    root_ctx = AsyncContext()
    children = [AsyncContext(parent=root_ctx) for _ in range(50)]
    assert len(root_ctx._children) == 50

    for child in children:
        child.cancel()

    assert len(root_ctx._children) == 0


@pytest.mark.asyncio
async def test_async_context_futures_cleaned() -> None:
    """Completed AsyncContext submissions are removed from _scopes tracking."""
    ctx = AsyncContext()
    async with EventLoopThreadPool(num_threads=2) as pool:

        async def quick() -> int:
            return 7

        for _ in range(200):
            fut = ctx.submit(pool, quick)
            await asyncio.wait_for(fut, timeout=5.0)
    assert len(ctx._scopes) == 0


@pytest.mark.asyncio
async def test_async_context_cancel_waits_for_worker_finally() -> None:
    """AsyncContext.submit with shielded async finally completes cleanup and marks caller future cancelled."""
    async with EventLoopThreadPool(num_threads=2) as pool:
        ctx = AsyncContext()
        finally_done = False
        started = threading.Event()

        async def worker() -> None:
            nonlocal finally_done
            try:
                started.set()
                await sleep(10.0)
            finally:
                async with CancelScope(shield=True):
                    await sleep(0.05)
                finally_done = True

        fut = ctx.submit(pool, worker)
        await asyncio.to_thread(started.wait, 2.0)
        assert started.is_set()

        ctx.cancel()
        with pytest.raises(asyncio.CancelledError):
            await fut

        assert finally_done is True
        assert fut.cancelled() is True


@pytest.mark.asyncio
async def test_fail_after_raises_timeout_across_shield_without_trailing_await() -> None:
    """Expired fail_after converts to TimeoutError on __aexit__ even across shield with no trailing await."""
    with pytest.raises(TimeoutError):
        async with fail_after(0.02):
            async with CancelScope(shield=True):
                await sleep(0.05)


@pytest.mark.asyncio
async def test_plain_cancel_scope_raises_cancelled_on_exit() -> None:
    """PLAIN CancelScope cancelled without subsequent await still raises CancelledError on exit."""
    with pytest.raises(asyncio.CancelledError):
        async with CancelScope() as scope:
            scope.cancel()


@pytest.mark.asyncio
async def test_move_on_after_untriggered_cancelled_caught_false() -> None:
    """Untriggered move_on_after reports cancelled_caught=False."""
    async with move_on_after(10.0) as scope:
        pass
    assert scope.cancelled_caught is False


@pytest.mark.asyncio
async def test_cancel_scope_cross_thread_deadline_update() -> None:
    """Updating scope.deadline from an external thread immediately expires and cancels the active task."""
    scope_ref: CancelScope | None = None
    started = asyncio.Event()

    async def worker() -> None:
        nonlocal scope_ref
        async with fail_after(100.0) as scope:
            scope_ref = scope
            started.set()
            await sleep(5.0)

    t = asyncio.create_task(worker())
    await started.wait()

    def bg_thread() -> None:
        time.sleep(0.02)
        assert scope_ref is not None
        scope_ref.deadline = 0.0

    th = threading.Thread(target=bg_thread, daemon=True)
    th.start()

    with pytest.raises(TimeoutError):
        await t
    th.join(timeout=1.0)


@pytest.mark.asyncio
async def test_cancel_scope_deep_nesting_and_cross_thread_cancel() -> None:
    """10 nested CancelScopes + shield + cross-thread cancel keeps task.cancelling() untouched."""
    task = asyncio.current_task()
    assert task is not None
    assert task.cancelling() == 0

    scopes = [CancelScope() for _ in range(10)]
    entered: list[CancelScope] = []
    try:
        for s in scopes:
            await s.__aenter__()
            entered.append(s)

        async with CancelScope(shield=True):

            def bg_cancel() -> None:
                for s in scopes:
                    s.cancel()

            th = threading.Thread(target=bg_cancel, daemon=True)
            th.start()
            th.join(timeout=1.0)

            assert task.cancelling() == 0
            await sleep(0.01)
            assert task.cancelling() == 0

        assert task.cancelling() == 0
        with pytest.raises(asyncio.CancelledError):
            await sleep(1.0)
        assert task.cancelling() == 0
    finally:
        for s in reversed(entered):
            try:
                await s.__aexit__(None, None, None)
            except asyncio.CancelledError:
                pass
    assert task.cancelling() == 0


@pytest.mark.asyncio
async def test_checkpoint_yields_control() -> None:
    """checkpoint() yields control to the event loop even outside a CancelScope."""
    order: list[str] = []

    async def worker() -> None:
        order.append("worker")

    t = asyncio.create_task(worker())
    order.append("before")
    await checkpoint()
    order.append("after")
    await t
    assert order == ["before", "worker", "after"]
