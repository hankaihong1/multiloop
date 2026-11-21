"""Cross-event-loop safe synchronization primitives for multiloop.

Includes Lock, Semaphore, CapacityLimiter, Event, Condition, and Barrier — all engineered
for Python 3.14t multi-core physical parallelism and cross-event-loop thread safety.
"""

from __future__ import annotations

import asyncio
import weakref
from collections.abc import Callable, Iterator
from typing import Any, Self

from multiloop._cancel import (
    CancelScope,
    _await_cancellable,
    _check_not_cancelled,
    _is_current_task_cancelled,
    _task_token,
)
from multiloop._rust import _try_import_rust_class

__all__ = [
    "Barrier",
    "BarrierWaitResult",
    "CapacityLimiter",
    "Condition",
    "Event",
    "Lock",
    "Semaphore",
]

_RawAsyncLock = _try_import_rust_class("multiloop._multiloop_core", "RawAsyncLock")
_RawAsyncSemaphore = _try_import_rust_class("multiloop._multiloop_core", "RawAsyncSemaphore")
_RawAsyncCapacityLimiter = _try_import_rust_class(
    "multiloop._multiloop_core", "RawAsyncCapacityLimiter"
)
_RawAsyncEvent = _try_import_rust_class("multiloop._multiloop_core", "RawAsyncEvent")
_RawAsyncCondition = _try_import_rust_class("multiloop._multiloop_core", "RawAsyncCondition")
_RawAsyncBarrier = _try_import_rust_class("multiloop._multiloop_core", "RawAsyncBarrier")


class _WaiterView:
    """Read-only view over a Rust waiter queue supporting ``len()`` and ``iter()``."""

    __slots__ = ("_raw",)

    def __init__(self, raw: Any) -> None:
        self._raw = raw

    def __len__(self) -> int:
        return int(self._raw.waiter_count())

    def __iter__(self) -> Iterator[int]:
        if hasattr(self._raw, "waiter_tickets"):
            return iter(self._raw.waiter_tickets())
        return iter(range(int(self._raw.waiter_count())))

    def __bool__(self) -> bool:
        return int(self._raw.waiter_count()) > 0


# ============================================================================
# Lock — fair FIFO mutex
# ============================================================================


class Lock:
    """A fair FIFO mutex that is safe to use across independent event loops and OS threads.

    Acquisition and release are bound to the *owning task*: only the task that acquired
    the lock may release it. Calling :meth:`release` from any other task or non-async context
    raises :class:`RuntimeError`.

    Unlike standard :class:`asyncio.Lock`, `multiloop.Lock` can be acquired by tasks running
    on physically different OS threads and distinct event loops.
    """

    __slots__ = ("__weakref__", "_owner_done_cb", "_raw")

    def __init__(self) -> None:
        if _RawAsyncLock is None:
            raise RuntimeError("_multiloop_core Rust extension is not compiled.")
        raw = _RawAsyncLock()
        self._raw = raw

        def _on_owner_done(task: asyncio.Task[Any]) -> None:
            raw.on_owner_task_done(_task_token(task))

        self._owner_done_cb: Callable[[asyncio.Task[Any]], None] = _on_owner_done

    @property
    def _waiters(self) -> _WaiterView:
        return _WaiterView(self._raw)

    def _reap_same_loop_dead_owner(self, loop_id: int) -> None:
        owner = self._raw.check_owner_same_loop(loop_id)
        if owner is not None and owner.done():
            self._raw.on_owner_task_done(_task_token(owner))

    @property
    def locked(self) -> bool:
        """Return True when the lock is currently held."""
        return bool(self._raw.locked())

    @property
    def owner(self) -> asyncio.Task[Any] | None:
        """Return the owning asyncio task, or None if free."""
        return self._raw.owner()  # type: ignore[no-any-return]

    async def acquire(self) -> None:
        """Acquire the lock, blocking in strict FIFO order until it becomes available.

        If the waiting task is cancelled while blocked, its waiter registration is safely
        removed and ownership token forwarding is executed if ownership was already transferred.
        """
        try:
            task = asyncio.current_task()
        except RuntimeError:
            raise RuntimeError("acquire() must be called from an active asyncio task") from None
        if task is None:
            raise RuntimeError("acquire() must be called from an active asyncio task")

        _check_not_cancelled()
        loop = asyncio.get_running_loop()
        loop_id = id(loop)
        self._reap_same_loop_dead_owner(loop_id)
        task_tok = _task_token(task)
        task_weak = weakref.ref(task)

        if self._raw.try_acquire(task_tok, loop_id, task_weak):
            task.add_done_callback(self._owner_done_cb)
            return

        fut = loop.create_future()
        ticket_id = self._raw.acquire_or_register(task_tok, loop_id, task_weak, loop, fut)
        if ticket_id is None:
            task.add_done_callback(self._owner_done_cb)
            return

        try:
            await _await_cancellable(loop, fut)
        except BaseException:
            self._raw.cancel_waiter(task_tok, ticket_id)
            raise
        task.add_done_callback(self._owner_done_cb)

    def try_acquire(self) -> bool:
        """Attempt to acquire the lock immediately without blocking.

        :returns: True if the lock was successfully acquired, False otherwise.
        :raises RuntimeError: If called by an active task that already owns the lock.
        """
        try:
            task = asyncio.current_task()
        except RuntimeError:
            raise RuntimeError("try_acquire() must be called from an active asyncio task") from None
        if task is None:
            raise RuntimeError("try_acquire() must be called from an active asyncio task")

        loop = asyncio.get_running_loop()
        loop_id = id(loop)
        self._reap_same_loop_dead_owner(loop_id)
        acquired = bool(self._raw.try_acquire(_task_token(task), loop_id, weakref.ref(task)))
        if acquired:
            task.add_done_callback(self._owner_done_cb)
        return acquired

    def release(self) -> None:
        """Release the lock, passing ownership to the next queued FIFO waiter.

        :raises RuntimeError: If called by a task that does not own the lock.
        """
        try:
            task = asyncio.current_task()
        except RuntimeError:
            raise RuntimeError("release() must be called from an active asyncio task") from None
        if task is None:
            raise RuntimeError("release() must be called from an active asyncio task")

        self._raw.release(_task_token(task))
        task.remove_done_callback(self._owner_done_cb)

    async def __aenter__(self) -> Self:
        await self.acquire()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> bool | None:
        self.release()
        return None


# ============================================================================
# Semaphore — cross-thread-safe async semaphore
# ============================================================================


class Semaphore:
    """A cross-thread-safe async semaphore with fair FIFO waiter queuing.

    Provides fair FIFO scheduling across multiple OS threads and event loops.
    Cancellation is token-conservative: if a waiter is cancelled after a permit has been
    transferred to it by a concurrent `release()`, that permit is automatically forwarded
    to the next waiting task or returned to the value pool.

    :param max_value: Maximum permit capacity. Must be >= 0.
    """

    __slots__ = ("_raw",)

    def __init__(self, max_value: int) -> None:
        if _RawAsyncSemaphore is None:
            raise RuntimeError("_multiloop_core Rust extension is not compiled.")
        self._raw = _RawAsyncSemaphore(max_value)

    @property
    def _waiters(self) -> _WaiterView:
        return _WaiterView(self._raw)

    def __repr__(self) -> str:
        return (
            f"<Semaphore value={self.value}, "
            f"max_value={self.max_value}, "
            f"waiters={len(self._waiters)}>"
        )

    @property
    def value(self) -> int:
        """Number of currently available permits."""
        return int(self._raw.value())

    @property
    def max_value(self) -> int:
        """Maximum permit capacity."""
        return int(self._raw.max_value())

    async def acquire(self) -> None:
        """Acquire a permit, blocking in FIFO order if none are currently available."""
        _check_not_cancelled()
        if self._raw.try_acquire():
            return

        loop = asyncio.get_running_loop()
        fut = loop.create_future()

        ticket_id = self._raw.acquire_or_register(loop, fut)
        if ticket_id is None:
            return

        try:
            await _await_cancellable(loop, fut)
        except BaseException:
            self._raw.cancel_waiter(ticket_id)
            raise
        self._raw.claim_acquire(ticket_id)

    def _cancel_waiter(self, ticket_id: int | Any) -> None:
        """Handle cancellation of a waiting task, preserving permit conservation."""
        if isinstance(ticket_id, int):
            self._raw.cancel_waiter(ticket_id)

    def try_acquire(self) -> bool:
        """Attempt to acquire a permit immediately without blocking.

        :returns: True if a permit was successfully acquired, False otherwise.
        """
        return bool(self._raw.try_acquire())

    def release(self) -> None:
        """Release a permit, waking the first FIFO waiter or restoring available count.

        :raises ValueError: If released more times than the initialized max_value without pending waiters.
        """
        self._raw.release()

    async def __aenter__(self) -> Self:
        await self.acquire()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object | None,
    ) -> bool | None:
        self.release()
        return None


# ============================================================================
# CapacityLimiter — dynamic token limiter with single-lock atomic state
# ============================================================================


class CapacityLimiter:
    """An async capacity limiter safe across event loops and OS threads under free-threading.

    Maintains a dynamically resizable token budget (`total_tokens`). All token allocations,
    borrowing counts, and waiter queues are protected by a single Rust `parking_lot::Mutex`,
    guaranteeing atomic updates under Python 3.14t multi-core parallelism.

    :param total_tokens: Initial total capacity token budget. Must be > 0.
    """

    __slots__ = ("_raw",)

    def __init__(self, total_tokens: float) -> None:
        if _RawAsyncCapacityLimiter is None:
            raise RuntimeError("_multiloop_core Rust extension is not compiled.")
        self._raw = _RawAsyncCapacityLimiter(float(total_tokens))

    @property
    def _waiters(self) -> _WaiterView:
        return _WaiterView(self._raw)

    def __repr__(self) -> str:
        total, avail, borrowed = self.snapshot()
        return f"<CapacityLimiter total_tokens={total}, available={avail}, borrowed={borrowed}>"

    @property
    def total_capacity(self) -> int:
        """Total integer token capacity."""
        return int(self._raw.total_tokens())

    @property
    def available_capacity(self) -> int:
        """Currently available discrete integer token capacity."""
        total, _avail, borrowed = self._raw.snapshot()
        return max(0, int(total) - int(borrowed))

    @property
    def total_tokens(self) -> float:
        """Total token capacity budget."""
        return float(self._raw.total_tokens())

    @total_tokens.setter
    def total_tokens(self, value: float) -> None:
        self._raw.set_total_tokens(float(value))

    @property
    def available_tokens(self) -> float:
        """Currently available tokens."""
        return float(self._raw.snapshot()[1])

    @property
    def borrowed_tokens(self) -> float:
        """Currently borrowed tokens."""
        return float(self._raw.snapshot()[2])

    def snapshot(self) -> tuple[float, float, float]:
        """Return ``(total_tokens, available_tokens, borrowed_tokens)`` atomically."""
        t, a, b = self._raw.snapshot()
        return (float(t), float(a), float(b))

    async def acquire(self) -> None:
        """Acquire one token, blocking if current borrowed count has reached capacity."""
        _check_not_cancelled()
        if self._raw.try_acquire():
            return

        loop = asyncio.get_running_loop()
        fut = loop.create_future()

        ticket_id = self._raw.acquire_or_register(loop, fut)
        if ticket_id is None:
            return

        try:
            await _await_cancellable(loop, fut)
        except BaseException:
            self._raw.cancel_waiter(ticket_id)
            raise
        self._raw.claim_acquire(ticket_id)

    def _cancel_waiter(self, ticket_id: int | Any) -> None:
        """Handle cancellation while waiting for a token."""
        if isinstance(ticket_id, int):
            self._raw.cancel_waiter(ticket_id)

    def try_acquire(self) -> bool:
        """Attempt to borrow one token immediately without blocking.

        :returns: True if a token was acquired, False if capacity has been reached.
        """
        return bool(self._raw.try_acquire())

    def release(self) -> None:
        """Release one borrowed token.

        :raises ValueError: If released more times than acquired.
        """
        self._raw.release()

    async def __aenter__(self) -> Self:
        await self.acquire()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object | None,
    ) -> bool | None:
        self.release()
        return None


# ============================================================================
# Event — cross-thread-safe event (sticky, no clear)
# ============================================================================


class Event:
    """A cross-event-loop safe one-shot event with sticky semantics (no clear).

    Once set via :meth:`set`, the event remains set permanently. Subsequent calls to
    :meth:`wait` return immediately.
    """

    __slots__ = ("_raw",)

    def __init__(self) -> None:
        if _RawAsyncEvent is None:
            raise RuntimeError("_multiloop_core Rust extension is not compiled.")
        self._raw = _RawAsyncEvent()

    @property
    def _waiters(self) -> _WaiterView:
        return _WaiterView(self._raw)

    def is_set(self) -> bool:
        """Return True if the event has been set."""
        return bool(self._raw.is_set())

    def set(self) -> None:
        """Set the event and wake all pending waiters across any event loops."""
        self._raw.set()

    async def wait(self) -> None:
        """Wait until the event is set. Returns immediately if already set."""
        _check_not_cancelled()
        if self._raw.is_set():
            return
        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        ticket_id = self._raw.wait_or_register(loop, fut)
        if ticket_id is None:
            return
        try:
            await _await_cancellable(loop, fut)
        except BaseException:
            self._raw.cancel_waiter(ticket_id)
            raise


# ============================================================================
# Condition — async condition variable atop multiloop.Lock
# ============================================================================


class Condition:
    """An async condition variable backed by a cross-thread-safe :class:`Lock`.

    Waiters release the underlying lock while suspended and re-acquire it before :meth:`wait`
    returns. The notification protocol does NOT require holding the lock, preventing notifier-sleeper
    deadlocks and thundering herd contention.

    :param lock: Optional underlying :class:`Lock` instance. If omitted, creates a new Lock.
    """

    __slots__ = ("_lock", "_raw")

    def __init__(self, lock: Lock | None = None) -> None:
        if _RawAsyncCondition is None:
            raise RuntimeError("_multiloop_core Rust extension is not compiled.")
        self._lock = lock if lock is not None else Lock()
        self._raw = _RawAsyncCondition()

    @property
    def _waiters(self) -> _WaiterView:
        return _WaiterView(self._raw)

    # -- context-manager support (delegates to the underlying Lock) ---------

    async def acquire(self) -> None:
        """Acquire the underlying lock."""
        await self._lock.acquire()

    def release(self) -> None:
        """Release the underlying lock."""
        self._lock.release()

    async def __aenter__(self) -> Self:
        await self._lock.acquire()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> bool | None:
        self._lock.release()
        return None

    # -- core protocol ------------------------------------------------------

    async def wait(self) -> None:
        """Wait until notified.

        The **underlying lock must be held** when calling this method. It is released
        during wait and re-acquired before returning. If the task is cancelled, the lock
        is re-acquired under a cancellation shield before propagating the cancellation.
        """
        try:
            task = asyncio.current_task()
        except RuntimeError:
            task = None
        if task is None or not self._lock.locked or self._lock.owner is not task:
            raise RuntimeError("cannot wait on un-acquired lock")

        _check_not_cancelled()
        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        ticket_id = self._raw.register_waiter(loop, fut)
        try:
            self._lock.release()
        except BaseException:
            self._raw.cancel_or_forward(ticket_id)
            raise

        try:
            await _await_cancellable(loop, fut)
        except BaseException as exc:
            self._raw.cancel_or_forward(ticket_id)
            try:
                async with CancelScope(shield=True):
                    await self._reacquire_lock()
            except asyncio.CancelledError:
                if not isinstance(exc, asyncio.CancelledError):
                    raise exc from None
                raise
            raise
        else:
            self._raw.claim_waiter(ticket_id)
            try:
                async with CancelScope(shield=True):
                    await self._reacquire_lock()
            except BaseException:
                self._raw.forward_notify()
                raise
            if _is_current_task_cancelled():
                self._raw.forward_notify()
                raise asyncio.CancelledError()

    async def wait_for(self, predicate: Callable[[], Any]) -> Any:
        """Wait until a predicate returns a truthy value.

        The **underlying lock must be held** when calling this method.

        :param predicate: Synchronous or asynchronous callable returning a boolean or truthy value.
        :returns: The truthy value returned by ``predicate``.
        """
        while True:
            result = predicate()
            if asyncio.iscoroutine(result) or asyncio.isfuture(result):
                result = await result
            if result:
                return result
            await self.wait()

    def notify(self, n: int = 1) -> None:
        """Wake up to `n` waiters (default: 1). Safe to call without holding the lock.

        :param n: Maximum number of waiting tasks to wake.
        """
        if n <= 0:
            return
        self._raw.notify(n)

    def notify_all(self) -> None:
        """Wake up all currently waiting tasks."""
        self._raw.notify_all()

    async def _reacquire_lock(self) -> None:
        cancelled = False
        while True:
            try:
                await self._lock.acquire()
                break
            except asyncio.CancelledError:
                cancelled = True
        if cancelled:
            raise asyncio.CancelledError()


# ============================================================================
# Barrier — synchronize N parties
# ============================================================================


class BarrierWaitResult:
    """Result object returned by :meth:`Barrier.wait`.

    :param parties: Total number of parties in this barrier round.
    :param index: Zero-based arrival index (0 to parties - 1) of the calling task.
    """

    __slots__ = ("index", "parties")

    def __init__(self, parties: int, index: int = 0) -> None:
        self.parties = parties
        self.index = index

    @property
    def is_leader(self) -> bool:
        """Return True if this task was selected as leader (arrival index 0)."""
        return self.index == 0

    def __repr__(self) -> str:
        return f"<BarrierWaitResult parties={self.parties} index={self.index}>"


class Barrier:
    """An async barrier that synchronizes *N* parties across event loops.

    Each :meth:`wait` call blocks until *parties* tasks have arrived, at which point all
    are released simultaneously and receive a :class:`BarrierWaitResult`. Automatically resets
    for the next generation upon round completion.

    :param parties: Number of parties required to pass the barrier. Must be >= 1.
    """

    __slots__ = ("_raw",)

    def __init__(self, parties: int) -> None:
        if _RawAsyncBarrier is None:
            raise RuntimeError("_multiloop_core Rust extension is not compiled.")
        self._raw = _RawAsyncBarrier(parties)

    @property
    def parties(self) -> int:
        """Total number of parties required to pass the barrier."""
        return int(self._raw.parties())

    @property
    def broken(self) -> bool:
        """Return True if the barrier is in a broken or aborted state."""
        return bool(self._raw.broken())

    @property
    def n_waiting(self) -> int:
        """Number of tasks currently waiting at the barrier."""
        return int(self._raw.n_waiting())

    async def wait(self) -> BarrierWaitResult:
        """Wait at the barrier until *parties* tasks have arrived.

        :raises RuntimeError: If the barrier is broken, aborted, or reset.
        """
        _check_not_cancelled()
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[int] = loop.create_future()

        generation, ticket_id = self._raw.arrive_or_register(loop, fut)
        if ticket_id is None:
            return BarrierWaitResult(parties=self.parties, index=0)

        try:
            outcome = int(await _await_cancellable(loop, fut))
        except BaseException:
            self._raw.cancel_waiter(generation, ticket_id)
            raise

        if outcome == -2:
            raise RuntimeError("barrier has been aborted")
        if outcome == -1:
            raise RuntimeError("barrier broken by concurrent cancellation")
        if outcome == -3:
            raise RuntimeError("barrier was reset while waiting")
        return BarrierWaitResult(parties=self.parties, index=outcome)

    def abort(self) -> None:
        """Abort the barrier, waking all waiting tasks with a :exc:`RuntimeError`."""
        self._raw.abort()

    def reset(self) -> None:
        """Reset the barrier to its initial un-broken state, waking existing waiters with an error."""
        self._raw.reset()
