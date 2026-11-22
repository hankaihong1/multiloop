"""Multi-event-loop thread pool engine with work-stealing shared queue architecture."""

from __future__ import annotations

import asyncio
import contextvars
import enum
import itertools
import math
import os
import threading
import time
import types
import weakref
from collections.abc import Callable, Iterable
from contextlib import suppress
from typing import Any, Protocol, Self

from multiloop._cancel import CancelScope
from multiloop._logging import get_logger
from multiloop._metrics import MetricsCollector
from multiloop._options import PoolOptions
from multiloop._rust import _try_import_rust_class
from multiloop._sync import Event
from multiloop.exceptions import ThreadPoolClosedError

__all__ = [
    "EventLoopThreadPool",
    "create_pool",
]

_logger = get_logger("pool")

_DEFAULT_DRAIN_TIMEOUT = 5.0  # Default maximum drain wait (s) before force-stopping loops
_DEFAULT_JOIN_TIMEOUT = 2.0  # Default shared join budget (s) per shutdown stage


def _safe_complete(
    loop: asyncio.AbstractEventLoop,
    fut: asyncio.Future[Any],
    result: Any = None,
    exc: BaseException | None = None,
    cancelled: bool = False,
) -> None:
    """Complete *fut* on its owning loop safely, tolerating concurrent cancellation or abort."""

    def _do() -> None:
        try:
            if fut.done():
                return
            if cancelled:
                fut.cancel()
            elif exc is not None:
                fut.set_exception(exc)
            else:
                fut.set_result(result)
        except (asyncio.InvalidStateError, RuntimeError):
            pass

    try:
        loop.call_soon_threadsafe(_do)
    except RuntimeError:
        pass


class _WorkerPoolProtocol(Protocol):
    """Protocol for the native Rust NativeWorkerPool class."""

    def __init__(self, num_threads: int) -> None: ...
    def pop_work(self, index: int) -> Any: ...
    def is_closed(self) -> bool: ...
    def is_drained(self) -> bool: ...
    def close(self) -> None: ...
    def push_global(self, task: Any) -> None: ...
    def push_global_batch(self, tasks: list[Any]) -> int: ...
    def push_local(self, index: int, task: Any) -> None: ...
    def set_metrics(self, metrics: Any) -> None: ...
    def mark_worker_idle(self, index: int) -> None: ...
    def clear_worker_idle(self, index: int) -> None: ...
    def claim_one_idle_worker(self) -> int | None: ...
    def complete_task(self) -> None: ...
    def outstanding_count(self) -> int: ...
    def wait_drained(self, timeout_secs: float) -> bool: ...
    def drain_all(self) -> list[Any]: ...


NativeWorkerPool: type[_WorkerPoolProtocol] | None = _try_import_rust_class(
    "multiloop._multiloop_core", "NativeWorkerPool"
)


def _join_threads_sync(threads: list[threading.Thread], timeout: float | None = None) -> None:
    """Join worker threads sequentially against one shared monotonic deadline."""
    budget = _DEFAULT_JOIN_TIMEOUT if timeout is None else max(0.0, timeout)
    deadline = time.monotonic() + budget
    for t in threads:
        remaining = deadline - time.monotonic()
        if remaining <= 0.0:
            break
        try:
            t.join(timeout=remaining)
        except RuntimeError:
            pass


def _cancel_tasks_and_stop_loop(loop: asyncio.AbstractEventLoop) -> None:
    """Cancel all non-completed tasks on *loop* and stop it on the loop's own thread."""
    for task in asyncio.all_tasks(loop):
        if not task.done():
            task.cancel()
    loop.stop()


class _PoolState(enum.IntEnum):
    """Lifecycle states of an :class:`EventLoopThreadPool`."""

    CREATED = 0
    RUNNING = 1
    CLOSING = 2
    CLOSED = 3


class _PoolWorkItem:
    """Slotted work item tying a submitted target, caller future, and optional CancelScope."""

    __slots__ = (
        "_args",
        "_caller_loop",
        "_cancel_scope",
        "_cleaned",
        "_ctx",
        "_fut",
        "_kwargs",
        "_pool_ref",
        "_target",
    )

    def __init__(
        self,
        pool: EventLoopThreadPool,
        target: Any,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        caller_loop: asyncio.AbstractEventLoop,
        fut: asyncio.Future[Any],
        cancel_scope: CancelScope | None,
    ) -> None:
        self._pool_ref = weakref.ref(pool)
        self._target = target
        self._args = args
        self._kwargs = kwargs
        self._caller_loop = caller_loop
        self._fut = fut
        self._cancel_scope = cancel_scope
        self._ctx = contextvars.copy_context()
        self._cleaned = False

    def _cleanup_unexecuted(self, exc: BaseException) -> None:
        """Idempotently close an unexecuted coroutine target and fail the caller future."""
        if self._cleaned:
            return
        self._cleaned = True
        target = self._target
        self._target = None
        if target is not None and asyncio.iscoroutine(target):
            target.close()
        _safe_complete(self._caller_loop, self._fut, exc=exc)
        pool = self._pool_ref()
        if pool is not None and pool._native_pool is not None:
            pool._native_pool.complete_task()

    async def _invoke_target(self) -> Any:
        target = self._target
        if asyncio.iscoroutine(target):
            self._target = None
            return await target
        if callable(target):
            res = target(*self._args, **self._kwargs)
            if asyncio.iscoroutine(res) or asyncio.isfuture(res):
                return await res
            return res
        return target

    async def run(self, worker_id: int, active_items: set[_PoolWorkItem]) -> None:
        """Execute this work item on *worker_id*'s loop and report completion safely."""
        pool = self._pool_ref()
        if pool is None:
            self._cleaned = True
            active_items.discard(self)
            target = self._target
            self._target = None
            if target is not None and asyncio.iscoroutine(target):
                target.close()
            return

        metrics = pool._metrics_collector
        native_pool = pool._native_pool
        metrics.inc_active(worker_id)
        res: Any = None
        try:
            if self._cancel_scope is not None:
                async with self._cancel_scope:
                    res = await self._invoke_target()
            else:
                res = await self._invoke_target()

            _safe_complete(self._caller_loop, self._fut, result=res)
        except BaseException as exc:
            if (
                isinstance(exc, asyncio.CancelledError)
                and self._cancel_scope is not None
                and self._cancel_scope.cancel_called
            ):
                _safe_complete(self._caller_loop, self._fut, cancelled=True)
            else:
                _safe_complete(self._caller_loop, self._fut, exc=exc)
        finally:
            pool = None
            self._cleaned = True
            target = self._target
            self._target = None
            if target is not None and asyncio.iscoroutine(target):
                target.close()
            active_items.discard(self)
            metrics.dec_active(worker_id)
            if native_pool is not None:
                native_pool.complete_task()


class EventLoopThreadPool:
    """Multi-Event-Loop Thread Pool with Work-Stealing Shared Queue Architecture.

    Orchestrates multiple OS worker threads, each executing an isolated :class:`asyncio.AbstractEventLoop`.
    Unpinned tasks are pushed to a global lock-free shared queue where idle worker loops pull and
    execute them dynamically. Tasks requiring event loop affinity (e.g., protocol connections)
    can be pinned to specific target loops.

    :param options: Optional :class:`PoolOptions` providing configuration defaults.
    :param num_threads: Number of worker threads (0 = auto-detect via :func:`os.cpu_count`).
    :param loop_factory: Factory callable returning a new event loop instance.
    """

    __slots__ = (
        "__weakref__",
        "_closed_event",
        "_lifecycle_lock",
        "_loop_factory",
        "_loops",
        "_loops_snapshot",
        "_metrics_collector",
        "_native_pool",
        "_notify_events",
        "_notify_events_snapshot",
        "_ready_count",
        "_ready_event",
        "_rr_counter",
        "_state",
        "_threads",
        "num_threads",
    )

    def __init__(
        self,
        options: PoolOptions | None = None,
        num_threads: int | None = None,
        loop_factory: Callable[[], asyncio.AbstractEventLoop] | None = None,
    ) -> None:
        if options is not None:
            if num_threads is None:
                num_threads = options.num_threads
            if loop_factory is None:
                loop_factory = options.loop_factory

        if num_threads is None:
            num_threads = os.cpu_count() or 4
        if num_threads < 0:
            raise ValueError("num_threads must not be negative")
        if num_threads == 0:
            num_threads = os.cpu_count() or 4

        self.num_threads = num_threads
        self._loop_factory = loop_factory or asyncio.new_event_loop
        self._threads: list[threading.Thread] = []
        self._loops: list[asyncio.AbstractEventLoop] = []
        self._notify_events: list[asyncio.Event | None] = []
        self._loops_snapshot: tuple[asyncio.AbstractEventLoop, ...] = ()
        self._notify_events_snapshot: tuple[asyncio.Event, ...] = ()
        self._lifecycle_lock = threading.Lock()
        self._state = _PoolState.CREATED
        self._rr_counter = itertools.count()
        self._ready_event = Event()
        self._ready_count = 0

        # Native Rust lock-free pool controller
        self._native_pool: _WorkerPoolProtocol | None = None
        if NativeWorkerPool is not None:
            self._native_pool = NativeWorkerPool(self.num_threads)

        self._metrics_collector = MetricsCollector(self.num_threads)
        if self._native_pool is not None and self._metrics_collector._metrics is not None:
            self._native_pool.set_metrics(self._metrics_collector._metrics)
        self._closed_event = Event()

    def __repr__(self) -> str:
        return (
            f"<EventLoopThreadPool running={self.is_running} "
            f"threads={self.num_threads} metrics={self.get_metrics()}>"
        )

    @property
    def _running(self) -> bool:
        return self._state == _PoolState.RUNNING

    @property
    def _started(self) -> bool:
        return self._state != _PoolState.CREATED

    @property
    def is_running(self) -> bool:
        """Return True if the thread pool is currently running and accepting tasks."""
        return self._state == _PoolState.RUNNING

    def _get_loop(self, index: int) -> asyncio.AbstractEventLoop:
        """Return the event loop at the given worker index."""
        loops = self._loops_snapshot
        if self._state != _PoolState.RUNNING or index >= len(loops):
            raise RuntimeError("pool is not running")
        return loops[index]

    def get_metrics(self) -> dict[str, Any]:
        """Return JSON-serializable health & performance metrics of the thread pool."""
        return self._metrics_collector.get_snapshot(self.is_running)

    async def _worker_dispatcher(
        self,
        index: int,
        notify_event: asyncio.Event,
        active_items: set[_PoolWorkItem],
    ) -> None:
        """Worker dispatch loop executing 3-source drain and event-driven task consumption."""
        loop = asyncio.get_running_loop()

        def _process_one(item: _PoolWorkItem) -> None:
            active_items.add(item)
            loop.create_task(item.run(index, active_items), context=item._ctx)

        while True:
            if self._native_pool is None:
                break

            processed_count = 0
            # Phase 1: Drain a batch of pending tasks from the Rust lock-free queues
            while processed_count < 64:
                try:
                    item = self._native_pool.pop_work(index)
                    if item is not None:
                        _process_one(item)
                        processed_count += 1
                    else:
                        break
                except Exception as exc:
                    if isinstance(exc, ThreadPoolClosedError):
                        return
                    if self._native_pool.is_closed():
                        return
                    break

            if processed_count > 0:
                await asyncio.sleep(0)
                continue

            # Phase 2: Idle wait on notify_event (with 3-source drain double-check)
            notify_event.clear()
            self._native_pool.mark_worker_idle(index)
            try:
                # Double-check after clearing event and marking idle to avoid lost wakeups
                try:
                    item = self._native_pool.pop_work(index)
                    if item is not None:
                        self._native_pool.clear_worker_idle(index)
                        _process_one(item)
                        await asyncio.sleep(0)
                        continue
                except Exception as exc:
                    if isinstance(exc, ThreadPoolClosedError) or self._native_pool.is_closed():
                        return

                if self._state != _PoolState.RUNNING or self._native_pool.is_closed():
                    if self._native_pool.is_closed():
                        continue
                    return

                await notify_event.wait()
            except asyncio.CancelledError:
                break
            finally:
                self._native_pool.clear_worker_idle(index)

    def _worker(self, index: int, loop: asyncio.AbstractEventLoop) -> None:
        def _exception_handler(_loop: asyncio.AbstractEventLoop, context: dict[str, Any]) -> None:
            exc = context.get("exception")
            if isinstance(exc, (BrokenPipeError, ConnectionResetError, ConnectionAbortedError)):
                return
            _loop.default_exception_handler(context)

        loop.set_exception_handler(_exception_handler)
        asyncio.set_event_loop(loop)
        notify_event = asyncio.Event()
        active_items: set[_PoolWorkItem] = set()
        with self._lifecycle_lock:
            if index < len(self._notify_events):
                self._notify_events[index] = notify_event
            self._ready_count += 1
            if self._ready_count >= self.num_threads:
                self._loops_snapshot = tuple(self._loops)
                self._notify_events_snapshot = tuple(
                    ev for ev in self._notify_events if ev is not None
                )
                self._ready_event.set()

        dispatcher_task = loop.create_task(
            self._worker_dispatcher(index, notify_event, active_items)
        )
        try:
            loop.run_forever()
        except BaseException:
            pass
        finally:
            try:
                try:
                    if not dispatcher_task.done():
                        dispatcher_task.cancel()
                    pending = asyncio.all_tasks(loop)
                    if pending:
                        loop.run_until_complete(asyncio.sleep(0))
                        pending = asyncio.all_tasks(loop)
                    for task in pending:
                        task.cancel()
                    if pending:
                        loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
                except RuntimeError:
                    with suppress(RuntimeError):
                        loop.run_until_complete(asyncio.sleep(0))
            finally:
                if active_items:
                    stop_exc = ThreadPoolClosedError("Pool closed before task ran")
                    for remaining in list(active_items):
                        remaining._cleanup_unexecuted(stop_exc)
                    active_items.clear()
                with suppress(RuntimeError):
                    loop.close()

    async def start(self) -> None:
        """Start all worker threads and work-stealing queue dispatchers."""
        started_by_self = False
        failed_start_exc: BaseException | None = None
        started_loops: list[asyncio.AbstractEventLoop] = []
        started_threads: list[threading.Thread] = []
        unstarted_loops: list[asyncio.AbstractEventLoop] = []

        with self._lifecycle_lock:
            if self._state == _PoolState.RUNNING:
                ready_event = self._ready_event
            elif self._state in (_PoolState.CLOSING, _PoolState.CLOSED):
                raise ThreadPoolClosedError("pool cannot be restarted after close()")
            else:
                native_pool = self._native_pool
                if native_pool is None:
                    raise RuntimeError(
                        "multiloop Rust extension (_multiloop_core) is not installed. "
                        "Install the package with the compiled extension."
                    )
                if native_pool.is_closed():
                    self._state = _PoolState.CLOSED
                    self._closed_event.set()
                    raise ThreadPoolClosedError("pool cannot be restarted after close()")

                new_loops: list[asyncio.AbstractEventLoop] = []
                new_threads: list[threading.Thread] = []
                try:
                    for i in range(self.num_threads):
                        loop = self._loop_factory()
                        new_loops.append(loop)
                        t = threading.Thread(
                            target=self._worker,
                            args=(i, loop),
                            name=f"EventLoopThread-{i}",
                            daemon=True,
                        )
                        new_threads.append(t)
                except BaseException:
                    for loop in new_loops:
                        try:
                            loop.close()
                        except Exception:
                            pass
                    raise

                self._state = _PoolState.RUNNING
                self._ready_count = 0
                self._notify_events = [None] * self.num_threads
                self._loops = new_loops
                self._threads = new_threads
                ready_event = self._ready_event
                started_by_self = True

                try:
                    for loop, t in zip(new_loops, new_threads, strict=True):
                        t.start()
                        started_loops.append(loop)
                        started_threads.append(t)
                except BaseException as exc:
                    failed_start_exc = exc
                    unstarted_loops = new_loops[len(started_loops) :]
                    native_pool.close()
                    self._state = _PoolState.CLOSING

        if failed_start_exc is not None:
            try:
                self._stop_loops(tuple(started_loops), cancel_tasks=True)
                if started_threads:
                    await asyncio.to_thread(_join_threads_sync, started_threads, 2.0)
                for loop in unstarted_loops:
                    try:
                        loop.close()
                    except Exception:
                        pass
            finally:
                with self._lifecycle_lock:
                    self._loops.clear()
                    self._threads.clear()
                    self._notify_events.clear()
                    self._loops_snapshot = ()
                    self._notify_events_snapshot = ()
                    self._state = _PoolState.CLOSED
                self._ready_event.set()
                self._closed_event.set()
            raise failed_start_exc

        await ready_event.wait()

        if started_by_self:
            _logger.info(
                "EventLoopThreadPool started with %d threads",
                self.num_threads,
                extra={"event": "pool_start", "thread_count": self.num_threads},
            )

    def _notify_worker(self, worker_idx: int) -> None:
        """Trigger instant Pipe/EventFD wakeup on the target worker's event loop."""
        loops = self._loops_snapshot
        events = self._notify_events_snapshot
        if (
            self._state != _PoolState.RUNNING
            or worker_idx >= len(loops)
            or worker_idx >= len(events)
        ):
            return
        try:
            loops[worker_idx].call_soon_threadsafe(events[worker_idx].set)
        except RuntimeError:
            pass

    def _notify_next_worker(self) -> None:
        """Wake up an idle worker or round-robin to the next worker for global work."""
        target_idx = (
            self._native_pool.claim_one_idle_worker() if self._native_pool is not None else None
        )
        if target_idx is None:
            target_idx = next(self._rr_counter) % self.num_threads
        self._notify_worker(target_idx)

    def _notify_workers_bounded(self, count: int) -> None:
        """Wake up at most ``min(count, num_threads)`` workers for a batch submission."""
        if count <= 0:
            return
        loops = self._loops_snapshot
        events = self._notify_events_snapshot
        if self._state != _PoolState.RUNNING or not loops or not events:
            return
        num_loops = len(loops)
        wake_n = min(count, num_loops)
        if wake_n >= num_loops:
            for loop, event in zip(loops, events, strict=False):
                try:
                    loop.call_soon_threadsafe(event.set)
                except RuntimeError:
                    pass
            return

        chosen: list[int] = []
        seen: set[int] = set()
        if self._native_pool is not None:
            while len(chosen) < wake_n:
                idx = self._native_pool.claim_one_idle_worker()
                if idx is None:
                    break
                if idx not in seen:
                    seen.add(idx)
                    chosen.append(idx)
        while len(chosen) < wake_n:
            idx = next(self._rr_counter) % num_loops
            if idx not in seen:
                seen.add(idx)
                chosen.append(idx)
        for idx in chosen:
            if idx < len(events):
                try:
                    loops[idx].call_soon_threadsafe(events[idx].set)
                except RuntimeError:
                    pass

    def _notify_all_workers(self) -> None:
        """Trigger instant Pipe/EventFD wakeup on all worker event loops."""
        loops = self._loops_snapshot or tuple(self._loops)
        events = self._notify_events_snapshot or tuple(
            ev for ev in self._notify_events if ev is not None
        )
        for loop, event in zip(loops, events, strict=False):
            try:
                loop.call_soon_threadsafe(event.set)
            except RuntimeError:
                pass

    @staticmethod
    def _stop_loops(
        loops: tuple[asyncio.AbstractEventLoop, ...],
        *,
        cancel_tasks: bool = False,
    ) -> None:
        """Request every worker event loop to stop, optionally cancelling pending tasks."""
        for loop in loops:
            try:
                if loop.is_closed():
                    continue
                if cancel_tasks:
                    loop.call_soon_threadsafe(_cancel_tasks_and_stop_loop, loop)
                else:
                    loop.call_soon_threadsafe(loop.stop)
            except RuntimeError:
                pass

    async def _shutdown(self, *, drain: bool, timeout: float | None) -> None:
        """Execute coordinated lifecycle teardown for ``close()`` and ``abort()``."""
        with self._lifecycle_lock:
            if self._state == _PoolState.CLOSED:
                self._closed_event.set()
                return
            if self._state == _PoolState.CLOSING:
                closed_event = self._closed_event
                wait_only = True
                loops: tuple[asyncio.AbstractEventLoop, ...] = ()
                threads: list[threading.Thread] = []
            elif self._state == _PoolState.CREATED:
                if self._native_pool is not None:
                    self._native_pool.close()
                self._state = _PoolState.CLOSED
                self._closed_event.set()
                return
            else:
                wait_only = False
                if self._native_pool is not None:
                    self._native_pool.close()
                self._state = _PoolState.CLOSING
                loops = self._loops_snapshot or tuple(self._loops)
                threads = list(self._threads)
                closed_event = self._closed_event

        if wait_only:
            await closed_event.wait()
            return

        effective_timeout = timeout if timeout is not None else _DEFAULT_DRAIN_TIMEOUT
        leftover_exc = ThreadPoolClosedError(
            "Pool closed before task ran" if drain else "Pool aborted"
        )
        try:
            if drain:
                self._notify_all_workers()
                if self._native_pool is not None and not (
                    self._native_pool.outstanding_count() == 0 and self._native_pool.is_drained()
                ):
                    drained = await asyncio.to_thread(
                        self._native_pool.wait_drained, effective_timeout
                    )
                    if not drained:
                        _logger.debug(
                            "EventLoopThreadPool drain timed out before force-stopping loops",
                            extra={
                                "event": "pool_close_drain_timeout",
                                "timeout": effective_timeout,
                            },
                        )
        finally:
            try:
                self._stop_loops(loops, cancel_tasks=False)
                if threads:
                    try:
                        await asyncio.to_thread(_join_threads_sync, threads, _DEFAULT_JOIN_TIMEOUT)
                    except BaseException:
                        self._stop_loops(loops, cancel_tasks=True)
                        raise
                    alive_pairs = [
                        (loop, t) for loop, t in zip(loops, threads, strict=False) if t.is_alive()
                    ]
                    if alive_pairs:
                        _logger.warning(
                            "EventLoopThreadPool worker threads still alive after loop.stop(); "
                            "cancelling remaining tasks",
                            extra={
                                "event": "pool_close_force_cancel",
                                "alive_count": len(alive_pairs),
                            },
                        )
                        self._stop_loops(
                            tuple(loop for loop, _ in alive_pairs),
                            cancel_tasks=True,
                        )
                        await asyncio.to_thread(
                            _join_threads_sync,
                            [t for _, t in alive_pairs],
                            _DEFAULT_JOIN_TIMEOUT,
                        )
                        still_alive = [t for _, t in alive_pairs if t.is_alive()]
                        if still_alive:
                            _logger.error(
                                "EventLoopThreadPool failed to join %d worker thread(s)",
                                len(still_alive),
                                extra={
                                    "event": "pool_close_thread_leak",
                                    "alive_count": len(still_alive),
                                },
                            )
            finally:
                if self._native_pool is not None:
                    for leftover in self._native_pool.drain_all():
                        leftover._cleanup_unexecuted(leftover_exc)

                with self._lifecycle_lock:
                    self._loops.clear()
                    self._threads.clear()
                    self._notify_events.clear()
                    self._loops_snapshot = ()
                    self._notify_events_snapshot = ()
                    self._state = _PoolState.CLOSED

                closed_event.set()
                if drain:
                    _logger.info(
                        "EventLoopThreadPool closed",
                        extra={"event": "pool_close", "thread_count": len(threads)},
                    )
                else:
                    _logger.info(
                        "EventLoopThreadPool aborted",
                        extra={"event": "pool_abort", "thread_count": len(threads)},
                    )

    async def close(self, timeout: float | None = None) -> None:
        """Gracefully shut down the thread pool and release all resources.

        Sets the closed state, wakes all worker dispatchers, and waits on the Rust native
        completion barrier for all active and queued tasks across all worker event loops to
        cleanly drain before stopping worker event loops and joining threads. Safe to call
        multiple times (idempotent).

        :param timeout: Maximum seconds to wait for active and queued tasks to drain
                        (defaults to 5.0 seconds if None).
        :raises ValueError: If *timeout* is NaN or negative infinity.
        """
        if timeout is not None:
            timeout = float(timeout)
            if math.isnan(timeout) or timeout == float("-inf"):
                raise ValueError("timeout must not be NaN or negative infinity")
        await self._shutdown(drain=True, timeout=timeout)

    async def abort(self) -> None:
        """Forcefully stop all event loop threads without waiting for pending tasks to drain.

        Immediately stops every worker loop and completes unexecuted futures with
        :class:`ThreadPoolClosedError`.
        """
        await self._shutdown(drain=False, timeout=None)

    async def wait_closed(self) -> None:
        """Wait until the pool has completely terminated."""
        with self._lifecycle_lock:
            state = self._state
            closed_event = self._closed_event
        if state in (_PoolState.CREATED, _PoolState.CLOSED) or closed_event.is_set():
            return
        await closed_event.wait()

    async def __aenter__(self) -> Self:
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: types.TracebackType | None,
    ) -> None:
        if asyncio.current_task() is not None:
            async with CancelScope(shield=True):
                await self.close()
        else:
            await self.close()

    def _resolve_target_worker(
        self, pin_target: asyncio.AbstractEventLoop | int | None
    ) -> tuple[int, asyncio.AbstractEventLoop] | None:
        loops = self._loops_snapshot
        if self._state != _PoolState.RUNNING or not loops:
            raise ThreadPoolClosedError("ThreadPool is not running")

        if pin_target is None:
            return None

        if isinstance(pin_target, int):
            if 0 <= pin_target < len(loops):
                return pin_target, loops[pin_target]
            raise ValueError(f"Worker index {pin_target} out of range (0-{len(loops) - 1})")

        if isinstance(pin_target, asyncio.AbstractEventLoop):  # pyright: ignore[reportUnnecessaryIsInstance]
            for idx, loop_item in enumerate(loops):
                if loop_item is pin_target:
                    return idx, loop_item
            raise ValueError("Target AbstractEventLoop is not managed by this thread pool")

        raise TypeError("pin_to argument must be an AbstractEventLoop, int index, or None")

    def submit(
        self,
        target: Callable[..., Any] | Any,
        *args: Any,
        pin_to: asyncio.AbstractEventLoop | int | None = None,
        cancel_scope: CancelScope | None = None,
        **kwargs: Any,
    ) -> asyncio.Future[Any]:
        """Submit a task to the thread pool for execution across worker event loops.

        :param target: A coroutine function, coroutine object, or callable task.
        :param args: Positional arguments to pass to `target`.
        :param pin_to: Optional target loop instance or worker index for task affinity pinning.
        :param cancel_scope: Optional :class:`CancelScope` tracking task cancellation.
        :param kwargs: Keyword arguments to pass to `target`.
        :returns: An :class:`asyncio.Future` tracking the task's execution.
        :raises ThreadPoolClosedError: If submitted to an unstarted or closed pool.
        """
        try:
            if asyncio.isfuture(target):
                raise TypeError(
                    "submit() target cannot be an asyncio.Future (futures are thread-bound). "
                    "Pass a coroutine function, callable, or coroutine instead."
                )
            if asyncio.iscoroutine(target) and (args or kwargs):
                raise TypeError(
                    "Cannot pass positional or keyword arguments when target is already a coroutine object"
                )

            pinned_info = self._resolve_target_worker(pin_to)

            try:
                caller_loop: asyncio.AbstractEventLoop | None = asyncio.get_running_loop()
            except RuntimeError:
                caller_loop = None

            if caller_loop is None or not caller_loop.is_running():
                raise RuntimeError(
                    "submit() must be called from a thread with a running asyncio event loop"
                )
        except BaseException:
            if asyncio.iscoroutine(target):
                target.close()
            raise

        fut: asyncio.Future[Any] = caller_loop.create_future()
        work_item = _PoolWorkItem(self, target, args, kwargs, caller_loop, fut, cancel_scope)

        if pinned_info is not None:
            target_idx, _target_loop = pinned_info
            if self._native_pool is not None:
                try:
                    self._native_pool.push_local(target_idx, work_item)
                except Exception as exc:
                    if asyncio.iscoroutine(target):
                        target.close()
                    if isinstance(exc, ThreadPoolClosedError):
                        raise ThreadPoolClosedError("ThreadPool is closed") from exc
                    raise
                self._notify_worker(target_idx)
        else:
            if self._native_pool is not None:
                try:
                    self._native_pool.push_global(work_item)
                except Exception as exc:
                    if asyncio.iscoroutine(target):
                        target.close()
                    if isinstance(exc, ThreadPoolClosedError):
                        raise ThreadPoolClosedError("ThreadPool is closed") from exc
                    raise
            self._notify_next_worker()

        return fut

    def submit_many(
        self,
        targets: Iterable[Callable[..., Any] | Any],
        cancel_scope: CancelScope | None = None,
    ) -> list[asyncio.Future[Any]]:
        """Submit a batch of tasks to the thread pool for distributed execution.

        Enqueues the entire batch under a single Rust queue acquisition and wakes at most
        ``num_threads`` worker event loops.

        :param targets: An iterable of coroutine objects, coroutine functions, or callables.
        :param cancel_scope: Optional :class:`CancelScope` tracking task cancellation (only allowed for single-target batches).
        :returns: A list of :class:`asyncio.Future` instances corresponding to each submitted task.
        :raises ValueError: If more than one target is passed together with ``cancel_scope``.
        :raises ThreadPoolClosedError: If submitted to an unstarted or closed pool.
        """
        target_list = list(targets)
        if not target_list:
            return []

        if len(target_list) > 1 and cancel_scope is not None:
            for t in target_list:
                if asyncio.iscoroutine(t):
                    t.close()
            raise ValueError(
                "Cannot pass a single CancelScope to submit_many() with multiple targets"
            )

        try:
            for target in target_list:
                if asyncio.isfuture(target):
                    raise TypeError(
                        "submit() target cannot be an asyncio.Future (futures are thread-bound). "
                        "Pass a coroutine function, callable, or coroutine instead."
                    )
            self._resolve_target_worker(None)
            try:
                caller_loop: asyncio.AbstractEventLoop | None = asyncio.get_running_loop()
            except RuntimeError:
                caller_loop = None

            if caller_loop is None or not caller_loop.is_running():
                raise RuntimeError(
                    "submit_many() must be called from a thread with a running asyncio event loop"
                )
        except BaseException:
            for t in target_list:
                if asyncio.iscoroutine(t):
                    t.close()
            raise

        futures: list[asyncio.Future[Any]] = []
        items: list[_PoolWorkItem] = []
        for target in target_list:
            fut: asyncio.Future[Any] = caller_loop.create_future()
            futures.append(fut)
            items.append(_PoolWorkItem(self, target, (), {}, caller_loop, fut, cancel_scope))

        if self._native_pool is not None:
            try:
                self._native_pool.push_global_batch(items)
            except Exception as exc:
                for t in target_list:
                    if asyncio.iscoroutine(t):
                        t.close()
                if isinstance(exc, ThreadPoolClosedError):
                    raise ThreadPoolClosedError("ThreadPool is closed") from exc
                raise

        self._notify_workers_bounded(len(items))
        return futures


async def create_pool(
    num_threads: int = 0,
    options: PoolOptions | None = None,
    **kwargs: Any,
) -> EventLoopThreadPool:
    """Create, start, and return an active :class:`EventLoopThreadPool` instance.

    :param num_threads: Number of worker threads (0 = auto-detect via :func:`os.cpu_count`).
    :param options: Optional :class:`PoolOptions` defaults.
    :param kwargs: Additional arguments forwarded to :class:`EventLoopThreadPool`.
    :returns: An active, already-started :class:`EventLoopThreadPool`.
    """
    pool = EventLoopThreadPool(num_threads=num_threads, options=options, **kwargs)
    await pool.start()
    return pool
