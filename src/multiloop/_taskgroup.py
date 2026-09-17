from __future__ import annotations

import asyncio
import enum
import threading
from typing import TYPE_CHECKING, Any, Self

from multiloop._cancel import CancelScope
from multiloop._sync import CapacityLimiter

if TYPE_CHECKING:
    from multiloop.pool import EventLoopThreadPool

__all__ = ["TaskGroup", "TaskHandle", "TaskStatus"]


def _retrieve_task_exception(task: asyncio.Future[Any]) -> None:
    """Consume a finished task's exception so asyncio does not log unretrieved exception warnings.

    Used for orphan tasks cancelled on start_soon after group exit.
    """
    if not task.cancelled():
        task.exception()


class _TaskStatus(enum.Enum):
    """Internal task lifecycle states."""

    PENDING = 0
    STARTED = 1
    FINISHED = 2


class TaskStatus:
    """Status tracker used with :meth:`TaskGroup.start`.

    Call :meth:`started` once the spawned coroutine has initialized so that
    :meth:`TaskGroup.start` can unblock and return the handle. A child that exits
    without calling :meth:`started` causes :meth:`TaskGroup.start` to raise :class:`RuntimeError`.
    """

    def __init__(self) -> None:
        self._started: asyncio.Event = asyncio.Event()
        self._called = False

    def started(self) -> None:
        """Mark the task as started, unblocking :meth:`TaskGroup.start`."""
        self._called = True
        self._started.set()


class TaskHandle:
    """A handle to a child task spawned inside a :class:`TaskGroup`.

    Awaiting the handle returns the task's result or raises its exception.
    """

    def __init__(self, task: asyncio.Future[Any]) -> None:
        self._task: asyncio.Future[Any] = task
        self._start_event: asyncio.Event | None = None

    @property
    def status(self) -> _TaskStatus:
        """Current lifecycle status of the wrapped task."""
        if self._task.done():
            return _TaskStatus.FINISHED
        if self._start_event is not None and self._start_event.is_set():
            return _TaskStatus.STARTED
        return _TaskStatus.PENDING

    @property
    def result(self) -> Any:
        """Return the task result once finished.

        :raises RuntimeError: If the task has not finished yet.
        :raises asyncio.CancelledError: If the task was cancelled.
        """
        if not self._task.done():
            raise RuntimeError("Task is not finished")
        if self._task.cancelled():
            raise asyncio.CancelledError()
        return self._task.result()

    @property
    def exception(self) -> BaseException | None:
        """Return the exception if the task failed, or None.

        :raises RuntimeError: If the task has not finished yet.
        :raises asyncio.CancelledError: If the task was cancelled.
        """
        if not self._task.done():
            raise RuntimeError("Task is not finished")
        if self._task.cancelled():
            return asyncio.CancelledError()
        return self._task.exception()

    def __await__(self) -> Any:
        return self._task.__await__()


class TaskGroup:
    """An async context manager for structured concurrency (nursery).

    Guarantees that all spawned child tasks finish (or are cleanly cancelled) before
    the context manager block exits. If any child task fails, sibling tasks are automatically
    cancelled and exceptions are aggregated into an exception group.

    Usage::

        async with TaskGroup() as tg:
            h1 = tg.start_soon(worker, "a")
            h2 = tg.start_soon(worker, "b")
        # All child tasks are guaranteed finished here.

    When initialized with an ``EventLoopThreadPool`` (e.g. ``TaskGroup(pool=pool)``),
    child tasks spawned via :meth:`start_soon` or :meth:`start` are distributed across
    the worker threads of the pool while fully preserving structured concurrency guarantees:
    isolated cancellation scopes, automatic sibling cancellation on failure, exception
    group aggregation, and remote drain barrier on context exit.
    """

    def __init__(
        self,
        name: str | None = None,
        max_concurrency: int | None = None,
        limiter: CapacityLimiter | None = None,
        pool: EventLoopThreadPool | None = None,
    ) -> None:
        """Initialize a new TaskGroup.

        :param name: Optional human-readable identifier for debugging and logging.
        :param max_concurrency: Optional positive integer constraining maximum concurrent
                                active child tasks via an internal :class:`CapacityLimiter`.
        :param limiter: Optional external :class:`CapacityLimiter` to share concurrency
                        budgets across multiple task groups.
        :param pool: Optional :class:`~multiloop.EventLoopThreadPool` to distribute spawned
                     child tasks across multi-loop worker threads with full structured
                     concurrency invariants (remote drain barrier and cascading cancellation).
        :raises ValueError: If ``max_concurrency`` is non-positive, or if both
                            ``max_concurrency`` and ``limiter`` are specified.
        """
        if max_concurrency is not None and max_concurrency <= 0:
            raise ValueError("max_concurrency must be >= 1")
        if max_concurrency is not None and limiter is not None:
            raise ValueError("Cannot pass both max_concurrency and limiter")
        self._name: str | None = name
        self._pool: EventLoopThreadPool | None = pool
        if limiter is not None:
            self._limiter: CapacityLimiter | None = limiter
        elif max_concurrency is not None:
            self._limiter = CapacityLimiter(max_concurrency)
        else:
            self._limiter = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._children: set[TaskHandle] = set()
        self._child_cancel_scopes: set[CancelScope] = set()
        self._children_lock = threading.Lock()
        self._cancel_scope: CancelScope = CancelScope()
        self._exited = False
        self._consumed: set[asyncio.Future[Any]] = set()

    def _discard_child_scope(self, scope: CancelScope) -> None:
        with self._children_lock:
            self._child_cancel_scopes.discard(scope)

    # -- context manager -------------------------------------------------------

    async def __aenter__(self) -> Self:
        if self._cancel_scope.cancel_called:
            raise RuntimeError("TaskGroup is not reusable after failure")
        self._loop = asyncio.get_running_loop()
        with self._children_lock:
            was_exited = self._exited
            self._exited = False
            if was_exited:
                self._children.clear()
                self._consumed.clear()
                self._child_cancel_scopes.clear()
        await self._cancel_scope.__aenter__()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> bool | None:
        try:
            return await self._aexit_impl(exc_type, exc_val, exc_tb)
        finally:
            with self._children_lock:
                self._exited = True
            self._loop = None

    async def _aexit_impl(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> bool | None:
        # Structured concurrency: when the group body raises or cancels,
        # cancel all remaining child tasks before awaiting them.
        pre_cancelled: set[asyncio.Future[Any]] = set()
        if exc_val is not None:
            with self._children_lock:
                remaining = [h._task for h in self._children if not h._task.done()]
                scopes = list(self._child_cancel_scopes)
            for task in remaining:
                if isinstance(task, asyncio.Task):
                    task.cancel()
                pre_cancelled.add(task)
            for s in scopes:
                s.cancel()

        try:
            child_exceptions = await self._wait_children(pre_cancelled)
        except BaseException:
            # If the group's host task is cancelled while waiting for children,
            # cancel remaining tasks and wait for their completion before re-raising.
            with self._children_lock:
                self._exited = True
                remaining = [h._task for h in self._children if not h._task.done()]
                scopes = list(self._child_cancel_scopes)
            for task in remaining:
                if isinstance(task, asyncio.Task):
                    task.cancel()
            for s in scopes:
                s.cancel()
            await self._drain_cancelled_children(remaining)
            raise

        with self._children_lock:
            self._exited = True

        # External cancellation takes precedence
        if exc_val is not None and isinstance(exc_val, asyncio.CancelledError):
            await self._cancel_scope.__aexit__(exc_type, exc_val, exc_tb)
            return None

        if not child_exceptions:
            return await self._cancel_scope.__aexit__(exc_type, exc_val, exc_tb)

        # Soft exit: all children raised CancelledError while body finished normally
        if exc_val is None and all(isinstance(e, asyncio.CancelledError) for e in child_exceptions):
            await self._cancel_scope.__aexit__(None, None, exc_tb)
            if len(child_exceptions) == 1:
                raise child_exceptions[0]
            raise BaseExceptionGroup("taskgroup soft exit", child_exceptions)

        # At least one child task raised a non-cancellation exception
        self._cancel_scope.cancel()

        scope_exc_type = exc_type
        scope_exc_val = exc_val
        if exc_type is not None and issubclass(exc_type, asyncio.CancelledError):
            scope_exc_type = None
            scope_exc_val = None

        await self._cancel_scope.__aexit__(scope_exc_type, scope_exc_val, exc_tb)

        all_exceptions = list(child_exceptions)
        if exc_val is not None:
            all_exceptions.insert(0, exc_val)

        if len(all_exceptions) == 1:
            raise all_exceptions[0]
        raise BaseExceptionGroup("taskgroup crashed", all_exceptions)

    async def _wait_children(
        self, pre_cancelled: set[asyncio.Future[Any]] | None = None
    ) -> list[BaseException]:
        """Wait for all child tasks to complete, collecting non-trivial exceptions."""
        exceptions: list[BaseException] = []
        cancelled_by_scope: set[asyncio.Future[Any]] = set(pre_cancelled or ())
        scope_cancelled = False
        processed: set[asyncio.Future[Any]] = set()
        pending: set[asyncio.Future[Any]] = set()

        def cancel_siblings() -> None:
            nonlocal scope_cancelled
            for p in pending:
                if isinstance(p, asyncio.Task):
                    p.cancel()
                cancelled_by_scope.add(p)
            with self._children_lock:
                for s in list(self._child_cancel_scopes):
                    s.cancel()
            self._cancel_scope.cancel()
            cur = asyncio.current_task()
            if cur is not None and self._cancel_scope._take_injected():
                cur.uncancel()
            scope_cancelled = True

        def collect_one(task: asyncio.Future[Any]) -> None:
            with self._children_lock:
                if task in self._consumed:
                    return
            if task.cancelled():
                exc: BaseException = asyncio.CancelledError()
            else:
                task_exc = task.exception()
                if task_exc is None:
                    return
                exc = task_exc

            cancelling_count: int = getattr(task, "cancelling", lambda: 0)()
            if isinstance(exc, asyncio.CancelledError) and (
                task in cancelled_by_scope or cancelling_count > 0
            ):
                return
            exceptions.append(exc)
            if not scope_cancelled:
                cancel_siblings()

        def absorb() -> None:
            if self._cancel_scope.cancel_called and not scope_cancelled:
                cancel_siblings()
            with self._children_lock:
                current = [h._task for h in self._children]
            for task in current:
                if task in processed or task in pending:
                    continue
                if task.done():
                    collect_one(task)
                    processed.add(task)
                elif scope_cancelled:
                    if isinstance(task, asyncio.Task):
                        task.cancel()
                    cancelled_by_scope.add(task)
                    pending.add(task)
                else:
                    pending.add(task)

        async with CancelScope(shield=True):
            while True:
                absorb()
                if not pending:
                    break
                done, pending = await asyncio.wait(
                    pending,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for task in done:
                    processed.add(task)
                    collect_one(task)
        return exceptions

    async def _drain_cancelled_children(self, tasks: list[asyncio.Future[Any]]) -> None:
        """Wait for cancelled children to complete before propagating cancellation outwards."""
        pending = {t for t in tasks if not t.done()}
        while pending:
            try:
                done, pending = await asyncio.wait(pending)
            except asyncio.CancelledError:
                continue
            for t in done:
                _retrieve_task_exception(t)

    # -- public API ------------------------------------------------------------

    def start_soon(self, coro_fn: Any, *args: Any) -> TaskHandle:
        """Spawn a child task and return its handle immediately without blocking.

        If a :class:`~multiloop.EventLoopThreadPool` was provided when constructing the
        :class:`TaskGroup`, the task is dispatched to the thread pool with an isolated
        child :class:`~multiloop.CancelScope`, enabling true multi-core physical execution
        while retaining structured concurrency cancellation and exception aggregation.
        Otherwise, the task is scheduled as a local :class:`asyncio.Task` on the current loop.

        :param coro_fn: Coroutine function or coroutine object to spawn.
        :param args: Arguments to forward to `coro_fn`.
        :returns: A :class:`TaskHandle` referencing the child task.
        :raises RuntimeError: If called after the TaskGroup context has exited, or called
                            from a foreign event loop/thread when not pool-backed.
        """
        current_loop = asyncio.get_running_loop()
        if self._loop is not None and current_loop is not self._loop:
            raise RuntimeError(
                "TaskGroup is physically scoped to a single event loop and cannot spawn tasks "
                "from a foreign event loop or thread. Use EventLoopThreadPool for cross-loop tasks."
            )
        limiter = self._limiter
        if self._pool is not None:
            task_cancel_scope = CancelScope()
            with self._children_lock:
                if self._cancel_scope.cancel_called:
                    task_cancel_scope.cancel()
                self._child_cancel_scopes.add(task_cancel_scope)

            if limiter is not None:

                async def _guarded_coro() -> Any:
                    assert limiter is not None
                    async with limiter:
                        if asyncio.iscoroutine(coro_fn):
                            return await coro_fn
                        res = coro_fn(*args)
                        if asyncio.iscoroutine(res) or asyncio.isfuture(res):
                            return await res
                        return res

                fut = self._pool.submit(_guarded_coro, cancel_scope=task_cancel_scope)
            else:
                if asyncio.iscoroutine(coro_fn):
                    fut = self._pool.submit(coro_fn, cancel_scope=task_cancel_scope)
                else:
                    fut = self._pool.submit(coro_fn, *args, cancel_scope=task_cancel_scope)

            fut.add_done_callback(lambda _f: self._discard_child_scope(task_cancel_scope))
            handle = TaskHandle(fut)
            with self._children_lock:
                if self._exited:
                    task_cancel_scope.cancel()
                    fut.cancel()
                    raise RuntimeError(
                        "TaskGroup is not active: cannot start_soon() after the group exited"
                    )
                self._children.add(handle)
            return handle

        if limiter is not None:

            async def _guarded_coro() -> Any:
                assert limiter is not None
                async with limiter:
                    if asyncio.iscoroutine(coro_fn):
                        return await coro_fn
                    return await coro_fn(*args)

            task = asyncio.create_task(_guarded_coro())
        else:
            if asyncio.iscoroutine(coro_fn):
                task = asyncio.create_task(coro_fn)
            else:
                task = asyncio.create_task(coro_fn(*args))
        handle = TaskHandle(task)
        with self._children_lock:
            if self._exited:
                task.cancel()
                task.add_done_callback(_retrieve_task_exception)
                raise RuntimeError(
                    "TaskGroup is not active: cannot start_soon() after the group exited"
                )
            self._children.add(handle)
        return handle

    async def start(self, coro_fn: Any, *args: Any) -> TaskHandle:
        """Spawn a child task, suspending until the coroutine calls ``task_status.started()``.

        Supports both local event loop tasks and pool-backed cross-loop tasks. When
        executing in a pool, ``task_status.started()`` signals across thread boundaries
        to unblock this method on the caller loop.

        :param coro_fn: Coroutine function expecting a :class:`TaskStatus` as its first parameter.
        :param args: Additional arguments to forward to `coro_fn`.
        :returns: A :class:`TaskHandle` referencing the started child task.
        :raises RuntimeError: If the task exits or crashes before calling `task_status.started()`,
                            or if called after the TaskGroup has exited.
        """
        current_loop = asyncio.get_running_loop()
        if self._loop is not None and current_loop is not self._loop:
            raise RuntimeError(
                "TaskGroup is physically scoped to a single event loop and cannot spawn tasks "
                "from a foreign event loop or thread. Use EventLoopThreadPool for cross-loop tasks."
            )
        task_status = TaskStatus()
        limiter = self._limiter
        if self._pool is not None:
            task_cancel_scope = CancelScope()
            with self._children_lock:
                if self._cancel_scope.cancel_called:
                    task_cancel_scope.cancel()
                self._child_cancel_scopes.add(task_cancel_scope)

            if limiter is not None:

                async def _guarded_start_coro() -> Any:
                    assert limiter is not None
                    async with limiter:
                        return await coro_fn(task_status, *args)

                fut = self._pool.submit(_guarded_start_coro, cancel_scope=task_cancel_scope)
            else:
                fut = self._pool.submit(coro_fn, task_status, *args, cancel_scope=task_cancel_scope)

            fut.add_done_callback(lambda _f: self._discard_child_scope(task_cancel_scope))
            handle = TaskHandle(fut)
            handle._start_event = task_status._started
            fut.add_done_callback(lambda _f: task_status._started.set())
            with self._children_lock:
                if self._exited:
                    task_cancel_scope.cancel()
                    fut.cancel()
                    raise RuntimeError(
                        "TaskGroup is not active: cannot start() after the group exited"
                    )
                self._children.add(handle)
            try:
                await task_status._started.wait()
            finally:
                if fut.done():
                    exc: BaseException | None = None
                    if fut.cancelled():
                        if not task_status._called:
                            exc = asyncio.CancelledError()
                    else:
                        fut_exc = fut.exception()
                        if fut_exc is not None:
                            exc = fut_exc
                        elif not task_status._called:
                            raise RuntimeError("Child exited without calling task_status.started()")
                    if exc is not None:
                        with self._children_lock:
                            self._consumed.add(fut)
                            siblings = [
                                h._task
                                for h in self._children
                                if h._task is not fut and not h._task.done()
                            ]
                        for sibling in siblings:
                            if isinstance(sibling, asyncio.Task):
                                sibling.cancel()
                        with self._children_lock:
                            for s in list(self._child_cancel_scopes):
                                s.cancel()
                        raise exc
            return handle

        if limiter is not None:

            async def _guarded_start_coro() -> Any:
                assert limiter is not None
                async with limiter:
                    return await coro_fn(task_status, *args)

            task = asyncio.create_task(_guarded_start_coro())
        else:
            task = asyncio.create_task(coro_fn(task_status, *args))
        handle = TaskHandle(task)
        handle._start_event = task_status._started
        task.add_done_callback(lambda _t: task_status._started.set())
        with self._children_lock:
            if self._exited:
                task.cancel()
                task.add_done_callback(_retrieve_task_exception)
                raise RuntimeError("TaskGroup is not active: cannot start() after the group exited")
            self._children.add(handle)
        try:
            await task_status._started.wait()
        finally:
            if task.done():
                exc = None
                if task.cancelled():
                    if not task_status._called:
                        exc = asyncio.CancelledError()
                else:
                    task_exc = task.exception()
                    if task_exc is not None:
                        exc = task_exc
                    elif not task_status._called:
                        raise RuntimeError("Child exited without calling task_status.started()")
                if exc is not None:
                    with self._children_lock:
                        self._consumed.add(task)
                        siblings = [
                            h._task
                            for h in self._children
                            if h._task is not task and not h._task.done()
                        ]
                    for sibling in siblings:
                        sibling.cancel()
                    raise exc
        return handle

    def cancel_all(self) -> None:
        """Cancel all child tasks safely across threads and event loops.

        Local :class:`asyncio.Task` instances are cancelled thread-safely via
        ``loop.call_soon_threadsafe(task.cancel)``, while pool-backed tasks running
        on remote worker threads are cancelled via their dedicated :class:`CancelScope`
        instances, triggering prompt cleanup without tearing down waiting futures prematurely.
        """
        with self._children_lock:
            handles = list(self._children)
            scopes = list(self._child_cancel_scopes)
        for h in handles:
            if isinstance(h._task, asyncio.Task):
                loop = h._task.get_loop()
                try:
                    loop.call_soon_threadsafe(h._task.cancel)
                except RuntimeError:
                    pass
        for s in scopes:
            s.cancel()
