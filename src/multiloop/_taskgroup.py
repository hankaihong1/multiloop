from __future__ import annotations

import asyncio
import enum
import sys
import threading
from typing import TYPE_CHECKING, Any, Self

from multiloop._cancel import MOVE_ON, CancelScope, _await_cancellable, _check_not_cancelled
from multiloop._sync import CapacityLimiter, Event

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


class _TGState(enum.IntEnum):
    """Lifecycle states of a TaskGroup."""

    IDLE = 0
    ACTIVE = 1
    DRAINING = 2
    EXITED_CLEAN = 3
    FAILED = 4


class _CancelReason(enum.IntEnum):
    """Source reason initiating TaskGroup cancellation."""

    NONE = 0
    USER_CANCEL_ALL = 1
    CONTEXT_CANCEL = 2
    CHILD_FAILURE = 3


async def _wrap_with_limiter(
    limiter: CapacityLimiter | None,
    coro: Any,
    args: tuple[Any, ...],
) -> Any:
    """Execute *coro* (coroutine object or callable + *args*) optionally under *limiter*.

    Guarantees an unstepped coroutine object is closed if cancelled before first execution step.
    """
    stepped = False
    try:
        if limiter is not None:
            async with limiter:
                _check_not_cancelled()
                if asyncio.iscoroutine(coro):
                    stepped = True
                    return await coro
                stepped = True
                res = coro(*args)
                if asyncio.iscoroutine(res) or asyncio.isfuture(res):
                    return await res
                return res
        _check_not_cancelled()
        if asyncio.iscoroutine(coro):
            stepped = True
            return await coro
        stepped = True
        res = coro(*args)
        if asyncio.iscoroutine(res) or asyncio.isfuture(res):
            return await res
        return res
    finally:
        if not stepped and asyncio.iscoroutine(coro):
            coro.close()


async def _run_local_child(
    child_scope: CancelScope,
    limiter: CapacityLimiter | None,
    coro: Any,
    args: tuple[Any, ...],
) -> Any:
    """Execute a local child task inside its dedicated CancelScope and optional limiter."""
    async with child_scope:
        return await _wrap_with_limiter(limiter, coro, args)


class TaskStatus:
    """Status tracker used with :meth:`TaskGroup.start`.

    Call :meth:`started` once the spawned coroutine has initialized so that
    :meth:`TaskGroup.start` can unblock and return the handle. A child that exits
    without calling :meth:`started` causes :meth:`TaskGroup.start` to raise :class:`RuntimeError`.
    """

    __slots__ = ("_called", "_started")

    def __init__(self) -> None:
        self._started: Event = Event()
        self._called = False

    def started(self) -> None:
        """Mark the task as started, unblocking :meth:`TaskGroup.start`."""
        self._called = True
        self._started.set()


class TaskHandle:
    """A handle to a child task spawned inside a :class:`TaskGroup`.

    Awaiting the handle returns the task's result or raises its exception.
    """

    __slots__ = ("_start_event", "_task")

    def __init__(self, task: asyncio.Future[Any]) -> None:
        self._task: asyncio.Future[Any] = task
        self._start_event: Event | None = None

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

    async def _await_impl(self) -> Any:
        _check_not_cancelled()
        if self._task.done():
            return self._task.result()
        loop = asyncio.get_running_loop()
        wait_fut: asyncio.Future[None] = loop.create_future()

        def _on_done(_f: asyncio.Future[Any]) -> None:
            if not wait_fut.done():
                wait_fut.set_result(None)

        self._task.add_done_callback(_on_done)
        try:
            await _await_cancellable(loop, wait_fut)
        finally:
            self._task.remove_done_callback(_on_done)
        return self._task.result()

    def __await__(self) -> Any:
        return self._await_impl().__await__()


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

    __slots__ = (
        "__weakref__",
        "_cancel_reason",
        "_cancel_scope",
        "_cancelled_by_group",
        "_child_cancel_scopes",
        "_children",
        "_children_lock",
        "_consumed",
        "_limiter",
        "_loop",
        "_name",
        "_pool",
        "_starting_futures",
        "_state",
        "_wake_fut",
    )

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
        self._cancelled_by_group: set[asyncio.Future[Any]] = set()
        self._starting_futures: set[asyncio.Future[Any]] = set()
        self._children_lock = threading.Lock()
        self._cancel_scope: CancelScope = CancelScope()
        self._state: _TGState = _TGState.IDLE
        self._cancel_reason: _CancelReason = _CancelReason.NONE
        self._consumed: set[asyncio.Future[Any]] = set()
        self._wake_fut: asyncio.Future[None] | None = None

    def _discard_child_scope(self, scope: CancelScope) -> None:
        with self._children_lock:
            self._child_cancel_scopes.discard(scope)

    def _wake_waiter_locked(self) -> None:
        wake_fut = self._wake_fut
        loop = self._loop
        if wake_fut is None or loop is None:
            return
        try:
            current = asyncio.get_running_loop()
        except RuntimeError:
            current = None
        if current is loop:
            if not wake_fut.done():
                wake_fut.set_result(None)
        else:

            def _complete_wake() -> None:
                if not wake_fut.done():
                    wake_fut.set_result(None)

            try:
                loop.call_soon_threadsafe(_complete_wake)
            except RuntimeError:
                pass

    def _has_unseen_children(
        self,
        processed: set[asyncio.Future[Any]],
        pending: set[asyncio.Future[Any]],
    ) -> bool:
        with self._children_lock:
            return any(h._task not in processed and h._task not in pending for h in self._children)

    def _on_child_done(self, child_fut: asyncio.Future[Any]) -> None:
        with self._children_lock:
            self._wake_waiter_locked()
            if (
                self._state in (_TGState.DRAINING, _TGState.EXITED_CLEAN, _TGState.FAILED)
                or child_fut in self._consumed
                or child_fut in self._starting_futures
            ):
                return

        if child_fut.cancelled():
            return
        try:
            exc = child_fut.exception()
        except BaseException:
            return
        if exc is None or isinstance(exc, asyncio.CancelledError):
            return

        with self._children_lock:
            if (
                self._state in (_TGState.DRAINING, _TGState.EXITED_CLEAN, _TGState.FAILED)
                or child_fut in self._consumed
                or child_fut in self._starting_futures
            ):
                return
            self._state = _TGState.FAILED
            if self._cancel_reason == _CancelReason.NONE:
                self._cancel_reason = _CancelReason.CHILD_FAILURE
            for h in self._children:
                if h._task is not child_fut:
                    self._cancelled_by_group.add(h._task)
            scopes = list(self._child_cancel_scopes)

        for s in scopes:
            s.cancel()
        self._cancel_scope.cancel()

    # -- context manager -------------------------------------------------------

    async def __aenter__(self) -> Self:
        loop = asyncio.get_running_loop()
        with self._children_lock:
            if self._state == _TGState.FAILED:
                raise RuntimeError("TaskGroup is not reusable after failure")
            if self._state not in (_TGState.IDLE, _TGState.EXITED_CLEAN):
                raise RuntimeError("TaskGroup is already active")
            if self._state == _TGState.EXITED_CLEAN:
                self._cancel_reason = _CancelReason.NONE
                self._children.clear()
                self._consumed.clear()
                self._cancelled_by_group.clear()
                self._starting_futures.clear()
                self._child_cancel_scopes.clear()
                self._cancel_scope = CancelScope()
            self._state = _TGState.ACTIVE
            self._loop = loop
        await self._cancel_scope.__aenter__()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> bool | None:
        with self._children_lock:
            if self._state in (_TGState.IDLE, _TGState.ACTIVE):
                self._state = _TGState.DRAINING
        try:
            return await self._aexit_impl(exc_type, exc_val, exc_tb)
        finally:
            with self._children_lock:
                if self._state != _TGState.FAILED:
                    self._state = _TGState.EXITED_CLEAN
                self._wake_fut = None
            self._loop = None

    def _has_outer_cancellation(self, host_task: asyncio.Task[Any] | None) -> bool:
        state = self._cancel_scope._state
        entry_id = self._cancel_scope._entry_id
        if state is not None and entry_id is not None and state.has_outer_cancellation(entry_id):
            return True
        return host_task is not None and host_task.cancelling() > 0

    async def _aexit_impl(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> bool | None:
        host_task = asyncio.current_task()
        has_outer = self._has_outer_cancellation(host_task)
        internal_scope_cancel = bool(
            exc_val is not None
            and isinstance(exc_val, asyncio.CancelledError)
            and self._cancel_scope.cancel_called
            and not has_outer
        )

        effective_exc_type = None if internal_scope_cancel else exc_type
        effective_exc_val = None if internal_scope_cancel else exc_val

        # Structured concurrency: when the group body raises or cancels,
        # cancel all remaining child tasks before awaiting them.
        pre_cancelled: set[asyncio.Future[Any]] = set()
        if effective_exc_val is not None:
            with self._children_lock:
                remaining = [h._task for h in self._children if not h._task.done()]
                self._cancelled_by_group.update(remaining)
                scopes = list(self._child_cancel_scopes)
            pre_cancelled.update(remaining)
            for s in scopes:
                s.cancel()
            self._cancel_scope.cancel()

        try:
            child_exceptions = await self._wait_children(pre_cancelled)
        except BaseException:
            # Host task received external cancellation or wait error while draining:
            # cancel remaining child scopes and finish draining before propagating.
            with self._children_lock:
                if self._state != _TGState.FAILED:
                    self._state = _TGState.EXITED_CLEAN
                remaining = [h._task for h in self._children if not h._task.done()]
                self._cancelled_by_group.update(remaining)
                scopes = list(self._child_cancel_scopes)
            for s in scopes:
                s.cancel()
            self._cancel_scope.cancel()
            try:
                await self._drain_cancelled_children(remaining)
            finally:
                await self._cancel_scope.__aexit__(*sys.exc_info())
            raise

        has_outer = self._has_outer_cancellation(host_task)

        # External cancellation takes precedence
        if effective_exc_val is not None and isinstance(effective_exc_val, asyncio.CancelledError):
            await self._cancel_scope.__aexit__(exc_type, exc_val, exc_tb)
            return None

        if not child_exceptions:
            with self._children_lock:
                cancel_reason = self._cancel_reason
                self._state = _TGState.EXITED_CLEAN
            if cancel_reason == _CancelReason.CONTEXT_CANCEL:
                self._cancel_scope._mode = MOVE_ON
                await self._cancel_scope.__aexit__(None, None, None)
                raise asyncio.CancelledError()
            if internal_scope_cancel or cancel_reason == _CancelReason.USER_CANCEL_ALL:
                if not has_outer:
                    self._cancel_scope._mode = MOVE_ON
                return await self._cancel_scope.__aexit__(None, None, None)
            if not has_outer and self._cancel_scope.cancel_called:
                self._cancel_scope._mode = MOVE_ON
            return await self._cancel_scope.__aexit__(exc_type, exc_val, exc_tb)

        # Soft exit: all children raised CancelledError while body finished normally
        if effective_exc_val is None and all(
            isinstance(e, asyncio.CancelledError) for e in child_exceptions
        ):
            self._cancel_scope._mode = MOVE_ON
            await self._cancel_scope.__aexit__(None, None, None)
            if len(child_exceptions) == 1:
                raise child_exceptions[0]
            raise BaseExceptionGroup("taskgroup soft exit", child_exceptions)

        # At least one child task raised a non-cancellation exception
        with self._children_lock:
            self._state = _TGState.FAILED
        self._cancel_scope._mode = MOVE_ON
        try:
            await self._cancel_scope.__aexit__(effective_exc_type, effective_exc_val, exc_tb)
        except asyncio.CancelledError:
            pass

        all_exceptions = list(child_exceptions)
        if effective_exc_val is not None:
            all_exceptions.insert(0, effective_exc_val)

        if len(all_exceptions) == 1:
            raise all_exceptions[0]
        raise BaseExceptionGroup("taskgroup crashed", all_exceptions)

    async def _wait_children(
        self, pre_cancelled: set[asyncio.Future[Any]] | None = None
    ) -> list[BaseException]:
        """Wait for all child tasks to complete via _wake_fut, collecting non-trivial exceptions."""
        exceptions: list[BaseException] = []
        with self._children_lock:
            cancelled_by_scope: set[asyncio.Future[Any]] = (
                set(pre_cancelled or ()) | self._cancelled_by_group
            )
        scope_cancelled = False
        processed: set[asyncio.Future[Any]] = set()
        pending: set[asyncio.Future[Any]] = set()
        loop = asyncio.get_running_loop()
        host_task = asyncio.current_task()

        def cancel_siblings() -> None:
            nonlocal scope_cancelled
            with self._children_lock:
                self._cancelled_by_group.update(pending)
                cancelled_by_scope.update(pending)
                if self._cancel_reason == _CancelReason.NONE:
                    self._cancel_reason = _CancelReason.CHILD_FAILURE
                scopes = list(self._child_cancel_scopes)
            for s in scopes:
                s.cancel()
            self._cancel_scope.cancel()
            scope_cancelled = True

        def collect_one(task: asyncio.Future[Any]) -> None:
            with self._children_lock:
                if task in self._consumed:
                    return
                if task in self._cancelled_by_group:
                    cancelled_by_scope.add(task)
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
                scopes = list(self._child_cancel_scopes) if scope_cancelled else []
            for s in scopes:
                s.cancel()
            for task in current:
                if task in processed:
                    continue
                if task.done():
                    pending.discard(task)
                    processed.add(task)
                    collect_one(task)
                elif task not in pending:
                    if scope_cancelled:
                        with self._children_lock:
                            self._cancelled_by_group.add(task)
                        cancelled_by_scope.add(task)
                    pending.add(task)

        host_cancelled: asyncio.CancelledError | None = None
        try:
            # Phase 1: Wait unshielded until cancelled or all children complete
            while True:
                absorb()
                if not pending:
                    return exceptions
                if scope_cancelled:
                    break
                wake_fut = loop.create_future()
                with self._children_lock:
                    self._wake_fut = wake_fut
                if any(t.done() for t in pending) or self._has_unseen_children(processed, pending):
                    with self._children_lock:
                        self._wake_fut = None
                    continue
                try:
                    await _await_cancellable(loop, wake_fut)
                except asyncio.CancelledError as c_exc:
                    if (
                        self._has_outer_cancellation(host_task)
                        or not self._cancel_scope.cancel_called
                    ):
                        host_cancelled = c_exc
                    cancel_siblings()
                    break
                finally:
                    with self._children_lock:
                        if self._wake_fut is wake_fut:
                            self._wake_fut = None

            # Phase 2: Shielded exit drain while awaiting cancelled children
            async with CancelScope(shield=True):
                while True:
                    absorb()
                    if not pending:
                        break
                    wake_fut = loop.create_future()
                    with self._children_lock:
                        self._wake_fut = wake_fut
                    if any(t.done() for t in pending) or self._has_unseen_children(
                        processed, pending
                    ):
                        with self._children_lock:
                            self._wake_fut = None
                        continue
                    try:
                        await _await_cancellable(loop, wake_fut)
                    except asyncio.CancelledError:
                        continue
                    finally:
                        with self._children_lock:
                            if self._wake_fut is wake_fut:
                                self._wake_fut = None
        finally:
            with self._children_lock:
                self._wake_fut = None

        if host_cancelled is not None and not any(
            not isinstance(e, asyncio.CancelledError) for e in exceptions
        ):
            raise host_cancelled
        return exceptions

    async def _drain_cancelled_children(self, tasks: list[asyncio.Future[Any]]) -> None:
        """Wait for cancelled children to complete before propagating cancellation outwards."""
        loop = asyncio.get_running_loop()
        pending = {t for t in tasks if not t.done()}
        if not pending:
            for t in tasks:
                if t.done():
                    _retrieve_task_exception(t)
            return

        try:
            async with CancelScope(shield=True):
                while pending:
                    pending = {t for t in pending if not t.done()}
                    if not pending:
                        break
                    wake_fut = loop.create_future()
                    with self._children_lock:
                        self._wake_fut = wake_fut
                    if any(t.done() for t in pending):
                        with self._children_lock:
                            self._wake_fut = None
                        continue
                    try:
                        await _await_cancellable(loop, wake_fut)
                    except asyncio.CancelledError:
                        continue
                    finally:
                        with self._children_lock:
                            if self._wake_fut is wake_fut:
                                self._wake_fut = None
        finally:
            with self._children_lock:
                self._wake_fut = None
            for t in tasks:
                if t.done():
                    _retrieve_task_exception(t)

    # -- public API ------------------------------------------------------------

    def start_soon(self, coro_fn: Any, *args: Any) -> TaskHandle:
        """Spawn a child task and return its handle immediately without blocking.

        If a :class:`~multiloop.EventLoopThreadPool` was provided when constructing the
        :class:`TaskGroup`, the task is dispatched to the thread pool with an isolated
        child :class:`~multiloop.CancelScope`, enabling true multi-core physical execution
        while retaining structured concurrency cancellation and exception aggregation.
        Otherwise, the task is scheduled as a local :class:`asyncio.Task` wrapped in its own
        :class:`~multiloop.CancelScope` on the current loop.

        :param coro_fn: Coroutine function or coroutine object to spawn.
        :param args: Arguments to forward to `coro_fn`.
        :returns: A :class:`TaskHandle` referencing the child task.
        :raises RuntimeError: If called after the TaskGroup context has exited, or called
                            from a foreign event loop/thread when not pool-backed.
        """
        current_loop = asyncio.get_running_loop()
        if self._loop is not None and current_loop is not self._loop:
            if asyncio.iscoroutine(coro_fn):
                coro_fn.close()
            raise RuntimeError(
                "TaskGroup is physically scoped to a single event loop and cannot spawn tasks "
                "from a foreign event loop or thread. Use EventLoopThreadPool for cross-loop tasks."
            )

        with self._children_lock:
            if self._state not in (_TGState.IDLE, _TGState.ACTIVE, _TGState.DRAINING):
                if asyncio.iscoroutine(coro_fn):
                    coro_fn.close()
                raise RuntimeError(
                    "TaskGroup is not active: cannot start_soon() after the group exited"
                )
            child_scope = CancelScope()
            pre_cancel = (
                self._cancel_reason != _CancelReason.NONE or self._cancel_scope.cancel_called
            )
            if pre_cancel:
                child_scope.cancel()
            self._child_cancel_scopes.add(child_scope)

        limiter = self._limiter
        if self._pool is not None:
            try:
                if limiter is not None:
                    fut = self._pool.submit(
                        _wrap_with_limiter,
                        limiter,
                        coro_fn,
                        args,
                        cancel_scope=child_scope,
                    )
                elif asyncio.iscoroutine(coro_fn):
                    fut = self._pool.submit(coro_fn, cancel_scope=child_scope)
                else:
                    fut = self._pool.submit(coro_fn, *args, cancel_scope=child_scope)
            except BaseException:
                self._discard_child_scope(child_scope)
                if asyncio.iscoroutine(coro_fn):
                    coro_fn.close()
                raise

            fut.add_done_callback(lambda _f: self._discard_child_scope(child_scope))
            handle = TaskHandle(fut)
            with self._children_lock:
                if self._state not in (_TGState.IDLE, _TGState.ACTIVE, _TGState.DRAINING):
                    child_scope.cancel()
                    raise RuntimeError(
                        "TaskGroup is not active: cannot start_soon() after the group exited"
                    )
                if (
                    pre_cancel
                    or self._cancel_reason != _CancelReason.NONE
                    or self._cancel_scope.cancel_called
                ):
                    self._cancelled_by_group.add(fut)
                    child_scope.cancel()
                self._children.add(handle)
            fut.add_done_callback(self._on_child_done)
            return handle

        task = current_loop.create_task(_run_local_child(child_scope, limiter, coro_fn, args))
        task.add_done_callback(lambda _t: self._discard_child_scope(child_scope))
        handle = TaskHandle(task)
        with self._children_lock:
            if self._state not in (_TGState.IDLE, _TGState.ACTIVE, _TGState.DRAINING):
                child_scope.cancel()
                task.add_done_callback(_retrieve_task_exception)
                raise RuntimeError(
                    "TaskGroup is not active: cannot start_soon() after the group exited"
                )
            if (
                pre_cancel
                or self._cancel_reason != _CancelReason.NONE
                or self._cancel_scope.cancel_called
            ):
                self._cancelled_by_group.add(task)
                child_scope.cancel()
            self._children.add(handle)
        task.add_done_callback(self._on_child_done)
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
        with self._children_lock:
            if self._state not in (_TGState.IDLE, _TGState.ACTIVE, _TGState.DRAINING):
                raise RuntimeError("TaskGroup is not active: cannot start() after the group exited")
            child_scope = CancelScope()
            pre_cancel = (
                self._cancel_reason != _CancelReason.NONE or self._cancel_scope.cancel_called
            )
            if pre_cancel:
                child_scope.cancel()
            self._child_cancel_scopes.add(child_scope)

        task_status = TaskStatus()
        limiter = self._limiter
        child_args = (task_status, *args)

        if self._pool is not None:
            try:
                if limiter is not None:
                    fut: asyncio.Future[Any] = self._pool.submit(
                        _wrap_with_limiter,
                        limiter,
                        coro_fn,
                        child_args,
                        cancel_scope=child_scope,
                    )
                else:
                    fut = self._pool.submit(coro_fn, task_status, *args, cancel_scope=child_scope)
            except BaseException:
                self._discard_child_scope(child_scope)
                raise
        else:
            fut = current_loop.create_task(
                _run_local_child(child_scope, limiter, coro_fn, child_args)
            )

        fut.add_done_callback(lambda _f: self._discard_child_scope(child_scope))
        handle = TaskHandle(fut)
        handle._start_event = task_status._started
        with self._children_lock:
            if self._state not in (_TGState.IDLE, _TGState.ACTIVE, _TGState.DRAINING):
                child_scope.cancel()
                if self._pool is None:
                    fut.add_done_callback(_retrieve_task_exception)
                raise RuntimeError("TaskGroup is not active: cannot start() after the group exited")
            if (
                pre_cancel
                or self._cancel_reason != _CancelReason.NONE
                or self._cancel_scope.cancel_called
            ):
                self._cancelled_by_group.add(fut)
                child_scope.cancel()
            self._starting_futures.add(fut)
            self._children.add(handle)
        fut.add_done_callback(lambda _f: task_status._started.set())
        fut.add_done_callback(self._on_child_done)
        try:
            await task_status._started.wait()
        finally:
            with self._children_lock:
                self._starting_futures.discard(fut)
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
                        self._cancelled_by_group.update(siblings)
                        if self._cancel_reason == _CancelReason.NONE:
                            self._cancel_reason = _CancelReason.CHILD_FAILURE
                        scopes = list(self._child_cancel_scopes)
                    for s in scopes:
                        s.cancel()
                    raise exc
        return handle

    def _cancel_from_context(self) -> None:
        with self._children_lock:
            self._cancel_reason = _CancelReason.CONTEXT_CANCEL
            handles = list(self._children)
            for h in handles:
                self._cancelled_by_group.add(h._task)
            scopes = list(self._child_cancel_scopes)
        for s in scopes:
            s.cancel()
        self._cancel_scope.cancel()

    def cancel_all(self) -> None:
        """Cancel all child tasks safely across threads and event loops via CancelScope."""
        with self._children_lock:
            if self._cancel_reason == _CancelReason.NONE:
                self._cancel_reason = _CancelReason.USER_CANCEL_ALL
            handles = list(self._children)
            for h in handles:
                self._cancelled_by_group.add(h._task)
            scopes = list(self._child_cancel_scopes)
        for s in scopes:
            s.cancel()
        self._cancel_scope.cancel()
