"""Cross-thread task context and cancellation manager (Go context.Context style)."""

from __future__ import annotations

import asyncio
import threading
import weakref
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Self

from multiloop._cancel import CancelScope
from multiloop._taskgroup import TaskGroup
from multiloop.pool import EventLoopThreadPool

if TYPE_CHECKING:
    from multiloop._sync import CapacityLimiter

__all__ = ["AsyncContext"]


class AsyncContext:
    """Hierarchical cross-thread cancellation context inspired by Go's ``context.Context``.

    Enables structured, cascading task cancellation across independent OS threads and isolated
    asyncio event loops. Supports explicit programmatic cancellation (:meth:`cancel`) as well
    as automatic cancellation upon exiting an ``async with`` block.

    :param parent: Optional parent context. If provided, cancelling the parent automatically
                   cascades cancellation to this child context.
    """

    __slots__ = (
        "__weakref__",
        "_cancelled",
        "_children",
        "_lock",
        "_parent",
        "_scopes",
        "_task_groups",
    )

    def __init__(self, parent: AsyncContext | None = None) -> None:
        self._parent = parent
        self._cancelled = False
        self._lock = threading.Lock()
        self._children: set[AsyncContext] = set()
        self._scopes: dict[asyncio.Future[Any], CancelScope] = {}
        self._task_groups: weakref.WeakSet[TaskGroup] = weakref.WeakSet()

        if parent is not None:
            parent._add_child(self)

    def __repr__(self) -> str:
        return f"<AsyncContext cancelled={self.is_cancelled}>"

    @property
    def parent(self) -> AsyncContext | None:
        """Return the parent context, or ``None`` for a root context (read-only)."""
        return self._parent

    def _add_child(self, child: AsyncContext) -> None:
        cancelled = False
        with self._lock:
            if self._cancelled:
                cancelled = True
            else:
                self._children.add(child)
        if cancelled:
            child.cancel()

    def _remove_child(self, child: AsyncContext) -> None:
        """Remove a child context from the registered set to prevent memory retention."""
        with self._lock:
            self._children.discard(child)

    @property
    def is_cancelled(self) -> bool:
        """Return True if this context has been cancelled."""
        with self._lock:
            return self._cancelled

    def submit(
        self,
        pool: EventLoopThreadPool,
        target: Callable[..., Any] | Any,
        *args: Any,
        **kwargs: Any,
    ) -> asyncio.Future[Any]:
        """Submit a task to a thread pool bound to this cancellation context.

        :param pool: The :class:`~multiloop.EventLoopThreadPool` instance to execute the task.
        :param target: The target coroutine function or callable task.
        :param args: Positional arguments to forward to `target`.
        :param kwargs: Keyword arguments to forward to `target`.
        :returns: An :class:`asyncio.Future` tracking the task's execution.
        """
        with self._lock:
            if self._cancelled:
                if asyncio.iscoroutine(target):
                    target.close()
                caller_loop = asyncio.get_running_loop()
                fut: asyncio.Future[Any] = caller_loop.create_future()
                fut.cancel()
                return fut
            scope = CancelScope()

        fut_res: asyncio.Future[Any] = pool.submit(target, *args, cancel_scope=scope, **kwargs)

        cancel_now = False
        with self._lock:
            if self._cancelled:
                cancel_now = True
            elif not fut_res.done():
                self._scopes[fut_res] = scope
                fut_res.add_done_callback(self._discard_future)

        if cancel_now:
            scope.cancel()

        return fut_res

    def _discard_future(self, fut: asyncio.Future[Any]) -> None:
        """Remove a completed future's CancelScope from tracking."""
        with self._lock:
            self._scopes.pop(fut, None)

    def cancel(self) -> None:
        """Cancel this context and thread-safely propagate cancellation via CancelScopes."""
        with self._lock:
            if self._cancelled:
                return
            self._cancelled = True
            children = list(self._children)
            scopes = list(self._scopes.values())
            task_groups = list(self._task_groups)
            self._children.clear()
            self._scopes.clear()
            self._task_groups.clear()

        # Detach from parent to prevent reference cycle leaks
        if self._parent is not None:
            self._parent._remove_child(self)

        for tg in task_groups:
            tg._cancel_from_context()

        for child in children:
            child.cancel()

        for scope in scopes:
            scope.cancel()

    def task_group(
        self,
        pool: EventLoopThreadPool | None = None,
        name: str | None = None,
        max_concurrency: int | None = None,
        limiter: CapacityLimiter | None = None,
    ) -> TaskGroup:
        """Create a :class:`~multiloop.TaskGroup` integrated with this cancellation context.

        The created task group is registered with this context. If this context is cancelled
        (either explicitly via :meth:`cancel` or implicitly by exiting an ``async with ctx:``
        block), cancellation automatically cascades across thread boundaries to all child tasks
        and scopes managed by the task group.

        :param pool: Optional :class:`~multiloop.EventLoopThreadPool` to distribute spawned tasks
                     across worker event loops with structured concurrency guarantees.
        :param name: Optional identifier for the task group.
        :param max_concurrency: Optional positive integer constraining maximum concurrent tasks.
        :param limiter: Optional external :class:`~multiloop.CapacityLimiter` to share concurrency
                        budgets across multiple task groups.
        :returns: A :class:`~multiloop.TaskGroup` instance linked to this context.
        """
        tg = TaskGroup(name=name, max_concurrency=max_concurrency, limiter=limiter, pool=pool)
        with self._lock:
            if self._cancelled:
                tg._cancel_from_context()
            else:
                self._task_groups.add(tg)
        return tg

    async def __aenter__(self) -> Self:
        """Enter the async context manager."""
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        """Exit the async context manager and cancel the context."""
        self.cancel()
