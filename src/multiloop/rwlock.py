"""Asynchronous Read-Write Mutex (AsyncRWMutex) with writer-preference fairness."""

from __future__ import annotations

import asyncio
from contextlib import AbstractAsyncContextManager
from typing import Any

from multiloop._cancel import _await_cancellable, _check_not_cancelled, _task_token
from multiloop._rust import _try_import_rust_class

__all__ = ["AsyncRWMutex"]

_RawAsyncRWMutex = _try_import_rust_class("multiloop._multiloop_core", "RawAsyncRWMutex")


class _ReadLockGuard(AbstractAsyncContextManager[None]):
    """Zero-generator async context guard for :meth:`AsyncRWMutex.reader`."""

    __slots__ = ("_raw", "_task_token")

    def __init__(self, raw: Any) -> None:
        self._raw = raw
        self._task_token: int = 0

    async def __aenter__(self) -> None:
        task = asyncio.current_task()
        if task is None:
            raise RuntimeError("reader() must be used inside an active asyncio task")
        _check_not_cancelled()
        token = _task_token(task)
        if self._raw.try_acquire_read_task(token):
            self._task_token = token
            return

        loop = asyncio.get_running_loop()
        fut: asyncio.Future[None] = loop.create_future()
        ticket_id = self._raw.acquire_read_ticket(token, loop, fut)
        if ticket_id is not None:
            try:
                await _await_cancellable(loop, fut)
            except BaseException:
                self._raw.cancel_read_ticket(token, ticket_id)
                raise
            self._raw.claim_read_ticket(ticket_id)
        self._task_token = token

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        self._raw.release_read_task(self._task_token)


class _WriteLockGuard(AbstractAsyncContextManager[None]):
    """Zero-generator async context guard for :meth:`AsyncRWMutex.writer`."""

    __slots__ = ("_raw", "_task_token")

    def __init__(self, raw: Any) -> None:
        self._raw = raw
        self._task_token: int = 0

    async def __aenter__(self) -> None:
        task = asyncio.current_task()
        if task is None:
            raise RuntimeError("writer() must be used inside an active asyncio task")
        _check_not_cancelled()
        token = _task_token(task)
        if self._raw.try_acquire_write_task(token):
            self._task_token = token
            return

        loop = asyncio.get_running_loop()
        fut: asyncio.Future[None] = loop.create_future()
        ticket_id = self._raw.acquire_write_ticket(token, loop, fut)
        if ticket_id is not None:
            try:
                await _await_cancellable(loop, fut)
            except BaseException:
                self._raw.cancel_write_ticket(token, ticket_id)
                raise
        self._task_token = token

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        self._raw.release_write_task(self._task_token)


class AsyncRWMutex:
    """Read-Write Lock (RWMutex) for asynchronous Python coroutines.

    Allows concurrent shared access for multiple readers while granting exclusive access
    to a single writer. Built upon Rust ``RawAsyncRWMutex`` with writer-preference fairness
    to eliminate writer starvation under heavy read traffic.

    Nesting rules:
    - ``reader()`` is re-entrant for the same task.
    - Upgrades from reader to writer or recursive acquisitions of ``writer()`` raise
      :class:`RuntimeError` immediately to prevent deadlock.
    """

    __slots__ = ("_raw",)

    def __init__(self) -> None:
        if _RawAsyncRWMutex is None:
            raise RuntimeError("_multiloop_core Rust extension is not compiled.")
        self._raw = _RawAsyncRWMutex()

    @property
    def _readers(self) -> int:
        return int(self._raw.snapshot()[0])

    @property
    def _writer(self) -> bool:
        return bool(self._raw.snapshot()[1])

    @property
    def _pending_writers(self) -> int:
        return int(self._raw.snapshot()[2])

    def __repr__(self) -> str:
        readers, writer, pending_writers = self._raw.snapshot()
        return (
            f"<AsyncRWMutex readers={int(readers)} writer={bool(writer)}"
            f" pending_writers={int(pending_writers)}>"
        )

    def reader(self) -> AbstractAsyncContextManager[None]:
        """Return an async context manager that acquires a shared read lock.

        :raises RuntimeError: If called by a task that already holds an active writer lock.
        """
        return _ReadLockGuard(self._raw)

    def writer(self) -> AbstractAsyncContextManager[None]:
        """Return an async context manager that acquires an exclusive write lock.

        :raises RuntimeError: If called by a task holding a reader lock or another writer lock.
        """
        return _WriteLockGuard(self._raw)
