"""Task-local cancellation scopes for multiloop (Rust task cancel stack ``RawTaskCancelState``)."""

from __future__ import annotations

import asyncio
import math
import threading
import weakref
from typing import TYPE_CHECKING, Any, Protocol, Self

from multiloop._rust import _try_import_rust_class
from multiloop.exceptions import TimeoutError

if TYPE_CHECKING:
    from collections.abc import Awaitable

__all__ = [
    "CancelScope",
    "checkpoint",
    "current_effective_deadline",
    "fail_after",
    "fail_at",
    "move_on_after",
    "move_on_at",
    "sleep",
    "wait_cancellable",
]

PLAIN = 0
MOVE_ON = 1
FAIL_AFTER = 2


class _RawTaskCancelStateProtocol(Protocol):
    @property
    def task_token(self) -> int: ...
    def push_scope(
        self,
        deadline: float,
        mode: int,
        shield: bool,
        pre_cancelled: bool,
    ) -> int: ...
    def pop_scope(self, entry_id: int) -> bool: ...
    def pop_scope_status(self, entry_id: int) -> tuple[bool, bool]: ...
    def park_waiter(self, loop_obj: Any, fut_obj: Any) -> int: ...
    def unpark_waiter(self, park_id: int) -> None: ...
    def cancel_scope(self, entry_id: int) -> bool: ...
    def set_shield(self, entry_id: int, shield: bool) -> None: ...
    def set_deadline(self, entry_id: int, deadline: float) -> None: ...
    def is_effectively_cancelled(self) -> bool: ...
    def is_scope_effectively_cancelled(self, entry_id: int) -> bool: ...
    def effective_deadline(self) -> float: ...
    def has_outer_cancellation(self, entry_id: int) -> bool: ...


_RawTaskCancelState: Any = _try_import_rust_class("multiloop._multiloop_core", "RawTaskCancelState")

_TASK_STATE_ATTR = "_multiloop_cancel_state"
_FALLBACK_STATES: weakref.WeakKeyDictionary[asyncio.Task[Any], _RawTaskCancelStateProtocol] = (
    weakref.WeakKeyDictionary()
)
_STATE_LOCK = threading.Lock()


def _get_cancel_state(task: asyncio.Task[Any]) -> _RawTaskCancelStateProtocol:
    """Return or lazily create the Rust cancel state bound to *task*."""
    state: _RawTaskCancelStateProtocol | None = getattr(task, _TASK_STATE_ATTR, None)
    if state is not None:
        return state
    with _STATE_LOCK:
        locked_state: _RawTaskCancelStateProtocol | None = getattr(task, _TASK_STATE_ATTR, None)
        if locked_state is not None:
            return locked_state
        fallback_state = _FALLBACK_STATES.get(task)
        if fallback_state is not None:
            return fallback_state
        if _RawTaskCancelState is None:
            raise RuntimeError("_multiloop_core Rust extension is not compiled.")
        created: _RawTaskCancelStateProtocol = _RawTaskCancelState()
        try:
            setattr(task, _TASK_STATE_ATTR, created)
        except (AttributeError, TypeError):
            _FALLBACK_STATES[task] = created
        return created


def _get_cancel_state_if_any(
    task: asyncio.Task[Any] | None,
) -> _RawTaskCancelStateProtocol | None:
    """Return the Rust cancel state for *task* without allocating one if absent."""
    if task is None:
        return None
    state: _RawTaskCancelStateProtocol | None = getattr(task, _TASK_STATE_ATTR, None)
    if state is not None or not _FALLBACK_STATES:
        return state
    with _STATE_LOCK:
        return _FALLBACK_STATES.get(task)


def _task_token(task: asyncio.Task[Any]) -> int:  # pyright: ignore[reportUnusedFunction]
    """Return the monotonic 64-bit token identifying *task*."""
    return int(_get_cancel_state(task).task_token)


def _is_current_task_cancelled() -> bool:
    """Return True if the currently running task has an effective cancellation."""
    try:
        task = asyncio.current_task()
    except RuntimeError:
        return False
    state = _get_cancel_state_if_any(task)
    return bool(state is not None and state.is_effectively_cancelled())


def _check_not_cancelled() -> None:
    """Raise :class:`asyncio.CancelledError` immediately if the current task is cancelled."""
    if _is_current_task_cancelled():
        raise asyncio.CancelledError()


async def _await_cancellable(
    loop: asyncio.AbstractEventLoop,
    fut: asyncio.Future[Any],
) -> Any:
    """Await *fut* while subscribing to the current task's active cancel stack."""
    task = asyncio.current_task()
    state = _get_cancel_state_if_any(task)
    if state is None:
        return await fut

    park_id = state.park_waiter(loop, fut)
    if park_id == 0:
        raise asyncio.CancelledError()

    try:
        return await fut
    finally:
        state.unpark_waiter(park_id)


def _cancel_handle_on_loop(
    loop: asyncio.AbstractEventLoop | None,
    handle: asyncio.TimerHandle | None,
) -> None:
    """Cancel a TimerHandle safely on its owning event loop thread."""
    if handle is None:
        return
    if loop is None:
        handle.cancel()
        return
    try:
        running = asyncio.get_running_loop()
    except RuntimeError:
        running = None
    if running is loop:
        handle.cancel()
    else:
        try:
            loop.call_soon_threadsafe(handle.cancel)
        except RuntimeError:
            pass


class CancelScope:
    """A task-local, level-triggered cancellation scope backed by Rust RawTaskCancelState."""

    __slots__ = (
        "__weakref__",
        "_cancel_called",
        "_cancelled_caught",
        "_deadline",
        "_deadline_gen",
        "_deadline_handle",
        "_enter_cancelling",
        "_entered",
        "_entry_id",
        "_exited",
        "_lock",
        "_loop",
        "_mode",
        "_shield",
        "_state",
    )

    def __init__(self, deadline: float = float("inf"), shield: bool = False) -> None:
        if not (math.isfinite(deadline) or deadline == float("inf")):
            raise ValueError("deadline must be a finite number or float('inf')")
        self._lock = threading.Lock()
        self._deadline = deadline
        self._deadline_gen = 0
        self._shield = shield
        self._mode = PLAIN
        self._cancel_called = False
        self._cancelled_caught = False
        self._enter_cancelling = 0
        self._entered = False
        self._exited = False
        self._entry_id: int | None = None
        self._state: _RawTaskCancelStateProtocol | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._deadline_handle: asyncio.TimerHandle | None = None

    @property
    def cancel_called(self) -> bool:
        with self._lock:
            return self._cancel_called

    @property
    def cancelled_caught(self) -> bool:
        with self._lock:
            return self._cancelled_caught

    @property
    def deadline(self) -> float:
        with self._lock:
            return self._deadline

    @deadline.setter
    def deadline(self, value: float) -> None:
        if not (math.isfinite(value) or value == float("inf")):
            raise ValueError("deadline must be a finite number or float('inf')")

        with self._lock:
            self._deadline = value
            self._deadline_gen += 1
            gen = self._deadline_gen
            state = self._state
            entry_id = self._entry_id
            loop = self._loop
            old_handle = self._deadline_handle
            self._deadline_handle = None
            exited = self._exited
            cancel_called = self._cancel_called
            if state is not None and entry_id is not None:
                state.set_deadline(entry_id, value)

        if loop is None or exited or cancel_called:
            _cancel_handle_on_loop(loop, old_handle)
            return

        def _reschedule() -> None:
            if old_handle is not None:
                old_handle.cancel()
            expired = False
            with self._lock:
                if (
                    self._exited
                    or self._cancel_called
                    or self._deadline_gen != gen
                    or self._deadline != value
                    or value == float("inf")
                ):
                    return
                if value <= loop.time():
                    expired = True
                else:
                    self._deadline_handle = loop.call_at(value, self._on_deadline_timer, gen)
            if expired:
                self.cancel()

        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            running = None

        if running is loop:
            _reschedule()
        else:
            try:
                loop.call_soon_threadsafe(_reschedule)
            except RuntimeError:
                pass

    @property
    def shield(self) -> bool:
        with self._lock:
            return self._shield

    @shield.setter
    def shield(self, value: bool) -> None:
        flag = bool(value)
        with self._lock:
            if self._shield == flag:
                return
            self._shield = flag
            if self._state is not None and self._entry_id is not None:
                self._state.set_shield(self._entry_id, flag)

    def _effectively_cancelled(self) -> bool:
        with self._lock:
            state = self._state
            entry_id = self._entry_id
            cancel_called = self._cancel_called
        if state is not None and entry_id is not None:
            return bool(state.is_scope_effectively_cancelled(entry_id))
        return cancel_called

    def _on_deadline_timer(self, expected_gen: int | None = None) -> None:
        with self._lock:
            if (
                self._exited
                or self._cancel_called
                or (expected_gen is not None and self._deadline_gen != expected_gen)
                or self._loop is None
                or self._deadline == float("inf")
            ):
                return
            if self._loop.time() < self._deadline:
                if expected_gen is not None:
                    self._deadline_handle = self._loop.call_at(
                        self._deadline, self._on_deadline_timer, expected_gen
                    )
                return
            self._deadline_handle = None
        self.cancel()

    async def __aenter__(self) -> Self:
        task = asyncio.current_task()
        if task is None:
            raise RuntimeError("CancelScope must be used inside an active asyncio task")
        loop = asyncio.get_running_loop()
        state = _get_cancel_state(task)

        with self._lock:
            if self._entered:
                raise RuntimeError("CancelScope cannot be re-entered or shared across tasks")
            self._entered = True
            self._loop = loop
            self._state = state
            self._enter_cancelling = task.cancelling()
            deadline = self._deadline
            if deadline != float("inf") and deadline <= loop.time():
                self._cancel_called = True
            entry_id = state.push_scope(
                deadline,
                self._mode,
                self._shield,
                self._cancel_called,
            )
            self._entry_id = entry_id
            if deadline != float("inf") and not self._cancel_called:
                self._deadline_gen += 1
                self._deadline_handle = loop.call_at(
                    deadline, self._on_deadline_timer, self._deadline_gen
                )
        return self

    def cancel(self) -> None:
        with self._lock:
            if self._cancel_called:
                return
            self._cancel_called = True
            self._deadline_gen += 1
            state = self._state
            entry_id = self._entry_id
            loop = self._loop
            old_handle = self._deadline_handle
            self._deadline_handle = None

        _cancel_handle_on_loop(loop, old_handle)
        if state is not None and entry_id is not None:
            state.cancel_scope(entry_id)

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> bool | None:
        with self._lock:
            self._exited = True
            self._deadline_gen += 1
            handle = self._deadline_handle
            self._deadline_handle = None
            state = self._state
            self._state = None
            entry_id = self._entry_id
            self._entry_id = None
            self._loop = None
            cancel_called = self._cancel_called
            enter_cancelling = self._enter_cancelling

        if handle is not None:
            handle.cancel()

        was_cancelled = False
        outer_cancelled = False
        if state is not None and entry_id is not None:
            was_cancelled, outer_cancelled = state.pop_scope_status(entry_id)

        if exc_type is not None and not issubclass(exc_type, asyncio.CancelledError):
            return False

        task = asyncio.current_task()
        external_cancelled = task is not None and task.cancelling() > enter_cancelling

        if exc_type is not None:
            if external_cancelled or outer_cancelled:
                return False
            if cancel_called or was_cancelled:
                if self._mode == MOVE_ON:
                    with self._lock:
                        self._cancelled_caught = True
                    return True
                if self._mode == FAIL_AFTER:
                    raise TimeoutError("Deadline exceeded") from exc_val
            return False

        if cancel_called or was_cancelled:
            if self._mode == MOVE_ON:
                if external_cancelled or outer_cancelled:
                    return False
                with self._lock:
                    self._cancelled_caught = True
                return True
            if self._mode == FAIL_AFTER:
                if external_cancelled:
                    raise asyncio.CancelledError()
                if outer_cancelled:
                    return False
                raise TimeoutError("Deadline exceeded")
            if self._mode == PLAIN:
                raise asyncio.CancelledError()

        return False


def _set_result_if_pending(fut: asyncio.Future[None]) -> None:
    if not fut.done():
        fut.set_result(None)


async def sleep(delay: float) -> None:
    """Sleep for *delay* seconds while honoring active :class:`CancelScope` cancellation."""
    if math.isnan(delay):
        raise ValueError("delay must not be NaN")
    if delay <= 0:
        await checkpoint()
        return
    _check_not_cancelled()
    loop = asyncio.get_running_loop()
    fut: asyncio.Future[None] = loop.create_future()
    handle = loop.call_later(delay, _set_result_if_pending, fut)
    try:
        await _await_cancellable(loop, fut)
    finally:
        handle.cancel()


async def wait_cancellable[T](awaitable: Awaitable[T]) -> T:
    """Await a foreign asyncio awaitable while honoring active :class:`CancelScope` cancellation."""
    task = asyncio.current_task()
    state = _get_cancel_state_if_any(task)
    if state is None:
        return await awaitable
    if state.is_effectively_cancelled():
        if asyncio.iscoroutine(awaitable):
            awaitable.close()
        raise asyncio.CancelledError()

    loop = asyncio.get_running_loop()
    child: asyncio.Future[T] = asyncio.ensure_future(awaitable, loop=loop)
    fut: asyncio.Future[T] = loop.create_future()

    def _on_child_done(done_fut: asyncio.Future[T]) -> None:
        if fut.done():
            if not done_fut.cancelled():
                done_fut.exception()
            return
        if done_fut.cancelled():
            fut.cancel()
            return
        exc = done_fut.exception()
        if exc is not None:
            fut.set_exception(exc)
        else:
            fut.set_result(done_fut.result())

    child.add_done_callback(_on_child_done)
    try:
        res: T = await _await_cancellable(loop, fut)
        return res
    except BaseException:
        if not child.done():
            child.cancel()
            try:
                await child
            except BaseException:
                pass
        elif not child.cancelled():
            child.exception()
        raise


def fail_after(seconds: float) -> CancelScope:
    """Return a :class:`CancelScope` that raises :class:`~multiloop.exceptions.TimeoutError` on expiry."""
    loop = asyncio.get_running_loop()
    scope = CancelScope(deadline=loop.time() + seconds)
    scope._mode = FAIL_AFTER
    return scope


def move_on_after(seconds: float) -> CancelScope:
    """Return a :class:`CancelScope` that quietly absorbs cancellation and exits on expiry."""
    loop = asyncio.get_running_loop()
    scope = CancelScope(deadline=loop.time() + seconds)
    scope._mode = MOVE_ON
    return scope


def fail_at(deadline: float) -> CancelScope:
    """Return a :class:`CancelScope` with an absolute monotonic deadline raising :class:`~multiloop.exceptions.TimeoutError`."""
    scope = CancelScope(deadline=deadline)
    scope._mode = FAIL_AFTER
    return scope


def move_on_at(deadline: float) -> CancelScope:
    """Return a :class:`CancelScope` with an absolute monotonic deadline quietly absorbing cancellation."""
    scope = CancelScope(deadline=deadline)
    scope._mode = MOVE_ON
    return scope


async def checkpoint() -> None:
    """Yield control and raise :class:`asyncio.CancelledError` if the current task is cancelled."""
    _check_not_cancelled()
    await asyncio.sleep(0)
    _check_not_cancelled()


def current_effective_deadline() -> float:
    """Return the tightest active monotonic deadline for the current task, or ``float('inf')``."""
    try:
        task = asyncio.current_task()
    except RuntimeError:
        return float("inf")
    state = _get_cancel_state_if_any(task)
    if state is not None:
        return float(state.effective_deadline())
    return float("inf")
