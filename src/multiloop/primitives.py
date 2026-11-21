"""Go-style concurrency primitives and high-performance channels for multiloop.

Exposes :class:`Channel`, :func:`select_channel`, :class:`AsyncWaitGroup`,
and :class:`AsyncOnce`.
"""

from __future__ import annotations

import asyncio
import copy
import threading
import time
import warnings
import weakref
from collections.abc import AsyncIterator, Callable, Coroutine, Generator
from contextlib import AbstractAsyncContextManager
from typing import Any, Protocol, Self

from multiloop._cancel import _await_cancellable, _check_not_cancelled, fail_after
from multiloop._rust import _try_import_rust_class
from multiloop.exceptions import ChannelClosedError, TimeoutError, WouldBlock

_CHANNEL_CLOSED_MSG = "Channel is closed"

__all__ = [
    "AsyncOnce",
    "AsyncWaitGroup",
    "Channel",
    "ReceiveChannel",
    "SendChannel",
    "select_channel",
]

_select_tls = threading.local()


def _next_select_index(num_channels: int) -> int:
    idx: int = getattr(_select_tls, "counter", 0)
    _select_tls.counter = idx + 1
    return idx % num_channels


class _RawAsyncChannelProtocol(Protocol):
    """Protocol for the native Rust RawAsyncChannel class."""

    def __init__(self, maxsize: int = ...) -> None: ...
    def close(self) -> None: ...
    def is_closed(self) -> bool: ...
    def is_drained(self) -> bool: ...
    def qsize(self) -> int: ...
    @property
    def maxsize(self) -> int: ...
    def empty(self) -> bool: ...
    def full(self) -> bool: ...
    def try_send(self, item: Any) -> bool: ...
    def send_or_register(self, item: Any, loop: Any, fut: Any) -> int | None: ...
    def try_recv(self) -> Any: ...
    def register_getter_ticket(self, loop: Any, fut: Any) -> tuple[int, Any]: ...
    def claim_get(self, waiter_id: int) -> tuple[bool, Any]: ...
    def cancel_getter(self, waiter_id: int) -> bool: ...
    def register_putter_front(self, loop: Any, fut: Any) -> int: ...
    def cancel_putter(self, waiter_id: int) -> bool: ...
    def claim_put(self, item: Any, waiter_id: int) -> bool: ...
    def register_select_watcher_ticket(
        self, loop: Any, arbiter_fut: Any, channel_weakref: weakref.ReferenceType[Any]
    ) -> int: ...
    def cancel_select_watcher(self, waiter_id: int) -> bool: ...
    def forward_select_wakeup(self) -> None: ...
    def send_sync(self, item: Any, timeout: float | None = ...) -> None: ...
    def recv_sync(self, timeout: float | None = ...) -> Any: ...


class _WaitGroupProtocol(Protocol):
    """Protocol for the native Rust RawAsyncWaitGroup class."""

    def __init__(self) -> None: ...
    def add(self, delta: int) -> int: ...
    def done(self) -> int: ...
    def is_zero(self) -> bool: ...
    def waiter_count(self) -> int: ...
    def register_waiter_ticket(self, loop: Any, fut: Any) -> int | None: ...
    def cancel_waiter_ticket(self, ticket_id: int) -> bool: ...


_RustRawAsyncChannel: type[_RawAsyncChannelProtocol] | None = _try_import_rust_class(
    "multiloop._multiloop_core", "RawAsyncChannel"
)
_RawAsyncWaitGroup: type[_WaitGroupProtocol] | None = _try_import_rust_class(
    "multiloop._multiloop_core", "RawAsyncWaitGroup"
)


class _LenShim:
    """Helper enabling len() inspection on Rust-backed waiter queues."""

    __slots__ = ("_len_fn",)

    def __init__(self, len_fn: Callable[[], int]) -> None:
        self._len_fn = len_fn

    def __len__(self) -> int:
        return int(self._len_fn())


class Channel:
    """High-performance cross-thread channel backed by native Rust state machines.

    Eliminates Python-level lock contention by delegating buffer queueing, FIFO waiting
    state machines, double-checked locking, and cross-thread wakeups directly to the Rust core.

    :param maxsize: Maximum buffer capacity. 0 indicates an unbounded channel.
    """

    __slots__ = ("__weakref__", "_inner", "_maxsize")

    def __init__(self, maxsize: int = 0) -> None:
        self._maxsize = max(0, int(maxsize))
        if _RustRawAsyncChannel is None:
            raise RuntimeError("_multiloop_core Rust extension is not compiled.")
        self._inner: Any = _RustRawAsyncChannel(self._maxsize)

    def close(self) -> None:
        """Close the channel, waking all pending senders and receivers with ChannelClosedError."""
        self._inner.close()

    def __repr__(self) -> str:
        return f"<Channel is_closed={self.is_closed}>"

    @property
    def is_closed(self) -> bool:
        """Return True if the channel is closed."""
        return bool(self._inner.is_closed())

    def _is_drained(self) -> bool:
        return bool(self._inner.is_drained())

    def try_send(self, item: Any) -> bool:
        """Non-blocking send. Return True if the item was enqueued, False if full.

        :param item: The item to enqueue into the channel.
        :returns: ``True`` if sent immediately, ``False`` if the buffer was full.
        :raises ChannelClosedError: If the channel is closed.
        """
        return bool(self._inner.try_send(item))

    def try_recv(self) -> Any:
        """Non-blocking receive. Return the item or raise WouldBlock.

        :returns: The dequeued item.
        :raises WouldBlock: If the channel is empty.
        :raises ChannelClosedError: If the channel is closed and completely drained.
        """
        return self._inner.try_recv()

    def qsize(self) -> int:
        """Return the current number of buffered items."""
        return int(self._inner.qsize())

    @property
    def maxsize(self) -> int:
        """Maximum allowed items in the channel buffer (0 = unbounded)."""
        return self._maxsize

    def empty(self) -> bool:
        """Return True if the channel buffer is currently empty."""
        return bool(self._inner.empty())

    def full(self) -> bool:
        """Return True if the channel buffer is currently full."""
        return bool(self._inner.full())

    async def send(self, item: Any, timeout: float | None = None) -> None:
        """Send an item, suspending if the buffer is full until capacity is available.

        :param item: The item to send.
        :param timeout: Optional maximum seconds to wait before raising TimeoutError.
        :raises ChannelClosedError: If the channel is closed.
        :raises TimeoutError: If timeout expires before buffer space becomes available.
        """
        _check_not_cancelled()
        if self._inner.try_send(item):
            return
        if timeout is not None:
            if timeout <= 0:
                raise TimeoutError("Channel send timed out")
            async with fail_after(timeout):
                await self._send_impl(item)
        else:
            await self._send_impl(item)

    async def _send_impl(self, item: Any) -> None:
        _check_not_cancelled()
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[Any] = loop.create_future()
        waiter_id = self._inner.send_or_register(item, loop, fut)
        if waiter_id is None:
            return

        while True:
            try:
                await _await_cancellable(loop, fut)
            except BaseException:
                self._inner.cancel_putter(waiter_id)
                raise

            if self._inner.claim_put(item, waiter_id):
                return

            fut = loop.create_future()
            waiter_id = self._inner.register_putter_front(loop, fut)

    def send_sync(self, item: Any, timeout: float | None = None) -> None:
        """Synchronously send an item into the channel from a worker or background OS thread.

        :param item: The item to send.
        :param timeout: Maximum seconds to block before raising TimeoutError.
        :raises TimeoutError: If timeout expired before buffer space became available.
        :raises ChannelClosedError: If the channel is closed.
        """
        self._inner.send_sync(item, timeout)

    async def recv(self, timeout: float | None = None) -> Any:
        """Receive an item, suspending until one is available or the channel is closed.

        :param timeout: Optional maximum seconds to wait before raising TimeoutError.
        :returns: The received item.
        :raises ChannelClosedError: If the channel is closed and empty.
        :raises TimeoutError: If timeout expires before an item is received.
        """
        _check_not_cancelled()
        try:
            return self._inner.try_recv()
        except WouldBlock:
            pass
        if timeout is not None:
            if timeout <= 0:
                raise TimeoutError("Channel recv timed out")
            async with fail_after(timeout):
                return await self._recv_impl()
        return await self._recv_impl()

    def recv_sync(self, timeout: float | None = None) -> Any:
        """Synchronously receive an item from the channel from a worker or background OS thread.

        :param timeout: Maximum seconds to block before raising TimeoutError.
        :returns: The dequeued item.
        :raises TimeoutError: If timeout expired before an item became available.
        :raises ChannelClosedError: If the channel is closed and drained.
        """
        return self._inner.recv_sync(timeout)

    async def _recv_impl(self) -> Any:
        _check_not_cancelled()
        loop = asyncio.get_running_loop()
        while True:
            try:
                return self._inner.try_recv()
            except WouldBlock:
                pass

            fut: asyncio.Future[Any] = loop.create_future()
            waiter_id, item = self._inner.register_getter_ticket(loop, fut)
            if waiter_id == 0:
                return item

            try:
                await _await_cancellable(loop, fut)
            except BaseException:
                self._inner.cancel_getter(waiter_id)
                raise

            has_item, claimed_item = self._inner.claim_get(waiter_id)
            if has_item:
                return claimed_item

    def _register_select_watcher(
        self, loop: asyncio.AbstractEventLoop, arbiter_fut: asyncio.Future[Any]
    ) -> int | None:
        waiter_id: int = self._inner.register_select_watcher_ticket(
            loop, arbiter_fut, weakref.ref(self)
        )
        return waiter_id if waiter_id > 0 else None

    def _unregister_select_watcher(self, token: Any) -> None:
        if token is not None:
            self._inner.cancel_select_watcher(token)

    def _forward_select_wakeup(self) -> None:
        self._inner.forward_select_wakeup()

    @property
    def _getters(self) -> Any:
        return _LenShim(self._inner.getters_len)

    @property
    def _putters(self) -> Any:
        return _LenShim(self._inner.putters_len)

    @property
    def _notifiers(self) -> Any:
        return _LenShim(self._inner.notifiers_len)

    def __aiter__(self) -> AsyncIterator[Any]:
        return self

    async def __anext__(self) -> Any:
        try:
            return await self.recv()
        except ChannelClosedError:
            raise StopAsyncIteration from None

    def split(self) -> tuple[SendChannel, ReceiveChannel]:
        """Split this channel into a (SendChannel, ReceiveChannel) pair for interface segregation."""
        return SendChannel(self), ReceiveChannel(self)


class SendChannel:
    """A send-only view of a :class:`Channel` for interface segregation (Go ``chan<- T`` style)."""

    __slots__ = ("_ch",)

    def __init__(self, ch: Channel) -> None:
        self._ch = ch

    def __repr__(self) -> str:
        return f"<SendChannel is_closed={self.is_closed}>"

    async def send(self, item: Any, timeout: float | None = None) -> None:
        """Send an item into the channel."""
        await self._ch.send(item, timeout=timeout)

    def send_sync(self, item: Any, timeout: float | None = None) -> None:
        """Synchronous blocking send."""
        self._ch.send_sync(item, timeout=timeout)

    def try_send(self, item: Any) -> bool:
        """Non-blocking send."""
        return self._ch.try_send(item)

    def close(self) -> None:
        """Close the underlying channel."""
        self._ch.close()

    @property
    def is_closed(self) -> bool:
        """Return True if the underlying channel is closed."""
        return self._ch.is_closed

    @property
    def maxsize(self) -> int:
        """Maximum capacity of the underlying channel."""
        return self._ch.maxsize

    def full(self) -> bool:
        """Return True if the underlying channel buffer is full."""
        return self._ch.full()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        self.close()


class ReceiveChannel:
    """A receive-only view of a :class:`Channel` for interface segregation (Go ``<-chan T`` style)."""

    __slots__ = ("__weakref__", "_ch")

    def __init__(self, ch: Channel) -> None:
        self._ch = ch

    def __repr__(self) -> str:
        return f"<ReceiveChannel is_closed={self.is_closed}>"

    async def recv(self, timeout: float | None = None) -> Any:
        """Receive an item from the channel."""
        return await self._ch.recv(timeout=timeout)

    def recv_sync(self, timeout: float | None = None) -> Any:
        """Synchronously receive an item from the channel from a worker or background OS thread.

        :param timeout: Maximum seconds to block before raising TimeoutError.
        :returns: The dequeued item.
        :raises TimeoutError: If timeout expired before an item became available.
        :raises ChannelClosedError: If the channel is closed and drained.
        """
        return self._ch.recv_sync(timeout=timeout)

    def try_recv(self) -> Any:
        """Non-blocking receive."""
        return self._ch.try_recv()

    def close(self) -> None:
        """Close the underlying channel."""
        self._ch.close()

    @property
    def is_closed(self) -> bool:
        """Return True if the underlying channel is closed."""
        return self._ch.is_closed

    def empty(self) -> bool:
        """Return True if the underlying channel is empty."""
        return self._ch.empty()

    def qsize(self) -> int:
        """Number of items currently buffered in the underlying channel."""
        return self._ch.qsize()

    def _is_drained(self) -> bool:
        return self._ch._is_drained()

    def _register_select_watcher(
        self, loop: asyncio.AbstractEventLoop, arbiter_fut: asyncio.Future[Any]
    ) -> int | None:
        waiter_id: int = self._ch._inner.register_select_watcher_ticket(
            loop, arbiter_fut, weakref.ref(self)
        )
        return waiter_id if waiter_id > 0 else None

    def _unregister_select_watcher(self, token: Any) -> None:
        if token is not None:
            self._ch._inner.cancel_select_watcher(token)

    def _forward_select_wakeup(self) -> None:
        self._ch._inner.forward_select_wakeup()

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> Any:
        try:
            return await self._ch.recv()
        except ChannelClosedError:
            raise StopAsyncIteration from None

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        self.close()


_UNSET: Any = object()


async def select_channel(
    *channels: Any,
    timeout: float | None = None,
    default: Any = _UNSET,
    _deadline: float | None = None,
) -> Any:
    """Select and receive from the first ready channel among multiple channels (Go select style).

    :param channels: One or more :class:`Channel` instances to monitor.
    :param timeout: Maximum seconds to wait.
    :param default: Optional non-blocking default fallback value.
    :param _deadline: Internal monotonic deadline timestamp for recursive iterations.
    :returns: A tuple ``(channel, received_value)``, or ``default`` if non-blocking and empty.
    :raises ValueError: If no channels are passed.
    :raises TimeoutError: If the operation times out.
    :raises ChannelClosedError: If all monitored channels are closed and drained.
    """
    if not channels:
        raise ValueError("select_channel requires at least one channel")

    _check_not_cancelled()
    loop = asyncio.get_running_loop()
    if timeout is not None and _deadline is None:
        _deadline = loop.time() + timeout

    num_channels = len(channels)
    while True:
        # Phase 1: Fast Probe (try_recv) with round-robin start index for anti-barging fairness
        start_idx = _next_select_index(num_channels) if num_channels > 1 else 0
        for i in range(num_channels):
            ch = channels[(start_idx + i) % num_channels]
            try:
                val = ch.try_recv()
                return ch, val
            except (ChannelClosedError, WouldBlock):
                continue

        if default is not _UNSET:
            return default

        if all(ch._is_drained() for ch in channels):
            raise ChannelClosedError(_CHANNEL_CLOSED_MSG)

        remaining_timeout: float | None = None
        if _deadline is not None:
            remaining_timeout = max(0.0, _deadline - loop.time())
            if remaining_timeout <= 0:
                raise TimeoutError("select_channel timed out")
        elif timeout is not None and timeout <= 0:
            raise TimeoutError("select_channel timed out")

        # Phase 2: Single-Arbiter Registration
        arbiter_fut: asyncio.Future[Any] = loop.create_future()
        registered_channels: list[tuple[Any, Any]] = []
        consumed_arbiter = False

        try:
            start_reg = (
                (time.perf_counter_ns() ^ id(loop)) % num_channels if num_channels > 1 else 0
            )
            stolen = False
            for i in range(num_channels):
                ch = channels[(start_reg + i) % num_channels]
                token = ch._register_select_watcher(loop, arbiter_fut)
                if token is not None:
                    registered_channels.append((ch, token))
                elif not arbiter_fut.done():
                    try:
                        return ch, ch.try_recv()
                    except WouldBlock:
                        stolen = True
                        break
                    except ChannelClosedError:
                        pass

            if stolen:
                continue

            if all(ch._is_drained() for ch in channels):
                raise ChannelClosedError(_CHANNEL_CLOSED_MSG)

            if remaining_timeout is not None:
                try:
                    async with fail_after(remaining_timeout):
                        ready_ch = await _await_cancellable(loop, arbiter_fut)
                except TimeoutError:
                    raise TimeoutError("select_channel timed out") from None
            else:
                ready_ch = await _await_cancellable(loop, arbiter_fut)

            consumed_arbiter = True
            try:
                return ready_ch, ready_ch.try_recv()
            except (WouldBlock, ChannelClosedError):
                continue
        finally:
            for ch, token in registered_channels:
                ch._unregister_select_watcher(token)
            if not consumed_arbiter:
                if (
                    arbiter_fut.done()
                    and not arbiter_fut.cancelled()
                    and arbiter_fut.exception() is None
                ):
                    unconsumed_ch = arbiter_fut.result()
                    if hasattr(unconsumed_ch, "_forward_select_wakeup"):
                        unconsumed_ch._forward_select_wakeup()
                elif not arbiter_fut.done():
                    arbiter_fut.cancel()


class _TrackedCoroutine(Coroutine[Any, Any, Any]):
    """Coroutine wrapper for AsyncWaitGroup.track that guards against counter leaks."""

    __slots__ = ("_gen", "_stepped", "_target", "_wg")

    def __init__(self, target: Any, wg: AsyncWaitGroup) -> None:
        self._target = target
        self._wg = wg
        self._stepped = False
        self._gen: Generator[Any, None, Any] | None = None

    def _ensure_gen(self) -> Generator[Any, None, Any]:
        if self._gen is None:
            self._gen = self._run().__await__()
        return self._gen

    async def _run(self) -> Any:
        self._stepped = True
        try:
            if asyncio.iscoroutine(self._target) or asyncio.isfuture(self._target):
                return await self._target
            elif callable(self._target):
                res = self._target()
                if asyncio.iscoroutine(res) or asyncio.isfuture(res):
                    return await res
                return res
            return self._target
        finally:
            self._wg.done()

    def send(self, value: Any) -> Any:
        self._stepped = True
        return self._ensure_gen().send(value)

    def throw(self, *args: Any, **kwargs: Any) -> Any:
        self._stepped = True
        return self._ensure_gen().throw(*args, **kwargs)

    def close(self) -> None:
        if not self._stepped:
            self._stepped = True
            try:
                self._wg.done()
            except Exception:
                pass
        if self._gen is not None:
            self._gen.close()
        if asyncio.iscoroutine(self._target):
            self._target.close()

    def __await__(self) -> Generator[Any, None, Any]:
        return self._ensure_gen()

    def __del__(self) -> None:
        # Under Python 3.14t (free-threaded), __del__ runs in an arbitrary GC finalizer thread.
        # Calling self._wg.done() here would risk unpredictable cross-thread re-entrancy and
        # deadlock under PyO3 lock acquisitions. Instead, emit RuntimeWarning to report the leak.
        if not self._stepped:
            self._stepped = True
            warnings.warn(
                "Tracked coroutine was never awaited; AsyncWaitGroup counter leaked",
                RuntimeWarning,
                stacklevel=2,
            )
            if self._gen is not None:
                self._gen.close()
            if hasattr(self, "_target") and asyncio.iscoroutine(self._target):
                self._target.close()


class _WaitGroupHoldingGuard(AbstractAsyncContextManager[None]):
    """Zero-generator RAII async context guard for :meth:`AsyncWaitGroup.holding`."""

    __slots__ = ("_wg",)

    def __init__(self, wg: AsyncWaitGroup) -> None:
        self._wg = wg

    async def __aenter__(self) -> None:
        self._wg.add(1)

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        self._wg.done()


class AsyncWaitGroup:
    """Cross-thread WaitGroup for coordinating multiple asynchronous tasks (Go sync.WaitGroup style).

    Coordinates a collection of parallel tasks across one or multiple event-loop threads.
    """

    __slots__ = ("_inner",)

    def __init__(self) -> None:
        if _RawAsyncWaitGroup is None:
            raise RuntimeError("_multiloop_core Rust extension is not compiled.")
        self._inner = _RawAsyncWaitGroup()

    def __repr__(self) -> str:
        return "<AsyncWaitGroup>"

    def add(self, delta: int = 1) -> None:
        """Adjust the internal task counter by ``delta``.

        :param delta: Integer delta to add (can be negative, but counter must not drop below zero).
        :raises RuntimeError: If delta would drive the counter below zero.
        """
        self._inner.add(delta)

    def done(self) -> None:
        """Decrement the task counter by 1. Wakes all waiters if the counter reaches 0.

        :raises RuntimeError: If done() is called when the counter is already zero.
        """
        self._inner.done()

    def holding(self) -> AbstractAsyncContextManager[None]:
        """RAII async context manager that increments counter on enter and decrements on exit."""
        return _WaitGroupHoldingGuard(self)

    def wrap(self, fn: Callable[..., Any]) -> Callable[..., Any]:
        """Wrap a callable to automatically track execution inside the waitgroup.

        :param fn: Synchronous or asynchronous callable to wrap.
        :returns: Wrapped callable that decrements counter upon completion.
        :raises TypeError: If fn is not callable.
        """
        if not callable(fn):
            raise TypeError(f"Expected callable, got {type(fn).__name__}")

        async def _wrapped(*args: Any, **kwargs: Any) -> Any:
            self.add(1)
            try:
                res = fn(*args, **kwargs)
                if asyncio.iscoroutine(res) or asyncio.isfuture(res):
                    return await res
                return res
            finally:
                self.done()

        return _wrapped

    def track(self, coro: Any) -> Any:
        """Wrap a coroutine or callable with leak-safe happens-before tracking.

        :param coro: Coroutine object or callable to track.
        :returns: Tracked coroutine object that decrements counter upon completion.
        :raises TypeError: If coro is not a coroutine, Future, or callable.
        """
        if not (asyncio.iscoroutine(coro) or asyncio.isfuture(coro) or callable(coro)):
            raise TypeError(f"Expected coroutine, Future, or callable, got {type(coro).__name__}")
        self.add(1)
        return _TrackedCoroutine(coro, self)

    async def wait(self) -> None:
        """Suspend execution until the counter reaches zero."""
        _check_not_cancelled()
        if self._inner.is_zero():
            return
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[Any] = loop.create_future()
        ticket_id = self._inner.register_waiter_ticket(loop, fut)
        if ticket_id is None:
            return
        try:
            await _await_cancellable(loop, fut)
        except BaseException:
            self._inner.cancel_waiter_ticket(ticket_id)
            raise


_RawAsyncOnce = _try_import_rust_class("multiloop._multiloop_core", "RawAsyncOnce")


class AsyncOnce:
    """Thread-safe single execution primitive across multiple event loops (Go sync.Once style).

    Guarantees that an initialization routine runs exactly once, even when invoked concurrently
    by many coroutines across different OS threads.
    """

    __slots__ = ("_raw",)

    def __init__(self) -> None:
        if _RawAsyncOnce is None:
            raise RuntimeError("_multiloop_core Rust extension is not compiled.")
        self._raw = _RawAsyncOnce()

    @property
    def _done(self) -> bool:
        return bool(self._raw.is_done())

    def __repr__(self) -> str:
        return f"<AsyncOnce done={self._done}>"

    async def do(self, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """Execute `fn` if and only if it has never been executed before.

        :param fn: Target callable or coroutine function.
        :param args: Positional arguments passed to `fn`.
        :param kwargs: Keyword arguments passed to `fn`.
        :returns: The result returned by `fn`.
        """
        _check_not_cancelled()
        completed = self._raw.try_get_completed()
        if completed is not None:
            is_exc, val_or_exc = completed
            if is_exc:
                raise copy.copy(val_or_exc)
            return val_or_exc

        loop = asyncio.get_running_loop()
        fut: asyncio.Future[Any] = loop.create_future()
        status, ticket_id, val_or_exc = self._raw.enter_or_register(loop, fut)

        if status == 2:
            return val_or_exc
        if status == 3:
            raise copy.copy(val_or_exc)

        if status == 1:
            assert ticket_id is not None
            try:
                await _await_cancellable(loop, fut)
            except BaseException:
                self._raw.cancel_waiter(ticket_id)
                raise
            has_exc, out_val = self._raw.outcome()
            if has_exc:
                raise copy.copy(out_val)
            return out_val

        res: Any = None
        exc_to_store: BaseException | None = None
        try:
            res = fn(*args, **kwargs)
            if asyncio.iscoroutine(res) or asyncio.isfuture(res):
                res = await res
        except BaseException as e:
            if isinstance(e, asyncio.CancelledError):
                wrapped = RuntimeError("AsyncOnce execution was cancelled")
                wrapped.__cause__ = e
                exc_to_store = wrapped
            else:
                exc_to_store = e
            raise
        finally:
            self._raw.complete(res, exc_to_store)

        return res
