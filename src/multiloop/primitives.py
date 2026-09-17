"""Go-style concurrency primitives and high-performance channels for multiloop.

Exposes :class:`Channel`, :func:`select_channel`, :class:`AsyncWaitGroup`,
and :class:`AsyncOnce`.
"""

from __future__ import annotations

import asyncio
import builtins
import collections
import threading
import time
import warnings
from collections.abc import AsyncGenerator, AsyncIterator, Callable, Coroutine, Generator
from contextlib import asynccontextmanager
from typing import Any, Protocol, Self

from multiloop._channel_base import _CHANNEL_CLOSED_MSG, _discard_waiter, _wake_all
from multiloop._rust import _try_import_rust_class
from multiloop.exceptions import ChannelClosedError, TimeoutError, WouldBlock

__all__ = [
    "AsyncOnce",
    "AsyncWaitGroup",
    "Channel",
    "ReceiveChannel",
    "SendChannel",
    "select_channel",
]


class _RawAsyncChannelProtocol(Protocol):
    """Protocol for the native Rust RawAsyncChannel class."""

    def __init__(
        self,
        maxsize: int = ...,
        wake_fn: Any = ...,
        select_wake_fn: Any = ...,
    ) -> None: ...
    def close(self) -> None: ...
    def is_closed(self) -> bool: ...
    def qsize(self) -> int: ...
    @property
    def maxsize(self) -> int: ...
    def empty(self) -> bool: ...
    def full(self) -> bool: ...
    def try_send(self, item: Any) -> bool: ...
    def try_recv(self) -> tuple[bool, Any]: ...
    def register_getter(self, loop: Any, fut: Any) -> tuple[bool, Any]: ...
    def unregister_getter(self, fut: Any) -> bool: ...
    def register_putter(self, loop: Any, fut: Any) -> bool: ...
    def unregister_putter(self, fut: Any) -> bool: ...
    def claim_put(self, item: Any) -> bool: ...
    def register_select_watcher(self, loop: Any, arbiter_fut: Any, channel_token: Any) -> bool: ...
    def unregister_select_watcher(self, fut: Any) -> bool: ...
    def forward_select_wakeup(self) -> None: ...
    def send_sync(self, item: Any, timeout: float | None = ...) -> None: ...
    def recv_sync(self, timeout: float | None = ...) -> Any: ...


class _WaitGroupProtocol(Protocol):
    """Protocol for the native Rust RawAsyncWaitGroup class."""

    def __init__(self) -> None: ...
    def add(self, delta: int) -> Any: ...
    def done(self) -> Any: ...
    def register_waiter(self, waiter: Any) -> bool: ...
    def unregister_waiter(self, fut: Any) -> bool: ...


_RustRawAsyncChannel: type[_RawAsyncChannelProtocol] | None = _try_import_rust_class(
    "multiloop._multiloop_core", "RawAsyncChannel"
)
_RawAsyncWaitGroup: type[_WaitGroupProtocol] | None = _try_import_rust_class(
    "multiloop._multiloop_core", "RawAsyncWaitGroup"
)


def _wake_fut(
    fut: asyncio.Future[Any],
    val: Any = None,
    is_exc: bool = False,
    has_val: bool = True,
) -> None:
    """Complete a future on its owning loop safely, tolerating concurrent cancellations.

    :param fut: The future to fulfill or fail.
    :param val: The value or exception to set on the future.
    :param is_exc: Whether ``val`` represents an exception.
    :param has_val: Whether ``val`` carries an actual payload value.
    """
    try:
        if is_exc:
            exc = val
            if not isinstance(exc, ChannelClosedError) and "closed" in str(exc).lower():
                exc = ChannelClosedError(_CHANNEL_CLOSED_MSG)
            fut.set_exception(exc)
        else:
            fut.set_result(val)
    except (asyncio.InvalidStateError, RuntimeError):
        if has_val and not is_exc:
            ch: Any = getattr(fut, "_ch", None)
            if ch is not None and not getattr(ch, "is_closed", False):
                try:
                    ch.try_send(val)
                except Exception:
                    pass


def _select_wake(arbiter_fut: asyncio.Future[Any], ch: Any) -> None:
    """Wake a select arbiter future, forwarding the notification if the arbiter was already fulfilled.

    :param arbiter_fut: The select arbiter future awaiting ready channel signal.
    :param ch: The ready channel instance.
    """
    try:
        if not arbiter_fut.done():
            arbiter_fut.set_result(ch)
        else:
            if hasattr(ch, "_forward_select_wakeup"):
                ch._forward_select_wakeup()
    except (asyncio.InvalidStateError, RuntimeError):
        pass


class _LenShim:
    """Helper enabling len() inspection on Rust-backed waiter queues."""

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

    def __init__(self, maxsize: int = 0) -> None:
        self._maxsize = max(0, int(maxsize))
        if _RustRawAsyncChannel is None:
            raise RuntimeError("_multiloop_core Rust extension is not compiled.")
        self._inner: Any = _RustRawAsyncChannel(self._maxsize, _wake_fut, _select_wake)

    def close(self) -> None:
        """Close the channel, waking all pending senders and receivers with ChannelClosedError."""
        self._inner.close()

    def __repr__(self) -> str:
        return f"<Channel is_closed={self.is_closed}>"

    @property
    def is_closed(self) -> bool:
        """Return True if the channel is closed."""
        return bool(self._inner.is_closed())

    def try_send(self, item: Any) -> bool:
        """Non-blocking send. Return True if the item was enqueued, False if full.

        :param item: The item to enqueue into the channel.
        :returns: ``True`` if sent immediately, ``False`` if the buffer was full.
        :raises ChannelClosedError: If the channel is closed.
        """
        try:
            return bool(self._inner.try_send(item))
        except RuntimeError as e:
            raise ChannelClosedError(_CHANNEL_CLOSED_MSG) from e

    def try_recv(self) -> Any:
        """Non-blocking receive. Return the item or raise WouldBlock.

        :returns: The dequeued item.
        :raises WouldBlock: If the channel is empty.
        :raises ChannelClosedError: If the channel is closed and completely drained.
        """
        try:
            has_item, item = self._inner.try_recv()
        except RuntimeError as e:
            raise ChannelClosedError(_CHANNEL_CLOSED_MSG) from e
        if has_item:
            return item
        raise WouldBlock("Channel is empty")

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
        if timeout is not None:
            await asyncio.wait_for(self._send_impl(item), timeout=timeout)
        else:
            await self._send_impl(item)

    async def _send_impl(self, item: Any) -> None:
        loop = asyncio.get_running_loop()
        while True:
            try:
                if self._inner.try_send(item):
                    return
            except RuntimeError as e:
                raise ChannelClosedError(_CHANNEL_CLOSED_MSG) from e

            fut: asyncio.Future[Any] = loop.create_future()
            try:
                if self._inner.register_putter(loop, fut):
                    if self._inner.try_send(item):
                        return
                    continue
            except RuntimeError as e:
                raise ChannelClosedError(_CHANNEL_CLOSED_MSG) from e

            try:
                await fut
            except BaseException:
                self._inner.unregister_putter(fut)
                raise

            try:
                self._inner.claim_put(item)
                return
            except RuntimeError as e:
                raise ChannelClosedError(_CHANNEL_CLOSED_MSG) from e

    def send_sync(self, item: Any, timeout: float | None = None) -> None:
        """Synchronously send an item into the channel from a worker or background OS thread.

        :param item: The item to send.
        :param timeout: Maximum seconds to block before raising TimeoutError.
        :raises TimeoutError: If timeout expired before buffer space became available.
        :raises ChannelClosedError: If the channel is closed.
        """
        try:
            self._inner.send_sync(item, timeout)
        except builtins.TimeoutError as e:
            raise TimeoutError("send_sync timed out") from e
        except RuntimeError as e:
            raise ChannelClosedError(_CHANNEL_CLOSED_MSG) from e

    async def recv(self, timeout: float | None = None) -> Any:
        """Receive an item, suspending until one is available or the channel is closed.

        :param timeout: Optional maximum seconds to wait before raising TimeoutError.
        :returns: The received item.
        :raises ChannelClosedError: If the channel is closed and empty.
        :raises TimeoutError: If timeout expires before an item is received.
        """
        if timeout is not None:
            return await asyncio.wait_for(self._recv_impl(), timeout=timeout)
        return await self._recv_impl()

    def recv_sync(self, timeout: float | None = None) -> Any:
        """Synchronously receive an item from the channel from a worker or background OS thread.

        :param timeout: Maximum seconds to block before raising TimeoutError.
        :returns: The dequeued item.
        :raises TimeoutError: If timeout expired before an item became available.
        :raises ChannelClosedError: If the channel is closed and drained.
        """
        try:
            return self._inner.recv_sync(timeout)
        except builtins.TimeoutError as e:
            raise TimeoutError("recv_sync timed out") from e
        except RuntimeError as e:
            raise ChannelClosedError(_CHANNEL_CLOSED_MSG) from e

    async def _recv_impl(self) -> Any:
        loop = asyncio.get_running_loop()
        while True:
            try:
                has_item, item = self._inner.try_recv()
                if has_item:
                    return item
            except RuntimeError as e:
                raise ChannelClosedError(_CHANNEL_CLOSED_MSG) from e

            fut: asyncio.Future[Any] = loop.create_future()
            fut._ch = self  # type: ignore[attr-defined]
            try:
                has_item, item = self._inner.register_getter(loop, fut)
                if has_item:
                    return item
            except RuntimeError as e:
                raise ChannelClosedError(_CHANNEL_CLOSED_MSG) from e

            try:
                return await fut
            except BaseException:
                self._inner.unregister_getter(fut)
                raise

    def _register_select_watcher(
        self, loop: asyncio.AbstractEventLoop, arbiter_fut: asyncio.Future[Any]
    ) -> Any | None:
        try:
            registered = self._inner.register_select_watcher(loop, arbiter_fut, self)
            if registered:
                return arbiter_fut
            return None
        except RuntimeError:
            return None

    def _unregister_select_watcher(self, token: Any) -> None:
        if token is not None:
            self._inner.unregister_select_watcher(token)

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

    __slots__ = ("_ch",)

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

    loop = asyncio.get_running_loop()
    if timeout is not None and _deadline is None:
        _deadline = loop.time() + timeout

    num_channels = len(channels)
    while True:
        # Phase 1: Fast Probe (try_recv) with randomized start index for anti-barging fairness
        start_idx = (time.perf_counter_ns() ^ id(loop)) % num_channels if num_channels > 1 else 0
        for i in range(num_channels):
            ch = channels[(start_idx + i) % num_channels]
            try:
                val = ch.try_recv()
                return ch, val
            except (ChannelClosedError, WouldBlock):
                continue

        if default is not _UNSET:
            return default

        if all(ch.is_closed and ch.qsize() == 0 for ch in channels):
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

            if all(ch.is_closed and ch.qsize() == 0 for ch in channels):
                raise ChannelClosedError(_CHANNEL_CLOSED_MSG)

            if remaining_timeout is not None:
                ready_ch = await asyncio.wait_for(arbiter_fut, timeout=remaining_timeout)
            else:
                ready_ch = await arbiter_fut

            try:
                return ready_ch, ready_ch.try_recv()
            except (WouldBlock, ChannelClosedError):
                continue
        except (builtins.TimeoutError, TimeoutError):
            raise TimeoutError("select_channel timed out") from None
        finally:
            for ch, token in registered_channels:
                ch._unregister_select_watcher(token)


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


class AsyncWaitGroup:
    """Cross-thread WaitGroup for coordinating multiple asynchronous tasks (Go sync.WaitGroup style).

    Coordinates a collection of parallel tasks across one or multiple event-loop threads.
    """

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
        waiters = self._inner.add(delta)
        if waiters:
            _wake_all(waiters)

    def done(self) -> None:
        """Decrement the task counter by 1. Wakes all waiters if the counter reaches 0.

        :raises RuntimeError: If done() is called when the counter is already zero.
        """
        waiters = self._inner.done()
        if waiters:
            _wake_all(waiters)

    @asynccontextmanager
    async def holding(self) -> AsyncGenerator[None]:
        """RAII async context manager that increments counter on enter and decrements on exit."""
        self.add(1)
        try:
            yield
        finally:
            self.done()

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
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[Any] = loop.create_future()
        already_done = self._inner.register_waiter((loop, fut))
        if already_done:
            return
        try:
            await fut
        except BaseException:
            self._inner.unregister_waiter(fut)
            raise


class AsyncOnce:
    """Thread-safe single execution primitive across multiple event loops (Go sync.Once style).

    Guarantees that an initialization routine runs exactly once, even when invoked concurrently
    by many coroutines across different OS threads.
    """

    def __init__(self) -> None:
        self._thread_lock = threading.Lock()
        self._done = False
        self._result: Any = None
        self._exc: BaseException | None = None
        self._waiters: collections.OrderedDict[asyncio.Future[Any], asyncio.AbstractEventLoop] = (
            collections.OrderedDict()
        )
        self._executing = False

    def __repr__(self) -> str:
        return f"<AsyncOnce done={self._done}>"

    async def do(self, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """Execute `fn` if and only if it has never been executed before.

        :param fn: Target callable or coroutine function.
        :param args: Positional arguments passed to `fn`.
        :param kwargs: Keyword arguments passed to `fn`.
        :returns: The result returned by `fn`.
        """
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[Any] | None = None

        with self._thread_lock:
            if self._done:
                if self._exc:
                    raise self._exc
                return self._result

            if not self._executing:
                self._executing = True
                is_leader = True
            else:
                is_leader = False

            if not is_leader:
                fut = loop.create_future()
                self._waiters[fut] = loop

        if not is_leader:
            assert fut is not None
            try:
                await fut
            finally:
                with self._thread_lock:
                    _discard_waiter(self._waiters, fut)
            if self._exc:
                raise self._exc
            return self._result

        try:
            res = fn(*args, **kwargs)
            if asyncio.iscoroutine(res) or asyncio.isfuture(res):
                res = await res
            self._result = res
        except BaseException as e:
            if isinstance(e, asyncio.CancelledError):
                self._exc = RuntimeError("AsyncOnce execution was cancelled")
                self._exc.__cause__ = e
            else:
                self._exc = e
            raise
        finally:
            with self._thread_lock:
                self._done = True
                _wake_all(self._waiters)

        return self._result
