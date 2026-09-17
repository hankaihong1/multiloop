"""Shared channel base class and cross-thread waiter queues for Channel."""

from __future__ import annotations

import asyncio
import collections
from typing import Any

_Waiter = tuple[asyncio.AbstractEventLoop, asyncio.Future[Any]]
_CHANNEL_CLOSED_MSG = "Channel is closed"

__all__ = ["_CHANNEL_CLOSED_MSG", "_discard_waiter", "_set_soon", "_wake_all"]


def _set_soon(
    loop: asyncio.AbstractEventLoop,
    fut: asyncio.Future[Any],
    exc: BaseException | None = None,
) -> None:
    """Complete a waiter future on its owning event loop, safely tolerating concurrent cancellation.

    :param loop: The event loop owning ``fut``.
    :param fut: The future to fulfill or fail.
    :param exc: Optional exception to deliver. If None, fulfills with None.
    """

    def _do() -> None:
        try:
            if exc is not None:
                fut.set_exception(exc)
            else:
                fut.set_result(None)
        except asyncio.InvalidStateError:
            pass

    try:
        loop.call_soon_threadsafe(_do)
    except RuntimeError:
        pass


def _wake_all(
    waiters: (
        collections.OrderedDict[asyncio.Future[Any], asyncio.AbstractEventLoop] | list[_Waiter]
    ),
    exc: BaseException | None = None,
    count: int | None = None,
) -> None:
    """Wake pending waiter futures on their respective owning event loops.

    :param waiters: Dictionary or list of waiter futures mapped to their event loops.
    :param exc: Optional exception to deliver instead of successful resolution.
    :param count: Maximum number of active waiters to wake (wake all if None).
    """
    if isinstance(waiters, collections.OrderedDict):
        woken = 0
        while waiters and (count is None or woken < count):
            fut, loop = waiters.popitem(last=False)
            if fut.done():
                continue
            _set_soon(loop, fut, exc)
            woken += 1
        return
    for loop, fut in waiters:
        if not fut.done():
            _set_soon(loop, fut, exc)


def _discard_waiter(
    waiters: collections.OrderedDict[asyncio.Future[Any], asyncio.AbstractEventLoop],
    fut: asyncio.Future[Any],
) -> bool:
    """Remove a waiter future from the waiter queue in O(1) if still present.

    :param waiters: The dictionary of active waiters mapped to their event loops.
    :param fut: The future to discard.
    :returns: ``True`` if the future was queued and removed; ``False`` if it had
              already been popped by a concurrent wakeup.
    """
    return waiters.pop(fut, None) is not None
