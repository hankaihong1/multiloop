"""Exception hierarchy for multiloop.

All public exceptions in multiloop inherit from :class:`MultiloopError`, allowing
callers to easily catch library-specific errors or target specific failure modes.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from multiloop._rust import _try_import_rust_class

__all__ = [
    "ChannelClosedError",
    "MultiloopError",
    "ThreadPoolClosedError",
    "TimeoutError",
    "WouldBlock",
]


class MultiloopError(Exception):
    """Base exception class for all multiloop errors."""


class WouldBlock(MultiloopError):  # noqa: N818
    """Raised when a non-blocking channel or lock operation cannot proceed immediately."""


class ChannelClosedError(MultiloopError):
    """Raised when attempting to send or receive on a closed channel."""


if TYPE_CHECKING:

    class ThreadPoolClosedError(MultiloopError, RuntimeError):
        """Raised when submitting tasks to a closed or aborted EventLoopThreadPool."""

else:
    _RustThreadPoolClosedError = _try_import_rust_class(
        "multiloop._multiloop_core", "ThreadPoolClosedError"
    )

    if _RustThreadPoolClosedError is not None:

        class ThreadPoolClosedError(MultiloopError, _RustThreadPoolClosedError):
            """Raised when submitting tasks to a closed or aborted EventLoopThreadPool."""

    else:

        class ThreadPoolClosedError(MultiloopError, RuntimeError):
            """Raised when submitting tasks to a closed or aborted EventLoopThreadPool."""


class TimeoutError(MultiloopError, asyncio.TimeoutError):
    """Raised when a multiloop concurrency operation times out."""
