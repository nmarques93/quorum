"""Time sources used by Quorum for timeouts and retry delays.

The bus, rules, and agents accept a :class:`Clock` so that tests can advance
time deterministically instead of waiting on wall-clock sleeps.
"""

from __future__ import annotations

import asyncio
import inspect
import time
from typing import Any, Awaitable, Callable, Protocol


class TimerHandle(Protocol):
    """A cancellable timer returned by :meth:`Clock.call_later`."""

    def cancel(self) -> None: ...


class Clock:
    """A time source. Subclasses implement :meth:`now` and :meth:`call_later`.

    :meth:`sleep` and :meth:`wait_for` are built on top of ``call_later`` so
    that both the real clock and the manual clock share one timeout path.
    """

    def now(self) -> float:
        raise NotImplementedError

    def call_later(
        self, seconds: float, callback: Callable[..., Any], *args: Any
    ) -> TimerHandle:
        raise NotImplementedError

    async def sleep(self, seconds: float) -> None:
        """Await a delay measured by this clock."""

        loop = asyncio.get_running_loop()
        future: asyncio.Future[None] = loop.create_future()
        handle = self.call_later(seconds, future.set_result, None)
        try:
            await future
        finally:
            handle.cancel()

    async def wait_for(self, awaitable: Awaitable[Any], timeout: float) -> Any:
        """Await ``awaitable``, raising ``TimeoutError`` after ``timeout``."""

        task = asyncio.ensure_future(awaitable)
        timed_out = False

        def on_timeout() -> None:
            nonlocal timed_out
            timed_out = True
            if not task.done():
                task.cancel()

        handle = self.call_later(timeout, on_timeout)
        try:
            return await task
        except asyncio.CancelledError:
            if timed_out:
                raise asyncio.TimeoutError from None
            task.cancel()
            raise
        finally:
            handle.cancel()


class SystemClock(Clock):
    """A clock backed by the real event loop and monotonic time."""

    def now(self) -> float:
        return time.monotonic()

    def call_later(
        self, seconds: float, callback: Callable[..., Any], *args: Any
    ) -> TimerHandle:
        return asyncio.get_running_loop().call_later(seconds, callback, *args)


class ManualTimer:
    """A cancellable timer tracked by :class:`ManualClock`."""

    def __init__(
        self, deadline: float, callback: Callable[..., Any], args: tuple[Any, ...]
    ) -> None:
        self.deadline = deadline
        self.callback = callback
        self.args = args
        self.cancelled = False

    def cancel(self) -> None:
        self.cancelled = True


class ManualClock(Clock):
    """A deterministic clock advanced explicitly by the test.

    Time does not move unless :meth:`advance` is called, and every scheduled
    timer fires in deadline order while advancing.
    """

    def __init__(self) -> None:
        self._now = 0.0
        self._timers: list[ManualTimer] = []

    def now(self) -> float:
        return self._now

    def call_later(
        self, seconds: float, callback: Callable[..., Any], *args: Any
    ) -> TimerHandle:
        timer = ManualTimer(self._now + seconds, callback, args)
        self._timers.append(timer)
        self._timers.sort(key=lambda item: item.deadline)
        return timer

    async def advance(self, seconds: float) -> None:
        """Move time forward and fire every timer that becomes due."""

        target = self._now + seconds
        while self._timers and self._timers[0].deadline <= target:
            timer = self._timers.pop(0)
            if timer.cancelled:
                continue
            self._now = timer.deadline
            result = timer.callback(*timer.args)
            if inspect.isawaitable(result):
                await result
            await asyncio.sleep(0)
        self._now = target
