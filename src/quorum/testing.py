"""Deterministic test helpers for Quorum workflows.

Use :class:`ManualClock` together with an ``EventBus(clock=...)`` so that
agent timeouts, retry backoff, and rule expiry can be driven by explicit
``advance`` calls instead of wall-clock sleeps.

Pytest fixtures (``clock``, ``bus``, ``agent_factory``) are exported only
when ``pytest`` is importable. Install ``quorum[dev]`` to use them.
"""

from __future__ import annotations

import asyncio
from typing import Any

from .agent import Agent
from .bus import EventBus
from .clock import ManualClock, SystemClock
from .event import Event

__all__ = [
    "ManualClock",
    "SystemClock",
    "run_until_quiescent",
    "publish_and_drain",
]


async def run_until_quiescent() -> None:
    """Yield control until no Quorum tasks remain pending on the loop."""

    loop = asyncio.get_running_loop()
    current = asyncio.current_task()
    for _ in range(10_000):
        pending = [task for task in asyncio.all_tasks(loop) if not task.done()]
        if len(pending) <= (0 if current is None else 1):
            return
        await asyncio.sleep(0)
    raise RuntimeError("event loop did not become quiescent")


async def publish_and_drain(bus: EventBus, event: Event | str) -> Event:
    """Publish an event and run the loop until it becomes quiescent."""

    published = await bus.publish(event)
    await run_until_quiescent()
    return published


try:
    import pytest  # type: ignore
except ImportError:  # pragma: no cover - exercised only without pytest
    pytest = None  # type: ignore


if pytest is not None:  # pragma: no cover - fixture definitions

    @pytest.fixture
    def clock() -> ManualClock:
        """A deterministic clock advanced explicitly by tests."""

        return ManualClock()

    @pytest.fixture
    def bus(clock: ManualClock) -> EventBus:
        """An event bus backed by the deterministic manual clock."""

        return EventBus(clock=clock)

    @pytest.fixture
    def agent_factory(bus: EventBus):
        """A factory for agents bound to the fixture bus."""

        def factory(name: str, **kwargs: Any) -> Agent:
            return Agent(name, bus, **kwargs)

        return factory
