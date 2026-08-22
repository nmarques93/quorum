"""A liveness watchdog for agents."""

from __future__ import annotations

import asyncio
import inspect
import logging
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

from .agent import Agent
from .bus import EventBus
from .clock import TimerHandle

OnChange = Callable[[Agent], Awaitable[object] | object]

logger = logging.getLogger("quorum.supervisor")


def _log_hang(agent: Agent) -> None:
    logger.warning("agent %r appears hung", agent.name)


def _log_recovered(agent: Agent) -> None:
    logger.info("agent %r recovered", agent.name)


@dataclass(slots=True)
class _Watch:
    agent: Agent
    timeout: float
    hung: bool = False


class Supervisor:
    """Watch agents and notify when one stops beating its heartbeat.

    An agent is considered hung when it goes longer than its ``timeout``
    without a heartbeat. Agents beat automatically when they handle or emit
    events and may call :meth:`Agent.beat` explicitly. The supervisor checks
    once per ``interval`` using the bus clock, so tests can drive it with a
    :class:`~quorum.clock.ManualClock`.
    """

    def __init__(
        self,
        bus: EventBus,
        *,
        interval: float = 1.0,
        timeout: float = 10.0,
        on_hang: OnChange | None = None,
        on_recovered: OnChange | None = None,
    ) -> None:
        self.bus = bus
        self.clock = bus.clock
        self.interval = interval
        self.default_timeout = timeout
        self.on_hang = on_hang if on_hang is not None else _log_hang
        self.on_recovered = (
            on_recovered if on_recovered is not None else _log_recovered
        )
        self._watched: list[_Watch] = []
        self._timer: TimerHandle | None = None
        self._running = False
        if interval <= 0:
            raise ValueError("interval must be greater than zero")
        if timeout <= 0:
            raise ValueError("timeout must be greater than zero")

    @property
    def running(self) -> bool:
        return self._running

    def watch(self, agent: Agent, *, timeout: float | None = None) -> Agent:
        """Start monitoring an agent; returns the agent for chaining."""

        if timeout is not None and timeout <= 0:
            raise ValueError("timeout must be greater than zero")
        if self._find(agent) is not None:
            raise ValueError(f"agent {agent.name!r} is already watched")

        agent.beat()
        self._watched.append(
            _Watch(agent, self.default_timeout if timeout is None else timeout)
        )
        return agent

    def unwatch(self, agent: Agent) -> bool:
        """Stop monitoring an agent."""

        registration = self._find(agent)
        if registration is None:
            return False
        self._watched.remove(registration)
        return True

    def is_hung(self, agent: Agent) -> bool:
        """Whether the agent is currently marked hung."""

        registration = self._find(agent)
        return registration is not None and registration.hung

    async def start(self) -> None:
        """Begin the monitoring loop."""

        if self._running:
            return
        self._running = True
        self._timer = self.clock.call_later(self.interval, self._tick)

    async def stop(self) -> None:
        """Stop the monitoring loop."""

        if not self._running:
            return
        self._running = False
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None

    def _tick(self) -> None:
        self._timer = self.clock.call_later(self.interval, self._tick)
        task = asyncio.create_task(self._check())
        task.add_done_callback(self._handle_result)

    @staticmethod
    def _handle_result(task: asyncio.Task[Any]) -> None:
        try:
            task.result()
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.exception("supervisor check failed")

    async def _check(self) -> None:
        now = self.clock.now()
        for registration in list(self._watched):
            silent = now - registration.agent.last_beat
            if silent > registration.timeout and not registration.hung:
                registration.hung = True
                await self._invoke(self.on_hang, registration.agent)
            elif registration.hung and silent <= registration.timeout:
                registration.hung = False
                await self._invoke(self.on_recovered, registration.agent)

    def _find(self, agent: Agent) -> _Watch | None:
        for registration in self._watched:
            if registration.agent is agent:
                return registration
        return None

    @staticmethod
    async def _invoke(callback: OnChange, agent: Agent) -> None:
        result = callback(agent)
        if inspect.isawaitable(result):
            await result
