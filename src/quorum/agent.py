"""Agent lifecycle and event execution policies."""

from __future__ import annotations

import asyncio
import inspect
from dataclasses import dataclass
from typing import Any, Callable, Mapping, overload
from uuid import uuid4

from .bus import EventBus, Handler
from .event import Event


_UNSET = object()


@dataclass(slots=True)
class _Registration:
    pattern: str
    handler: Handler
    timeout: float | None | object
    retries: int | None
    retry_delay: float | None
    unsubscribe: Callable[[], None] | None = None


class Agent:
    """A named component that reacts to and emits events.

    Handlers are registered with :meth:`on` and become active after
    :meth:`start`. A handler can be retried and timed out independently from
    the other subscribers on the bus.
    """

    def __init__(
        self,
        name: str,
        bus: EventBus,
        *,
        timeout: float | None = None,
        retries: int = 0,
        retry_delay: float = 0.0,
    ) -> None:
        self.name = name
        self.bus = bus
        self.timeout = timeout
        self.retries = retries
        self.retry_delay = retry_delay
        self._registrations: list[_Registration] = []
        self._active_tasks: set[asyncio.Task[Any]] = set()
        self._started = False
        self._validate_policy(timeout, retries, retry_delay)

    @property
    def started(self) -> bool:
        return self._started

    @overload
    def on(self, pattern: str, handler: Handler, **kwargs: Any) -> Handler: ...

    @overload
    def on(self, pattern: str, **kwargs: Any) -> Any: ...

    def on(
        self,
        pattern: str,
        handler: Handler | None = None,
        *,
        timeout: float | None | object = _UNSET,
        retries: int | None = None,
        retry_delay: float | None = None,
    ):
        """Register a handler, optionally overriding the agent policy."""

        self._validate_policy(
            None if timeout is _UNSET else timeout,
            self.retries if retries is None else retries,
            self.retry_delay if retry_delay is None else retry_delay,
        )

        def register(callback: Handler) -> Handler:
            registration = _Registration(
                pattern,
                callback,
                timeout,
                retries,
                retry_delay,
            )
            self._registrations.append(registration)
            if self._started:
                self._activate(registration)
            return callback

        return register(handler) if handler is not None else register

    async def start(self) -> None:
        """Activate all registered handlers."""

        if self._started:
            return
        self._started = True
        for registration in self._registrations:
            self._activate(registration)

    async def stop(self) -> None:
        """Deactivate handlers and cancel active handler executions."""

        if not self._started:
            return
        self._started = False
        self._deactivate()

        current = asyncio.current_task()
        tasks = [task for task in self._active_tasks if task is not current]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def emit(
        self,
        event_type: str,
        payload: Mapping[str, Any] | None = None,
        *,
        correlation_id: str | None = None,
        causation_id: str | None | object = _UNSET,
    ) -> Event:
        """Emit an event, inheriting context from the active handler."""

        current = self.bus.current_event
        if correlation_id is None:
            correlation_id = current.correlation_id if current else uuid4().hex
        if causation_id is _UNSET:
            actual_causation_id = current.event_id if current else None
        else:
            actual_causation_id = causation_id

        return await self.bus.publish(
            Event(
                type=event_type,
                payload=payload or {},
                correlation_id=correlation_id,
                causation_id=actual_causation_id,
                source=self.name,
            )
        )

    def close(self) -> None:
        """Remove all subscriptions owned by this agent."""

        self._deactivate()
        self._registrations.clear()
        self._started = False

    def _activate(self, registration: _Registration) -> None:
        if registration.unsubscribe is not None:
            return

        async def callback(event: Event) -> None:
            await self._dispatch(registration, event)

        registration.unsubscribe = self.bus.subscribe(registration.pattern, callback)

    def _deactivate(self) -> None:
        for registration in self._registrations:
            if registration.unsubscribe is not None:
                registration.unsubscribe()
                registration.unsubscribe = None

    async def _dispatch(self, registration: _Registration, event: Event) -> None:
        task = asyncio.current_task()
        if task is not None:
            self._active_tasks.add(task)

        timeout = self.timeout if registration.timeout is _UNSET else registration.timeout
        retries = self.retries if registration.retries is None else registration.retries
        retry_delay = (
            self.retry_delay
            if registration.retry_delay is None
            else registration.retry_delay
        )

        try:
            for attempt in range(retries + 1):
                try:
                    result = registration.handler(event)
                    if inspect.isawaitable(result):
                        if timeout is None:
                            await result
                        else:
                            await self.bus.clock.wait_for(result, timeout)
                    return
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    if attempt >= retries:
                        await self._report_failure(event, error, attempt + 1, timeout)
                        raise
                    if retry_delay:
                        await self.bus.clock.sleep(retry_delay * (2**attempt))
        finally:
            if task is not None:
                self._active_tasks.discard(task)

    async def _report_failure(
        self,
        event: Event,
        error: Exception,
        attempts: int,
        timeout: float | None,
    ) -> None:
        failure = Event(
            "agent.failed",
            {
                "agent": self.name,
                "event_type": event.type,
                "error_type": type(error).__name__,
                "error": str(error),
                "attempts": attempts,
                "timeout": timeout,
            },
            correlation_id=event.correlation_id,
            causation_id=event.event_id,
            source=self.name,
        )
        try:
            await self.bus.publish(failure)
        except Exception:
            # Preserve the original handler failure if the diagnostic event
            # itself has a failing subscriber.
            pass

    @staticmethod
    def _validate_policy(
        timeout: float | None | object,
        retries: int,
        retry_delay: float,
    ) -> None:
        if timeout is not _UNSET and timeout is not None and timeout <= 0:
            raise ValueError("timeout must be greater than zero")
        if retries < 0:
            raise ValueError("retries cannot be negative")
        if retry_delay < 0:
            raise ValueError("retry_delay cannot be negative")
