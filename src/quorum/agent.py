"""Agent lifecycle and event emission helpers."""

from __future__ import annotations

from typing import Any, Callable, Mapping, overload
from uuid import uuid4

from .bus import EventBus, Handler
from .event import Event


_UNSET = object()


class Agent:
    """A named component that reacts to and emits events."""

    def __init__(self, name: str, bus: EventBus) -> None:
        self.name = name
        self.bus = bus
        self._unsubscribers: list[Callable[[], None]] = []

    @overload
    def on(self, pattern: str, handler: Handler) -> Handler: ...

    @overload
    def on(self, pattern: str) -> Any: ...

    def on(self, pattern: str, handler: Handler | None = None):
        """Register a handler, either directly or as a decorator."""

        def register(callback: Handler) -> Handler:
            self._unsubscribers.append(self.bus.subscribe(pattern, callback))
            return callback

        return register(handler) if handler is not None else register

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

        for unsubscribe in self._unsubscribers:
            unsubscribe()
        self._unsubscribers.clear()
