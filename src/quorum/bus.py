"""The in-process event bus used by the first Quorum backend."""

from __future__ import annotations

import asyncio
import contextvars
import fnmatch
import inspect
from dataclasses import dataclass, replace
from typing import Any, Awaitable, Callable, Mapping
from uuid import uuid4

from .event import Event

Handler = Callable[[Event], Awaitable[Any] | Any]


@dataclass(frozen=True, slots=True)
class _Subscription:
    pattern: str
    handler: Handler


class EventDispatchError(Exception):
    """Raised after all matching handlers have run and one or more failed."""

    def __init__(self, event: Event, errors: list[BaseException]) -> None:
        self.event = event
        self.errors = errors
        super().__init__(
            f"{len(errors)} handler(s) failed while dispatching {event.type!r}"
        )


class EventBus:
    """Fan out events to matching async handlers within one process.

    Publishing records the event before dispatch and waits for all matching
    handlers. Matching handlers run concurrently; nested publications are
    supported and inherit the active event context through :class:`Agent`.
    """

    def __init__(self) -> None:
        self._subscriptions: list[_Subscription] = []
        self._log: list[Event] = []
        self._sequence = 0
        self._current_event: contextvars.ContextVar[Event | None] = (
            contextvars.ContextVar("quorum_current_event", default=None)
        )
        self._rule_engine: Any = None

    @property
    def log(self) -> tuple[Event, ...]:
        """Return the publication log in bus sequence order."""

        return tuple(self._log)

    @property
    def current_event(self) -> Event | None:
        """Return the event handled by the current async task, if any."""

        return self._current_event.get()

    def subscribe(self, pattern: str, handler: Handler) -> Callable[[], None]:
        """Subscribe a handler to an exact type or shell-style wildcard."""

        subscription = _Subscription(pattern, handler)
        self._subscriptions.append(subscription)

        def unsubscribe() -> None:
            if subscription in self._subscriptions:
                self._subscriptions.remove(subscription)

        return unsubscribe

    async def publish(
        self,
        event: Event | str,
        payload: Mapping[str, Any] | None = None,
        *,
        correlation_id: str | None = None,
        causation_id: str | None = None,
        source: str | None = None,
    ) -> Event:
        """Publish an event and wait for all matching handlers to finish."""

        if isinstance(event, str):
            event = Event(
                type=event,
                payload=payload or {},
                correlation_id=correlation_id or uuid4().hex,
                causation_id=causation_id,
                source=source,
            )

        self._sequence += 1
        published = replace(event, sequence=self._sequence)
        self._log.append(published)

        matching = [
            subscription
            for subscription in self._subscriptions
            if fnmatch.fnmatchcase(published.type, subscription.pattern)
        ]
        if not matching:
            return published

        async def invoke(subscription: _Subscription) -> Any:
            token = self._current_event.set(published)
            try:
                result = subscription.handler(published)
                if inspect.isawaitable(result):
                    return await result
                return result
            finally:
                self._current_event.reset(token)

        results = await asyncio.gather(
            *(invoke(subscription) for subscription in matching),
            return_exceptions=True,
        )
        errors = [result for result in results if isinstance(result, BaseException)]
        if errors:
            raise EventDispatchError(published, errors)
        return published

    def trace(self, correlation_id: str) -> tuple[Event, ...]:
        """Return all published events belonging to one logical task."""

        return tuple(event for event in self._log if event.correlation_id == correlation_id)

    def when(
        self,
        event_types: list[str] | tuple[str, ...],
        *,
        where: Mapping[str, Callable[[Event], bool]] | None = None,
        timeout: float | None = None,
    ):
        """Wait for one qualifying occurrence of each event type."""

        return self._get_rule_engine().when(
            event_types, where=where, timeout=timeout
        )

    def when_count(
        self,
        event_type: str,
        count: int,
        *,
        where: Callable[[Event], bool] | None = None,
        timeout: float | None = None,
    ):
        """Wait for ``count`` qualifying events of one type."""

        return self._get_rule_engine().when_count(
            event_type, count, where=where, timeout=timeout
        )

    def _get_rule_engine(self):
        from .rules import RuleEngine

        if self._rule_engine is None:
            self._rule_engine = RuleEngine(self)
        return self._rule_engine
