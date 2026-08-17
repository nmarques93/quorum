"""The in-process event bus used by the first Quorum backend."""

from __future__ import annotations

import asyncio
import contextvars
import fnmatch
import inspect
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta
from types import MappingProxyType
from typing import Any, Awaitable, Callable, Mapping
from uuid import uuid4

from .clock import Clock, SystemClock, TimerHandle
from .event import Event

Handler = Callable[[Event], Awaitable[Any] | Any]
Sink = Callable[[Event], Awaitable[Any] | Any]


@dataclass(frozen=True, slots=True)
class TaskContext:
    """Deadline, budget, and cancellation state for one logical task."""

    correlation_id: str
    deadline: float | None = None
    budget: Mapping[str, float] = field(default_factory=dict)
    cancelled: bool = False

    def remaining(self, now: float) -> float | None:
        """Seconds until ``deadline`` measured by the supplied clock time."""

        if self.deadline is None:
            return None
        return self.deadline - now


@dataclass(frozen=True, slots=True)
class TraceReport:
    """Aggregated metadata for one logical task."""

    correlation_id: str
    events: tuple[Event, ...]
    errors: tuple[Event, ...] = field(default_factory=tuple)

    @property
    def first_at(self) -> datetime | None:
        if not self.events:
            return None
        return min(event.timestamp for event in self.events)

    @property
    def last_at(self) -> datetime | None:
        if not self.events:
            return None
        return max(event.timestamp for event in self.events)

    @property
    def duration(self) -> timedelta | None:
        first = self.first_at
        last = self.last_at
        if first is None or last is None:
            return None
        return last - first


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

    def __init__(self, clock: Clock | None = None) -> None:
        self.clock: Clock = clock if clock is not None else SystemClock()
        self._subscriptions: list[_Subscription] = []
        self._log: list[Event] = []
        self._sequence = 0
        self._current_event: contextvars.ContextVar[Event | None] = (
            contextvars.ContextVar("quorum_current_event", default=None)
        )
        self._current_task_context: contextvars.ContextVar[TaskContext | None] = (
            contextvars.ContextVar("quorum_current_task_context", default=None)
        )
        self._rule_engine: Any = None
        self._sinks: list[Sink] = []
        self._task_contexts: dict[str, TaskContext] = {}
        self._deadline_timers: dict[str, TimerHandle] = {}
        self._active: dict[str, set[asyncio.Task[Any]]] = {}

    @property
    def log(self) -> tuple[Event, ...]:
        """Return the publication log in bus sequence order."""

        return tuple(self._log)

    @property
    def current_event(self) -> Event | None:
        """Return the event handled by the current async task, if any."""

        return self._current_event.get()

    @property
    def current_task_context(self) -> TaskContext | None:
        """Return the task context for the event being handled, if any."""

        return self._current_task_context.get()

    def task_context(self, correlation_id: str) -> TaskContext | None:
        """Return the registered context for a logical task."""

        return self._task_contexts.get(correlation_id)

    def remaining_time(self) -> float | None:
        """Seconds until the current task deadline, if one is set."""

        context = self._current_task_context.get()
        if context is None or context.deadline is None:
            return None
        return context.deadline - self.clock.now()

    def start_task(
        self,
        correlation_id: str,
        *,
        deadline: float | None = None,
        budget: Mapping[str, float] | None = None,
    ) -> TaskContext:
        """Register a logical task with an optional deadline and budget.

        A positive ``deadline`` schedules automatic cancellation of in-flight
        handlers for ``correlation_id`` when it expires.
        """

        if correlation_id in self._task_contexts:
            raise ValueError(f"task {correlation_id!r} is already started")
        if deadline is not None and deadline <= 0:
            raise ValueError("deadline must be a positive number of seconds")

        clock_deadline: float | None = None
        if deadline is not None:
            clock_deadline = self.clock.now() + deadline
            self._deadline_timers[correlation_id] = self.clock.call_later(
                deadline, self._on_deadline, correlation_id
            )

        context = TaskContext(
            correlation_id=correlation_id,
            deadline=clock_deadline,
            budget=MappingProxyType(dict(budget or {})),
        )
        self._task_contexts[correlation_id] = context
        return context

    def cancel(self, correlation_id: str) -> bool:
        """Cancel in-flight handlers for a task and mark it cancelled.

        Returns ``True`` if the task was active, ``False`` otherwise.
        """

        context = self._task_contexts.get(correlation_id)
        if context is None or context.cancelled:
            return False

        self._task_contexts[correlation_id] = replace(context, cancelled=True)

        timer = self._deadline_timers.pop(correlation_id, None)
        if timer is not None:
            timer.cancel()

        try:
            current = asyncio.current_task()
        except RuntimeError:
            current = None

        for task in list(self._active.get(correlation_id, ())):
            if task is not current and not task.done():
                task.cancel()

        if self._rule_engine is not None:
            self._rule_engine.cancel(correlation_id)
        return True

    def _on_deadline(self, correlation_id: str) -> None:
        self.cancel(correlation_id)

    def subscribe(self, pattern: str, handler: Handler) -> Callable[[], None]:
        """Subscribe a handler to an exact type or shell-style wildcard."""

        subscription = _Subscription(pattern, handler)
        self._subscriptions.append(subscription)

        def unsubscribe() -> None:
            if subscription in self._subscriptions:
                self._subscriptions.remove(subscription)

        return unsubscribe

    def add_sink(self, sink: Sink) -> Callable[[], None]:
        """Attach an event observer fired for every published event.

        Sinks are notified after the in-memory log is updated and before
        handlers run. A sink failure is silently dropped and must not affect
        the bus.
        """

        self._sinks.append(sink)

        def remove() -> None:
            if sink in self._sinks:
                self._sinks.remove(sink)

        return remove

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
        await self._notify_sinks(published)

        matching = [
            subscription
            for subscription in self._subscriptions
            if fnmatch.fnmatchcase(published.type, subscription.pattern)
        ]
        if not matching:
            return published

        context = self._task_contexts.get(published.correlation_id)

        async def invoke(subscription: _Subscription) -> Any:
            event_token = self._current_event.set(published)
            context_token = self._current_task_context.set(context)
            try:
                result = subscription.handler(published)
                if inspect.isawaitable(result):
                    return await result
                return result
            finally:
                self._current_event.reset(event_token)
                self._current_task_context.reset(context_token)

        tasks = [asyncio.create_task(invoke(subscription)) for subscription in matching]
        active = self._active.setdefault(published.correlation_id, set())
        active.update(tasks)
        try:
            results = await asyncio.gather(*tasks, return_exceptions=True)
        finally:
            for task in tasks:
                active.discard(task)
            if not active:
                self._active.pop(published.correlation_id, None)

        if any(isinstance(result, asyncio.CancelledError) for result in results):
            raise asyncio.CancelledError()
        errors = [result for result in results if isinstance(result, Exception)]
        if errors:
            raise EventDispatchError(published, errors)
        return published

    def trace(self, correlation_id: str) -> tuple[Event, ...]:
        """Return all published events belonging to one logical task."""

        return tuple(event for event in self._log if event.correlation_id == correlation_id)

    def trace_report(self, correlation_id: str) -> TraceReport:
        """Return aggregated metadata for one logical task."""

        events = self.trace(correlation_id)
        return TraceReport(
            correlation_id=correlation_id,
            events=events,
            errors=tuple(
                event for event in events if event.type == "agent.failed"
            ),
        )

    async def _notify_sinks(self, event: Event) -> None:
        for sink in self._sinks:
            try:
                result = sink(event)
                if inspect.isawaitable(result):
                    await result
            except Exception:
                pass

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
