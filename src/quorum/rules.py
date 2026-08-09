"""Small, in-process coordination rules for correlated events."""

from __future__ import annotations

import asyncio
import inspect
from collections import defaultdict
from dataclasses import dataclass
from typing import Awaitable, Callable, Mapping

from .event import Event

RuleHandler = Callable[["RuleMatch"], Awaitable[object] | object]
TimeoutHandler = Callable[["RuleTimeout"], Awaitable[object] | object]
Predicate = Callable[[Event], bool]


@dataclass(frozen=True, slots=True)
class RuleMatch:
    """The events that satisfied one coordination rule."""

    correlation_id: str
    events: tuple[Event, ...]


@dataclass(frozen=True, slots=True)
class RuleTimeout:
    """The partial events collected when a rule expires."""

    correlation_id: str
    events: tuple[Event, ...]


@dataclass(frozen=True, slots=True)
class _Requirement:
    count: int
    predicate: Predicate | None = None


class Rule:
    def __init__(
        self,
        requirements: Mapping[str, _Requirement],
        handler: RuleHandler,
        timeout: float | None,
    ) -> None:
        self.requirements = requirements
        self.handler = handler
        self.timeout = timeout
        self.timeout_handler: TimeoutHandler | None = None
        self._events: dict[str, dict[str, list[Event]]] = defaultdict(
            lambda: defaultdict(list)
        )
        self._seen_event_ids: dict[str, set[str]] = defaultdict(set)
        self._closed: set[str] = set()
        self._timers: dict[str, asyncio.TimerHandle] = {}

    async def observe(self, event: Event) -> None:
        if event.type not in self.requirements or event.correlation_id in self._closed:
            return

        seen_event_ids = self._seen_event_ids[event.correlation_id]
        if event.event_id in seen_event_ids:
            return
        seen_event_ids.add(event.event_id)

        requirement = self.requirements[event.type]
        if requirement.predicate is not None and not requirement.predicate(event):
            return

        self._start_timer(event.correlation_id)
        by_type = self._events[event.correlation_id]
        by_type[event.type].append(event)
        if not all(
            len(by_type[event_type]) >= requirement.count
            for event_type, requirement in self.requirements.items()
        ):
            return

        correlation_id = event.correlation_id
        self._closed.add(correlation_id)
        self._cancel_timer(correlation_id)
        matched = self._matched(by_type)
        self._events.pop(correlation_id, None)
        self._seen_event_ids.pop(correlation_id, None)
        result = self.handler(RuleMatch(event.correlation_id, matched))
        if inspect.isawaitable(result):
            await result

    def on_timeout(self, handler: TimeoutHandler) -> "Rule":
        """Register the callback invoked when a tracked correlation expires."""

        self.timeout_handler = handler
        return self

    async def expire(self, correlation_id: str) -> bool:
        """Expire a correlation immediately; useful for deterministic tests."""

        if correlation_id in self._closed or correlation_id not in self._events:
            return False

        by_type = self._events.pop(correlation_id)
        self._seen_event_ids.pop(correlation_id, None)
        self._closed.add(correlation_id)
        self._cancel_timer(correlation_id)
        if self.timeout_handler is None:
            return True

        result = self.timeout_handler(
            RuleTimeout(correlation_id, self._matched(by_type))
        )
        if inspect.isawaitable(result):
            await result
        return True

    def _start_timer(self, correlation_id: str) -> None:
        if self.timeout is None or correlation_id in self._timers:
            return
        loop = asyncio.get_running_loop()
        self._timers[correlation_id] = loop.call_later(
            self.timeout, self._schedule_expiry, correlation_id
        )

    def _schedule_expiry(self, correlation_id: str) -> None:
        task = asyncio.create_task(self.expire(correlation_id))
        task.add_done_callback(self._handle_expiry_result)

    @staticmethod
    def _handle_expiry_result(task: asyncio.Task[object]) -> None:
        try:
            task.result()
        except Exception as error:
            asyncio.get_running_loop().call_exception_handler(
                {
                    "message": "Quorum rule timeout handler failed",
                    "exception": error,
                }
            )

    def _cancel_timer(self, correlation_id: str) -> None:
        timer = self._timers.pop(correlation_id, None)
        if timer is not None:
            timer.cancel()

    def _matched(self, by_type: Mapping[str, list[Event]]) -> tuple[Event, ...]:
        return tuple(
            item
            for event_type, requirement in self.requirements.items()
            for item in by_type.get(event_type, [])[: requirement.count]
        )


class RuleBuilder:
    def __init__(
        self,
        engine: "RuleEngine",
        requirements: Mapping[str, _Requirement],
        timeout: float | None,
    ) -> None:
        self._engine = engine
        self._requirements = requirements
        self._timeout = timeout

    def then(self, handler: RuleHandler) -> Rule:
        """Register the action that runs once the requirements are met."""

        return self._engine.add(self._requirements, handler, self._timeout)


class RuleEngine:
    def __init__(self, bus) -> None:
        self._rules: list[Rule] = []
        bus.subscribe("*", self._observe)

    def when(
        self,
        event_types: list[str] | tuple[str, ...],
        *,
        where: Mapping[str, Predicate] | None = None,
        timeout: float | None = None,
    ) -> RuleBuilder:
        if not event_types:
            raise ValueError("a rule needs at least one event type")
        self._validate_timeout(timeout)
        predicates = where or {}
        unknown = set(predicates) - set(event_types)
        if unknown:
            raise ValueError(
                f"predicates reference event types not in the rule: {sorted(unknown)}"
            )
        return RuleBuilder(
            self,
            {
                event_type: _Requirement(1, predicates.get(event_type))
                for event_type in event_types
            },
            timeout,
        )

    def when_count(
        self,
        event_type: str,
        count: int,
        *,
        where: Predicate | None = None,
        timeout: float | None = None,
    ) -> RuleBuilder:
        if count < 1:
            raise ValueError("count must be at least 1")
        self._validate_timeout(timeout)
        return RuleBuilder(self, {event_type: _Requirement(count, where)}, timeout)

    def add(
        self,
        requirements: Mapping[str, _Requirement],
        handler: RuleHandler,
        timeout: float | None,
    ) -> Rule:
        rule = Rule(requirements, handler, timeout)
        self._rules.append(rule)
        return rule

    @staticmethod
    def _validate_timeout(timeout: float | None) -> None:
        if timeout is not None and timeout <= 0:
            raise ValueError("timeout must be greater than zero")

    async def _observe(self, event: Event) -> None:
        await asyncio.gather(*(rule.observe(event) for rule in self._rules))
