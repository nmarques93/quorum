"""Small, in-process coordination rules for correlated events."""

from __future__ import annotations

import asyncio
import inspect
from collections import defaultdict
from dataclasses import dataclass
from typing import Awaitable, Callable

from .event import Event

RuleHandler = Callable[["RuleMatch"], Awaitable[object] | object]


@dataclass(frozen=True, slots=True)
class RuleMatch:
    """The events that satisfied one coordination rule."""

    correlation_id: str
    events: tuple[Event, ...]


class Rule:
    def __init__(self, requirements: dict[str, int], handler: RuleHandler) -> None:
        self.requirements = requirements
        self.handler = handler
        self._events: dict[str, dict[str, list[Event]]] = defaultdict(
            lambda: defaultdict(list)
        )
        self._fired: set[str] = set()

    async def observe(self, event: Event) -> None:
        if event.type not in self.requirements or event.correlation_id in self._fired:
            return

        by_type = self._events[event.correlation_id]
        by_type[event.type].append(event)
        if not all(
            len(by_type[event_type]) >= count
            for event_type, count in self.requirements.items()
        ):
            return

        self._fired.add(event.correlation_id)
        matched = tuple(
            item
            for event_type, count in self.requirements.items()
            for item in by_type[event_type][:count]
        )
        result = self.handler(RuleMatch(event.correlation_id, matched))
        if inspect.isawaitable(result):
            await result


class RuleBuilder:
    def __init__(self, engine: "RuleEngine", requirements: dict[str, int]) -> None:
        self._engine = engine
        self._requirements = requirements

    def then(self, handler: RuleHandler) -> Rule:
        """Register the action that runs once the requirements are met."""

        return self._engine.add(self._requirements, handler)


class RuleEngine:
    def __init__(self, bus) -> None:
        self._rules: list[Rule] = []
        bus.subscribe("*", self._observe)

    def when(self, event_types: list[str] | tuple[str, ...]) -> RuleBuilder:
        if not event_types:
            raise ValueError("a rule needs at least one event type")
        return RuleBuilder(self, {event_type: 1 for event_type in event_types})

    def when_count(self, event_type: str, count: int) -> RuleBuilder:
        if count < 1:
            raise ValueError("count must be at least 1")
        return RuleBuilder(self, {event_type: count})

    def add(self, requirements: dict[str, int], handler: RuleHandler) -> Rule:
        rule = Rule(requirements, handler)
        self._rules.append(rule)
        return rule

    async def _observe(self, event: Event) -> None:
        await asyncio.gather(*(rule.observe(event) for rule in self._rules))
