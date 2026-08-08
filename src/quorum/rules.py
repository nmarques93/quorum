"""Small, in-process coordination rules for correlated events."""

from __future__ import annotations

import asyncio
import inspect
from collections import defaultdict
from dataclasses import dataclass
from typing import Awaitable, Callable, Mapping

from .event import Event

RuleHandler = Callable[["RuleMatch"], Awaitable[object] | object]
Predicate = Callable[[Event], bool]


@dataclass(frozen=True, slots=True)
class RuleMatch:
    """The events that satisfied one coordination rule."""

    correlation_id: str
    events: tuple[Event, ...]


@dataclass(frozen=True, slots=True)
class _Requirement:
    count: int
    predicate: Predicate | None = None


class Rule:
    def __init__(
        self, requirements: Mapping[str, _Requirement], handler: RuleHandler
    ) -> None:
        self.requirements = requirements
        self.handler = handler
        self._events: dict[str, dict[str, list[Event]]] = defaultdict(
            lambda: defaultdict(list)
        )
        self._seen_event_ids: dict[str, set[str]] = defaultdict(set)
        self._fired: set[str] = set()

    async def observe(self, event: Event) -> None:
        if event.type not in self.requirements or event.correlation_id in self._fired:
            return

        seen_event_ids = self._seen_event_ids[event.correlation_id]
        if event.event_id in seen_event_ids:
            return
        seen_event_ids.add(event.event_id)

        requirement = self.requirements[event.type]
        if requirement.predicate is not None and not requirement.predicate(event):
            return

        by_type = self._events[event.correlation_id]
        by_type[event.type].append(event)
        if not all(
            len(by_type[event_type]) >= requirement.count
            for event_type, requirement in self.requirements.items()
        ):
            return

        self._fired.add(event.correlation_id)
        matched = tuple(
            item
            for event_type, requirement in self.requirements.items()
            for item in by_type[event_type][: requirement.count]
        )
        result = self.handler(RuleMatch(event.correlation_id, matched))
        if inspect.isawaitable(result):
            await result


class RuleBuilder:
    def __init__(
        self, engine: "RuleEngine", requirements: Mapping[str, _Requirement]
    ) -> None:
        self._engine = engine
        self._requirements = requirements

    def then(self, handler: RuleHandler) -> Rule:
        """Register the action that runs once the requirements are met."""

        return self._engine.add(self._requirements, handler)


class RuleEngine:
    def __init__(self, bus) -> None:
        self._rules: list[Rule] = []
        bus.subscribe("*", self._observe)

    def when(
        self,
        event_types: list[str] | tuple[str, ...],
        *,
        where: Mapping[str, Predicate] | None = None,
    ) -> RuleBuilder:
        if not event_types:
            raise ValueError("a rule needs at least one event type")
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
        )

    def when_count(
        self,
        event_type: str,
        count: int,
        *,
        where: Predicate | None = None,
    ) -> RuleBuilder:
        if count < 1:
            raise ValueError("count must be at least 1")
        return RuleBuilder(self, {event_type: _Requirement(count, where)})

    def add(
        self, requirements: Mapping[str, _Requirement], handler: RuleHandler
    ) -> Rule:
        rule = Rule(requirements, handler)
        self._rules.append(rule)
        return rule

    async def _observe(self, event: Event) -> None:
        await asyncio.gather(*(rule.observe(event) for rule in self._rules))
