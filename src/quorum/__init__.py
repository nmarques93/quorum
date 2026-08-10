"""Event-driven coordination primitives for async agent collectives."""

from .agent import Agent
from .bus import EventBus, EventDispatchError, TraceReport
from .event import Event
from .rules import RuleMatch, RuleTimeout
from .sink import JsonlEventLog

__all__ = [
    "Agent",
    "Event",
    "EventBus",
    "EventDispatchError",
    "JsonlEventLog",
    "RuleMatch",
    "RuleTimeout",
    "TraceReport",
]
