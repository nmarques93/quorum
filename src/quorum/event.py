"""The event envelope shared by all Quorum components."""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from types import MappingProxyType
from typing import Any, Mapping
from uuid import uuid4


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True, slots=True)
class Event:
    """An immutable message describing something that happened or is requested."""

    type: str
    payload: Mapping[str, Any] = field(default_factory=dict)
    correlation_id: str = field(default_factory=lambda: uuid4().hex)
    causation_id: str | None = None
    source: str | None = None
    event_id: str = field(default_factory=lambda: uuid4().hex)
    timestamp: datetime = field(default_factory=_utc_now)
    sequence: int | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "payload", MappingProxyType(dict(self.payload)))

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation of the event envelope."""

        return {
            "type": self.type,
            "payload": dict(self.payload),
            "correlation_id": self.correlation_id,
            "causation_id": self.causation_id,
            "source": self.source,
            "event_id": self.event_id,
            "timestamp": self.timestamp.isoformat(),
            "sequence": self.sequence,
        }
