"""Observability sinks that persist or inspect event streams."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

from .event import Event


class JsonlEventLog:
    """Append every published event as one JSON line to a file.

    Lines are flushed eagerly by default so that external watchers such as
    ``python -m quorum.tail`` always see the latest event. Pass
    ``buffer_size`` > 1 to batch writes at the cost of delayed
    observability.
    """

    def __init__(
        self,
        path: str | Path,
        *,
        buffer_size: int = 1,
        serializer: Any | None = None,
    ) -> None:
        self._path = Path(path)
        self._buffer_size = max(1, buffer_size)
        self._pending: list[str] = []
        self._lock = asyncio.Lock()
        self._serializer = serializer if serializer is not None else _default_serialize

    async def __call__(self, event: Event) -> None:
        async with self._lock:
            self._pending.append(self._serializer(event) + "\n")
            if len(self._pending) >= self._buffer_size:
                await self._flush()

    async def flush(self) -> None:
        async with self._lock:
            await self._flush()

    async def close(self) -> None:
        await self.flush()

    async def _flush(self) -> None:
        if not self._pending:
            return
        with open(self._path, "a") as target:
            target.writelines(self._pending)
        self._pending.clear()


def _default_serialize(event: Event) -> str:
    return json.dumps(event.to_dict())
