"""Render causal event traces as DOT or Mermaid graphs."""

from __future__ import annotations

from typing import Iterable

from .event import Event

_FAILED_TYPE = "agent.failed"


def _quote(text: str) -> str:
    return text.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def _nodes_and_edges(
    events: Iterable[Event],
) -> tuple[list[Event], list[tuple[int, int]]]:
    events = list(events)
    by_id = {event.event_id: index for index, event in enumerate(events)}
    edges: list[tuple[int, int]] = []
    for index, event in enumerate(events):
        cause = (
            by_id.get(event.causation_id)
            if event.causation_id is not None
            else None
        )
        if cause is not None:
            edges.append((cause, index))
    return events, edges


def to_mermaid(events: Iterable[Event], *, direction: str = "LR") -> str:
    """Render a Mermaid flowchart of the causal chain."""

    events, edges = _nodes_and_edges(events)
    lines = [f"flowchart {direction}"]
    failed = []
    for index, event in enumerate(events):
        lines.append(f'  n{index}["{_quote(event.type)}"]')
        if event.type == _FAILED_TYPE:
            failed.append(index)
    for cause, effect in edges:
        lines.append(f"  n{cause} --> n{effect}")
    if failed:
        lines.append("  classDef failed fill:#f9d1d1,stroke:#c0392b")
        for index in failed:
            lines.append(f"  class n{index} failed")
    return "\n".join(lines)


def to_dot(events: Iterable[Event], *, name: str = "quorum") -> str:
    """Render a Graphviz DOT digraph of the causal chain."""

    events, edges = _nodes_and_edges(events)
    lines = [f"digraph {name} {{", "  rankdir=LR;"]
    for index, event in enumerate(events):
        label = _quote(event.type)
        if event.type == _FAILED_TYPE:
            lines.append(
                f'  n{index} [label="{label}", shape=box, style=filled, '
                'fillcolor="#f9d1d1"];'
            )
        else:
            lines.append(f'  n{index} [label="{label}", shape=box];')
    for cause, effect in edges:
        lines.append(f"  n{cause} -> n{effect};")
    lines.append("}")
    return "\n".join(lines)
