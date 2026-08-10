# Quorum

Quorum is a dependency-free Python library for coordinating asynchronous LLM agent collectives.

Agents react to events and emit new events instead of calling one another directly. This enables parallel work and dynamic coordination without encoding every workflow as a sequential chain or rigid graph. Rules and policies decide when enough evidence exists to trigger the next stage.

The first release is intentionally single-process and in-memory. It is an agent coordination runtime, not a replacement for Kafka or another durable distributed event log.

## Quickstart

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e .
python examples/basic.py
```

The example creates three independent researchers, waits for a quorum of two findings, and asks a synthesizer to produce an answer. The same event model can wrap LLM calls, tool use, validation, criticism, and human approval.

## Current Semantics

- Events are immutable envelopes with correlation and causation IDs.
- Matching handlers run concurrently in the current process.
- `publish()` waits for all matching handlers.
- A handler failure is raised after all matching handlers have run.
- Agents are inactive until `start()` and stop accepting events after `stop()`.
- Agent handlers support timeouts, retries with exponential backoff, and `agent.failed` diagnostics.
- A `JsonlEventLog` sink records every published event to a JSONL file independent of the in-memory log.
- `trace_report()` returns aggregated timing and error metadata for a task.
- `python -m quorum.tail` replays or watches JSONL log files.
- Rules fire once per correlation ID, can filter events with predicates, and ignore repeated delivery of the same event ID.
- Rules can have a timeout and an `on_timeout` callback for incomplete work.
- The in-memory log is diagnostic and is not durable.

These semantics are the initial contract and will be expanded only as concrete agent workflows require it.
