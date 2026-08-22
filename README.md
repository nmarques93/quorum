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

## Example Workflow

```python
import asyncio
from quorum import Agent, Event, EventBus

async def main():
    bus = EventBus()
    researcher = Agent("researcher", bus, timeout=60, retries=2)
    synthesizer = Agent("synthesizer", bus)

    @researcher.on("goal.created")
    async def research(event):
        # wrap your own model/tool call here
        await researcher.emit(
            "finding.created",
            {"finding": "..."},
            usage={"prompt_tokens": 120, "completion_tokens": 30, "cost_usd": 0.002},
        )

    @synthesizer.on("synthesis.requested")
    async def synthesize(event):
        await synthesizer.emit("answer.created", {"answer": "..."})

    async def request_synthesis(match):
        await synthesizer.emit(
            "synthesis.requested",
            {"findings": [e.payload for e in match.events]},
            correlation_id=match.correlation_id,
        )

    bus.when_count("finding.created", 3).then(request_synthesis)

    await asyncio.gather(researcher.start(), synthesizer.start())
    await bus.publish(Event("goal.created", {"question": "..."}, correlation_id="run-1"))

    print(bus.trace_report("run-1").total_usage)

asyncio.run(main())
```

Agents never call each other directly. They emit events, and rules decide when the next stage runs. Usage attached to events (tokens, cost, latency) is aggregated by `trace_report`, and a registered task budget surfaces in `remaining_budget`.

## Deterministic Testing

Inject a `ManualClock` so timeouts, retries, and rule expiry never rely on wall-clock sleeps:

```python
import asyncio
from quorum import Agent, Event, EventBus, ManualClock

clock = ManualClock()
bus = EventBus(clock=clock)
agent = Agent("worker", bus, timeout=5)

started = asyncio.Event()

@agent.on("job.created")
async def handle(event):
    started.set()
    await asyncio.sleep(1000)  # a long, real sleep

await agent.start()
task = asyncio.create_task(bus.publish(Event("job.created")))
await started.wait()     # handler is now running
await clock.advance(5)   # fire the timeout without waiting 5 real seconds
```

With pytest, `quorum.testing` provides `clock`, `bus`, and `agent_factory` fixtures.

## Task Context and Cancellation

Register a logical task with a deadline and budget, then cancel it — or let the deadline cancel it automatically:

```python
bus.start_task("run-1", deadline=60, budget={"tokens": 50000})

@agent.on("job.created")
async def handle(event):
    ctx = bus.current_task_context
    print(ctx.budget["tokens"], bus.remaining_time())

bus.cancel("run-1")  # cancels in-flight handlers for run-1
```

Inside a handler, `bus.current_task_context` exposes the deadline, budget, and cancellation state; `bus.remaining_time()` returns seconds until the deadline. A positive `deadline` schedules cancellation via the injected clock, so tests advance it deterministically.

## Supervision

Watch agents and react when one stops beating its heartbeat:

```python
supervisor = Supervisor(
    bus,
    interval=1.0,
    timeout=30.0,
    on_hang=lambda agent: alert(agent.name),       # restart, page, etc.
    on_recovered=lambda agent: log(agent.name),
)

supervisor.watch(researcher, timeout=60)   # per-agent override
await supervisor.start()
```

Agents beat automatically when they handle or emit events, and `agent.beat()` records an explicit heartbeat. An agent that goes silent longer than its `timeout` triggers `on_hang`; the next beat triggers `on_recovered`.

## Dead-Letter Routing

When a handler fails permanently, route the original event somewhere for inspection, replay, or alerting:

```python
agent = Agent("worker", bus, retries=2, dead_letter=True)  # -> event.deadlettered
bus.subscribe("event.deadlettered", send_to_ops)
```

The dead-letter event carries the full original event (`payload["event"]`), the failure type, message, and attempt count. Pass `dead_letter="my.dlq"` to use a custom type.

## Trace Visualization

Render the causal chain as Mermaid or DOT:

```python
print(bus.trace_report("run-1").to_mermaid())
print(bus.trace_report("run-1").to_dot())
```

Failed events (`agent.failed`) are highlighted.

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
- A `ManualClock` can be injected via `EventBus(clock=...)` so timeouts, retries, and rule expiry advance deterministically in tests.
- `quorum.testing` provides pytest fixtures (`clock`, `bus`, `agent_factory`) and a `run_until_quiescent` helper.
- Tasks can be registered with a deadline and budget, cancelled on demand, and auto-cancelled when the deadline expires.
- Events carry optional usage metrics (tokens, cost, latency) that `trace_report` aggregates against the task budget.
- A `Supervisor` watches agent heartbeats and fires `on_hang`/`on_recovered` when an agent stalls.
- Agents can dead-letter permanently-failed work to a configurable event type carrying the original event.
- Traces can be rendered as DOT or Mermaid causal graphs.
- Rules fire once per correlation ID, can filter events with predicates, and ignore repeated delivery of the same event ID.
- Rules can have a timeout and an `on_timeout` callback for incomplete work.
- The in-memory log is diagnostic and is not durable.

These semantics are the initial contract and will be expanded only as concrete agent workflows require it.
