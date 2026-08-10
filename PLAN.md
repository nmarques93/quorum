# Quorum Plan

## Purpose

Quorum is an event-driven coordination runtime for asynchronous LLM agent collectives. It should let agents work in parallel and react to shared progress without forcing clients to encode every workflow as a sequential chain or rigid graph.

The library is not intended to replace Kafka. Kafka and similar systems provide distributed event transport and durable logs; Quorum provides the agent-facing runtime, event context, coordination policies, supervision, and testing model. The initial implementation is dependency-free, single-process, and in-memory.

## Product Thesis

Agents should communicate through events rather than direct calls. Independent researchers, critics, validators, tool users, and synthesizers can contribute results to a correlated task. Policies determine when enough evidence exists, when an agent should be retried or cancelled, and when a task is complete.

Event-driven coordination does not remove complexity. Quorum must make the transferred complexity explicit and controllable through correlation, causation, thresholds, deadlines, budgets, cancellation, quality gates, and observability.

## Initial Vertical Slice

- Immutable event envelope with event type, payload, event ID, source, timestamp, correlation ID, causation ID, and sequence.
- In-process async event bus with fan-out and shell-style wildcard subscriptions.
- Named agents that subscribe to events and automatically propagate causality when emitting.
- Explicit agent lifecycle with handler timeouts, retries, cancellation on stop, and failure events.
- Rules for all-of conditions and quorum counts scoped to a correlation ID.
- Predicate-aware rules for quality gates and duplicate-event protection during rule evaluation.
- Optional rule timeouts with partial-match callbacks and correlation cleanup.
- Causal trace for one logical task.
- Runnable example with parallel research agents and a synthesizer.
- Tests covering fan-out, failures, correlation, causation, ordering, and rules.
- JSONL event sink, enriched trace reports, and a CLI log watcher.

## Next Phases

### Phase 1: Define and Stabilize Semantics

- Decide handler concurrency, ordering, retries, duplicate delivery, cancellation, and shutdown behavior.
- Define the payload and serialization contract for LLM prompts, outputs, tool calls, and errors.
- Define task budgets, deadlines, quality thresholds, and termination policies.

### Phase 2: Agent Runtime

- Add health reporting.
- Add richer per-agent timeout, retry, cancellation, and failure policies.
- Add task context for budget, deadline, and LLM usage propagation.

### Phase 3: Coordination Policies

- Add richer thresholds and conflict handling beyond the current count, predicate, and timeout rules.
- Define transport-level duplicate and idempotency behavior beyond the current rule-level event-ID protection.
- Add expiry policies that prevent runaway loops and unbounded rule state.

### Phase 4: Observability and Testing

- Add LLM usage metadata to trace reports (prompt tokens, cached tokens, model latency).
- Add deterministic stepping and fake agent/provider utilities.

### Phase 5: Backend Abstraction

- Introduce a transport protocol only after in-process semantics are stable.
- Add a concrete distributed backend only for a real use case.
- Preserve explicit distinctions between delivery, persistence, replay, and coordination state.

### Phase 6: Packaging and Distribution

- Expand documentation with workflow patterns and tradeoffs.
- Add CI, changelog, release process, and private Git installation guidance.

## Out of Scope Initially

- A general LLM provider abstraction.
- Durable exactly-once processing.
- A distributed scheduler.
- Redis, NATS, or Kafka integrations without a concrete requirement.
- Replacing established workflow systems for workloads that require strict deterministic execution.
