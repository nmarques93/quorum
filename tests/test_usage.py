import unittest

from quorum import Agent, Event, EventBus


class UsageTests(unittest.IsolatedAsyncioTestCase):
    async def test_event_usage_roundtrips_through_dict(self):
        event = Event(
            "finding.created",
            {"finding": "x"},
            usage={"prompt_tokens": 100, "completion_tokens": 50},
        )
        as_dict = event.to_dict()
        self.assertEqual(as_dict["usage"]["prompt_tokens"], 100)
        self.assertEqual(as_dict["usage"]["completion_tokens"], 50)

    async def test_emit_propagates_usage(self):
        bus = EventBus()
        agent = Agent("worker", bus)

        await agent.emit(
            "finding.created",
            {"finding": "x"},
            correlation_id="run-1",
            usage={"prompt_tokens": 10, "cost_usd": 0.001},
        )

        published = bus.log[-1]
        self.assertEqual(published.usage["prompt_tokens"], 10)
        self.assertEqual(published.usage["cost_usd"], 0.001)

    async def test_publish_string_event_accepts_usage(self):
        bus = EventBus()
        await bus.publish(
            "job.created",
            {"work": 1},
            correlation_id="run-1",
            usage={"latency_ms": 42},
        )
        self.assertEqual(bus.log[-1].usage["latency_ms"], 42)

    async def test_trace_report_aggregates_usage(self):
        bus = EventBus()
        await bus.publish(
            "finding.created",
            {"n": 1},
            correlation_id="run-1",
            usage={"prompt_tokens": 100, "completion_tokens": 30},
        )
        await bus.publish(
            "finding.created",
            {"n": 2},
            correlation_id="run-1",
            usage={"prompt_tokens": 150, "completion_tokens": 70},
        )

        report = bus.trace_report("run-1")
        self.assertEqual(report.total_usage["prompt_tokens"], 250)
        self.assertEqual(report.total_usage["completion_tokens"], 100)

    async def test_trace_report_remaining_budget(self):
        bus = EventBus()
        bus.start_task("run-1", budget={"tokens": 1000, "cost_usd": 1.0})
        await bus.publish(
            "finding.created",
            {"n": 1},
            correlation_id="run-1",
            usage={"tokens": 400, "cost_usd": 0.25},
        )

        report = bus.trace_report("run-1")
        self.assertEqual(report.remaining_budget["tokens"], 600)
        self.assertEqual(report.remaining_budget["cost_usd"], 0.75)

    async def test_usage_ignores_non_numeric_values(self):
        bus = EventBus()
        await bus.publish(
            "finding.created",
            {"n": 1},
            correlation_id="run-1",
            usage={"tokens": 100, "model": "gpt-4"},
        )
        self.assertEqual(bus.trace_report("run-1").total_usage["tokens"], 100)
        self.assertNotIn("model", bus.trace_report("run-1").total_usage)


if __name__ == "__main__":
    unittest.main()
