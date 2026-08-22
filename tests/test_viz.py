import unittest

from quorum import Agent, Event, EventBus
from quorum.viz import to_dot, to_mermaid


def _chain():
    return [
        Event("goal.created", {"q": "x"}, event_id="a", correlation_id="run-1"),
        Event(
            "finding.created",
            {"n": 1},
            event_id="b",
            causation_id="a",
            correlation_id="run-1",
        ),
        Event(
            "answer.created",
            {"a": "y"},
            event_id="c",
            causation_id="b",
            correlation_id="run-1",
        ),
    ]


class VizTests(unittest.IsolatedAsyncioTestCase):
    async def test_mermaid_renders_causal_edges(self):
        graph = to_mermaid(_chain())
        self.assertIn("flowchart LR", graph)
        self.assertIn('n0["goal.created"]', graph)
        self.assertIn("n0 --> n1", graph)
        self.assertIn("n1 --> n2", graph)

    async def test_dot_renders_causal_edges(self):
        graph = to_dot(_chain())
        self.assertIn("digraph quorum {", graph)
        self.assertIn("n0 -> n1;", graph)
        self.assertIn("n1 -> n2;", graph)

    async def test_trace_report_convenience_methods(self):
        bus = EventBus()
        first = Event("goal.created", event_id="a", correlation_id="run-1")
        await bus.publish(first)
        await bus.publish(
            Event(
                "finding.created",
                causation_id="a",
                correlation_id="run-1",
            )
        )

        report = bus.trace_report("run-1")
        self.assertIn("n0 --> n1", report.to_mermaid())
        self.assertIn("n0 -> n1;", report.to_dot())

    async def test_failed_events_are_marked(self):
        bus = EventBus()
        agent = Agent("worker", bus, retries=1)

        @agent.on("job.created")
        async def handle(event):
            raise RuntimeError("boom")

        await agent.start()
        try:
            await bus.publish(Event("job.created", correlation_id="run-1"))
        except Exception:
            pass

        graph = bus.trace_report("run-1").to_mermaid()
        self.assertIn("classDef failed", graph)


if __name__ == "__main__":
    unittest.main()
