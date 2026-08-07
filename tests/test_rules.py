import unittest

from quorum import Event, EventBus


class RuleTests(unittest.IsolatedAsyncioTestCase):
    async def test_rule_fires_once_for_a_correlation_in_any_order(self):
        bus = EventBus()
        matches = []

        async def handle(match):
            matches.append(match)

        bus.when(["research.complete", "outline.done"]).then(handle)
        await bus.publish(Event("outline.done", correlation_id="one"))
        await bus.publish(Event("research.complete", correlation_id="one"))
        await bus.publish(Event("research.complete", correlation_id="one"))
        await bus.publish(Event("research.complete", correlation_id="two"))

        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0].correlation_id, "one")
        self.assertEqual(
            {event.type for event in matches[0].events},
            {"research.complete", "outline.done"},
        )

    async def test_count_rule_waits_for_a_quorum(self):
        bus = EventBus()
        matches = []
        bus.when_count("finding.created", 2).then(matches.append)

        await bus.publish(Event("finding.created", {"n": 1}, correlation_id="one"))
        await bus.publish(Event("finding.created", {"n": 2}, correlation_id="two"))
        await bus.publish(Event("finding.created", {"n": 3}, correlation_id="one"))

        self.assertEqual(len(matches), 1)
        self.assertEqual(
            [event.payload["n"] for event in matches[0].events],
            [1, 3],
        )


if __name__ == "__main__":
    unittest.main()
