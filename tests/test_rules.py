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

    async def test_count_rule_can_filter_low_quality_events_and_deduplicate(self):
        bus = EventBus()
        matches = []
        bus.when_count(
            "finding.created",
            2,
            where=lambda event: event.payload["quality"] >= 0.8,
        ).then(matches.append)

        low_quality = Event(
            "finding.created",
            {"quality": 0.2},
            correlation_id="one",
        )
        high_quality = Event(
            "finding.created",
            {"quality": 0.9},
            correlation_id="one",
        )
        await bus.publish(low_quality)
        await bus.publish(high_quality)
        await bus.publish(high_quality)
        self.assertEqual(matches, [])

        await bus.publish(
            Event("finding.created", {"quality": 0.95}, correlation_id="one")
        )

        self.assertEqual(len(matches), 1)
        self.assertEqual(
            [event.payload["quality"] for event in matches[0].events],
            [0.9, 0.95],
        )

    async def test_all_rule_rejects_predicates_for_unknown_event_types(self):
        bus = EventBus()

        with self.assertRaises(ValueError):
            bus.when(
                ["research.complete"],
                where={"outline.done": lambda event: True},
            )

    async def test_rule_can_expire_an_incomplete_correlation(self):
        bus = EventBus()
        matches = []
        timeouts = []
        rule = (
            bus.when_count("finding.created", 2, timeout=60)
            .then(matches.append)
            .on_timeout(timeouts.append)
        )

        await bus.publish(Event("finding.created", {"n": 1}, correlation_id="one"))
        self.assertTrue(await rule.expire("one"))
        self.assertEqual(matches, [])
        self.assertEqual([event.payload["n"] for event in timeouts[0].events], [1])

        await bus.publish(Event("finding.created", {"n": 2}, correlation_id="one"))
        self.assertEqual(matches, [])
        self.assertFalse(await rule.expire("one"))

    async def test_rule_rejects_non_positive_timeout(self):
        bus = EventBus()

        with self.assertRaises(ValueError):
            bus.when(["finding.created"], timeout=0)


if __name__ == "__main__":
    unittest.main()
