import unittest

from quorum import Agent, Event, EventBus, EventDispatchError


class EventBusTests(unittest.IsolatedAsyncioTestCase):
    async def test_fan_out_and_trace(self):
        bus = EventBus()
        seen = []
        first = Agent("first", bus)
        second = Agent("second", bus)

        @first.on("goal.*")
        async def handle_first(event):
            seen.append((first.name, event.type))
            await first.emit("first.done", {})

        @second.on("goal.created")
        async def handle_second(event):
            seen.append((second.name, event.type))

        await first.start()
        await second.start()
        await bus.publish(Event("goal.created", {"value": 1}, correlation_id="run-1"))

        self.assertEqual(seen, [("first", "goal.created"), ("second", "goal.created")])
        self.assertEqual(
            [event.type for event in bus.trace("run-1")],
            ["goal.created", "first.done"],
        )
        self.assertEqual(bus.log[0].sequence, 1)
        self.assertEqual(bus.log[1].sequence, 2)
        self.assertEqual(bus.log[1].causation_id, bus.log[0].event_id)

    async def test_all_handlers_run_before_error_is_raised(self):
        bus = EventBus()
        seen = []

        async def failing(event):
            raise RuntimeError("boom")

        async def succeeding(event):
            seen.append(event.type)

        bus.subscribe("goal.created", failing)
        bus.subscribe("goal.created", succeeding)

        with self.assertRaises(EventDispatchError):
            await bus.publish("goal.created")

        self.assertEqual(seen, ["goal.created"])


if __name__ == "__main__":
    unittest.main()
