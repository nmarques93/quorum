import unittest

from quorum import Agent, Event, EventBus, EventDispatchError


class DeadLetterTests(unittest.IsolatedAsyncioTestCase):
    async def test_dead_letter_emitted_on_permanent_failure(self):
        bus = EventBus()
        dead = []
        bus.subscribe("event.deadlettered", dead.append)
        agent = Agent("worker", bus, retries=1, dead_letter=True)

        @agent.on("job.created")
        async def handle(event):
            raise RuntimeError("boom")

        await agent.start()
        with self.assertRaises(EventDispatchError):
            await bus.publish(Event("job.created", {"n": 1}, correlation_id="run-1"))

        self.assertEqual(len(dead), 1)
        self.assertEqual(dead[0].type, "event.deadlettered")
        self.assertEqual(dead[0].payload["event"]["type"], "job.created")
        self.assertEqual(dead[0].payload["event"]["payload"], {"n": 1})
        self.assertEqual(dead[0].payload["error_type"], "RuntimeError")
        self.assertEqual(dead[0].payload["attempts"], 2)
        self.assertEqual(dead[0].correlation_id, "run-1")
        self.assertEqual(dead[0].causation_id, bus.log[0].event_id)

    async def test_custom_dead_letter_type(self):
        bus = EventBus()
        dead = []
        bus.subscribe("my.dlq", dead.append)
        agent = Agent("worker", bus, dead_letter="my.dlq")

        @agent.on("job.created")
        async def handle(event):
            raise RuntimeError("boom")

        await agent.start()
        with self.assertRaises(EventDispatchError):
            await bus.publish(Event("job.created", correlation_id="run-1"))

        self.assertEqual(len(dead), 1)
        self.assertEqual(dead[0].type, "my.dlq")

    async def test_no_dead_letter_by_default(self):
        bus = EventBus()
        dead = []
        bus.subscribe("event.deadlettered", dead.append)
        agent = Agent("worker", bus)

        @agent.on("job.created")
        async def handle(event):
            raise RuntimeError("boom")

        await agent.start()
        with self.assertRaises(EventDispatchError):
            await bus.publish(Event("job.created", correlation_id="run-1"))

        self.assertEqual(dead, [])


if __name__ == "__main__":
    unittest.main()
