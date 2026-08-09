import asyncio
import unittest

from quorum import Agent, Event, EventBus, EventDispatchError


class AgentTests(unittest.IsolatedAsyncioTestCase):
    async def test_handlers_are_inactive_until_started_and_stop_deactivates_them(self):
        bus = EventBus()
        agent = Agent("worker", bus)
        seen = []

        @agent.on("job.created")
        async def handle(event):
            seen.append(event.type)

        await bus.publish(Event("job.created"))
        self.assertEqual(seen, [])

        await agent.start()
        await bus.publish(Event("job.created"))
        self.assertEqual(seen, ["job.created"])

        await agent.stop()
        await bus.publish(Event("job.created"))
        self.assertEqual(seen, ["job.created"])

    async def test_handler_retries_and_succeeds(self):
        bus = EventBus()
        agent = Agent("worker", bus, retries=1)
        attempts = []

        @agent.on("job.created")
        async def handle(event):
            attempts.append(event.type)
            if len(attempts) == 1:
                raise RuntimeError("temporary failure")

        await agent.start()
        await bus.publish(Event("job.created"))
        self.assertEqual(attempts, ["job.created", "job.created"])

    async def test_exhausted_handler_emits_failure_event_and_raises(self):
        bus = EventBus()
        agent = Agent("worker", bus, retries=1)
        failures = []
        bus.subscribe("agent.failed", failures.append)

        @agent.on("job.created")
        async def handle(event):
            raise RuntimeError("permanent failure")

        await agent.start()
        with self.assertRaises(EventDispatchError):
            await bus.publish(Event("job.created", correlation_id="run-1"))

        self.assertEqual(len(failures), 1)
        self.assertEqual(failures[0].payload["attempts"], 2)
        self.assertEqual(failures[0].correlation_id, "run-1")
        self.assertEqual(failures[0].causation_id, bus.log[0].event_id)

    async def test_handler_timeout_is_retried(self):
        bus = EventBus()
        agent = Agent("worker", bus, timeout=0.01, retries=1)
        attempts = []

        @agent.on("job.created")
        async def handle(event):
            attempts.append(event.type)
            await asyncio.sleep(1)

        await agent.start()
        with self.assertRaises(EventDispatchError):
            await bus.publish(Event("job.created"))

        self.assertEqual(len(attempts), 2)


if __name__ == "__main__":
    unittest.main()
