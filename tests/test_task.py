import asyncio
import unittest

from quorum import Agent, Event, EventBus, ManualClock


class TaskContextTests(unittest.IsolatedAsyncioTestCase):
    async def test_start_task_registers_context_and_budget(self):
        bus = EventBus()
        context = bus.start_task("run-1", deadline=30, budget={"tokens": 10000})
        self.assertEqual(context.correlation_id, "run-1")
        self.assertEqual(context.budget["tokens"], 10000)
        self.assertFalse(context.cancelled)
        self.assertIs(bus.task_context("run-1"), context)
        self.assertIsNone(bus.task_context("missing"))

    async def test_start_task_rejects_duplicate_and_bad_deadline(self):
        bus = EventBus()
        bus.start_task("run-1")
        with self.assertRaises(ValueError):
            bus.start_task("run-1")
        with self.assertRaises(ValueError):
            bus.start_task("run-2", deadline=0)

    async def test_handler_reads_current_task_context(self):
        bus = EventBus()
        bus.start_task("run-1", deadline=100, budget={"cost": 5})
        seen = {}

        bus.subscribe(
            "job.created",
            lambda event: seen.update(
                {"remaining": bus.remaining_time(), "cost": bus.current_task_context.budget["cost"]}
            ),
        )

        await bus.publish(Event("job.created", correlation_id="run-1"))

        self.assertIsNotNone(seen["remaining"])
        self.assertEqual(seen["cost"], 5)

    async def test_current_task_context_is_none_without_a_task(self):
        bus = EventBus()
        seen = []
        bus.subscribe("job.created", lambda event: seen.append(bus.current_task_context))
        await bus.publish(Event("job.created", correlation_id="run-1"))
        self.assertEqual(seen, [None])

    async def test_cancel_cancels_inflight_handlers(self):
        bus = EventBus()
        bus.start_task("run-1")
        agent = Agent("worker", bus)
        started = asyncio.Event()

        @agent.on("job.created")
        async def handle(event):
            started.set()
            await asyncio.sleep(1000)

        await agent.start()
        task = asyncio.create_task(bus.publish(Event("job.created", correlation_id="run-1")))
        await started.wait()
        self.assertTrue(bus.cancel("run-1"))

        with self.assertRaises(asyncio.CancelledError):
            await task

        self.assertTrue(bus.task_context("run-1").cancelled)

    async def test_cancel_returns_false_when_task_missing_or_done(self):
        bus = EventBus()
        self.assertFalse(bus.cancel("missing"))

    async def test_deadline_cancels_task_deterministically(self):
        clock = ManualClock()
        bus = EventBus(clock=clock)
        bus.start_task("run-1", deadline=5)
        agent = Agent("worker", bus)
        started = asyncio.Event()

        @agent.on("job.created")
        async def handle(event):
            started.set()
            await asyncio.sleep(1000)

        await agent.start()
        task = asyncio.create_task(bus.publish(Event("job.created", correlation_id="run-1")))
        await started.wait()
        self.assertFalse(bus.task_context("run-1").cancelled)

        await clock.advance(5)

        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertTrue(bus.task_context("run-1").cancelled)

    async def test_cancel_cleans_up_pending_rules(self):
        bus = EventBus()
        bus.start_task("run-1")
        matches = []
        timeouts = []
        bus.when_count("finding.created", 2, timeout=30).then(matches.append).on_timeout(
            timeouts.append
        )

        await bus.publish(Event("finding.created", {"n": 1}, correlation_id="run-1"))
        bus.cancel("run-1")
        await bus.publish(Event("finding.created", {"n": 2}, correlation_id="run-1"))

        self.assertEqual(matches, [])
        self.assertEqual(timeouts, [])


if __name__ == "__main__":
    unittest.main()
