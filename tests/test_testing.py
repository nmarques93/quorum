import asyncio
import unittest

from quorum import Agent, Event, EventBus, EventDispatchError, ManualClock


class ManualClockTests(unittest.IsolatedAsyncioTestCase):
    async def test_time_does_not_move_without_advance(self):
        clock = ManualClock()
        self.assertEqual(clock.now(), 0.0)
        await asyncio.sleep(0)
        self.assertEqual(clock.now(), 0.0)

    async def test_advance_fires_timers_in_deadline_order(self):
        clock = ManualClock()
        fired = []
        clock.call_later(5, lambda: fired.append("late"))
        clock.call_later(1, lambda: fired.append("early"))
        await clock.advance(5)
        self.assertEqual(fired, ["early", "late"])
        self.assertEqual(clock.now(), 5.0)

    async def test_advance_skips_cancelled_timers(self):
        clock = ManualClock()
        fired = []
        handle = clock.call_later(1, lambda: fired.append("kept"))
        clock.call_later(2, lambda: fired.append("dropped"))
        handle.cancel()
        await clock.advance(3)
        self.assertEqual(fired, ["dropped"])
        self.assertEqual(clock.now(), 3.0)

    async def test_sleep_resolves_only_when_advanced(self):
        clock = ManualClock()
        state = []

        async def sleeper():
            await clock.sleep(10)
            state.append("awake")

        task = asyncio.create_task(sleeper())
        await asyncio.sleep(0)
        self.assertEqual(state, [])
        await clock.advance(10)
        await task
        self.assertEqual(state, ["awake"])


class DeterministicExecutionTests(unittest.IsolatedAsyncioTestCase):
    async def test_agent_timeout_is_advanced_without_real_sleep(self):
        clock = ManualClock()
        bus = EventBus(clock=clock)
        agent = Agent("worker", bus, timeout=5)
        started = asyncio.Event()

        @agent.on("job.created")
        async def handle(event):
            started.set()
            await asyncio.sleep(1000)

        await agent.start()
        task = asyncio.create_task(
            bus.publish(Event("job.created", correlation_id="run-1"))
        )
        await started.wait()
        await clock.advance(5)

        with self.assertRaises(EventDispatchError):
            await task

    async def test_agent_retry_backoff_uses_manual_clock(self):
        clock = ManualClock()
        bus = EventBus(clock=clock)
        agent = Agent("worker", bus, retries=1, retry_delay=2)
        failed_once = asyncio.Event()
        attempts = []

        @agent.on("job.created")
        async def handle(event):
            attempts.append(event.type)
            if len(attempts) == 1:
                failed_once.set()
                raise RuntimeError("retry me")

        await agent.start()
        task = asyncio.create_task(
            bus.publish(Event("job.created", correlation_id="run-1"))
        )
        await failed_once.wait()
        self.assertEqual(len(attempts), 1)
        await clock.advance(2)
        await task
        self.assertEqual(len(attempts), 2)

    async def test_rule_timeout_fires_deterministically(self):
        clock = ManualClock()
        bus = EventBus(clock=clock)
        timeouts = []
        bus.when_count("finding.created", 2, timeout=5).then(
            lambda match: None
        ).on_timeout(timeouts.append)

        await bus.publish(Event("finding.created", {"n": 1}, correlation_id="one"))
        self.assertEqual(timeouts, [])
        await clock.advance(5)
        await asyncio.sleep(0)
        self.assertEqual(len(timeouts), 1)
        self.assertEqual([event.payload["n"] for event in timeouts[0].events], [1])


if __name__ == "__main__":
    unittest.main()
