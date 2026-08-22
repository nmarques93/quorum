import asyncio
import unittest

from quorum import Agent, Event, EventBus, ManualClock, Supervisor


class SupervisorTests(unittest.IsolatedAsyncioTestCase):
    async def test_supervisor_flags_hung_and_recovers(self):
        clock = ManualClock()
        bus = EventBus(clock=clock)
        agent = Agent("worker", bus)
        hangs = []
        recoveries = []
        supervisor = Supervisor(
            bus,
            interval=1,
            timeout=2,
            on_hang=hangs.append,
            on_recovered=recoveries.append,
        )

        await agent.start()
        supervisor.watch(agent)
        await supervisor.start()

        await clock.advance(1)  # t=1, silent=1
        self.assertEqual(hangs, [])
        await clock.advance(1)  # t=2, silent=2
        self.assertEqual(hangs, [])
        await clock.advance(1)  # t=3, silent=3 > 2
        self.assertEqual(hangs, [agent])
        self.assertTrue(supervisor.is_hung(agent))

        agent.beat()
        await clock.advance(1)  # t=4, silent=1 <= 2
        self.assertEqual(recoveries, [agent])
        self.assertFalse(supervisor.is_hung(agent))

        await supervisor.stop()

    async def test_per_agent_timeout_override(self):
        clock = ManualClock()
        bus = EventBus(clock=clock)
        fast = Agent("fast", bus)
        slow = Agent("slow", bus)
        hangs = []
        supervisor = Supervisor(
            bus, interval=1, timeout=10, on_hang=hangs.append
        )

        await fast.start()
        await slow.start()
        supervisor.watch(fast, timeout=2)
        supervisor.watch(slow, timeout=10)
        await supervisor.start()

        await clock.advance(3)  # fast: silent=3 > 2; slow: silent=3 <= 10
        self.assertEqual(hangs, [fast])

        await supervisor.stop()

    async def test_unwatch_stops_monitoring(self):
        clock = ManualClock()
        bus = EventBus(clock=clock)
        agent = Agent("worker", bus)
        hangs = []
        supervisor = Supervisor(bus, interval=1, timeout=2, on_hang=hangs.append)

        await agent.start()
        supervisor.watch(agent)
        await supervisor.start()

        supervisor.unwatch(agent)
        await clock.advance(5)
        self.assertEqual(hangs, [])

        await supervisor.stop()

    async def test_async_callbacks_are_awaited(self):
        clock = ManualClock()
        bus = EventBus(clock=clock)
        agent = Agent("worker", bus)
        state = []

        async def on_hang(agent):
            state.append(agent.name)

        supervisor = Supervisor(bus, interval=1, timeout=2, on_hang=on_hang)
        await agent.start()
        supervisor.watch(agent)
        await supervisor.start()

        await clock.advance(3)
        self.assertEqual(state, ["worker"])
        await supervisor.stop()

    async def test_double_watch_and_invalid_args_rejected(self):
        bus = EventBus()
        agent = Agent("worker", bus)
        supervisor = Supervisor(bus, timeout=10)

        with self.assertRaises(ValueError):
            Supervisor(bus, interval=0)
        with self.assertRaises(ValueError):
            Supervisor(bus, timeout=-1)

        supervisor.watch(agent)
        with self.assertRaises(ValueError):
            supervisor.watch(agent)
        with self.assertRaises(ValueError):
            supervisor.watch(Agent("other", bus), timeout=0)

    async def test_agent_beats_on_emit(self):
        clock = ManualClock()
        bus = EventBus(clock=clock)
        agent = Agent("worker", bus)

        self.assertEqual(agent.last_beat, 0.0)
        await clock.advance(10)
        await agent.emit("job.done", {}, correlation_id="run-1")
        self.assertEqual(agent.last_beat, 10.0)

    async def test_agent_beats_on_dispatch(self):
        clock = ManualClock()
        bus = EventBus(clock=clock)
        agent = Agent("worker", bus)
        started = asyncio.Event()

        @agent.on("job.created")
        async def handle(event):
            started.set()
            await asyncio.sleep(1000)

        await agent.start()
        await clock.advance(10)
        task = asyncio.create_task(bus.publish(Event("job.created")))
        await started.wait()
        self.assertEqual(agent.last_beat, 10.0)

        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


if __name__ == "__main__":
    unittest.main()
