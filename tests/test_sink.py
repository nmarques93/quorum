import json
import tempfile
import unittest
from pathlib import Path

from quorum import Agent, Event, EventBus, JsonlEventLog


class SinkTests(unittest.IsolatedAsyncioTestCase):
    async def test_jsonl_sink_writes_events_to_file(self):
        bus = EventBus()
        with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as tmp:
            path = Path(tmp.name)

        try:
            sink = JsonlEventLog(path)
            bus.add_sink(sink)

            await bus.publish(
                Event("job.created", {"work": 1}, correlation_id="run-1")
            )
            await sink.flush()

            lines = path.read_text().strip().splitlines()
            self.assertEqual(len(lines), 1)
            event = json.loads(lines[0])
            self.assertEqual(event["type"], "job.created")
            self.assertEqual(event["correlation_id"], "run-1")
            self.assertEqual(event["payload"]["work"], 1)
        finally:
            path.unlink(missing_ok=True)

    async def test_jsonl_sink_captures_events_regardless_of_handler_errors(self):
        bus = EventBus()
        with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as tmp:
            path = Path(tmp.name)

        try:
            sink = JsonlEventLog(path)
            bus.add_sink(sink)

            agent = Agent("worker", bus, retries=1)

            @agent.on("job.created")
            async def handle(event):
                raise RuntimeError("boom")

            await agent.start()

            try:
                await bus.publish(
                    Event("job.created", {"work": 1}, correlation_id="run-1")
                )
            except Exception:
                pass

            await sink.flush()
            lines = path.read_text().strip().splitlines()
            self.assertEqual(len(lines), 2)  # job.created + agent.failed
            types = [json.loads(line)["type"] for line in lines]
            self.assertIn("job.created", types)
            self.assertIn("agent.failed", types)
        finally:
            path.unlink(missing_ok=True)

    async def test_sink_removal_stops_writing(self):
        bus = EventBus()
        with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as tmp:
            path = Path(tmp.name)

        try:
            sink = JsonlEventLog(path)
            remove = bus.add_sink(sink)

            await bus.publish(Event("first.item", correlation_id="run-1"))
            remove()
            await bus.publish(Event("second.item", correlation_id="run-1"))
            await sink.flush()

            lines = path.read_text().strip().splitlines()
            self.assertEqual(len(lines), 1)
            self.assertEqual(json.loads(lines[0])["type"], "first.item")
        finally:
            path.unlink(missing_ok=True)

    async def test_buffer_delays_write_until_threshold(self):
        bus = EventBus()
        with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as tmp:
            path = Path(tmp.name)

        try:
            sink = JsonlEventLog(path, buffer_size=3)
            bus.add_sink(sink)

            await bus.publish(Event("a", {}, correlation_id="run-1"))
            await bus.publish(Event("b", {}, correlation_id="run-1"))
            content = path.read_text().strip()
            self.assertEqual(
                content, "", "file should be empty before buffer threshold"
            )

            await bus.publish(Event("c", {}, correlation_id="run-1"))
            await sink.flush()
            self.assertEqual(len(path.read_text().strip().splitlines()), 3)
        finally:
            path.unlink(missing_ok=True)

    async def test_trace_report_aggregates_timing_and_errors(self):
        bus = EventBus()

        agent = Agent("worker", bus, retries=1)

        @agent.on("job.created")
        async def handle(event):
            raise RuntimeError("fatal")

        await agent.start()
        try:
            await bus.publish(Event("job.created", {"v": 1}, correlation_id="run-1"))
        except Exception:
            pass

        report = bus.trace_report("run-1")
        self.assertEqual(report.correlation_id, "run-1")
        self.assertGreaterEqual(len(report.events), 2)
        self.assertGreaterEqual(len(report.errors), 1)
        self.assertIsNotNone(report.first_at)
        self.assertIsNotNone(report.last_at)
        self.assertIsNotNone(report.duration)


if __name__ == "__main__":
    unittest.main()
