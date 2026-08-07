"""A small parallel research collective using Quorum."""

import asyncio

from quorum import Agent, Event, EventBus, RuleMatch


async def main() -> None:
    bus = EventBus()
    researchers = [Agent(f"researcher-{index}", bus) for index in range(3)]
    synthesizer = Agent("synthesizer", bus)

    for researcher in researchers:
        @researcher.on("goal.created")
        async def research(event: Event, researcher=researcher) -> None:
            await researcher.emit(
                "finding.created",
                {"researcher": researcher.name, "finding": "an independent result"},
            )

    @synthesizer.on("synthesis.requested")
    async def synthesize(event: Event) -> None:
        findings = event.payload["findings"]
        await synthesizer.emit(
            "answer.created",
            {"finding_count": len(findings), "answer": "synthesized result"},
        )

    async def request_synthesis(match: RuleMatch) -> None:
        await synthesizer.emit(
            "synthesis.requested",
            {"findings": [event.payload for event in match.events]},
            correlation_id=match.correlation_id,
            causation_id=match.events[-1].event_id,
        )

    bus.when_count("finding.created", 2).then(request_synthesis)

    bus.subscribe(
        "answer.created",
        lambda event: print(f"{event.type}: {event.payload['answer']}"),
    )
    await bus.publish(Event("goal.created", {"question": "What should we investigate?"}))

    print("trace:", " -> ".join(event.type for event in bus.trace(bus.log[0].correlation_id)))


if __name__ == "__main__":
    asyncio.run(main())
