import asyncio
from pathlib import Path
from typing import Any

import pytest

from compgraph import Graph
from compgraph.events import AbstractEvent, event_from_json
from compgraph.event.archiver import InMemoryEventArchiver, JsonlFileEventArchiver


class DummyEvent(AbstractEvent):
    item: int


@pytest.mark.asyncio
async def test_in_memory_event_archiver(log_config: dict[str, Any]) -> None:
    config = log_config | {
        "event.sender": {"class": "compgraph.event.sender.EventSenderFactory"},
        "event.archiver": {
            "class": "compgraph.event.archiver.InMemoryEventArchiver",
            "events": ["tests.compgraph.event.test_archiver.DummyEvent"],
        },
    }

    graph = await Graph.from_config(config)

    archiver = graph.event.archiver
    assert isinstance(archiver, InMemoryEventArchiver)

    sender = await graph.event.sender(DummyEvent)
    events = [DummyEvent(item=i) for i in range(10)]
    for event in events:
        await sender.send(event)

    recorder = archiver._event_recorders[DummyEvent]
    assert recorder.events == events


@pytest.mark.asyncio
async def test_jsonl_file_event_archiver(
    log_config: dict[str, Any],
    tmp_path: Path,
) -> None:
    archive_file = tmp_path / "archive.jsonl"

    config = log_config | {
        "event.sender": {"class": "compgraph.event.sender.EventSenderFactory"},
        "event.archiver": {
            "class": "compgraph.event.archiver.JsonlFileEventArchiver",
            "events": ["tests.compgraph.event.test_archiver.DummyEvent"],
            "fpath": archive_file,
            "interval": 0.01,
        },
    }

    graph = await Graph.from_config(config)

    archiver = graph.event.archiver
    assert isinstance(archiver, JsonlFileEventArchiver)

    sender = await graph.event.sender(DummyEvent)
    events = [DummyEvent(item=i) for i in range(10)]

    async with asyncio.TaskGroup() as tg:
        run_task = tg.create_task(graph.run())
        _ = [tg.create_task(sender.send(event)) for event in events]
        await asyncio.sleep(0.02)
        run_task.cancel()

    archived_events = [
        event_from_json(line) for line in archive_file.read_text().splitlines()
    ]
    assert archived_events == events
