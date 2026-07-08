import asyncio
from pathlib import Path
from typing import Any

import pytest

from modulon import Graph
from modulon.events import AbstractEvent, event_from_json
from modulon.event.sink.archive import (
    InMemoryEventArchiver,
    JsonlFileEventArchiver,
    SQLiteEventArchiver,
)
from modulon.utils import get_type_location


class DummyEvent(AbstractEvent):
    item: int


@pytest.mark.asyncio
async def test_in_memory_event_archiver(log_config: dict[str, Any]) -> None:
    config = log_config | {
        "event.sender": {"class": "modulon.event.sender.EventSenderFactory"},
        "event.sink.archive": {
            "class": "modulon.event.sink.archive.InMemoryEventArchiver",
            "events": ["tests.modulon.event.sink.test_archive.DummyEvent"],
        },
    }

    graph = await Graph.from_config(config)

    archiver = graph.event.sink.archive
    assert isinstance(archiver, InMemoryEventArchiver)

    sender = await graph.event.sender(DummyEvent)
    events = [DummyEvent(item=i) for i in range(10)]
    for event in events:
        await sender.send_event(event)

    recorder = archiver._event_recorders[DummyEvent]
    assert recorder.events == events


@pytest.mark.asyncio
async def test_jsonl_file_event_archiver(
    log_config: dict[str, Any],
    tmp_path: Path,
) -> None:
    archive_file = tmp_path / "archive.jsonl"

    config = log_config | {
        "event.sender": {"class": "modulon.event.sender.EventSenderFactory"},
        "event.sink.archive": {
            "class": "modulon.event.sink.archive.JsonlFileEventArchiver",
            "events": ["tests.modulon.event.sink.test_archive.DummyEvent"],
            "fpath": archive_file,
            "interval": 0.01,
        },
    }

    graph = await Graph.from_config(config)

    archiver = graph.event.sink.archive
    assert isinstance(archiver, JsonlFileEventArchiver)

    sender = await graph.event.sender(DummyEvent)
    events = [DummyEvent(item=i) for i in range(10)]

    async with asyncio.TaskGroup() as tg:
        run_task = tg.create_task(graph.run())
        _ = [tg.create_task(sender.send_event(event)) for event in events]
        await asyncio.sleep(0.02)
        run_task.cancel()

    archived_events = [
        event_from_json(line) for line in archive_file.read_text().splitlines()
    ]
    assert archived_events == events


@pytest.mark.asyncio
async def test_sqlite_event_archiver(
    log_config: dict[str, Any],
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "archive.db"

    config = log_config | {
        "event.sender": {"class": "modulon.event.sender.EventSenderFactory"},
        "event.sink.archive": {
            "class": "modulon.event.sink.archive.SQLiteEventArchiver",
            "events": ["tests.modulon.event.sink.test_archive.DummyEvent"],
            "db_path": db_path,
            "interval": 0.01,
        },
    }

    graph = await Graph.from_config(config)

    archiver = graph.event.sink.archive
    assert isinstance(archiver, SQLiteEventArchiver)

    sender = await graph.event.sender(DummyEvent)
    events = [DummyEvent(item=i) for i in range(10)]

    async with asyncio.TaskGroup() as tg:
        run_task = tg.create_task(graph.run())
        _ = [tg.create_task(sender.send_event(event)) for event in events]
        await asyncio.sleep(0.05)
        run_task.cancel()

    cursor = archiver._conn.execute(
        "SELECT event_type, as_of, data FROM events ORDER BY id"
    )
    rows = cursor.fetchall()

    assert len(rows) == len(events)
    for (event_type, as_of, data), event in zip(rows, events):
        assert event_type == get_type_location(event)
        assert as_of == event.as_of.isoformat()
        assert event_from_json(data) == event
