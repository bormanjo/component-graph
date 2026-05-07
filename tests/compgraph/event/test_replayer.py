import asyncio
from pathlib import Path
from textwrap import dedent
from typing import Any

import pytest
from pydantic import ValidationError

from compgraph import Graph
from compgraph.event.replayer import JsonlFileEventReplayer
from compgraph.event.sender import EventRecorder
from compgraph.events import AbstractEvent, event_from_json


class DummyEvent(AbstractEvent):
    item: int


@pytest.mark.asyncio
async def test_config_event_replayer(log_config: dict[str, Any]) -> None:
    expected_events = [DummyEvent(item=i) for i in range(10)]

    config = log_config | {
        "event.sender": {"class": "compgraph.event.sender.EventSenderFactory"},
        "event.replayer": {
            "class": "compgraph.event.replayer.ConfigEventReplayer",
            "events": expected_events,
        },
    }

    graph = await Graph.from_config(config)

    recorder: EventRecorder[DummyEvent] = EventRecorder()
    sender = await graph.event.sender(DummyEvent)
    sender.register_callback(recorder)

    async with asyncio.timeout(1):
        await graph.run()

    assert recorder.events == expected_events


@pytest.mark.asyncio
async def test_jsonl_file_event_replayer(
    log_config: dict[str, Any],
    tmp_path: Path,
) -> None:
    jsonl_content = dedent(
        """
    {"_event_type": "tests.compgraph.event.test_replayer.DummyEvent", "as_of": "2026-05-06T19:19:29.557774-04:00", "item": 0}
    {"_event_type": "tests.compgraph.event.test_replayer.DummyEvent", "as_of": "2026-05-06T19:19:29.557792-04:00", "item": 1}
    {"_event_type": "tests.compgraph.event.test_replayer.DummyEvent", "as_of": "2026-05-06T19:19:29.557799-04:00", "item": 2}
    {"_event_type": "tests.compgraph.event.test_replayer.DummyEvent", "as_of": "2026-05-06T19:19:29.557805-04:00", "item": 3}
    {"_event_type": "tests.compgraph.event.test_replayer.DummyEvent", "as_of": "2026-05-06T19:19:29.557811-04:00", "item": 4}
    {"_event_type": "tests.compgraph.event.test_replayer.DummyEvent", "as_of": "2026-05-06T19:19:29.557816-04:00", "item": 5}
    {"_event_type": "tests.compgraph.event.test_replayer.DummyEvent", "as_of": "2026-05-06T19:19:29.557822-04:00", "item": 6}
    {"_event_type": "tests.compgraph.event.test_replayer.DummyEvent", "as_of": "2026-05-06T19:19:29.557827-04:00", "item": 7}
    {"_event_type": "tests.compgraph.event.test_replayer.DummyEvent", "as_of": "2026-05-06T19:19:29.557833-04:00", "item": 8}
    {"_event_type": "tests.compgraph.event.test_replayer.DummyEvent", "as_of": "2026-05-06T19:19:29.557838-04:00", "item": 9}
    """.strip(" \t\n")
    )

    archive_file = tmp_path / "archive.jsonl"
    archive_file.write_text(jsonl_content)

    config = log_config | {
        "event.sender": {"class": "compgraph.event.sender.EventSenderFactory"},
        "event.replayer": {
            "class": "compgraph.event.replayer.JsonlFileEventReplayer",
            "fpath": archive_file,
        },
    }

    graph = await Graph.from_config(config)

    recorder: EventRecorder[DummyEvent] = EventRecorder()
    sender = await graph.event.sender(DummyEvent)
    sender.register_callback(recorder)

    async with asyncio.timeout(1):
        await graph.run()

    expected_events = [event_from_json(line) for line in jsonl_content.splitlines()]

    assert recorder.events == expected_events


def test_jsonl_file_event_archiver_bad_fpath() -> None:
    with pytest.raises(ValidationError):
        JsonlFileEventReplayer(fpath="bad-file.txt")
