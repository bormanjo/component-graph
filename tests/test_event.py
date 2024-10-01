import time
from typing import Any

import pytest
from pydantic import BaseModel

from cg.event import AbstractEvent, CallbackPriority
from cg.graph import Graph


class DummyEvent(AbstractEvent):
    data: str


@pytest.mark.asyncio
async def test_event_sender_callbacks_with_priority(log_config: dict[str, Any]) -> None:
    """
    Registers 3 callbacks to fire when an instance of `DummyEvent` is sent.
    Asserts that callbacks are fired in prioritized order.
    """
    config = {
        "event_sender": {"class": "cg.event.EventSenderFactory"},
    }

    graph = await Graph.from_config(config | log_config)

    sender = graph.event_sender(event_cls=DummyEvent)
    assert sender.kind == DummyEvent
    expected_event = DummyEvent(data="expected")

    class EventTracer(BaseModel):
        timestamp: int
        priority: CallbackPriority

        def __gt__(self, other: "EventTracer") -> bool:
            called_before = self.timestamp < other.timestamp
            higher_priority = self.priority > other.priority
            return called_before and higher_priority

    tracers: list[EventTracer] = []

    async def low_priority_callback() -> None:
        tracer = EventTracer(timestamp=time.time_ns(), priority=CallbackPriority.LOW)
        tracers.append(tracer)

    async def medium_priority_callback() -> None:
        tracer = EventTracer(timestamp=time.time_ns(), priority=CallbackPriority.MEDIUM)
        tracers.append(tracer)

    async def high_priority_callback() -> None:
        tracer = EventTracer(timestamp=time.time_ns(), priority=CallbackPriority.HIGH)
        tracers.append(tracer)

    sender.register_callback(low_priority_callback, CallbackPriority.LOW)
    sender.register_callback(medium_priority_callback, CallbackPriority.MEDIUM)
    sender.register_callback(high_priority_callback, CallbackPriority.HIGH)

    await sender.send(event=expected_event)

    assert len(tracers) == 3
    assert tracers[0] > tracers[1]
    assert tracers[1] > tracers[2]
