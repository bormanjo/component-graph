from modulon.core.lookup import Lookup
from modulon.event.sender import EventSenderFactory
from modulon.event.sink import EventSinkSubGraph
from modulon.event.source import EventSourceSubGraph


class EventSubGraph(Lookup):
    sender: EventSenderFactory
    sink: EventSinkSubGraph
    source: EventSourceSubGraph


__all__ = ["EventSubGraph"]
