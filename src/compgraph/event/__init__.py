from compgraph.core.lookup import Lookup
from compgraph.event.sender import EventSenderFactory


class EventSubGraph(Lookup):
    sender: EventSenderFactory


__all__ = ["EventSenderFactory"]
