from modulon.core.lookup import Lookup
from modulon.event.sender import EventSenderFactory


class EventSubGraph(Lookup):
    sender: EventSenderFactory


__all__ = ["EventSenderFactory"]
