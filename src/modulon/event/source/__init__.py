from modulon.core.lookup import Lookup
from modulon.event.source.replay import AbstractEventReplayFactory


class EventSourceSubGraph(Lookup):
    replay: AbstractEventReplayFactory


__all__ = ["EventSourceSubGraph"]
