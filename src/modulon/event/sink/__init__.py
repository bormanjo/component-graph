from modulon.core.lookup import Lookup
from modulon.event.sink.archive import AbstractEventArchiveFactory


class EventSinkSubGraph(Lookup):
    archive: AbstractEventArchiveFactory


__all__ = ["EventSinkSubGraph"]
