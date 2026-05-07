import asyncio
from abc import abstractmethod


from compgraph.event.sender import CallbackPriority, EventRecorder
from compgraph.events import AbstractEvent, ImportableEvent, event_to_json
from compgraph.graph import AbstractFactory, requires
from compgraph.utils import JsonlFilePath


@requires("event.sender")
class AbstractEventArchiver(AbstractFactory, node_namespace="event.archiver"):
    """
    An `event.archiver` records the emitted events of the registered types
    by registering a callback at setup.

    Subclasses are responsible for implementing the private `._archive()` method.
    """

    events: set[ImportableEvent]
    priority: CallbackPriority = CallbackPriority.HIGH

    async def _setup(self) -> None:
        for event_cls in self.events:
            sender = await self.dep.event.sender(event_cls)
            sender.register_callback(self._archive, priority=self.priority)
            self.log.info("Event type to be archived: %s", event_cls)

    async def _run(self) -> None: ...

    @abstractmethod
    async def _archive(self, event: AbstractEvent) -> None: ...


class InMemoryEventArchiver(AbstractEventArchiver):
    """
    This archiver utilizes an `EventRecorder` per type of event to collect
    emitted events.
    """

    _event_recorders: dict[type[AbstractEvent], EventRecorder]

    async def _setup(self) -> None:
        await super()._setup()
        self._event_recorders = {
            event_cls: EventRecorder() for event_cls in self.events
        }

    async def _archive(self, event: AbstractEvent) -> None:
        recorder = self._event_recorders[type(event)]
        await recorder(event=event)


class JsonlFileEventArchiver(AbstractEventArchiver):
    """
    This archiver writes events in a JSON lines format (.jsonl) such that
    each line in the file corresponds to one event.
    """

    fpath: JsonlFilePath
    interval: float = 5.0

    _buffer: list[AbstractEvent] = []

    async def _setup(self) -> None:
        await super()._setup()
        self.fpath.touch()

    async def _run(self) -> None:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(self._write_buffer_to_file())

    async def _write_buffer_to_file(self) -> None:
        with self.fpath.open("a+") as file:
            while True:
                if self._buffer:
                    lines = map(
                        lambda line: line + "\n", map(event_to_json, self._buffer)
                    )
                    self.log.info("Archiving %d event(s)", len(self._buffer))
                    file.writelines(lines)
                    self._buffer.clear()
                    self.log.info("Clearing buffer")

                await asyncio.sleep(self.interval)

    async def _archive(self, event: AbstractEvent) -> None:
        self._buffer.append(event)
