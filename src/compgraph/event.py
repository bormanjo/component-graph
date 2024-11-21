import asyncio
import inspect
from collections import defaultdict
from enum import IntEnum
from typing import Generic, Protocol, TypeVar

from pydantic import BaseModel, PrivateAttr

from compgraph.graph import BaseComponent, BaseFactory


class AbstractEvent(BaseModel):
    pass


EventT = TypeVar("EventT", bound=AbstractEvent)


class CallbackPriority(IntEnum):
    HIGHEST = 4
    HIGH = 3
    MEDIUM = 2
    LOW = 1


class EventSender(BaseComponent, Generic[EventT]):
    kind: type[AbstractEvent]

    class Callback(Protocol):
        async def __call__(self) -> None: ...

    class CallbackWithEvent(Protocol):
        async def __call__(self, event: EventT) -> None: ...

    _callback_map: dict[CallbackPriority, list[Callback | CallbackWithEvent]] = (
        defaultdict(list)
    )

    @staticmethod
    def _send_event(callback: Callback | CallbackWithEvent) -> bool:
        return "event" in inspect.signature(callback).parameters

    async def send(self, event: EventT) -> None:
        """Triggers all callbacks registered with this event type"""
        self.log.debug("Sending event: %s", event)

        for priority in CallbackPriority:
            if (callbacks := self._callback_map.get(priority)) is None:
                continue  # pragma: no cover

            await asyncio.gather(
                *(
                    clbk(event=event) if self._send_event(clbk) else clbk()
                    for clbk in callbacks
                )
            )

    def register_callback(
        self,
        callback: Callback | CallbackWithEvent,
        priority=CallbackPriority.LOW,
    ) -> None:
        self._callback_map[priority].append(callback)


class EventSenderFactory(BaseFactory, node_namespace="event_sender"):
    _event_senders: dict[type[AbstractEvent], EventSender] = PrivateAttr(default={})

    def __call__(self, event_cls: type[EventT]) -> EventSender[EventT]:
        if event_cls not in self._event_senders:
            self.log.debug("Creating new EventSender for event")
            self._event_senders[event_cls] = self._create_component(
                klass=EventSender,
                kind=event_cls,
            )

        return self._event_senders[event_cls]
