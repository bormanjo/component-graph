from __future__ import annotations  # tells pydantic to defer type hint evaluation

import asyncio
import inspect
from collections import defaultdict
from enum import IntEnum
from typing import Generic, Protocol, TypeVar

from pydantic import PrivateAttr

from compgraph.events import AbstractEvent
from compgraph.graph import AbstractComponent, AbstractFactory


EventT = TypeVar("EventT", bound=AbstractEvent)


class CallbackPriority(IntEnum):
    HIGHEST = 4
    HIGH = 3
    MEDIUM = 2
    LOW = 1


class EventSender(AbstractComponent, Generic[EventT], skip_setup=True, skip_run=True):
    kind: type[EventT]

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
        self.log.debug("Sending event: %s", repr(event))

        for priority in CallbackPriority:
            if (callbacks := self._callback_map.get(priority)) is None:
                continue  # pragma: no cover

            coros = [
                clbk(event=event) if self._send_event(clbk) else clbk()  # type: ignore
                for clbk in callbacks
            ]
            async with asyncio.TaskGroup() as tg:
                _ = [tg.create_task(coro) for coro in coros]

    def register_callback(
        self,
        callback: Callback | CallbackWithEvent,
        priority: CallbackPriority = CallbackPriority.LOW,
    ) -> None:
        self._callback_map[priority].append(callback)


class EventSenderFactory(
    AbstractFactory, node_namespace="event.sender", skip_setup=True, skip_run=True
):
    _event_senders: dict[type[AbstractEvent], EventSender] = PrivateAttr(default={})

    async def __call__(self, event_cls: type[EventT]) -> EventSender[EventT]:
        if event_cls not in self._event_senders:
            self.log.debug("Creating new EventSender[%s]", event_cls)
            self._event_senders[event_cls] = await self._create_component(
                klass=EventSender,  # type: ignore
                kind=event_cls,
            )

        return self._event_senders[event_cls]
