from __future__ import annotations  # tells pydantic to defer type hint evaluation

import asyncio
import inspect
from collections import defaultdict
from collections.abc import Coroutine
from enum import IntEnum
from uuid import uuid4
from typing import Generic, Protocol, TypeAlias, TypeVar

from pydantic import BaseModel, PrivateAttr, UUID4

from modulon.events import AbstractEvent
from modulon.graph import AbstractComponent, AbstractFactory


EventT = TypeVar("EventT", bound=AbstractEvent)


CallbackID: TypeAlias = UUID4


class CallbackPriority(IntEnum):
    HIGHEST = 4
    HIGH = 3
    MEDIUM = 2
    LOW = 1


class Callback(Protocol):
    async def __call__(self) -> None: ...


class CallbackWithEvent(Protocol):
    async def __call__(self, event: EventT) -> None: ...


CallbackT = Callback | CallbackWithEvent


class EventRecorder(BaseModel, Generic[EventT]):
    """A simple event sender callback that accumulates the given event type"""

    events: list[EventT] = []

    async def __call__(self, event: EventT) -> None:
        self.events.append(event)


class EventSender(AbstractComponent, Generic[EventT], skip_setup=True, skip_run=True):
    kind: type[EventT]

    _callback_map: dict[CallbackPriority, dict[CallbackID, CallbackT]] = defaultdict(
        dict
    )

    @staticmethod
    def _send_event(callback: CallbackT) -> bool:
        return "event" in inspect.signature(callback).parameters

    def _invoke_callback(
        self, clbk: CallbackT, event: EventT
    ) -> Coroutine[None, None, None]:
        return clbk(event=event) if self._send_event(clbk) else clbk()  # type: ignore

    def _one_and_done(self, clbk: CallbackT, callback_id: CallbackID) -> CallbackT:
        async def wrapped_callback(event: EventT) -> None:
            await self._invoke_callback(clbk, event)
            self.deregister_callback(callback_id)

        return wrapped_callback

    async def send(self, event: EventT) -> None:
        """Triggers all callbacks registered with this event type"""
        self.log.debug("Sending event: %s", repr(event))

        for priority in CallbackPriority:
            if (callbacks := self._callback_map.get(priority)) is None:
                continue  # pragma: no cover

            coros = [self._invoke_callback(clbk, event) for clbk in callbacks.values()]
            async with asyncio.TaskGroup() as tg:
                _ = [tg.create_task(coro) for coro in coros]

    def register_callback(
        self,
        callback: Callback | CallbackWithEvent,
        *,
        priority: CallbackPriority = CallbackPriority.LOW,
        one_and_done: bool = False,
    ) -> CallbackID:
        """
        Registers a callback to be invoked when an event is sent and returns an identifier
        that can be used to de-register the callback.

        Args:
            callback (CallbackT): the callback to be fired
            priority (CallbackPriority): high priority callbacks are run first. Default
                is LOW
            one_and_done (bool): If True, the callback is removed after it returns.
                Default is False
        """
        callback_id = uuid4()
        if one_and_done:
            callback = self._one_and_done(callback, callback_id)

        self._callback_map[priority][callback_id] = callback
        return callback_id

    def deregister_callback(self, callback_id: CallbackID) -> None:
        for callbacks in self._callback_map.values():
            if callback_id in callbacks:
                callbacks.pop(callback_id)
                return


class EventSenderFactory(
    AbstractFactory,
    node_namespace="event.sender",
    skip_setup=True,
    skip_run=True,
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
