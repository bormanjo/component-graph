from compgraph.events import AbstractEvent, event_from_json, event_to_json


class DummyEvent(AbstractEvent):
    number: int
    data: str


def test_event_json_roundtrip() -> None:
    event = DummyEvent(number=1, data="abc")
    serialized = event_to_json(event)
    deserialized = event_from_json(serialized)

    assert deserialized == event
