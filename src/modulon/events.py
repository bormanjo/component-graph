import json

from pydantic import AwareDatetime, BaseModel, Field, ImportString

from modulon.utils import est_now, get_type_location, import_object


class AbstractEvent(BaseModel):
    as_of: AwareDatetime = Field(default_factory=est_now)


ImportableEvent = ImportString[type[AbstractEvent]]


def event_to_json(event: AbstractEvent) -> str:
    """Serialized an event to JSON"""
    data = {"_event_type": get_type_location(event)} | event.model_dump(mode="json")
    return json.dumps(data, sort_keys=True)


def event_from_json(data: str) -> AbstractEvent:
    """Deserialize an event from JSON"""
    data = json.loads(data.strip())
    assert isinstance(data, dict)
    event_type_location = data.pop("_event_type")
    event_type = import_object(event_type_location)
    assert issubclass(event_type, AbstractEvent)
    return event_type.model_validate(data)
