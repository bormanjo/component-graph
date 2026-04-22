from pydantic import AwareDatetime, BaseModel, Field

from compgraph.utils import est_now


class AbstractEvent(BaseModel):
    as_of: AwareDatetime = Field(default_factory=est_now)
