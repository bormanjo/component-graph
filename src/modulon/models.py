from pydantic import BaseModel, ConfigDict


class _BaseModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ImmutableModel(_BaseModel):
    model_config = ConfigDict(frozen=True)


class MutableModel(_BaseModel):
    pass
