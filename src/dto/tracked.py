from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import Field

from .common import BaseDTO

TrackedEntityType = Literal["domain", "field", "subfield", "topic", "keyword"]


class AddTrackedEntityRequestDTO(BaseDTO):
    entity_type: TrackedEntityType
    entity_id: int


class RemoveTrackedEntityRequestDTO(BaseDTO):
    entity_type: TrackedEntityType
    entity_id: int


class TrackedEntityDTO(BaseDTO):
    entity_type: TrackedEntityType
    id: int
    name: str
    created_at: datetime | None = None


class UserTrackedEntitiesDTO(BaseDTO):
    user_id: int
    domains: list[TrackedEntityDTO] = Field(default_factory=list)
    fields: list[TrackedEntityDTO] = Field(default_factory=list)
    subfields: list[TrackedEntityDTO] = Field(default_factory=list)
    topics: list[TrackedEntityDTO] = Field(default_factory=list)
    keywords: list[TrackedEntityDTO] = Field(default_factory=list)


__all__ = [
    "AddTrackedEntityRequestDTO",
    "RemoveTrackedEntityRequestDTO",
    "TrackedEntityDTO",
    "TrackedEntityType",
    "UserTrackedEntitiesDTO",
]
