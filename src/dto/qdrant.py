from __future__ import annotations

from enum import StrEnum
from typing import Any

from pydantic import Field

from .common import BaseDTO


class QdrantPointDTO(BaseDTO):
    id: int | str
    vector: list[float]
    payload: dict[str, Any]


class QdrantSearchHitDTO(BaseDTO):
    id: int | str
    score: float
    payload: dict[str, Any] = Field(default_factory=dict[str, Any])
    vector: list[float] | None = None


class QdrantPayloadIndexDTO(BaseDTO):
    field_name: str
    field_schema: str


class QdrantDistanceCalculationType(StrEnum):
    COSINE = "Cosine"
    DOT = "Dot"
    EUCLID = "Euclid"
    MANHATTAN = "Manhattan"


class QdrantCollectionConfigDTO(BaseDTO):
    collection_name: str
    vector_size: int
    distance: QdrantDistanceCalculationType = QdrantDistanceCalculationType.COSINE
    payload_indexes: list[QdrantPayloadIndexDTO] = Field(default_factory=list)


__all__ = [
    "QdrantCollectionConfigDTO",
    "QdrantPayloadIndexDTO",
    "QdrantPointDTO",
    "QdrantSearchHitDTO",
]
