from __future__ import annotations

from typing import Any, Literal

from pydantic import Field

from .common import BaseDTO


class QdrantPointDTO(BaseDTO):
    id: int | str
    vector: list[float]
    payload: dict[str, Any]


class QdrantSearchHitDTO(BaseDTO):
    id: int | str
    score: float
    payload: dict[str, Any] = Field(default_factory=dict)
    vector: list[float] | None = None


class QdrantPayloadIndexDTO(BaseDTO):
    field_name: str
    field_schema: str


class QdrantCollectionConfigDTO(BaseDTO):
    collection_name: str
    vector_size: int
    distance: Literal["Cosine", "Dot", "Euclid", "Manhattan"] = "Cosine"
    payload_indexes: list[QdrantPayloadIndexDTO] = Field(default_factory=list)


__all__ = [
    "QdrantCollectionConfigDTO",
    "QdrantPayloadIndexDTO",
    "QdrantPointDTO",
    "QdrantSearchHitDTO",
]
