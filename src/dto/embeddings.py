from __future__ import annotations

from pydantic import Field

from .common import BaseDTO


class EmbeddingRequestDTO(BaseDTO):
    text: str
    model: str | None = None


class EmbeddingBatchRequestDTO(BaseDTO):
    texts: list[str] = Field(default_factory=list)
    model: str | None = None


class EmbeddingResultDTO(BaseDTO):
    vector: list[float]
    model: str
    dimension: int
    token_count: int | None = None


__all__ = [
    "EmbeddingBatchRequestDTO",
    "EmbeddingRequestDTO",
    "EmbeddingResultDTO",
]
