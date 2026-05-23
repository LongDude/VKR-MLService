from __future__ import annotations

from datetime import date

from pydantic import Field

from .common import BaseDTO


class KeywordExtractionMetadataDTO(BaseDTO):
    title: str
    abstract: str | None = None
    language: str | None = None
    paper_id: int | None = None


class KeywordExtractionItemDTO(BaseDTO):
    keyword: str
    score: float


class KeywordExtractionResponseDTO(BaseDTO):
    paper_id: int | None = None
    keywords: list[str] = Field(default_factory=list)
    items: list[KeywordExtractionItemDTO] = Field(default_factory=list)


class KeywordExtractionBatchRequestDTO(BaseDTO):
    items: list[KeywordExtractionMetadataDTO] = Field(default_factory=list)
    top_k: int = Field(default=10, ge=0)
    min_score: float | None = None


class PaperKeywordExtractionBatchRequestDTO(BaseDTO):
    paper_ids: list[int] = Field(default_factory=list)
    date_from: date | None = None
    date_to: date | None = None
    topic_id: int | None = None
    field_id: int | None = None
    limit: int | None = Field(default=None, ge=1)
    offset: int = Field(default=0, ge=0)
    batch_size: int = Field(default=200, ge=1, le=1000)
    top_k: int = Field(default=10, ge=0)
    min_score: float | None = None
    skip_processed: bool = True
    skip_non_english: bool = False


__all__ = [
    "KeywordExtractionBatchRequestDTO",
    "KeywordExtractionItemDTO",
    "KeywordExtractionMetadataDTO",
    "KeywordExtractionResponseDTO",
    "PaperKeywordExtractionBatchRequestDTO",
]
