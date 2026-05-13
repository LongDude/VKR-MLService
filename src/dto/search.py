from __future__ import annotations

from datetime import date
from typing import Any

from pydantic import Field

from .common import BaseDTO, PaginationDTO
from .external import ExternalPaperDTO
from .papers import CachePolicy, PaperShortDTO


class SearchFiltersDTO(BaseDTO):
    publication_year_from: int | None = None
    publication_year_to: int | None = None
    date_from: date | None = None
    date_to: date | None = None
    domain_ids: list[int] = Field(default_factory=list)
    field_ids: list[int] = Field(default_factory=list)
    subfield_ids: list[int] = Field(default_factory=list)
    topic_ids: list[int] = Field(default_factory=list)
    keyword_ids: list[int] = Field(default_factory=list)
    language: str | None = None
    is_open_access: bool | None = None
    type: str | None = None


class SemanticSearchRequestDTO(BaseDTO):
    query: str
    top_k: int = Field(default=20, ge=1, le=100)
    filters: SearchFiltersDTO = Field(default_factory=SearchFiltersDTO)
    cache_policy: CachePolicy = "local_first_fetch_missing"


class SemanticSearchHitDTO(BaseDTO):
    paper: PaperShortDTO
    score: float
    highlights: list[str] = Field(default_factory=list)
    payload: dict[str, Any] = Field(default_factory=dict)


class SemanticSearchMLHitDTO(BaseDTO):
    paper_id: int | str
    score: float
    payload: dict[str, Any] = Field(default_factory=dict)


class SemanticSearchMLResponseDTO(BaseDTO):
    query: str
    hits: list[SemanticSearchMLHitDTO] = Field(default_factory=list)
    external_candidates: list[ExternalPaperDTO] = Field(default_factory=list)
    embedding_model: str | None = None
    elapsed_ms: int | None = None


class SemanticSearchResponseDTO(BaseDTO):
    query: str
    hits: list[SemanticSearchHitDTO] = Field(default_factory=list)
    total: int = 0
    pagination: PaginationDTO = Field(default_factory=PaginationDTO)
    filters: SearchFiltersDTO = Field(default_factory=SearchFiltersDTO)


__all__ = [
    "SearchFiltersDTO",
    "SemanticSearchHitDTO",
    "SemanticSearchMLHitDTO",
    "SemanticSearchMLResponseDTO",
    "SemanticSearchRequestDTO",
    "SemanticSearchResponseDTO",
]
