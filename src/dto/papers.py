from __future__ import annotations

from datetime import date, datetime
from typing import Any

from pydantic import Field

from .authors import PaperAuthorDTO
from .common import BaseDTO
from .enums import CachePolicy, IndexingStatus, PaperSource, WorkflowGranularity
from .taxonomy import PaperKeywordDTO, PaperTopicDTO

ExtractedKeywordsPayload = list[str] | list[dict[str, Any]]


class PaperCreateDTO(BaseDTO):
    title: str
    doi: str | None = None
    publication_year: int | None = None
    publication_date: date | None = None
    language: str | None = None
    abstract: str | None = None
    is_open_access: bool | None = None
    cited_by_count: int | None = 0
    openalex_id: str | None = None
    references_count: int | None = 0
    primary_topic_id: int | None = None
    extracted_keywords: ExtractedKeywordsPayload | None = None


class PaperUpdateDTO(BaseDTO):
    title: str | None = None
    doi: str | None = None
    publication_year: int | None = None
    publication_date: date | None = None
    language: str | None = None
    abstract: str | None = None
    is_open_access: bool | None = None
    cited_by_count: int | None = None
    openalex_id: str | None = None
    references_count: int | None = None
    primary_topic_id: int | None = None
    extracted_keywords: ExtractedKeywordsPayload | None = None
    text_hash: str | None = None
    is_indexed: bool | None = None
    indexed_at: datetime | None = None


class PaperShortDTO(BaseDTO):
    id: int
    title: str
    doi: str | None = None
    publication_year: int | None = None
    publication_date: date | None = None
    language: str | None = None
    is_open_access: bool | None = None
    cited_by_count: int | None = 0
    openalex_id: str | None = None
    references_count: int | None = 0
    primary_topic_id: int | None = None
    extracted_keywords: ExtractedKeywordsPayload | None = None


class PaperDTO(PaperShortDTO):
    abstract: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    text_hash: str | None = None
    is_indexed: bool = False
    indexed_at: datetime | None = None
    authors: list[PaperAuthorDTO] = Field(default_factory=list)
    topics: list[PaperTopicDTO] = Field(default_factory=list)
    keywords: list[PaperKeywordDTO] = Field(default_factory=list)


class PaperResolveRequestDTO(BaseDTO):
    doi: str | None = None
    external_id: str | None = None
    source_name: str | None = "openalex"
    title: str | None = None
    publication_year: int | None = None
    fetch_missing: bool = True
    persist_missing: bool = True
    cache_policy: CachePolicy = CachePolicy.LOCAL_FIRST_FETCH_MISSING


class PaperResolveResponseDTO(BaseDTO):
    paper: PaperShortDTO | None
    source: PaperSource
    was_created_locally: bool
    indexing_status: IndexingStatus | None


class PaperIndexingRequestDTO(BaseDTO):
    paper_id: int
    force_reindex: bool = False
    source_topic_ids: list[int] = Field(default_factory=list)
    workflow_date_from: date | None = None
    workflow_date_to: date | None = None
    workflow_granularity: WorkflowGranularity = WorkflowGranularity.MONTH
    enqueue_cluster_dynamics: bool = False


class PaperIndexingResponseDTO(BaseDTO):
    paper_id: int
    status: IndexingStatus
    message: str | None = None


class PaperBatchIndexingRequestDTO(BaseDTO):
    paper_ids: list[int] = Field(default_factory=list)
    date_from: date | None = None
    date_to: date | None = None
    limit: int | None = Field(default=None, ge=1)
    offset: int = Field(default=0, ge=0)
    batch_size: int = Field(default=200, ge=1, le=1000)
    force_reindex: bool = False
    source_topic_ids: list[int] = Field(default_factory=list)
    workflow_date_from: date | None = None
    workflow_date_to: date | None = None
    workflow_granularity: WorkflowGranularity = WorkflowGranularity.MONTH
    enqueue_cluster_dynamics: bool = False


__all__ = [
    "CachePolicy",
    "ExtractedKeywordsPayload",
    "IndexingStatus",
    "PaperBatchIndexingRequestDTO",
    "PaperCreateDTO",
    "PaperDTO",
    "PaperIndexingRequestDTO",
    "PaperIndexingResponseDTO",
    "PaperResolveRequestDTO",
    "PaperResolveResponseDTO",
    "PaperShortDTO",
    "PaperUpdateDTO",
]
