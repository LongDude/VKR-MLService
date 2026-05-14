from __future__ import annotations

from datetime import date, datetime
from typing import Literal

from pydantic import Field

from .authors import PaperAuthorDTO
from .common import BaseDTO
from .taxonomy import PaperKeywordDTO, PaperTopicDTO

CachePolicy = Literal[
    "local_only",
    "local_first_fetch_missing",
    "force_external_refresh",
]
IndexingStatus = Literal["not_indexed", "pending", "indexed", "failed"]


class PaperCreateDTO(BaseDTO):
    title: str
    doi: str | None = None
    publication_year: int | None = None
    publication_date: date | None = None
    type: str | None = None
    language: str | None = None
    abstract: str | None = None
    is_open_access: bool | None = None
    cited_by_count: int | None = 0
    created_by_user_id: int | None = None


class PaperUpdateDTO(BaseDTO):
    title: str | None = None
    doi: str | None = None
    publication_year: int | None = None
    publication_date: date | None = None
    type: str | None = None
    language: str | None = None
    abstract: str | None = None
    is_open_access: bool | None = None
    cited_by_count: int | None = None
    created_by_user_id: int | None = None


class PaperShortDTO(BaseDTO):
    id: int
    title: str
    doi: str | None = None
    publication_year: int | None = None
    publication_date: date | None = None
    type: str | None = None
    language: str | None = None
    is_open_access: bool | None = None
    cited_by_count: int | None = 0


class PaperDTO(PaperShortDTO):
    abstract: str | None = None
    created_by_user_id: int | None = None
    created_at: datetime | None = None
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
    cache_policy: CachePolicy = "local_first_fetch_missing"


class PaperResolveResponseDTO(BaseDTO):
    paper: PaperShortDTO | None
    source: Literal["local_cache", "external_openalex", "not_found"]
    was_created_locally: bool
    indexing_status: IndexingStatus | None


class PaperIndexingRequestDTO(BaseDTO):
    paper_id: int
    force_reindex: bool = False


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


__all__ = [
    "CachePolicy",
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
