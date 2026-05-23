from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

from pydantic import Field

from .common import BaseDTO, PaginationDTO


class ExternalInstitutionDTO(BaseDTO):
    external_id: str | None = None
    display_name: str
    ror: str | None = None
    country_code: str | None = None
    type: str | None = None
    raw: dict[str, Any] | None = None


class ExternalAuthorDTO(BaseDTO):
    external_id: str | None = None
    display_name: str
    orcid: str | None = None
    author_order: int | None = None
    is_corresponding: bool | None = None
    institutions: list[ExternalInstitutionDTO] = Field(default_factory=list)
    raw: dict[str, Any] | None = None


class ExternalTopicDTO(BaseDTO):
    external_id: str | None = None
    name: str
    score: Decimal | None = None
    domain_name: str | None = None
    field_name: str | None = None
    subfield_name: str | None = None
    raw: dict[str, Any] | None = None


class ExternalKeywordDTO(BaseDTO):
    value: str
    score: Decimal | None = None
    raw: dict[str, Any] | None = None


class ExternalLandingDTO(BaseDTO):
    landing_url: str
    pdf_url: str | None = None
    license: str | None = None
    version: str | None = None
    is_best: bool | None = None
    raw: dict[str, Any] | None = None


class ExternalPaperDTO(BaseDTO):
    external_id: str | None = None
    doi: str | None = None
    title: str
    abstract: str | None = None
    publication_year: int | None = None
    publication_date: date | None = None
    type: str | None = None
    language: str | None = None
    is_open_access: bool | None = None
    cited_by_count: int | None = None
    references_count: int | None = None
    authors: list[ExternalAuthorDTO] = Field(default_factory=list)
    institutions: list[ExternalInstitutionDTO] = Field(default_factory=list)
    topics: list[ExternalTopicDTO] = Field(default_factory=list)
    keywords: list[ExternalKeywordDTO] = Field(default_factory=list)
    landings: list[ExternalLandingDTO] = Field(default_factory=list)
    raw: dict[str, Any] | None = None


class OpenAlexSearchFiltersDTO(BaseDTO):
    query: str | None = None
    publication_year_from: int | None = None
    publication_year_to: int | None = None
    date_from: date | None = None
    date_to: date | None = None
    type: str | None = None
    language: str | None = None
    is_open_access: bool | None = None
    pagination: PaginationDTO = Field(default_factory=PaginationDTO)


__all__ = [
    "ExternalAuthorDTO",
    "ExternalInstitutionDTO",
    "ExternalKeywordDTO",
    "ExternalLandingDTO",
    "ExternalPaperDTO",
    "ExternalTopicDTO",
    "OpenAlexSearchFiltersDTO",
]
