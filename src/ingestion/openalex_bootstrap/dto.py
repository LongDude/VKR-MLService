from __future__ import annotations

from datetime import date
from typing import Any, Literal

from pydantic import Field

from dto.common import BaseDTO


NormalizeMode = Literal["none", "year", "month"]


class OpenAlexBootstrapRequestDTO(BaseDTO):
    target_count: int = Field(ge=0)
    date_from: date
    date_to: date
    sample: bool = False
    normalize: NormalizeMode = "month"
    monthly_counts_csv: str | None = None
    languages: list[str] = Field(default_factory=lambda: ["en", "ru"])
    types: list[str] = Field(default_factory=lambda: ["article"])
    batch_size: int = Field(default=500, ge=1, le=500)
    request_workers: int = Field(default=8, ge=1)
    db_workers: int = Field(default=2, ge=1)
    rate_limit_rps: float = Field(default=70.0, gt=0)
    seed: int = 42
    max_rounds: int = Field(default=5, ge=1)
    per_page: int = Field(default=100, ge=1, le=100)
    max_retries: int = Field(default=5, ge=0)
    skip_existing: bool = False
    enqueue_indexing: bool = False
    dry_run: bool = False


class OpenAlexLoadPlanItemDTO(BaseDTO):
    date_from: date
    date_to: date
    sample_size: int = Field(ge=0)
    language: str
    type: str
    seed: int
    seed_offset: int = 0

    @property
    def effective_seed(self) -> int:
        return self.seed + self.seed_offset


class OpenAlexLoadPlanDTO(BaseDTO):
    items: list[OpenAlexLoadPlanItemDTO] = Field(default_factory=list)
    total_sample_count: int = 0
    estimated_requests: int = 0
    normalize: NormalizeMode = "month"


class BatchImportResultDTO(BaseDTO):
    total: int = 0
    normalized: int = 0
    created: int = 0
    updated: int = 0
    existing: int = 0
    skipped_empty_title: int = 0
    skipped_duplicates: int = 0
    failed: int = 0
    errors: list[dict[str, Any]] = Field(default_factory=list)
    paper_ids: list[int] = Field(default_factory=list)


class OpenAlexBootstrapReportDTO(BaseDTO):
    target_count: int
    initial_count: int
    final_count: int
    required_new_count: int
    planned_sample_count: int = 0
    fetched: int = 0
    normalized: int = 0
    created: int = 0
    updated: int = 0
    existing: int = 0
    skipped_empty_title: int = 0
    skipped_duplicates: int = 0
    failed: int = 0
    openalex_requests: int = 0
    elapsed_seconds: float = 0.0
    rounds: list[dict[str, Any]] = Field(default_factory=list)
    errors: list[dict[str, Any]] = Field(default_factory=list)


__all__ = [
    "BatchImportResultDTO",
    "NormalizeMode",
    "OpenAlexBootstrapReportDTO",
    "OpenAlexBootstrapRequestDTO",
    "OpenAlexLoadPlanDTO",
    "OpenAlexLoadPlanItemDTO",
]
