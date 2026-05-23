from __future__ import annotations

from datetime import date
from typing import Any, Literal

from pydantic import Field

from dto.common import BaseDTO


NormalizeMode = Literal["none", "year", "month"]
MissingStatsPolicy = Literal["error", "fetch"]
MonthlyStatsSource = Literal["redis", "csv"]
TargetCountScope = Literal["total", "period", "year", "month"]
TargetCountUnit = Literal["aggregate", "topic"]


class OpenAlexBootstrapTopicTargetDTO(BaseDTO):
    topic_id: int = Field(ge=1)
    filter_part: str


class OpenAlexBootstrapRequestDTO(BaseDTO):
    target_count: int = Field(ge=0)
    target_count_scope: TargetCountScope = "total"
    target_count_unit: TargetCountUnit = "aggregate"
    topic_targets: list[OpenAlexBootstrapTopicTargetDTO] = Field(default_factory=list)
    date_from: date
    date_to: date
    sample: bool = True
    normalize: NormalizeMode = "none"
    monthly_stats_source: MonthlyStatsSource = "redis"
    monthly_counts_redis_key: str | None = None
    monthly_counts_csv: str | None = None
    missing_stats_policy: MissingStatsPolicy = "error"
    languages: list[str] = Field(default_factory=lambda: ["en", "ru"])
    types: list[str] = Field(default_factory=lambda: ["article"])
    openalex_filter_parts: list[str] = Field(default_factory=list)
    local_field_ids: list[int] = Field(default_factory=list)
    local_subfield_ids: list[int] = Field(default_factory=list)
    batch_size: int = Field(default=500, ge=1, le=500)
    request_workers: int = Field(default=8, ge=1)
    db_workers: int = Field(default=2, ge=1)
    rate_limit_rps: float = Field(default=70.0, gt=0)
    seed: int = 42
    max_rounds: int = Field(default=1, ge=1)
    per_page: int = Field(default=100, ge=1, le=100)
    max_retries: int = Field(default=5, ge=0)
    skip_existing: bool = False
    enqueue_indexing: bool = False
    dry_run: bool = False
    show_progress: bool = True


class OpenAlexLoadPlanItemDTO(BaseDTO):
    date_from: date
    date_to: date
    sample_size: int = Field(ge=0)
    language: str
    type: str
    filter_parts: list[str] = Field(default_factory=list)
    seed: int
    seed_offset: int = 0
    unit_key: str = "aggregate"
    period: str | None = None
    topic_id: int | None = None
    requested_count: int = Field(default=0, ge=0)

    @property
    def effective_seed(self) -> int:
        return self.seed + self.seed_offset


class OpenAlexPlanUnitDTO(BaseDTO):
    unit_key: str
    period: str
    date_from: date
    date_to: date
    requested: int = Field(ge=0)
    topic_id: int | None = None
    filter_parts: list[str] = Field(default_factory=list)


class OpenAlexLoadPlanDTO(BaseDTO):
    items: list[OpenAlexLoadPlanItemDTO] = Field(default_factory=list)
    units: list[OpenAlexPlanUnitDTO] = Field(default_factory=list)
    total_sample_count: int = 0
    estimated_requests: int = 0
    normalize: NormalizeMode = "none"


class OpenAlexPendingPageDTO(BaseDTO):
    date_from: date
    date_to: date
    sample_size: int = Field(ge=0)
    language: str
    type: str
    filter_parts: list[str] = Field(default_factory=list)
    seed: int
    seed_offset: int = 0
    page: int = Field(ge=1)
    per_page: int = Field(ge=1, le=100)
    sample: bool = True
    unit_key: str = "aggregate"
    period: str | None = None
    topic_id: int | None = None
    requested_count: int = Field(default=0, ge=0)

    @property
    def effective_seed(self) -> int:
        return self.seed + self.seed_offset

    def to_plan_item(self) -> OpenAlexLoadPlanItemDTO:
        return OpenAlexLoadPlanItemDTO(
            date_from=self.date_from,
            date_to=self.date_to,
            sample_size=self.sample_size,
            language=self.language,
            type=self.type,
            filter_parts=self.filter_parts,
            seed=self.seed,
            seed_offset=self.seed_offset,
            unit_key=self.unit_key,
            period=self.period,
            topic_id=self.topic_id,
            requested_count=self.requested_count,
        )


class OpenAlexUnitSummaryDTO(BaseDTO):
    unit_key: str
    period: str | None = None
    topic_id: int | None = None
    requested: int = 0
    fetched: int = 0
    created: int = 0
    updated: int = 0
    exhausted: bool = False
    reason: str | None = None

    @property
    def missing_count(self) -> int:
        return max(0, self.requested - self.fetched)


class BatchImportResultDTO(BaseDTO):
    total: int = 0
    normalized: int = 0
    created: int = 0
    updated: int = 0
    existing: int = 0
    skipped_empty_title: int = 0
    skipped_empty_abstract: int = 0
    skipped_duplicates: int = 0
    failed: int = 0
    errors: list[dict[str, Any]] = Field(default_factory=list)
    paper_ids: list[int] = Field(default_factory=list)


class OpenAlexBootstrapReportDTO(BaseDTO):
    target_count: int
    target_count_scope: TargetCountScope = "total"
    target_count_unit: TargetCountUnit = "aggregate"
    target_scope_units: int = 1
    target_topic_count: int = 0
    target_goal_count: int | None = None
    initial_count: int
    final_count: int
    initial_total_count: int | None = None
    final_total_count: int | None = None
    required_new_count: int
    planned_sample_count: int = 0
    plan_items: int = 0
    quota_units: int = 0
    exhausted_units: int = 0
    exhausted_missing_count: int = 0
    duplicate_shortfall: int = 0
    fetched: int = 0
    normalized: int = 0
    created: int = 0
    updated: int = 0
    existing: int = 0
    skipped_empty_title: int = 0
    skipped_empty_abstract: int = 0
    skipped_duplicates: int = 0
    failed: int = 0
    openalex_requests: int = 0
    elapsed_seconds: float = 0.0
    deferred: bool = False
    deferred_pages: int = 0
    pending_redis_key: str | None = None
    retry_after_seconds: float | None = None
    unit_summaries: list[dict[str, Any]] = Field(default_factory=list)
    rounds: list[dict[str, Any]] = Field(default_factory=list)
    errors: list[dict[str, Any]] = Field(default_factory=list)


__all__ = [
    "BatchImportResultDTO",
    "MissingStatsPolicy",
    "MonthlyStatsSource",
    "NormalizeMode",
    "OpenAlexBootstrapReportDTO",
    "OpenAlexBootstrapRequestDTO",
    "OpenAlexBootstrapTopicTargetDTO",
    "OpenAlexLoadPlanDTO",
    "OpenAlexLoadPlanItemDTO",
    "OpenAlexPendingPageDTO",
    "OpenAlexPlanUnitDTO",
    "OpenAlexUnitSummaryDTO",
    "TargetCountScope",
    "TargetCountUnit",
]
