from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Literal

from pydantic import Field

from .charts import ChartDTO
from .common import BaseDTO, DateRangeDTO, PaginationDTO

TrendStatus = Literal["new", "emerging", "growing", "stable", "declining"]


class TrendMetricsDTO(BaseDTO):
    paper_count: int = 0
    previous_paper_count: int = 0
    growth_rate: Decimal | None = None
    trend_score: Decimal | None = None
    semantic_drift: Decimal | None = None
    citation_count_sum: int | None = None
    avg_cited_by_count: Decimal | None = None


class TrendClusterDTO(BaseDTO):
    id: int
    cluster_key: str
    cluster_type: str
    name: str
    summary: str | None = None
    status: TrendStatus | None = None
    source_topic_id: int | None = None
    metrics: TrendMetricsDTO = Field(default_factory=TrendMetricsDTO)
    top_keywords: list[str] = Field(default_factory=list)
    representative_paper_ids: list[int] = Field(default_factory=list)


class TrendDashboardRequestDTO(BaseDTO):
    date_range: DateRangeDTO = Field(default_factory=DateRangeDTO)
    statuses: list[TrendStatus] = Field(default_factory=list)
    cluster_type: str | None = None
    pagination: PaginationDTO = Field(default_factory=PaginationDTO)


class TrendDashboardResponseDTO(BaseDTO):
    clusters: list[TrendClusterDTO] = Field(default_factory=list)
    total: int = 0
    generated_at: date | None = None


class ClusterChartsRequestDTO(BaseDTO):
    cluster_id: int
    date_range: DateRangeDTO = Field(default_factory=DateRangeDTO)


class ClusterChartsResponseDTO(BaseDTO):
    cluster_id: int
    charts: list[ChartDTO] = Field(default_factory=list)


__all__ = [
    "ClusterChartsRequestDTO",
    "ClusterChartsResponseDTO",
    "TrendClusterDTO",
    "TrendDashboardRequestDTO",
    "TrendDashboardResponseDTO",
    "TrendMetricsDTO",
    "TrendStatus",
]
