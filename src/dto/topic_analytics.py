from __future__ import annotations

from datetime import date
from typing import Literal

from pydantic import Field, model_validator

from .common import BaseDTO


MetricLevel = Literal["low", "medium", "high"]
RelationType = Literal["same subfield", "embedding similarity", "shared keyphrases"]
TrendStatus = Literal["emerging", "popular", "declining", "stable"]


class TopicAnalyticsInsightRequestDTO(BaseDTO):
    topic_id: int = Field(ge=1)
    period_start: date
    period_end: date
    comparison_window_months: Literal[6, 12, 24] = 12
    forecast_months: Literal[6, 12] = 12
    max_related: int = Field(default=12, ge=1, le=50)

    @model_validator(mode="after")
    def validate_period(self) -> "TopicAnalyticsInsightRequestDTO":
        if self.period_start > self.period_end:
            raise ValueError("period_start must be before or equal to period_end")
        return self


class TopicForecastPointDTO(BaseDTO):
    period_start: date
    forecast_count: float
    lower_bound: float
    upper_bound: float
    forecast_share: float | None = None
    lower_share: float | None = None
    upper_share: float | None = None
    model_name: str
    share_model_name: str | None = None
    subfield_model_name: str | None = None
    backtest_error_mae: float | None = None
    backtest_error_mape: float | None = None
    backtest_error_smape: float | None = None


class TopicDecompositionMetricDTO(BaseDTO):
    key: str
    label: str
    value: float | None = None
    unit: str = "score"
    normalized: float | None = Field(default=None, ge=0, le=1)
    level: MetricLevel | None = None


class RelatedTopicDTO(BaseDTO):
    topic_id: int
    name: str
    relation_type: RelationType
    similarity: float | None = None
    shared_keyphrases: list[str] = Field(default_factory=list)
    common_papers: int | None = None
    common_citations: int | None = None
    trend_status: TrendStatus | None = None


class TopicAnalyticsInsightResponseDTO(BaseDTO):
    forecast: list[TopicForecastPointDTO] = Field(default_factory=list)
    decomposition: list[TopicDecompositionMetricDTO] = Field(default_factory=list)
    related_topics: list[RelatedTopicDTO] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)


__all__ = [
    "RelatedTopicDTO",
    "TopicAnalyticsInsightRequestDTO",
    "TopicAnalyticsInsightResponseDTO",
    "TopicDecompositionMetricDTO",
    "TopicForecastPointDTO",
]
