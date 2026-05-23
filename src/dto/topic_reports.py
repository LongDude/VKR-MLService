from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any, Literal

from pydantic import Field, model_validator

from .common import BaseDTO


TopicQuarterReportItemType = Literal[
    "research_problem",
    "method",
    "approach",
    "future_direction",
]
TopicQuarterReportMaturity = Literal[
    "emerging",
    "growing",
    "stable",
    "declining",
    "mature",
]
TopicQuarterReportPaperRole = Literal[
    "representative",
    "highly_cited",
    "emerging",
    "method_evidence",
    "problem_evidence",
]


class TopicQuarterReportGenerateRequestDTO(BaseDTO):
    """Request to generate one quarterly report for one topic."""

    topic_id: int = Field(ge=1)
    period_start: date
    period_end: date
    force: bool = False
    report_language: str = "ru"

    @model_validator(mode="after")
    def validate_period(self) -> "TopicQuarterReportGenerateRequestDTO":
        """Reject inverted report periods."""
        if self.period_start > self.period_end:
            raise ValueError("period_start must be before or equal to period_end")
        return self


class TopicQuarterReportBatchRequestDTO(BaseDTO):
    """Batch request for quarterly topic report generation."""

    requests: list[TopicQuarterReportGenerateRequestDTO] = Field(default_factory=list)


class TopicQuarterReportItemInputDTO(BaseDTO):
    """Structured report item produced by the LLM."""

    item_type: TopicQuarterReportItemType
    title: str
    description: str | None = None
    maturity: TopicQuarterReportMaturity | None = None
    evidence: dict[str, Any] = Field(default_factory=dict)
    sort_order: int = 0


class TopicQuarterReportPaperInputDTO(BaseDTO):
    """Paper link to attach to a quarterly report."""

    paper_id: int
    role: TopicQuarterReportPaperRole
    score: Decimal | None = None
    note: str | None = None


class TopicQuarterReportItemDTO(TopicQuarterReportItemInputDTO):
    """Stored quarterly report item."""

    id: int
    report_id: int
    created_at: datetime | None = None


class TopicQuarterReportPaperDTO(TopicQuarterReportPaperInputDTO):
    """Stored quarterly report paper link."""

    report_id: int


class TopicQuarterReportDTO(BaseDTO):
    """Stored quarterly report for a topic."""

    id: int
    topic_id: int
    period_start: date
    period_end: date
    period_key: str
    summary: str | None = None
    period_characterization: str | None = None
    dynamics_summary: str | None = None
    future_dynamics: str | None = None
    metrics: dict[str, Any] = Field(default_factory=dict)
    keyword_dynamics: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime | None = None
    updated_at: datetime | None = None
    items: list[TopicQuarterReportItemDTO] = Field(default_factory=list)
    papers: list[TopicQuarterReportPaperDTO] = Field(default_factory=list)


__all__ = [
    "TopicQuarterReportBatchRequestDTO",
    "TopicQuarterReportDTO",
    "TopicQuarterReportGenerateRequestDTO",
    "TopicQuarterReportItemDTO",
    "TopicQuarterReportItemInputDTO",
    "TopicQuarterReportItemType",
    "TopicQuarterReportMaturity",
    "TopicQuarterReportPaperDTO",
    "TopicQuarterReportPaperInputDTO",
    "TopicQuarterReportPaperRole",
]
