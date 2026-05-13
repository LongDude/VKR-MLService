from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import Field

from .common import BaseDTO
from .papers import PaperShortDTO


class UserProfileDTO(BaseDTO):
    user_id: int
    source_counts: dict[str, Any] = Field(default_factory=dict)
    vector_dimension: int
    updated_at: datetime


class RecommendationRequestDTO(BaseDTO):
    user_id: int | None = None
    seed_paper_ids: list[int] = Field(default_factory=list)
    topic_ids: list[int] = Field(default_factory=list)
    keyword_ids: list[int] = Field(default_factory=list)
    limit: int = Field(default=20, ge=1, le=100)
    strategy: Literal["profile", "similar_papers", "hybrid"] = "hybrid"


class RecommendationScoreDetailsDTO(BaseDTO):
    semantic_score: float | None = None
    profile_score: float | None = None
    trend_score: float | None = None
    recency_score: float | None = None
    citation_score: float | None = None
    meta: dict[str, Any] = Field(default_factory=dict)


class RecommendationItemDTO(BaseDTO):
    paper: PaperShortDTO
    score: float
    reason: str | None = None
    score_details: RecommendationScoreDetailsDTO = Field(
        default_factory=RecommendationScoreDetailsDTO
    )


class RecommendationResponseDTO(BaseDTO):
    items: list[RecommendationItemDTO] = Field(default_factory=list)
    total: int = 0
    strategy: str | None = None


__all__ = [
    "RecommendationItemDTO",
    "RecommendationRequestDTO",
    "RecommendationResponseDTO",
    "RecommendationScoreDetailsDTO",
    "UserProfileDTO",
]
