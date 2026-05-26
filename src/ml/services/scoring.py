from __future__ import annotations

from datetime import date
from math import log1p

from core.exceptions import InvalidRequestError


class ScoringService:
    TREND_GROWTH_WEIGHT = 0.60
    TREND_RECENT_COUNT_WEIGHT = 0.30
    TREND_ACCELERATION_WEIGHT = 0.10

    RECOMMENDATION_SEMANTIC_WEIGHT = 0.55
    RECOMMENDATION_TREND_WEIGHT = 0.25
    RECOMMENDATION_RECENCY_WEIGHT = 0.15
    RECOMMENDATION_CITATION_WEIGHT = 0.05
    RECOMMENDATION_SEMANTIC_WITH_TAGS_WEIGHT = 0.45
    RECOMMENDATION_TAG_MATCH_WEIGHT = 0.25
    RECOMMENDATION_TREND_WITH_TAGS_WEIGHT = 0.15
    RECOMMENDATION_RECENCY_WITH_TAGS_WEIGHT = 0.10
    RECOMMENDATION_CITATION_WITH_TAGS_WEIGHT = 0.05

    GROWTH_RATE_MIN = -1.0
    GROWTH_RATE_MAX = 1.0
    ACCELERATION_MIN = -1.0
    ACCELERATION_MAX = 1.0
    RECENT_PAPER_COUNT_MAX = 100.0
    CITATION_COUNT_MAX = 1000
    RECENCY_WINDOW_DAYS = 365 * 5

    def calculate_growth_rate(
        self,
        current_count: int,
        previous_count: int,
    ) -> float:
        self._validate_non_negative_count(current_count, "current_count")
        self._validate_non_negative_count(previous_count, "previous_count")
        return (current_count + 1) / (previous_count + 1) - 1

    def calculate_acceleration(
        self,
        current_growth_rate: float,
        previous_growth_rate: float,
    ) -> float:
        return current_growth_rate - previous_growth_rate

    def calculate_trend_score(
        self,
        growth_rate: float,
        recent_paper_count: int,
        acceleration: float = 0.0,
        citation_growth: float = 0.0,
    ) -> float:
        self._validate_non_negative_count(recent_paper_count, "recent_paper_count")
        _ = citation_growth

        normalized_growth_rate = self._normalize_score(
            growth_rate,
            self.GROWTH_RATE_MIN,
            self.GROWTH_RATE_MAX,
        )
        normalized_recent_paper_count = self._normalize_score(
            float(recent_paper_count),
            0.0,
            self.RECENT_PAPER_COUNT_MAX,
        )
        normalized_acceleration = self._normalize_score(
            acceleration,
            self.ACCELERATION_MIN,
            self.ACCELERATION_MAX,
        )

        score = (
            self.TREND_GROWTH_WEIGHT * normalized_growth_rate
            + self.TREND_RECENT_COUNT_WEIGHT * normalized_recent_paper_count
            + self.TREND_ACCELERATION_WEIGHT * normalized_acceleration
        )
        return self._clamp_score(score)

    def calculate_recency_score(
        self,
        publication_date: date | None,
        reference_date: date | None = None,
    ) -> float:
        if publication_date is None:
            return 0.0
        reference = reference_date or date.today()
        age_days = (reference - publication_date).days
        if age_days <= 0:
            return 1.0
        score = 1.0 - (age_days / self.RECENCY_WINDOW_DAYS)
        return self._clamp_score(score)

    def calculate_citation_score(
        self,
        cited_by_count: int | None,
    ) -> float:
        if cited_by_count is None or cited_by_count <= 0:
            return 0.0
        score = log1p(cited_by_count) / log1p(self.CITATION_COUNT_MAX)
        return self._clamp_score(score)

    def calculate_recommendation_score(
        self,
        semantic_score: float,
        trend_score: float = 0.0,
        recency_score: float = 0.0,
        citation_score: float = 0.0,
        tag_match_score: float | None = None,
    ) -> float:
        if tag_match_score is not None:
            score = (
                self.RECOMMENDATION_SEMANTIC_WITH_TAGS_WEIGHT * self._clamp_score(semantic_score)
                + self.RECOMMENDATION_TAG_MATCH_WEIGHT * self._clamp_score(tag_match_score)
                + self.RECOMMENDATION_TREND_WITH_TAGS_WEIGHT * self._clamp_score(trend_score)
                + self.RECOMMENDATION_RECENCY_WITH_TAGS_WEIGHT * self._clamp_score(recency_score)
                + self.RECOMMENDATION_CITATION_WITH_TAGS_WEIGHT * self._clamp_score(citation_score)
            )
            return self._clamp_score(score)

        score = (
            self.RECOMMENDATION_SEMANTIC_WEIGHT * self._clamp_score(semantic_score)
            + self.RECOMMENDATION_TREND_WEIGHT * self._clamp_score(trend_score)
            + self.RECOMMENDATION_RECENCY_WEIGHT * self._clamp_score(recency_score)
            + self.RECOMMENDATION_CITATION_WEIGHT * self._clamp_score(citation_score)
        )
        return self._clamp_score(score)

    def _validate_non_negative_count(self, value: int, name: str) -> None:
        if value < 0:
            raise InvalidRequestError(
                "Count values must be non-negative",
                details={"name": name, "value": value},
            )

    def _normalize_score(
        self,
        value: float,
        min_value: float,
        max_value: float,
    ) -> float:
        if max_value <= min_value:
            raise InvalidRequestError(
                "max_value must be greater than min_value",
                details={"min_value": min_value, "max_value": max_value},
            )
        return self._clamp_score((value - min_value) / (max_value - min_value))

    def _clamp_score(self, value: float) -> float:
        return max(0.0, min(1.0, float(value)))


__all__ = ["ScoringService"]
