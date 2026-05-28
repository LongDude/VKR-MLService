from __future__ import annotations

from dto.topic_analytics import (
    TopicAnalyticsInsightRequestDTO,
    TopicAnalyticsInsightResponseDTO,
)
from ml.facades.topic_analytics import TopicAnalyticsFacade


class TopicAnalyticsPipeline:
    """Pipeline wrapper for read-only Topic analytics insight calculations."""

    def __init__(self, facade: TopicAnalyticsFacade) -> None:
        self.facade = facade

    def insights(
        self, request: TopicAnalyticsInsightRequestDTO
    ) -> TopicAnalyticsInsightResponseDTO:
        return self.facade.insights(request)


__all__ = ["TopicAnalyticsPipeline"]
