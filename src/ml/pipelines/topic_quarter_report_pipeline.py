from __future__ import annotations

from dto.common import BatchOperationResultDTO, OperationResultDTO
from dto.topic_reports import TopicQuarterReportGenerateRequestDTO
from ml.facades.topic_quarter_reports import TopicQuarterReportFacade


class TopicQuarterReportPipeline:
    """Pipeline wrapper for quarterly topic report generation."""

    def __init__(self, facade: TopicQuarterReportFacade) -> None:
        self.facade = facade

    def generate_one(
        self,
        request: TopicQuarterReportGenerateRequestDTO,
    ) -> OperationResultDTO:
        """Generate one quarterly topic report."""
        return self.facade.generate_one(request)

    def generate_many(
        self,
        requests: list[TopicQuarterReportGenerateRequestDTO],
        *,
        show_progress: bool = True,
    ) -> BatchOperationResultDTO:
        """Generate many quarterly topic reports."""
        _ = show_progress
        return self.facade.generate_many(requests)


__all__ = ["TopicQuarterReportPipeline"]
