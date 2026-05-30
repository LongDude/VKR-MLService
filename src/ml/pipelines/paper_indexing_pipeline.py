from __future__ import annotations

from datetime import date

from dto.common import BatchOperationResultDTO
from dto.papers import (
    PaperBatchIndexingRequestDTO,
    PaperIndexingRequestDTO,
    PaperIndexingResponseDTO,
    WorkflowGranularity,
)
from ml.facades.paper_indexing import PaperIndexingFacade


class PaperIndexingPipeline:
    def __init__(self, facade: PaperIndexingFacade) -> None:
        self.facade = facade

    def run_one(
        self,
        paper_id: int,
        force_reindex: bool = False,
        source_topic_ids: list[int] | None = None,
        workflow_date_from: date | None = None,
        workflow_date_to: date | None = None,
        workflow_granularity: str = "month",
        enqueue_cluster_dynamics: bool = False,
    ) -> PaperIndexingResponseDTO:
        return self.facade.index_paper(
            PaperIndexingRequestDTO(
                paper_id=paper_id,
                force_reindex=force_reindex,
                source_topic_ids=source_topic_ids or [],
                workflow_date_from=workflow_date_from,
                workflow_date_to=workflow_date_to,
                workflow_granularity=WorkflowGranularity(workflow_granularity),
                enqueue_cluster_dynamics=enqueue_cluster_dynamics,
            )
        )

    def run_many(
        self,
        paper_ids: list[int],
        force_reindex: bool = False,
        source_topic_ids: list[int] | None = None,
        workflow_date_from: date | None = None,
        workflow_date_to: date | None = None,
        workflow_granularity: str = "month",
        enqueue_cluster_dynamics: bool = False,
    ) -> BatchOperationResultDTO:
        return self.facade.index_batch(
            PaperBatchIndexingRequestDTO(
                paper_ids=paper_ids,
                force_reindex=force_reindex,
                source_topic_ids=source_topic_ids or [],
                workflow_date_from=workflow_date_from,
                workflow_date_to=workflow_date_to,
                workflow_granularity=WorkflowGranularity(workflow_granularity),
                enqueue_cluster_dynamics=enqueue_cluster_dynamics,
            )
        )


__all__ = ["PaperIndexingPipeline"]
