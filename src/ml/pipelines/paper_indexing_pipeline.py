from __future__ import annotations

from dto.common import BatchOperationResultDTO
from dto.papers import (
    PaperBatchIndexingRequestDTO,
    PaperIndexingRequestDTO,
    PaperIndexingResponseDTO,
)
from ml.facades.paper_indexing import PaperIndexingFacade


class PaperIndexingPipeline:
    def __init__(self, facade: PaperIndexingFacade) -> None:
        self.facade = facade

    def run_one(
        self,
        paper_id: int,
        force_reindex: bool = False,
    ) -> PaperIndexingResponseDTO:
        return self.facade.index_paper(
            PaperIndexingRequestDTO(
                paper_id=paper_id,
                force_reindex=force_reindex,
            )
        )

    def run_many(
        self,
        paper_ids: list[int],
        force_reindex: bool = False,
    ) -> BatchOperationResultDTO:
        return self.facade.index_batch(
            PaperBatchIndexingRequestDTO(
                paper_ids=paper_ids,
                force_reindex=force_reindex,
            )
        )


__all__ = ["PaperIndexingPipeline"]
