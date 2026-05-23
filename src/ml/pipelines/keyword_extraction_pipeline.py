from __future__ import annotations

from dto.common import BatchOperationResultDTO
from dto.keywords import (
    KeywordExtractionBatchRequestDTO,
    KeywordExtractionMetadataDTO,
    KeywordExtractionResponseDTO,
    PaperKeywordExtractionBatchRequestDTO,
)
from ml.facades.keyword_extraction import KeywordExtractionFacade


class KeywordExtractionPipeline:
    def __init__(self, facade: KeywordExtractionFacade) -> None:
        self.facade = facade

    def run_one(
        self,
        metadata: KeywordExtractionMetadataDTO,
        *,
        top_k: int = 10,
        min_score: float | None = None,
    ) -> KeywordExtractionResponseDTO:
        return self.facade.extract_metadata(
            metadata,
            top_k=top_k,
            min_score=min_score,
        )

    def run_many(
        self,
        items: list[KeywordExtractionMetadataDTO],
        *,
        top_k: int = 10,
        min_score: float | None = None,
    ) -> list[KeywordExtractionResponseDTO]:
        return self.facade.extract_batch(
            KeywordExtractionBatchRequestDTO(
                items=items,
                top_k=top_k,
                min_score=min_score,
            )
        )

    def run_papers(
        self,
        request: PaperKeywordExtractionBatchRequestDTO,
    ) -> BatchOperationResultDTO:
        return self.facade.extract_papers(request)


__all__ = ["KeywordExtractionPipeline"]
