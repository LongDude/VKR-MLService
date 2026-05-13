from __future__ import annotations

from dto.search import SemanticSearchMLResponseDTO, SemanticSearchRequestDTO
from ml.facades.semantic_search import SemanticSearchFacade


class SemanticSearchPipeline:
    def __init__(self, facade: SemanticSearchFacade) -> None:
        self.facade = facade

    def search(
        self,
        request: SemanticSearchRequestDTO,
    ) -> SemanticSearchMLResponseDTO:
        return self.facade.search(request)


__all__ = ["SemanticSearchPipeline"]
