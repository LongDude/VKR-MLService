from __future__ import annotations

from dto.recommendations import RecommendationRequestDTO, RecommendationResponseDTO
from ml.facades.recommendations import RecommendationFacade


class RecommendationPipeline:
    def __init__(self, facade: RecommendationFacade) -> None:
        self.facade = facade

    def recommend(
        self,
        request: RecommendationRequestDTO,
    ) -> RecommendationResponseDTO:
        return self.facade.recommend_for_user(request)


__all__ = ["RecommendationPipeline"]
