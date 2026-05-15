from __future__ import annotations

from dto.common import BatchOperationResultDTO
from ml.facades.research_entity_indexing import ResearchEntityIndexingFacade


class ResearchEntitiesPipeline:
    def __init__(self, facade: ResearchEntityIndexingFacade) -> None:
        self.facade = facade

    def run(
        self,
        force_reindex: bool = False,
        limit: int | None = None,
        offset: int = 0,
        entity_type: str = "all",
        batch_size: int = 128,
    ) -> BatchOperationResultDTO:
        return self.facade.index_all_entities(
            force_reindex=force_reindex,
            limit=limit,
            offset=offset,
            entity_type=entity_type,
            batch_size=batch_size,
        )


__all__ = ["ResearchEntitiesPipeline"]
