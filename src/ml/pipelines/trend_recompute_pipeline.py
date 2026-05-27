from __future__ import annotations

from datetime import date

from dto.common import BatchOperationResultDTO
from dto.trends import TrendClusterDTO
from ml.facades.cluster_analytics import ClusterAnalyticsFacade


class TrendRecomputePipeline:
    def __init__(self, facade: ClusterAnalyticsFacade) -> None:
        self.facade = facade

    def recompute_all(
        self,
        force_summary: bool = False,
        limit: int | None = None,
        date_from: date | None = None,
        date_to: date | None = None,
        batch_size: int = 500,
    ) -> BatchOperationResultDTO:
        return self.facade.recompute_all_clusters(
            date_from=date_from,
            date_to=date_to,
            limit=limit,
            force_summary=force_summary,
            batch_size=batch_size,
        )

    def recompute_cluster(
        self,
        cluster_id: str,
        force_summary: bool = False,
    ) -> TrendClusterDTO:
        return self.facade.recompute_cluster(
            cluster_id,
            force_summary=force_summary,
        )


__all__ = ["TrendRecomputePipeline"]
