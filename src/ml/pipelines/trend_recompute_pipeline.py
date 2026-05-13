from __future__ import annotations

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
    ) -> BatchOperationResultDTO:
        return self.facade.recompute_all_clusters(
            limit=limit,
            force_summary=force_summary,
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
