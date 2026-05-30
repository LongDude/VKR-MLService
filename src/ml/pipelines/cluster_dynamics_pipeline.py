from __future__ import annotations

from datetime import date

from dto.common import BatchOperationResultDTO
from ml.facades.cluster_dynamics import ClusterDynamicsFacade
from dto.enums import WorkflowGranularity


class ClusterDynamicsPipeline:
    def __init__(self, facade: ClusterDynamicsFacade) -> None:
        self.facade = facade

    def recompute(
        self,
        cluster_id: str,
        date_from: date,
        date_to: date,
        granularity: WorkflowGranularity = WorkflowGranularity.MONTH,
    ) -> BatchOperationResultDTO:
        return self.facade.recompute_cluster_periods(
            cluster_id=cluster_id,
            date_from=date_from,
            date_to=date_to,
            granularity=granularity,
        )


__all__ = ["ClusterDynamicsPipeline"]
