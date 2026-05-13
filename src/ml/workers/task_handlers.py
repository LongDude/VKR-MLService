from __future__ import annotations

from datetime import date
from typing import Any, Literal

from core.exceptions import InvalidRequestError
from dto.common import BatchOperationResultDTO, OperationResultDTO
from ml.pipelines.cluster_dynamics_pipeline import ClusterDynamicsPipeline
from ml.pipelines.paper_indexing_pipeline import PaperIndexingPipeline
from ml.pipelines.research_entities_pipeline import ResearchEntitiesPipeline
from ml.pipelines.trend_recompute_pipeline import TrendRecomputePipeline
from ml.pipelines.user_profile_pipeline import UserProfilePipeline


Granularity = Literal["week", "month"]


class MLTaskHandler:
    def __init__(
        self,
        *,
        paper_indexing_pipeline: PaperIndexingPipeline | None = None,
        research_entities_pipeline: ResearchEntitiesPipeline | None = None,
        trend_recompute_pipeline: TrendRecomputePipeline | None = None,
        cluster_dynamics_pipeline: ClusterDynamicsPipeline | None = None,
        user_profile_pipeline: UserProfilePipeline | None = None,
    ) -> None:
        self.paper_indexing_pipeline = paper_indexing_pipeline
        self.research_entities_pipeline = research_entities_pipeline
        self.trend_recompute_pipeline = trend_recompute_pipeline
        self.cluster_dynamics_pipeline = cluster_dynamics_pipeline
        self.user_profile_pipeline = user_profile_pipeline

    def handle(self, message: dict) -> OperationResultDTO:
        if not isinstance(message, dict):
            raise InvalidRequestError("Task message must be a JSON object")

        task_type = self._required_str(message, "task_type")
        if task_type == "paper_indexing":
            return self._handle_paper_indexing(message)
        if task_type in {"entity_indexing", "research_entities_indexing"}:
            return self._handle_entity_indexing(message)
        if task_type in {"cluster_recompute", "recompute_topic_clusters"}:
            return self._handle_cluster_recompute(message)
        if task_type == "cluster_dynamics_recompute":
            return self._handle_cluster_dynamics_recompute(message)
        if task_type == "user_profile_recompute":
            return self._handle_user_profile_recompute(message)

        raise InvalidRequestError(
            "Unknown ML task type",
            details={"task_type": task_type},
        )

    def _handle_paper_indexing(self, message: dict[str, Any]) -> OperationResultDTO:
        pipeline = self._required_pipeline(
            self.paper_indexing_pipeline,
            "paper_indexing_pipeline",
        )
        force_reindex = self._bool_field(message, "force_reindex", default=False)

        if "paper_ids" in message:
            result = pipeline.run_many(
                self._int_list_field(message, "paper_ids"),
                force_reindex=force_reindex,
            )
            return self._batch_result(
                result,
                "Paper indexing batch completed",
                task_type="paper_indexing",
            )

        response = pipeline.run_one(
            self._required_int(message, "paper_id"),
            force_reindex=force_reindex,
        )
        return OperationResultDTO(
            success=True,
            message=response.message or "Paper indexing completed",
            details=self._dump_dto(response),
        )

    def _handle_entity_indexing(self, message: dict[str, Any]) -> OperationResultDTO:
        pipeline = self._required_pipeline(
            self.research_entities_pipeline,
            "research_entities_pipeline",
        )
        result = pipeline.run(
            force_reindex=self._bool_field(message, "force_reindex", default=False),
            limit=self._optional_int(message, "limit"),
        )
        return self._batch_result(
            result,
            "Research entity indexing completed",
            task_type="entity_indexing",
        )

    def _handle_cluster_recompute(self, message: dict[str, Any]) -> OperationResultDTO:
        pipeline = self._required_pipeline(
            self.trend_recompute_pipeline,
            "trend_recompute_pipeline",
        )
        force_summary = self._bool_field(message, "force_summary", default=False)

        cluster_id = message.get("cluster_id")
        if cluster_id:
            cluster = pipeline.recompute_cluster(
                str(cluster_id),
                force_summary=force_summary,
            )
            return OperationResultDTO(
                success=True,
                message="Cluster recompute completed",
                details=self._dump_dto(cluster),
            )

        topic_ids = self._topic_ids_from_message(message)
        if topic_ids:
            result = BatchOperationResultDTO(total=len(topic_ids))
            for topic_id in topic_ids:
                pipeline.recompute_cluster(
                    f"topic:{topic_id}",
                    force_summary=force_summary,
                )
                result.updated += 1
            return self._batch_result(
                result,
                "Topic cluster recompute completed",
                task_type="cluster_recompute",
            )

        result = pipeline.recompute_all(
            force_summary=force_summary,
            limit=self._optional_int(message, "limit"),
        )
        return self._batch_result(
            result,
            "All clusters recompute completed",
            task_type="cluster_recompute",
        )

    def _handle_cluster_dynamics_recompute(
        self,
        message: dict[str, Any],
    ) -> OperationResultDTO:
        pipeline = self._required_pipeline(
            self.cluster_dynamics_pipeline,
            "cluster_dynamics_pipeline",
        )
        result = pipeline.recompute(
            cluster_id=self._required_str(message, "cluster_id"),
            date_from=self._required_date(message, "date_from"),
            date_to=self._required_date(message, "date_to"),
            granularity=self._granularity(message.get("granularity", "month")),
        )
        return self._batch_result(
            result,
            "Cluster dynamics recompute completed",
            task_type="cluster_dynamics_recompute",
        )

    def _handle_user_profile_recompute(
        self,
        message: dict[str, Any],
    ) -> OperationResultDTO:
        pipeline = self._required_pipeline(
            self.user_profile_pipeline,
            "user_profile_pipeline",
        )
        profile = pipeline.recompute_user(self._required_int(message, "user_id"))
        return OperationResultDTO(
            success=True,
            message="User profile recompute completed",
            details=self._dump_dto(profile),
        )

    def _topic_ids_from_message(self, message: dict[str, Any]) -> list[int]:
        if "topic_ids" in message:
            return self._int_list_field(message, "topic_ids")
        if "topic_id" in message:
            return [self._required_int(message, "topic_id")]
        return []

    def _batch_result(
        self,
        result: BatchOperationResultDTO,
        message: str,
        *,
        task_type: str,
    ) -> OperationResultDTO:
        return OperationResultDTO(
            success=result.failed == 0,
            message=message,
            details={"task_type": task_type, **self._dump_dto(result)},
        )

    def _required_pipeline(self, pipeline: Any, name: str) -> Any:
        if pipeline is None:
            raise InvalidRequestError(
                "Pipeline is not configured",
                details={"pipeline": name},
            )
        return pipeline

    def _required_str(self, message: dict[str, Any], field: str) -> str:
        value = message.get(field)
        if value is None or not str(value).strip():
            raise InvalidRequestError(
                "Required task field is missing",
                details={"field": field},
            )
        return str(value).strip()

    def _required_int(self, message: dict[str, Any], field: str) -> int:
        value = message.get(field)
        try:
            return int(value)
        except (TypeError, ValueError) as exc:
            raise InvalidRequestError(
                "Required task field must be an integer",
                details={"field": field, "value": value},
            ) from exc

    def _optional_int(self, message: dict[str, Any], field: str) -> int | None:
        value = message.get(field)
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError) as exc:
            raise InvalidRequestError(
                "Task field must be an integer",
                details={"field": field, "value": value},
            ) from exc

    def _bool_field(
        self,
        message: dict[str, Any],
        field: str,
        *,
        default: bool,
    ) -> bool:
        value = message.get(field, default)
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            lowered = value.strip().lower()
            if lowered in {"true", "1", "yes"}:
                return True
            if lowered in {"false", "0", "no"}:
                return False
        raise InvalidRequestError(
            "Task field must be a boolean",
            details={"field": field, "value": value},
        )

    def _int_list_field(self, message: dict[str, Any], field: str) -> list[int]:
        value = message.get(field)
        if not isinstance(value, list):
            raise InvalidRequestError(
                "Task field must be a list",
                details={"field": field, "value": value},
            )
        try:
            return [int(item) for item in value]
        except (TypeError, ValueError) as exc:
            raise InvalidRequestError(
                "Task list field must contain integers",
                details={"field": field, "value": value},
            ) from exc

    def _required_date(self, message: dict[str, Any], field: str) -> date:
        value = message.get(field)
        if isinstance(value, date):
            return value
        if isinstance(value, str):
            try:
                return date.fromisoformat(value[:10])
            except ValueError as exc:
                raise InvalidRequestError(
                    "Task field must be an ISO date",
                    details={"field": field, "value": value},
                ) from exc
        raise InvalidRequestError(
            "Task field must be an ISO date",
            details={"field": field, "value": value},
        )

    def _granularity(self, value: Any) -> Granularity:
        if value in {"week", "month"}:
            return value
        raise InvalidRequestError(
            "granularity must be 'week' or 'month'",
            details={"granularity": value},
        )

    def _dump_dto(self, value: Any) -> dict[str, Any]:
        if hasattr(value, "model_dump"):
            return value.model_dump(mode="json")
        if hasattr(value, "dict"):
            return value.dict()
        if isinstance(value, dict):
            return value
        return {"value": value}


__all__ = ["MLTaskHandler"]
