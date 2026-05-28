from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, is_dataclass
from datetime import date
from typing import Any, Callable, Literal, cast

from core.exceptions import AppError, InvalidRequestError
from dto.common import BatchOperationResultDTO, OperationResultDTO
from dto.keywords import PaperKeywordExtractionBatchRequestDTO
from dto.topic_reports import TopicQuarterReportGenerateRequestDTO
from ml.pipelines.cluster_dynamics_pipeline import ClusterDynamicsPipeline
from ml.pipelines.keyword_extraction_pipeline import KeywordExtractionPipeline
from ml.pipelines.paper_indexing_pipeline import PaperIndexingPipeline
from ml.pipelines.research_entities_pipeline import ResearchEntitiesPipeline
from ml.pipelines.topic_quarter_report_pipeline import TopicQuarterReportPipeline
from ml.pipelines.trend_recompute_pipeline import TrendRecomputePipeline
from ml.pipelines.user_profile_pipeline import UserProfilePipeline
from ml.services.cluster_dynamics_tasks import (
    acquire_cluster_dynamics_cluster_ids,
    build_cluster_dynamics_message,
    release_cluster_dynamics_dedupe_keys,
)
from ml.services.events import EventSink, MLEvent, NoopEventSink
from ml.services.openalex_cooldown import (
    OPENALEX_TOPIC_STATS_PENDING_QUEUE,
    set_openalex_cooldown,
)
from src.ml.services.openalex_topic_stats import OpenAlexTopicStatsCollector

Granularity = Literal["week", "month"]


class MLTaskHandler:
    def __init__(
        self,
        *,
        session: Any | None = None,
        keyword_extraction_pipeline: KeywordExtractionPipeline | None = None,
        paper_indexing_pipeline: PaperIndexingPipeline | None = None,
        research_entities_pipeline: ResearchEntitiesPipeline | None = None,
        trend_recompute_pipeline: TrendRecomputePipeline | None = None,
        cluster_dynamics_pipeline: ClusterDynamicsPipeline | None = None,
        topic_quarter_report_pipeline: TopicQuarterReportPipeline | None = None,
        user_profile_pipeline: UserProfilePipeline | None = None,
        openalex_topic_stats_collector_factory: Callable[
            [dict[str, Any]], OpenAlexTopicStatsCollector
        ]
        | None = None,
        openalex_paper_bootstrap_runner: Callable[[dict[str, Any]], Any] | None = None,
        redis_adapter: Any | None = None,
        event_sink: EventSink | None = None,
        cluster_recompute_workers: int = 1,
        cluster_recompute_pipeline_factory: Callable[
            [], tuple[TrendRecomputePipeline, Any]
        ]
        | None = None,
    ) -> None:
        self.session = session
        self.keyword_extraction_pipeline = keyword_extraction_pipeline
        self.paper_indexing_pipeline = paper_indexing_pipeline
        self.research_entities_pipeline = research_entities_pipeline
        self.trend_recompute_pipeline = trend_recompute_pipeline
        self.cluster_dynamics_pipeline = cluster_dynamics_pipeline
        self.topic_quarter_report_pipeline = topic_quarter_report_pipeline
        self.user_profile_pipeline = user_profile_pipeline
        self.openalex_topic_stats_collector_factory = (
            openalex_topic_stats_collector_factory
        )
        self.openalex_paper_bootstrap_runner = openalex_paper_bootstrap_runner
        self.redis_adapter = redis_adapter
        self.event_sink = event_sink or NoopEventSink()
        self.cluster_recompute_workers = max(1, int(cluster_recompute_workers))
        self.cluster_recompute_pipeline_factory = cluster_recompute_pipeline_factory

    def handle(self, message: dict[str, Any]) -> OperationResultDTO:
        try:
            result = self._handle(message)
        except Exception:
            if self.session is not None:
                self.session.rollback()
            raise
        if self.session is not None:
            self.session.commit()
        return result

    def _handle(self, message: Any) -> OperationResultDTO:
        if not isinstance(message, dict):
            raise InvalidRequestError("Task message must be a JSON object")
        message = cast(dict[str, Any], message)  # purely for type-checking

        task_type = self._required_str(message, "task_type")
        if task_type == "keyword_extraction":
            return self._handle_keyword_extraction(message)
        if task_type in {"collect_topic_stats", "collect-topic-stats"}:
            return self._handle_collect_topic_stats(message)
        if task_type in {
            "bootstrap_papers",
            "bootstrap-papers",
            "resume_bootstrap_papers",
        }:
            return self._handle_bootstrap_papers(message)
        if task_type == "paper_indexing":
            return self._handle_paper_indexing(message)
        if task_type in {"entity_indexing", "research_entities_indexing"}:
            return self._handle_entity_indexing(message)
        if task_type in {"cluster_recompute", "recompute_topic_clusters"}:
            return self._handle_cluster_recompute(message)
        if task_type == "cluster_dynamics_recompute":
            return self._handle_cluster_dynamics_recompute(message)
        if task_type == "topic_quarter_report":
            return self._handle_topic_quarter_report(message)
        if task_type == "user_profile_recompute":
            return self._handle_user_profile_recompute(message)

        raise InvalidRequestError(
            "Unknown ML task type",
            details={"task_type": task_type},
        )

    def _handle_keyword_extraction(self, message: dict[str, Any]) -> OperationResultDTO:
        pipeline = self._required_pipeline(
            self.keyword_extraction_pipeline,
            "keyword_extraction_pipeline",
        )
        if "paper_ids" in message:
            paper_ids = self._int_list_field(message, "paper_ids")
        elif "paper_id" in message:
            paper_ids = [self._required_int(message, "paper_id")]
        else:
            raise InvalidRequestError(
                "paper_id or paper_ids is required for keyword extraction",
            )

        top_k = self._optional_int(message, "top_k")
        result = pipeline.run_papers(
            PaperKeywordExtractionBatchRequestDTO(
                paper_ids=paper_ids,
                top_k=10 if top_k is None else top_k,
                min_score=self._optional_float(message, "min_score"),
                skip_processed=self._bool_field(
                    message, "skip_processed", default=True
                ),
                skip_non_english=self._bool_field(
                    message, "skip_non_english", default=False
                ),
            )
        )
        return self._batch_result(
            result,
            "Keyword extraction completed",
            task_type="keyword_extraction",
        )

    def _handle_paper_indexing(self, message: dict[str, Any]) -> OperationResultDTO:
        pipeline = self._required_pipeline(
            self.paper_indexing_pipeline,
            "paper_indexing_pipeline",
        )
        force_reindex = self._bool_field(message, "force_reindex", default=False)
        workflow_options = self._paper_workflow_options(message)

        if "paper_ids" in message:
            result = pipeline.run_many(
                self._int_list_field(message, "paper_ids"),
                force_reindex=force_reindex,
                **workflow_options,
            )
            return self._batch_result(
                result,
                "Paper indexing batch completed",
                task_type="paper_indexing",
            )

        response = pipeline.run_one(
            self._required_int(message, "paper_id"),
            force_reindex=force_reindex,
            **workflow_options,
        )
        return OperationResultDTO(
            success=True,
            message=response.message or "Paper indexing completed",
            details=self._dump_dto(response),
        )

    def _handle_collect_topic_stats(
        self, message: dict[str, Any]
    ) -> OperationResultDTO:
        if self.openalex_topic_stats_collector_factory is None:
            raise InvalidRequestError(
                "Pipeline is not configured",
                details={"pipeline": "openalex_topic_stats_collector"},
            )
        collector = self.openalex_topic_stats_collector_factory(message)
        result = collector.collect_and_store(
            date_from=self._required_date(message, "date_from"),
            date_to=self._required_date(message, "date_to"),
            topic_ids=self._optional_int_list_field(message, "topic_ids"),
            field_ids=self._optional_int_list_field(message, "field_ids"),
            subfield_ids=self._optional_int_list_field(message, "subfield_ids"),
            taxonomy_scope=self._taxonomy_scope(message.get("taxonomy_scope", "topic")),
            limit=self._optional_int(message, "limit"),
            offset=self._optional_int(message, "offset") or 0,
            languages=self._optional_str_list_field(message, "languages"),
            types=self._optional_str_list_field(message, "types"),
            batch_size=self._optional_int(message, "batch_size") or 500,
            group_by_page_size=self._optional_int(message, "group_by_page_size") or 200,
            normalize_january_first=self._bool_field(
                message,
                "normalize_january_first",
                default=True,
            ),
            dry_run=self._bool_field(message, "dry_run", default=False),
            show_progress=self._bool_field(message, "show_progress", default=False),
        )
        if result.deferred:
            self._enqueue_deferred_topic_stats(message, self._dump_dto(result))
        return OperationResultDTO(
            success=result.failed == 0,
            message="OpenAlex topic stats collection completed",
            details={
                "task_type": "collect_topic_stats",
                **self._dump_dto(result),
            },
        )

    def _enqueue_deferred_topic_stats(
        self,
        source_message: dict[str, Any],
        result: dict[str, Any],
    ) -> None:
        if self.redis_adapter is None:
            return
        pending_tasks = result.get("pending_tasks") or []
        if not isinstance(pending_tasks, list):
            return
        source_options = {
            key: source_message[key]
            for key in (
                "openalex_url",
                "openalex_api_key",
                "openalex_mailto",
                "rate_limit_rps",
                "max_retries",
                "primary_topic_only",
            )
            if key in source_message
        }
        for pending_task in pending_tasks:
            if not isinstance(pending_task, dict):
                continue
            self.redis_adapter.enqueue(
                OPENALEX_TOPIC_STATS_PENDING_QUEUE,
                {**source_options, **pending_task},
            )
        set_openalex_cooldown(
            self.redis_adapter,
            retry_after_seconds=result.get("retry_after_seconds"),
            source_queue=str(
                source_message.get("source_queue") or "queue:openalex_topic_stats"
            ),
            task_type="collect_topic_stats",
        )

    def _handle_bootstrap_papers(self, message: dict[str, Any]) -> OperationResultDTO:
        if self.openalex_paper_bootstrap_runner is None:
            raise InvalidRequestError(
                "Pipeline is not configured",
                details={"pipeline": "openalex_paper_bootstrap"},
            )
        result = self.openalex_paper_bootstrap_runner(message)
        details = self._dump_dto(result)
        failed = int(details.get("failed") or 0) if isinstance(details, dict) else 0
        return OperationResultDTO(
            success=failed == 0,
            message="OpenAlex paper bootstrap completed",
            details={
                "task_type": "bootstrap_papers",
                **details,
            },
        )

    def _handle_entity_indexing(self, message: dict[str, Any]) -> OperationResultDTO:
        pipeline = self._required_pipeline(
            self.research_entities_pipeline,
            "research_entities_pipeline",
        )
        result = pipeline.run(
            force_reindex=self._bool_field(message, "force_reindex", default=False),
            limit=self._optional_int(message, "limit"),
            offset=self._optional_int(message, "offset") or 0,
            entity_type=str(message.get("entity_type") or "all"),
            batch_size=self._optional_int(message, "batch_size") or 128,
        )
        return self._batch_result(
            result,
            "Research entity indexing completed",
            task_type="entity_indexing",
        )

    def _handle_cluster_recompute(self, message: dict[str, Any]) -> OperationResultDTO:
        pipeline: TrendRecomputePipeline = self._required_pipeline(
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
            topic_ids = self._topic_ids_from_message(message)
            if not topic_ids and str(cluster_id).startswith("topic:"):
                try:
                    topic_ids = [int(str(cluster_id).removeprefix("topic:"))]
                except ValueError:
                    topic_ids = []
            self._enqueue_cluster_dynamics_if_requested(message, topic_ids, 0)
            return OperationResultDTO(
                success=True,
                message="Cluster recompute completed",
                details=self._dump_dto(cluster),
            )

        topic_ids = self._topic_ids_from_message(message)
        if topic_ids:
            result = BatchOperationResultDTO(total=len(topic_ids))
            cluster_workers = (
                self._optional_int(message, "cluster_workers")
                or self.cluster_recompute_workers
            )
            cluster_workers = max(1, cluster_workers)
            self._emit(
                "cluster_batch_started",
                "cluster_recompute",
                entity_id="worker_batch",
                stage="topics",
                current=0,
                total=len(topic_ids),
                message=f"Recomputing {len(topic_ids)} topic clusters",
            )
            if cluster_workers == 1:
                for index, topic_id in enumerate(topic_ids, start=1):
                    self._recompute_one_topic_cluster(
                        pipeline,
                        topic_id,
                        force_summary=force_summary,
                        result=result,
                    )
                    self._emit_cluster_batch_progress(
                        index, len(topic_ids), topic_id, result
                    )
            else:
                with ThreadPoolExecutor(max_workers=cluster_workers) as executor:
                    futures = {
                        executor.submit(
                            self._recompute_one_topic_cluster_isolated,
                            pipeline,
                            topic_id,
                            force_summary=force_summary,
                        ): topic_id
                        for topic_id in topic_ids
                    }
                    for index, future in enumerate(as_completed(futures), start=1):
                        topic_id = futures[future]
                        try:
                            future.result()
                        except Exception as exc:
                            result.failed += 1
                            result.errors.append(
                                self._task_error_payload(f"topic:{topic_id}", exc)
                            )
                        else:
                            result.updated += 1
                        self._emit_cluster_batch_progress(
                            index, len(topic_ids), topic_id, result
                        )
            self._emit(
                "cluster_batch_completed",
                "cluster_recompute",
                entity_id="worker_batch",
                stage="topics",
                current=len(topic_ids),
                total=len(topic_ids),
                message=(
                    f"Topic cluster batch completed: updated={result.updated} "
                    f"failed={result.failed}"
                ),
                payload=result.model_dump(mode="json"),
            )
            self._enqueue_cluster_dynamics_if_requested(
                message, topic_ids, result.failed
            )
            return self._batch_result(
                result,
                "Topic cluster recompute completed",
                task_type="cluster_recompute",
            )

        result = pipeline.recompute_all(
            force_summary=force_summary,
            limit=self._optional_int(message, "limit"),
            date_from=self._optional_date(message, "date_from"),
            date_to=self._optional_date(message, "date_to"),
            batch_size=self._optional_int(message, "batch_size") or 500,
        )
        return self._batch_result(
            result,
            "All clusters recompute completed",
            task_type="cluster_recompute",
        )

    def _paper_workflow_options(self, message: dict[str, Any]) -> dict[str, Any]:
        return {
            "source_topic_ids": self._optional_int_list_field(
                message,
                "source_topic_ids",
            )
            or self._optional_int_list_field(message, "topic_ids")
            or [],
            "workflow_date_from": self._optional_date(message, "workflow_date_from"),
            "workflow_date_to": self._optional_date(message, "workflow_date_to"),
            "workflow_granularity": str(message.get("workflow_granularity") or "month"),
            "enqueue_cluster_dynamics": self._bool_field(
                message,
                "enqueue_cluster_dynamics",
                default=False,
            ),
        }

    def _enqueue_cluster_dynamics_if_requested(
        self,
        message: dict[str, Any],
        topic_ids: list[int],
        failed_count: int,
    ) -> None:
        if failed_count:
            return
        if not self._bool_field(message, "enqueue_cluster_dynamics", default=False):
            return
        if self.redis_adapter is None:
            return
        date_from = self._optional_date(message, "workflow_date_from")
        date_to = self._optional_date(message, "workflow_date_to")
        if date_from is None or date_to is None:
            return
        granularity = self._granularity(message.get("workflow_granularity", "month"))
        cluster_ids = [f"topic:{topic_id}" for topic_id in topic_ids]
        if not cluster_ids:
            return
        accepted_cluster_ids = acquire_cluster_dynamics_cluster_ids(
            self.redis_adapter,
            cluster_ids,
            date_from=date_from,
            date_to=date_to,
            granularity=granularity,
        )
        if not accepted_cluster_ids:
            return
        task_message = build_cluster_dynamics_message(
            accepted_cluster_ids,
            date_from=date_from,
            date_to=date_to,
            granularity=granularity,
        )
        try:
            self.redis_adapter.enqueue("queue:cluster_dynamics_recompute", task_message)
        except Exception:
            release_cluster_dynamics_dedupe_keys(self.redis_adapter, task_message)
            raise

    def _recompute_one_topic_cluster(
        self,
        pipeline: TrendRecomputePipeline,
        topic_id: int,
        *,
        force_summary: bool,
        result: BatchOperationResultDTO,
    ) -> None:
        cluster_id = f"topic:{topic_id}"
        try:
            pipeline.recompute_cluster(
                cluster_id,
                force_summary=force_summary,
            )
        except Exception as exc:
            if self.session is not None:
                self.session.rollback()
            result.failed += 1
            result.errors.append(self._task_error_payload(cluster_id, exc))
        else:
            result.updated += 1

    def _recompute_one_topic_cluster_isolated(
        self,
        fallback_pipeline: TrendRecomputePipeline,
        topic_id: int,
        *,
        force_summary: bool,
    ) -> None:
        if self.cluster_recompute_pipeline_factory is None:
            fallback_pipeline.recompute_cluster(
                f"topic:{topic_id}",
                force_summary=force_summary,
            )
            return

        pipeline, session = self.cluster_recompute_pipeline_factory()
        try:
            pipeline.recompute_cluster(
                f"topic:{topic_id}",
                force_summary=force_summary,
            )
            if session is not None:
                session.commit()
        except Exception:
            if session is not None:
                session.rollback()
            raise
        finally:
            if session is not None:
                session.close()

    def _emit_cluster_batch_progress(
        self,
        index: int,
        total: int,
        topic_id: int,
        result: BatchOperationResultDTO,
    ) -> None:
        self._emit(
            "cluster_batch_progress",
            "cluster_recompute",
            entity_id="worker_batch",
            stage="topics",
            current=index,
            total=total,
            message=(
                f"topic={topic_id} updated={result.updated} failed={result.failed}"
            ),
        )

    def _handle_cluster_dynamics_recompute(
        self,
        message: dict[str, Any],
    ) -> OperationResultDTO:
        pipeline = self._required_pipeline(
            self.cluster_dynamics_pipeline,
            "cluster_dynamics_pipeline",
        )
        if "cluster_ids" in message:
            cluster_ids = self._str_list_field(message, "cluster_ids")
            date_from = self._required_date(message, "date_from")
            date_to = self._required_date(message, "date_to")
            granularity = self._granularity(message.get("granularity", "month"))
            result = BatchOperationResultDTO(total=len(cluster_ids))
            for cluster_id in cluster_ids:
                try:
                    partial = pipeline.recompute(
                        cluster_id=cluster_id,
                        date_from=date_from,
                        date_to=date_to,
                        granularity=granularity,
                    )
                except Exception as exc:
                    result.failed += 1
                    result.errors.append(self._task_error_payload(cluster_id, exc))
                    continue
                result.updated += partial.updated
                result.skipped += partial.skipped
                result.failed += partial.failed
                result.errors.extend(
                    {"cluster_id": cluster_id, **error} for error in partial.errors
                )
            return self._batch_result(
                result,
                "Cluster dynamics recompute batch completed",
                task_type="cluster_dynamics_recompute",
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

    def _handle_topic_quarter_report(
        self,
        message: dict[str, Any],
    ) -> OperationResultDTO:
        pipeline = self._required_pipeline(
            self.topic_quarter_report_pipeline,
            "topic_quarter_report_pipeline",
        )
        requests = self._topic_report_requests(message)
        if len(requests) == 1:
            return pipeline.generate_one(requests[0])
        result = pipeline.generate_many(requests)
        return self._batch_result(
            result,
            "Topic quarter report batch completed",
            task_type="topic_quarter_report",
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

    def _topic_report_requests(
        self,
        message: dict[str, Any],
    ) -> list[TopicQuarterReportGenerateRequestDTO]:
        force = self._bool_field(message, "force", default=False)
        report_language = str(message.get("report_language") or "ru")
        raw_requests = message.get("requests")
        if raw_requests is None:
            raw_requests = [
                {
                    "topic_id": self._required_int(message, "topic_id"),
                    "period_start": self._required_date(message, "period_start"),
                    "period_end": self._required_date(message, "period_end"),
                }
            ]
        if not isinstance(raw_requests, list):
            raise InvalidRequestError(
                "Topic report task requests must be a list",
                details={"field": "requests", "value": raw_requests},
            )
        requests: list[TopicQuarterReportGenerateRequestDTO] = []
        for item in raw_requests:
            if not isinstance(item, dict):
                raise InvalidRequestError(
                    "Topic report task request must be an object",
                    details={"item": item},
                )
            payload = {
                **item,
                "force": item.get("force", force),
                "report_language": item.get("report_language", report_language),
            }
            requests.append(
                TopicQuarterReportGenerateRequestDTO.model_validate(payload)
            )
        if not requests:
            raise InvalidRequestError("Topic report task requires at least one request")
        return requests

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

    def _optional_float(self, message: dict[str, Any], field: str) -> float | None:
        value = message.get(field)
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError) as exc:
            raise InvalidRequestError(
                "Task field must be a number",
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

    def _optional_int_list_field(
        self,
        message: dict[str, Any],
        field: str,
    ) -> list[int] | None:
        if field not in message or message.get(field) is None:
            return None
        return self._int_list_field(message, field)

    def _str_list_field(self, message: dict[str, Any], field: str) -> list[str]:
        value = message.get(field)
        if not isinstance(value, list):
            raise InvalidRequestError(
                "Task field must be a list",
                details={"field": field, "value": value},
            )
        result = [str(item).strip() for item in value if str(item).strip()]
        if not result:
            raise InvalidRequestError(
                "Task list field must contain strings",
                details={"field": field, "value": value},
            )
        return result

    def _optional_str_list_field(
        self,
        message: dict[str, Any],
        field: str,
    ) -> list[str] | None:
        if field not in message or message.get(field) is None:
            return None
        value = message.get(field)
        if isinstance(value, str):
            return [item.strip() for item in value.split(",") if item.strip()]
        return self._str_list_field(message, field)

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

    def _optional_date(self, message: dict[str, Any], field: str) -> date | None:
        if field not in message or message.get(field) is None:
            return None
        return self._required_date(message, field)

    def _granularity(self, value: Any) -> Granularity:
        if value in {"week", "month"}:
            return value
        raise InvalidRequestError(
            "granularity must be 'week' or 'month'",
            details={"granularity": value},
        )

    def _taxonomy_scope(self, value: Any) -> str:
        if value in {"topic", "field", "subfield"}:
            return str(value)
        raise InvalidRequestError(
            "taxonomy_scope must be one of: topic, field, subfield",
            details={"taxonomy_scope": value},
        )

    def _dump_dto(self, value: Any) -> dict[str, Any]:
        if hasattr(value, "model_dump"):
            return value.model_dump(mode="json")
        if hasattr(value, "dict"):
            return value.dict()
        if is_dataclass(value):
            return asdict(value)
        if isinstance(value, dict):
            return value
        return {"value": value}

    def _task_error_payload(self, task_id: str, exc: Exception) -> dict[str, Any]:
        if isinstance(exc, AppError):
            return {
                "task_id": task_id,
                "code": exc.code,
                "message": exc.message,
                "details": exc.details or {},
            }
        return {
            "task_id": task_id,
            "code": exc.__class__.__name__,
            "message": str(exc),
            "details": {},
        }

    def _emit(
        self,
        event_type: str,
        task_type: str,
        *,
        entity_id: str | int | None = None,
        stage: str | None = None,
        current: int | None = None,
        total: int | None = None,
        message: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> None:
        self.event_sink.emit(
            MLEvent(
                event_type=event_type,
                task_type=task_type,
                entity_id=entity_id,
                stage=stage,
                current=current,
                total=total,
                message=message,
                payload=payload or {},
            )
        )


__all__ = ["MLTaskHandler"]
