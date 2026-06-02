from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterable
from uuid import uuid4

from sqlalchemy import bindparam, text
from sqlalchemy.orm import Session

from adapters.redis_adapter import RedisAdapter
from core.config import Settings, get_settings
from repositories.taxonomy import TaxonomyRepository
from ml.services.worker_heartbeat import ML_WORKER_HEARTBEAT_KEY
from ml.task_contracts import (
    ADMIN_COVERAGE_TASK_RESULT_KEY_PREFIX,
    BOOTSTRAP_PAPERS_TASK,
    CLUSTER_DYNAMICS_RECOMPUTE_QUEUE,
    CLUSTER_DYNAMICS_RECOMPUTE_TASK,
    CLUSTER_RECOMPUTE_QUEUE,
    COLLECT_TOPIC_STATS_TASK,
    FAILED_TASKS_QUEUE,
    KEYWORD_EXTRACTION_QUEUE,
    KEYWORD_EXTRACTION_TASK,
    OPENALEX_BOOTSTRAP_PAPERS_QUEUE,
    OPENALEX_TOPIC_STATS_QUEUE,
    PAPER_INDEXING_QUEUE,
    PAPER_INDEXING_TASK,
    RECOMPUTE_TOPIC_CLUSTERS_TASK,
    TOPIC_QUARTER_REPORT_QUEUE,
    TOPIC_QUARTER_REPORT_TASK,
)

ACTIVE_TASKS_QUEUE = "queue:admin_data_coverage_active"
RECENT_TASKS_QUEUE = "queue:admin_data_coverage_recent"
ACTIVE_WORKFLOWS_QUEUE = "queue:admin_data_coverage_workflows_active"
RECENT_WORKFLOWS_QUEUE = "queue:admin_data_coverage_workflows_recent"
TASK_KEY_PREFIX = "admin:data_coverage:task"
WORKFLOW_KEY_PREFIX = "admin:data_coverage:workflow"
TRACKING_TTL_SECONDS = 7 * 24 * 60 * 60
RECENT_TTL_SECONDS = 24 * 60 * 60
EXHAUSTED_UNIT_KEY_PREFIX = "admin:data_coverage:exhausted"
STALE_AFTER = timedelta(hours=24)
MAX_TRACKED_ITEMS = 5000

PANEL_KEYS = {
    "monthly-stats",
    "sample-papers",
    "indexing",
    "keyphrases",
    "cluster-dynamics",
    "quarter-reports",
}

WORKFLOW_PRESETS = {
    "load-and-index": ["monthly-stats", "sample-papers", "indexing"],
    "analytics": ["cluster-dynamics", "keyphrases", "quarter-reports"],
    "full": [
        "monthly-stats",
        "sample-papers",
        "indexing",
        "cluster-dynamics",
        "keyphrases",
        "quarter-reports",
    ],
}


class AdminCoverageTaskService:
    """Orchestrate admin-triggered ML tasks while keeping workers unchanged."""

    def __init__(
        self,
        session: Session | None,
        redis_adapter: RedisAdapter,
        settings: Settings | None = None,
    ) -> None:
        self.session = session
        self.redis = redis_adapter
        self.settings = settings or get_settings()

    def worker_status(self) -> dict[str, Any]:
        try:
            redis_available = self.redis.ping()
            heartbeat = self.redis.get_json(ML_WORKER_HEARTBEAT_KEY)
            heartbeat_ttl = self.redis.ttl(ML_WORKER_HEARTBEAT_KEY)
        except Exception as exc:
            return {
                "redisAvailable": False,
                "workerAvailable": False,
                "canEnqueue": False,
                "heartbeat": None,
                "message": f"Redis is unavailable: {exc}",
            }

        worker_available = bool(redis_available and heartbeat and heartbeat_ttl > 0)
        return {
            "redisAvailable": bool(redis_available),
            "workerAvailable": worker_available,
            "canEnqueue": worker_available,
            "heartbeat": heartbeat,
            "message": (
                "Worker is available."
                if worker_available
                else "Redis is available, but an active worker heartbeat was not found."
            ),
        }

    def enqueue_panel(
        self,
        *,
        panel_key: str,
        topic_ids: list[int],
        period_from: str,
        period_to: str,
        workflow_id: str | None = None,
    ) -> dict[str, Any]:
        self._require_worker()
        if panel_key not in PANEL_KEYS:
            raise ValueError("Unknown coverage panel.")
        topics = self._existing_topic_ids(topic_ids)
        if not topics:
            raise ValueError("At least one existing topic id is required.")

        tasks: list[dict[str, Any]] = []
        skipped_periods = 0
        for period in self._periods(panel_key, period_from, period_to):
            missing_topics = self._missing_topic_ids(panel_key, topics, period)
            if not missing_topics:
                skipped_periods += 1
                continue
            queued = self._enqueue_period_task(
                panel_key=panel_key,
                topic_ids=missing_topics,
                period=period,
                workflow_id=workflow_id,
            )
            if queued is None:
                skipped_periods += 1
                continue
            tasks.append(queued)

        return {
            "panelKey": panel_key,
            "tasks": tasks,
            "enqueued": len(tasks),
            "skippedPeriods": skipped_periods,
        }

    def enqueue_workflow(
        self,
        *,
        preset: str,
        topic_ids: list[int],
        period_from: str,
        period_to: str,
    ) -> dict[str, Any]:
        self._require_worker()
        if preset not in WORKFLOW_PRESETS:
            raise ValueError("Unknown workflow preset.")
        topics = self._existing_topic_ids(topic_ids)
        if not topics:
            raise ValueError("At least one existing topic id is required.")
        self._parse_month(period_from)
        self._parse_month(period_to)

        workflow_id = str(uuid4())
        record = {
            "id": workflow_id,
            "preset": preset,
            "topicIds": topics,
            "periodFrom": period_from,
            "periodTo": period_to,
            "stages": list(WORKFLOW_PRESETS[preset]),
            "stageIndex": 0,
            "currentStage": None,
            "status": "queued",
            "createdAt": self._now_iso(),
            "updatedAt": self._now_iso(),
            "message": "Workflow created.",
        }
        self._save_workflow(record)
        self.redis.enqueue(ACTIVE_WORKFLOWS_QUEUE, {"workflow_id": workflow_id})
        self._advance_workflow(record)
        return record

    def list_tasks(self) -> dict[str, Any]:
        completed_panels = self._reconcile_tasks()
        return {
            "items": self._records(ACTIVE_TASKS_QUEUE, "task_id")
            + self._records(RECENT_TASKS_QUEUE, "task_id", limit=100),
            "completedPanelKeys": sorted(completed_panels),
        }

    def list_workflows(self) -> dict[str, Any]:
        completed_panels = self._reconcile_tasks()
        self._reconcile_workflows()
        return {
            "items": self._records(ACTIVE_WORKFLOWS_QUEUE, "workflow_id")
            + self._records(RECENT_WORKFLOWS_QUEUE, "workflow_id", limit=50),
            "completedPanelKeys": sorted(completed_panels),
        }

    def _require_worker(self) -> None:
        status = self.worker_status()
        if not status["canEnqueue"]:
            raise RuntimeError(str(status["message"]))

    def _enqueue_period_task(
        self,
        *,
        panel_key: str,
        topic_ids: list[int],
        period: dict[str, Any],
        workflow_id: str | None,
    ) -> dict[str, Any] | None:
        for active in self._records(ACTIVE_TASKS_QUEUE, "task_id"):
            if (
                active.get("panelKey") == panel_key
                and active.get("period") == period["key"]
                and active.get("workflowId") == workflow_id
                and {int(value) for value in active.get("topicIds", [])} == set(topic_ids)
            ):
                return None
        task_id = str(uuid4())
        message = self._task_message(
            task_id=task_id,
            workflow_id=workflow_id,
            panel_key=panel_key,
            topic_ids=topic_ids,
            period=period,
        )
        if message is None:
            return None
        queue_name, payload, tracked_topic_ids = message
        if not tracked_topic_ids:
            return None

        record = {
            "id": task_id,
            "workflowId": workflow_id,
            "panelKey": panel_key,
            "period": period["key"],
            "periodFrom": period["date_from"].isoformat(),
            "periodTo": period["date_to"].isoformat(),
            "topicIds": tracked_topic_ids,
            "queue": queue_name,
            "status": "queued",
            "createdAt": self._now_iso(),
            "updatedAt": self._now_iso(),
            "message": "Task queued.",
        }
        self.redis.set_json(
            self._task_key(task_id),
            record,
            self.settings.admin.tracking_ttl_seconds,
        )
        self.redis.enqueue(ACTIVE_TASKS_QUEUE, {"task_id": task_id})
        self.redis.enqueue(queue_name, payload)
        return record

    def _task_message(
        self,
        *,
        task_id: str,
        workflow_id: str | None,
        panel_key: str,
        topic_ids: list[int],
        period: dict[str, Any],
    ) -> tuple[str, dict[str, Any], list[int]] | None:
        common = {
            "client_task_id": task_id,
            **({"client_workflow_id": workflow_id} if workflow_id else {}),
        }
        date_from = period["date_from"].isoformat()
        date_to = period["date_to"].isoformat()

        if panel_key == "monthly-stats":
            return (
                OPENALEX_TOPIC_STATS_QUEUE,
                {
                    **common,
                    "task_type": COLLECT_TOPIC_STATS_TASK,
                    "date_from": date_from,
                    "date_to": date_to,
                    "taxonomy_scope": "topic",
                    "topic_ids": topic_ids,
                    "languages": list(self.settings.openalex.languages),
                    "types": list(self.settings.openalex.types),
                    "batch_size": self.settings.openalex.stats_batch_size,
                    "request_workers": self.settings.openalex.stats_request_workers,
                    "rate_limit_rps": self.settings.openalex.stats_rate_limit_rps,
                    "max_retries": self.settings.openalex.stats_max_retries,
                    "group_by_page_size": self.settings.openalex.stats_group_by_page_size,
                    "normalize_january_first": self.settings.openalex.normalize_january_first,
                    "primary_topic_only": self.settings.openalex.primary_topic_only,
                    "show_progress": False,
                },
                topic_ids,
            )

        if panel_key == "sample-papers":
            return (
                OPENALEX_BOOTSTRAP_PAPERS_QUEUE,
                {
                    **common,
                    "task_type": BOOTSTRAP_PAPERS_TASK,
                    "date_from": date_from,
                    "date_to": date_to,
                    "topic_ids": topic_ids,
                    "source_topic_ids": topic_ids,
                    "target_count": self.settings.openalex.bootstrap_target_count,
                    "target_scope": "month",
                    "target_unit": "topic",
                    "languages": list(self.settings.openalex.languages),
                    "types": list(self.settings.openalex.types),
                    "batch_size": self.settings.openalex.bootstrap_batch_size,
                    "request_workers": self.settings.openalex.bootstrap_request_workers,
                    "db_workers": self.settings.openalex.bootstrap_db_workers,
                    "rate_limit_rps": self.settings.openalex.bootstrap_rate_limit_rps,
                    "seed": self.settings.openalex.bootstrap_seed,
                    "per_page": self.settings.openalex.bootstrap_per_page,
                    "max_retries": self.settings.openalex.bootstrap_max_retries,
                    "skip_existing": False,
                    "enqueue_indexing": False,
                    "enqueue_cluster_dynamics": False,
                    "primary_topic_only": self.settings.openalex.primary_topic_only,
                    "workflow_date_from": date_from,
                    "workflow_date_to": date_to,
                    "workflow_granularity": "month",
                    "show_progress": False,
                },
                topic_ids,
            )

        if panel_key in {"indexing", "keyphrases"}:
            paper_rows = self._paper_candidates(panel_key, topic_ids, period)
            paper_ids = [paper_id for paper_id, _topic_id in paper_rows]
            tracked_topics = sorted({topic_id for _paper_id, topic_id in paper_rows})
            if not paper_ids:
                return None
            if panel_key == "indexing":
                return (
                    PAPER_INDEXING_QUEUE,
                    {
                        **common,
                        "task_type": PAPER_INDEXING_TASK,
                        "paper_ids": paper_ids,
                        "force_reindex": False,
                        "source_topic_ids": tracked_topics,
                        "workflow_date_from": date_from,
                        "workflow_date_to": date_to,
                        "workflow_granularity": "month",
                        "enqueue_cluster_dynamics": False,
                    },
                    tracked_topics,
                )
            return (
                KEYWORD_EXTRACTION_QUEUE,
                {
                    **common,
                    "task_type": KEYWORD_EXTRACTION_TASK,
                    "paper_ids": paper_ids,
                    "skip_processed": True,
                    "skip_non_english": False,
                },
                tracked_topics,
            )

        if panel_key == "cluster-dynamics":
            if workflow_id:
                return (
                    CLUSTER_RECOMPUTE_QUEUE,
                    {
                        **common,
                        "task_type": RECOMPUTE_TOPIC_CLUSTERS_TASK,
                        "topic_ids": topic_ids,
                        "workflow_date_from": date_from,
                        "workflow_date_to": date_to,
                        "workflow_granularity": "month",
                        "enqueue_cluster_dynamics": True,
                    },
                    topic_ids,
                )
            return (
                CLUSTER_DYNAMICS_RECOMPUTE_QUEUE,
                {
                    **common,
                    "task_type": CLUSTER_DYNAMICS_RECOMPUTE_TASK,
                    "cluster_ids": [f"topic:{topic_id}" for topic_id in topic_ids],
                    "date_from": date_from,
                    "date_to": date_to,
                    "granularity": "month",
                },
                topic_ids,
            )

        if panel_key == "quarter-reports":
            return (
                TOPIC_QUARTER_REPORT_QUEUE,
                {
                    **common,
                    "task_type": TOPIC_QUARTER_REPORT_TASK,
                    "requests": [
                        {
                            "topic_id": topic_id,
                            "period_start": date_from,
                            "period_end": date_to,
                        }
                        for topic_id in topic_ids
                    ],
                    "force": False,
                    "report_language": self.settings.operations.topic_report_language,
                },
                topic_ids,
            )
        return None

    def _reconcile_tasks(self) -> set[str]:
        completed_panels: set[str] = set()
        failed_ids = self._failed_task_ids()
        for marker in self.redis.peek_queue(
            ACTIVE_TASKS_QUEUE,
            self.settings.admin.max_tracked_items,
        ):
            task_id = str(marker.get("task_id") or "")
            if not task_id:
                self.redis.remove_from_queue(ACTIVE_TASKS_QUEUE, marker)
                continue
            record = self.redis.get_json(self._task_key(task_id))
            if record is None:
                self.redis.remove_from_queue(ACTIVE_TASKS_QUEUE, marker)
                continue
            if task_id in failed_ids:
                self._finish_task(marker, record, "failed", "Worker reported a task failure.")
                continue
            self._record_exhausted_units(task_id, record)
            period = self._period_from_record(record)
            missing = self._missing_topic_ids(
                str(record["panelKey"]),
                [int(value) for value in record.get("topicIds", [])],
                period,
            )
            if not missing:
                completed_panels.add(str(record["panelKey"]))
                self.redis.remove_from_queue(ACTIVE_TASKS_QUEUE, marker)
                self.redis.delete(self._task_key(task_id))
                continue
            if self._is_stale(record):
                self._finish_task(marker, record, "stale", "Task result was not detected before timeout.")
        return completed_panels

    def _finish_task(
        self,
        marker: dict[str, Any],
        record: dict[str, Any],
        status: str,
        message: str,
    ) -> None:
        record["status"] = status
        record["message"] = message
        record["updatedAt"] = self._now_iso()
        self.redis.set_json(
            self._task_key(str(record["id"])),
            record,
            self.settings.admin.recent_ttl_seconds,
        )
        self.redis.remove_from_queue(ACTIVE_TASKS_QUEUE, marker)
        self.redis.enqueue(RECENT_TASKS_QUEUE, {"task_id": str(record["id"])})

    def _reconcile_workflows(self) -> None:
        for marker in self.redis.peek_queue(
            ACTIVE_WORKFLOWS_QUEUE,
            self.settings.admin.max_tracked_items,
        ):
            workflow_id = str(marker.get("workflow_id") or "")
            record = self.redis.get_json(self._workflow_key(workflow_id)) if workflow_id else None
            if record is None:
                self.redis.remove_from_queue(ACTIVE_WORKFLOWS_QUEUE, marker)
                continue
            self._advance_workflow(record)

    def _advance_workflow(self, record: dict[str, Any]) -> None:
        if record.get("status") in {"completed", "failed", "stale", "blocked"}:
            return
        stages = [str(value) for value in record.get("stages", [])]
        while int(record.get("stageIndex", 0)) < len(stages):
            stage_index = int(record.get("stageIndex", 0))
            stage = stages[stage_index]
            record["currentStage"] = stage
            recent_failures = [
                item
                for item in self._records(RECENT_TASKS_QUEUE, "task_id", limit=500)
                if item.get("workflowId") == record["id"]
                and item.get("panelKey") == stage
                and item.get("status") in {"failed", "stale"}
            ]
            if recent_failures:
                record["status"] = "failed"
                record["message"] = f"Workflow stopped at {stage}."
                self._save_workflow(record)
                self._move_workflow_to_recent(record)
                return

            missing = self._missing_for_range(
                stage,
                [int(value) for value in record["topicIds"]],
                str(record["periodFrom"]),
                str(record["periodTo"]),
            )
            if not missing:
                record["stageIndex"] = stage_index + 1
                record["updatedAt"] = self._now_iso()
                continue

            active = [
                item
                for item in self._records(ACTIVE_TASKS_QUEUE, "task_id")
                if item.get("workflowId") == record["id"] and item.get("panelKey") == stage
            ]
            if not active:
                result = self.enqueue_panel(
                    panel_key=stage,
                    topic_ids=[int(value) for value in record["topicIds"]],
                    period_from=str(record["periodFrom"]),
                    period_to=str(record["periodTo"]),
                    workflow_id=str(record["id"]),
                )
                if int(result["enqueued"]) == 0:
                    record["status"] = "blocked"
                    record["message"] = f"No executable tasks could be created for {stage}."
                    self._save_workflow(record)
                    self._move_workflow_to_recent(record)
                    return

            record["status"] = "queued"
            record["message"] = f"Waiting for {stage}."
            self._save_workflow(record)
            return

        record["status"] = "completed"
        record["currentStage"] = None
        record["message"] = "Workflow completed."
        self._save_workflow(record, ttl_seconds=self.settings.admin.recent_ttl_seconds)
        self._move_workflow_to_recent(record)

    def _move_workflow_to_recent(self, record: dict[str, Any]) -> None:
        marker = {"workflow_id": str(record["id"])}
        self.redis.remove_from_queue(ACTIVE_WORKFLOWS_QUEUE, marker)
        self.redis.enqueue(RECENT_WORKFLOWS_QUEUE, marker)

    def _missing_for_range(
        self,
        panel_key: str,
        topic_ids: list[int],
        period_from: str,
        period_to: str,
    ) -> bool:
        return any(
            self._missing_topic_ids(panel_key, topic_ids, period)
            for period in self._periods(panel_key, period_from, period_to)
        )

    def _missing_topic_ids(
        self,
        panel_key: str,
        topic_ids: list[int],
        period: dict[str, Any],
    ) -> list[int]:
        if not topic_ids:
            return []
        covered = self._covered_topic_ids(panel_key, topic_ids, period)
        if panel_key == "sample-papers":
            covered.update(self._exhausted_topic_ids(panel_key, topic_ids, period))
        return sorted(set(topic_ids) - covered)

    def _record_exhausted_units(
        self,
        task_id: str,
        record: dict[str, Any],
    ) -> None:
        if record.get("panelKey") != "sample-papers":
            return
        result = self.redis.get_json(
            f"{ADMIN_COVERAGE_TASK_RESULT_KEY_PREFIX}:{task_id}"
        )
        if result is None:
            return
        details = result.get("details")
        if not isinstance(details, dict):
            return
        summaries = details.get("unit_summaries")
        if not isinstance(summaries, list):
            return
        expected_period = str(record.get("period") or "")
        expected_topics = {int(value) for value in record.get("topicIds", [])}
        for summary in summaries:
            if not isinstance(summary, dict) or not summary.get("exhausted"):
                continue
            period = str(summary.get("period") or "")
            try:
                topic_id = int(summary.get("topic_id"))
            except (TypeError, ValueError):
                continue
            if period != expected_period or topic_id not in expected_topics:
                continue
            self.redis.set_json(
                self._exhausted_unit_key("sample-papers", period, topic_id),
                {
                    "panelKey": "sample-papers",
                    "period": period,
                    "topicId": topic_id,
                    "reason": str(summary.get("reason") or "source_exhausted"),
                    "missingCount": int(summary.get("missing_count") or 0),
                    "updatedAt": self._now_iso(),
                },
            )

    def _exhausted_topic_ids(
        self,
        panel_key: str,
        topic_ids: list[int],
        period: dict[str, Any],
    ) -> set[int]:
        return {
            topic_id
            for topic_id in topic_ids
            if self.redis.exists(
                self._exhausted_unit_key(panel_key, str(period["key"]), topic_id)
            )
        }

    def _exhausted_unit_key(
        self,
        panel_key: str,
        period: str,
        topic_id: int,
    ) -> str:
        return f"{EXHAUSTED_UNIT_KEY_PREFIX}:{panel_key}:{period}:topic:{topic_id}"

    def _covered_topic_ids(
        self,
        panel_key: str,
        topic_ids: list[int],
        period: dict[str, Any],
    ) -> set[int]:
        date_from = period["date_from"]
        date_to = period["date_to"]
        if panel_key == "monthly-stats":
            sql = """
                SELECT DISTINCT topic_id
                FROM openalex_montly_topic_stats
                WHERE topic_id IN :topic_ids AND period_start = :date_from
            """
        elif panel_key == "sample-papers":
            sql = """
                SELECT DISTINCT primary_topic_id
                FROM papers
                WHERE primary_topic_id IN :topic_ids
                  AND publication_date >= :date_from AND publication_date <= :date_to
            """
        elif panel_key == "indexing":
            sql = """
                SELECT DISTINCT primary_topic_id
                FROM papers
                WHERE primary_topic_id IN :topic_ids
                  AND publication_date >= :date_from AND publication_date <= :date_to
                  AND is_indexed = TRUE
            """
        elif panel_key == "keyphrases":
            sql = """
                SELECT DISTINCT primary_topic_id
                FROM papers
                WHERE primary_topic_id IN :topic_ids
                  AND publication_date >= :date_from AND publication_date <= :date_to
                  AND extracted_keywords IS NOT NULL
                  AND extracted_keywords <> '{}'::jsonb
                  AND extracted_keywords <> '[]'::jsonb
            """
        elif panel_key == "cluster-dynamics":
            sql = """
                SELECT DISTINCT COALESCE(
                    c.source_topic_id,
                    CASE WHEN c.cluster_key ~ '^topic:[0-9]+$'
                         THEN SUBSTRING(c.cluster_key FROM 7)::bigint END
                ) AS topic_id
                FROM research_clusters c
                JOIN research_cluster_period_stats s ON s.cluster_id = c.id
                WHERE COALESCE(
                    c.source_topic_id,
                    CASE WHEN c.cluster_key ~ '^topic:[0-9]+$'
                         THEN SUBSTRING(c.cluster_key FROM 7)::bigint END
                ) IN :topic_ids
                  AND s.period_start <= :date_to AND s.period_end >= :date_from
            """
        elif panel_key == "quarter-reports":
            sql = """
                SELECT DISTINCT topic_id
                FROM topic_quarter_reports
                WHERE topic_id IN :topic_ids
                  AND period_start <= :date_to AND period_end >= :date_from
            """
        else:
            return set()
        statement = text(sql).bindparams(bindparam("topic_ids", expanding=True))
        values = self.session.scalars(
            statement,
            {"topic_ids": topic_ids, "date_from": date_from, "date_to": date_to},
        ).all()
        return {int(value) for value in values if value is not None}

    def _paper_candidates(
        self,
        panel_key: str,
        topic_ids: list[int],
        period: dict[str, Any],
    ) -> list[tuple[int, int]]:
        condition = (
            "p.is_indexed = FALSE"
            if panel_key == "indexing"
            else "(p.extracted_keywords IS NULL OR p.extracted_keywords = '{}'::jsonb OR p.extracted_keywords = '[]'::jsonb)"
        )
        statement = text(
            f"""
                SELECT p.id, p.primary_topic_id
                FROM papers p
                WHERE p.primary_topic_id IN :topic_ids
                  AND p.publication_date >= :date_from AND p.publication_date <= :date_to
                  AND {condition}
                ORDER BY p.id
            """
        ).bindparams(bindparam("topic_ids", expanding=True))
        rows = self.session.execute(
            statement,
            {
                "topic_ids": topic_ids,
                "date_from": period["date_from"],
                "date_to": period["date_to"],
            },
        ).all()
        return [(int(row[0]), int(row[1])) for row in rows]

    def _failed_task_ids(self) -> set[str]:
        result: set[str] = set()
        for wrapper in self.redis.peek_queue(FAILED_TASKS_QUEUE, 1000):
            message = wrapper.get("message")
            if isinstance(message, dict) and message.get("client_task_id"):
                result.add(str(message["client_task_id"]))
        return result

    def _existing_topic_ids(self, topic_ids: Iterable[int]) -> list[int]:
        requested = list(dict.fromkeys(int(value) for value in topic_ids if int(value) > 0))
        return [int(topic.id) for topic in TaxonomyRepository(self.session).list_topics_by_ids(requested)]

    def _periods(self, panel_key: str, period_from: str, period_to: str) -> list[dict[str, Any]]:
        month_from = self._parse_month(period_from)
        month_to = self._parse_month(period_to)
        if month_from > month_to:
            raise ValueError("periodFrom must not be later than periodTo.")
        if panel_key == "quarter-reports":
            return self._quarter_periods(month_from, month_to)
        return self._month_periods(month_from, month_to)

    def _month_periods(self, month_from: date, month_to: date) -> list[dict[str, Any]]:
        periods: list[dict[str, Any]] = []
        cursor = month_from
        while cursor <= month_to:
            next_month = self._add_months(cursor, 1)
            periods.append(
                {
                    "key": cursor.strftime("%Y-%m"),
                    "date_from": cursor,
                    "date_to": next_month - timedelta(days=1),
                }
            )
            cursor = next_month
        return periods

    def _quarter_periods(self, month_from: date, month_to: date) -> list[dict[str, Any]]:
        start_month = ((month_from.month - 1) // 3) * 3 + 1
        cursor = date(month_from.year, start_month, 1)
        periods: list[dict[str, Any]] = []
        while cursor <= month_to:
            next_quarter = self._add_months(cursor, 3)
            periods.append(
                {
                    "key": f"{cursor.year}-Q{((cursor.month - 1) // 3) + 1}",
                    "date_from": cursor,
                    "date_to": next_quarter - timedelta(days=1),
                }
            )
            cursor = next_quarter
        return periods

    def _period_from_record(self, record: dict[str, Any]) -> dict[str, Any]:
        return {
            "key": str(record["period"]),
            "date_from": date.fromisoformat(str(record["periodFrom"])[:10]),
            "date_to": date.fromisoformat(str(record["periodTo"])[:10]),
        }

    def _parse_month(self, value: str) -> date:
        try:
            parsed = date.fromisoformat(f"{value}-01")
        except ValueError as exc:
            raise ValueError("Periods must use YYYY-MM format.") from exc
        if parsed.strftime("%Y-%m") != value:
            raise ValueError("Periods must use YYYY-MM format.")
        return parsed

    def _add_months(self, value: date, count: int) -> date:
        month_index = value.year * 12 + value.month - 1 + count
        return date(month_index // 12, month_index % 12 + 1, 1)

    def _records(
        self,
        queue_name: str,
        marker_field: str,
        *,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        limit = limit or self.settings.admin.max_tracked_items
        records: list[dict[str, Any]] = []
        seen: set[str] = set()
        prefix = TASK_KEY_PREFIX if marker_field == "task_id" else WORKFLOW_KEY_PREFIX
        for marker in self.redis.peek_queue(queue_name, limit):
            item_id = str(marker.get(marker_field) or "")
            if not item_id or item_id in seen:
                continue
            seen.add(item_id)
            record = self.redis.get_json(f"{prefix}:{item_id}")
            if record is not None:
                records.append(record)
        return records

    def _save_workflow(
        self,
        record: dict[str, Any],
        *,
        ttl_seconds: int | None = None,
    ) -> None:
        ttl_seconds = ttl_seconds or self.settings.admin.tracking_ttl_seconds
        record["updatedAt"] = self._now_iso()
        self.redis.set_json(self._workflow_key(str(record["id"])), record, ttl_seconds)

    def _is_stale(self, record: dict[str, Any]) -> bool:
        try:
            created = datetime.fromisoformat(str(record["createdAt"]))
        except ValueError:
            return True
        return (
            datetime.now(timezone.utc) - created
            > timedelta(hours=self.settings.admin.stale_after_hours)
        )

    def _task_key(self, task_id: str) -> str:
        return f"{TASK_KEY_PREFIX}:{task_id}"

    def _workflow_key(self, workflow_id: str) -> str:
        return f"{WORKFLOW_KEY_PREFIX}:{workflow_id}"

    def _now_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat()


__all__ = [
    "ACTIVE_TASKS_QUEUE",
    "ACTIVE_WORKFLOWS_QUEUE",
    "AdminCoverageTaskService",
    "PANEL_KEYS",
    "WORKFLOW_PRESETS",
]
