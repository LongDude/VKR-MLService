from __future__ import annotations

from collections import defaultdict
import logging
import time
from typing import Any, Sequence

from adapters.redis_adapter import RedisAdapter
from core.exceptions import AppError
from ml.services.openalex_cooldown import (
    OPENALEX_BOOTSTRAP_PAPERS_PENDING_QUEUE,
    OPENALEX_COOLDOWN_KEY,
    OPENALEX_TOPIC_STATS_PENDING_QUEUE,
)
from ml.services.events import EventSink, MLEvent, NoopEventSink, TqdmEventSink
from ml.workers.task_handlers import MLTaskHandler


KEYWORD_EXTRACTION_QUEUE = "queue:keyword_extraction"
OPENALEX_TOPIC_STATS_QUEUE = "queue:openalex_topic_stats"
OPENALEX_BOOTSTRAP_PAPERS_QUEUE = "queue:openalex_bootstrap_papers"
PAPER_INDEXING_QUEUE = "queue:paper_indexing"
ENTITY_INDEXING_QUEUE = "queue:entity_indexing"
CLUSTER_RECOMPUTE_QUEUE = "queue:cluster_recompute"
CLUSTER_DYNAMICS_RECOMPUTE_QUEUE = "queue:cluster_dynamics_recompute"
TOPIC_QUARTER_REPORT_QUEUE = "queue:topic_quarter_reports"
USER_PROFILE_RECOMPUTE_QUEUE = "queue:user_profile_recompute"
FAILED_TASKS_QUEUE = "queue:failed_tasks"

DEFAULT_QUEUE_ORDER = (
    OPENALEX_TOPIC_STATS_PENDING_QUEUE,
    OPENALEX_BOOTSTRAP_PAPERS_PENDING_QUEUE,
    OPENALEX_TOPIC_STATS_QUEUE,
    OPENALEX_BOOTSTRAP_PAPERS_QUEUE,
    KEYWORD_EXTRACTION_QUEUE,
    PAPER_INDEXING_QUEUE,
    ENTITY_INDEXING_QUEUE,
    CLUSTER_RECOMPUTE_QUEUE,
    CLUSTER_DYNAMICS_RECOMPUTE_QUEUE,
    TOPIC_QUARTER_REPORT_QUEUE,
    USER_PROFILE_RECOMPUTE_QUEUE,
)

OPENALEX_RESOURCE_QUEUES = frozenset(
    {
        OPENALEX_TOPIC_STATS_PENDING_QUEUE,
        OPENALEX_BOOTSTRAP_PAPERS_PENDING_QUEUE,
        OPENALEX_TOPIC_STATS_QUEUE,
        OPENALEX_BOOTSTRAP_PAPERS_QUEUE,
    }
)


class RedisMLWorker:
    def __init__(
        self,
        *,
        redis_adapter: RedisAdapter,
        task_handler: MLTaskHandler,
        queues: Sequence[str] = DEFAULT_QUEUE_ORDER,
        batch_sizes: dict[str, int] | None = None,
        max_task_sizes: dict[str, int] | None = None,
        failed_queue: str = FAILED_TASKS_QUEUE,
        dequeue_timeout_seconds: int = 30,
        idle_sleep_seconds: float = 1.0,
        show_progress: bool = False,
        event_sink: EventSink | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self.redis_adapter = redis_adapter
        self.task_handler = task_handler
        self.queues = tuple(queues)
        self.batch_sizes = {
            queue_name: max(1, int(batch_size))
            for queue_name, batch_size in (batch_sizes or {}).items()
        }
        self.max_task_sizes = {
            queue_name: max(1, int(task_size))
            for queue_name, task_size in (max_task_sizes or {}).items()
        }
        self.failed_queue = failed_queue
        self.dequeue_timeout_seconds = dequeue_timeout_seconds
        self.idle_sleep_seconds = idle_sleep_seconds
        self.show_progress = show_progress
        self.event_sink = event_sink or (
            TqdmEventSink() if show_progress else NoopEventSink()
        )
        self.logger = logger or logging.getLogger(__name__)
        self._stop_requested = False
        self._current_queue_name: str | None = None
        self._current_message: dict[str, Any] | None = None
        self.last_processed_message_count = 0

    def run_forever(self) -> None:
        self._stop_requested = False
        while not self._stop_requested:
            handled = self.run_once()
            if not handled and not self._active_queues():
                time.sleep(self._idle_sleep_seconds())

    def run_once(self, max_messages: int | None = None) -> bool:
        self.last_processed_message_count = 0
        if max_messages is not None and max_messages <= 0:
            return False

        active_queues = self._active_queues()
        if not active_queues:
            return False

        try:
            result = self.redis_adapter.dequeue_any(
                active_queues,
                timeout_seconds=self._dequeue_timeout_seconds(),
            )
        except Exception:
            self.logger.exception(
                "Failed to dequeue ML task",
                extra={"queue_names": active_queues},
            )
            return False

        if result is None:
            return False

        queue_name, message = result
        message = self._normalize_dequeued_message(queue_name, message)
        messages = self._dequeue_batch(
            queue_name,
            first_message=message,
            max_messages=max_messages,
        )
        self.last_processed_message_count = len(messages)
        for task_message in self._coalesce_messages(queue_name, messages):
            for executable_message in self._split_oversized_message(
                queue_name,
                task_message,
            ):
                self._handle_task_message(queue_name, executable_message)
        return True

    def stop(self) -> None:
        self._stop_requested = True

    def _dequeue_batch(
        self,
        queue_name: str,
        *,
        first_message: dict[str, Any],
        max_messages: int | None,
    ) -> list[dict[str, Any]]:
        batch_limit = self.batch_sizes.get(queue_name, 1)
        if max_messages is not None:
            batch_limit = min(batch_limit, max_messages)
        messages = [first_message]

        while len(messages) < batch_limit:
            try:
                message = self.redis_adapter.dequeue_nowait(queue_name)
            except Exception:
                self.logger.exception(
                    "Failed to dequeue additional ML task",
                    extra={"queue_name": queue_name},
                )
                break
            if message is None:
                break
            messages.append(self._normalize_dequeued_message(queue_name, message))

        return messages

    def _normalize_dequeued_message(
        self,
        queue_name: str,
        message: dict[str, Any],
    ) -> dict[str, Any]:
        if (
            queue_name == OPENALEX_BOOTSTRAP_PAPERS_PENDING_QUEUE
            and "task_type" not in message
        ):
            return {"task_type": "resume_bootstrap_papers", "page": message}
        return message

    def _active_queues(self) -> tuple[str, ...]:
        if self._openalex_cooldown_active():
            return tuple(
                queue_name
                for queue_name in self.queues
                if queue_name not in OPENALEX_RESOURCE_QUEUES
            )
        return self.queues

    def _openalex_cooldown_active(self) -> bool:
        try:
            return self.redis_adapter.exists(OPENALEX_COOLDOWN_KEY)
        except Exception:
            self.logger.exception(
                "Failed to check OpenAlex cooldown",
                extra={"cooldown_key": OPENALEX_COOLDOWN_KEY},
            )
            return False

    def _openalex_cooldown_ttl(self) -> int | None:
        try:
            ttl = self.redis_adapter.ttl(OPENALEX_COOLDOWN_KEY)
        except Exception:
            self.logger.exception(
                "Failed to read OpenAlex cooldown TTL",
                extra={"cooldown_key": OPENALEX_COOLDOWN_KEY},
            )
            return None
        return ttl if ttl > 0 else None

    def _dequeue_timeout_seconds(self) -> int:
        ttl = self._openalex_cooldown_ttl()
        if ttl is None:
            return max(1, int(self.dequeue_timeout_seconds))
        return max(1, min(int(self.dequeue_timeout_seconds), ttl))

    def _idle_sleep_seconds(self) -> float:
        ttl = self._openalex_cooldown_ttl()
        if ttl is None:
            return self.idle_sleep_seconds
        return max(0.0, min(float(self.idle_sleep_seconds), float(ttl)))

    def _coalesce_messages(
        self,
        queue_name: str,
        messages: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        if queue_name == PAPER_INDEXING_QUEUE:
            return self._coalesce_paper_indexing_messages(messages)
        if queue_name == KEYWORD_EXTRACTION_QUEUE:
            return self._coalesce_keyword_extraction_messages(messages)
        if queue_name == CLUSTER_RECOMPUTE_QUEUE:
            return self._coalesce_cluster_recompute_messages(messages)
        return messages

    def _coalesce_paper_indexing_messages(
        self,
        messages: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        grouped_ids: dict[bool, list[int]] = defaultdict(list)
        seen_by_force: dict[bool, set[int]] = defaultdict(set)
        result: list[dict[str, Any]] = []

        for message in messages:
            if not self._is_simple_paper_indexing_message(message):
                result.append(message)
                continue

            force_reindex = bool(message.get("force_reindex", False))
            for paper_id in self._paper_ids_from_message(message):
                if paper_id in seen_by_force[force_reindex]:
                    continue
                grouped_ids[force_reindex].append(paper_id)
                seen_by_force[force_reindex].add(paper_id)

        for force_reindex, paper_ids in grouped_ids.items():
            if paper_ids:
                result.append(
                    {
                        "task_type": "paper_indexing",
                        "paper_ids": paper_ids,
                        "force_reindex": force_reindex,
                    }
                )
        return result

    def _coalesce_keyword_extraction_messages(
        self,
        messages: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        grouped_ids: dict[tuple[int | None, float | None, bool, bool], list[int]] = defaultdict(list)
        seen_by_options: dict[tuple[int | None, float | None, bool, bool], set[int]] = defaultdict(set)
        result: list[dict[str, Any]] = []

        for message in messages:
            if not self._is_simple_keyword_extraction_message(message):
                result.append(message)
                continue

            options = self._keyword_extraction_options(message)
            for paper_id in self._paper_ids_from_message(message):
                if paper_id in seen_by_options[options]:
                    continue
                grouped_ids[options].append(paper_id)
                seen_by_options[options].add(paper_id)

        for options, paper_ids in grouped_ids.items():
            top_k, min_score, skip_processed, skip_non_english = options
            if paper_ids:
                result.append(
                    {
                        "task_type": "keyword_extraction",
                        "paper_ids": paper_ids,
                        "top_k": top_k,
                        "min_score": min_score,
                        "skip_processed": skip_processed,
                        "skip_non_english": skip_non_english,
                    }
                )
        return result

    def _coalesce_cluster_recompute_messages(
        self,
        messages: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        grouped_topic_ids: dict[bool, list[int]] = defaultdict(list)
        seen_by_force: dict[bool, set[int]] = defaultdict(set)
        result: list[dict[str, Any]] = []

        for message in messages:
            if not self._is_simple_cluster_recompute_message(message):
                result.append(message)
                continue

            force_summary = bool(message.get("force_summary", False))
            for topic_id in self._topic_ids_from_message(message):
                if topic_id in seen_by_force[force_summary]:
                    continue
                grouped_topic_ids[force_summary].append(topic_id)
                seen_by_force[force_summary].add(topic_id)

        for force_summary, topic_ids in grouped_topic_ids.items():
            if topic_ids:
                result.append(
                    {
                        "task_type": "recompute_topic_clusters",
                        "topic_ids": topic_ids,
                        "force_summary": force_summary,
                    }
                )
        return result

    def _handle_task_message(
        self,
        queue_name: str,
        message: dict[str, Any],
    ) -> None:
        task_summary = self._task_summary(queue_name, message)
        self.logger.info(
            "Starting ML task queue=%s task_type=%s item_count=%s item_field=%s",
            task_summary["queue_name"],
            task_summary["task_type"],
            task_summary["item_count"],
            task_summary["item_field"],
            extra=task_summary,
        )
        started_at = time.monotonic()
        self._current_queue_name = queue_name
        self._current_message = message
        self._emit_worker_event(
            "worker_task_started",
            task_summary,
            current=0,
            total=task_summary["item_count"],
            message="Worker task started",
        )
        try:
            result = self.task_handler.handle(message)
        except KeyboardInterrupt as exc:
            self._rollback_handler_session()
            self._emit_worker_event(
                "worker_task_interrupted",
                task_summary,
                current=task_summary["item_count"],
                total=task_summary["item_count"],
                message="Task interrupted",
                payload={"error_type": "Interrupt"},
            )
            self._handle_task_interrupt(queue_name, message, exc)
            raise
        except Exception as exc:
            error_payload = self._error_payload(exc)
            self._emit_worker_event(
                "worker_task_failed",
                task_summary,
                current=task_summary["item_count"],
                total=task_summary["item_count"],
                message=str(exc),
                payload={
                    "error_type": exc.__class__.__name__,
                    "error": error_payload,
                },
            )
            self._handle_task_error(queue_name, message, exc)
        else:
            elapsed_seconds = round(time.monotonic() - started_at, 3)
            result_summary = self._safe_result_summary(result)
            self._emit_worker_event(
                "worker_task_completed",
                task_summary,
                current=task_summary["item_count"],
                total=task_summary["item_count"],
                message="Worker task completed",
                payload={
                    "elapsed_seconds": elapsed_seconds,
                    "result": result_summary,
                },
            )
            self.logger.info(
                "ML task completed queue=%s task_type=%s item_count=%s elapsed_seconds=%s result=%s",
                task_summary["queue_name"],
                task_summary["task_type"],
                task_summary["item_count"],
                elapsed_seconds,
                result_summary,
                extra={
                    **task_summary,
                    "elapsed_seconds": elapsed_seconds,
                    "task_result": result_summary,
                },
            )
        finally:
            self._current_queue_name = None
            self._current_message = None

    def _split_oversized_message(
        self,
        queue_name: str,
        message: dict[str, Any],
    ) -> list[dict[str, Any]]:
        if queue_name == PAPER_INDEXING_QUEUE:
            return self._split_oversized_paper_indexing_message(queue_name, message)
        if queue_name == KEYWORD_EXTRACTION_QUEUE:
            return self._split_oversized_keyword_extraction_message(queue_name, message)
        if queue_name == CLUSTER_RECOMPUTE_QUEUE:
            return self._split_oversized_cluster_recompute_message(queue_name, message)
        return [message]

    def _split_oversized_paper_indexing_message(
        self,
        queue_name: str,
        message: dict[str, Any],
    ) -> list[dict[str, Any]]:
        if message.get("task_type") != "paper_indexing":
            return [message]
        max_task_size = self.max_task_sizes.get(queue_name)
        if max_task_size is None:
            return [message]

        paper_ids = self._paper_ids_from_message(message)
        if len(paper_ids) <= max_task_size:
            return [message]

        force_reindex = bool(message.get("force_reindex", False))
        return [
            {
                "task_type": "paper_indexing",
                "paper_ids": paper_ids[index : index + max_task_size],
                "force_reindex": force_reindex,
            }
            for index in range(0, len(paper_ids), max_task_size)
        ]

    def _split_oversized_keyword_extraction_message(
        self,
        queue_name: str,
        message: dict[str, Any],
    ) -> list[dict[str, Any]]:
        if message.get("task_type") != "keyword_extraction":
            return [message]
        max_task_size = self.max_task_sizes.get(queue_name)
        if max_task_size is None:
            return [message]

        paper_ids = self._paper_ids_from_message(message)
        if len(paper_ids) <= max_task_size:
            return [message]

        top_k, min_score, skip_processed, skip_non_english = self._keyword_extraction_options(message)
        return [
            {
                "task_type": "keyword_extraction",
                "paper_ids": paper_ids[index : index + max_task_size],
                "top_k": top_k,
                "min_score": min_score,
                "skip_processed": skip_processed,
                "skip_non_english": skip_non_english,
            }
            for index in range(0, len(paper_ids), max_task_size)
        ]

    def _split_oversized_cluster_recompute_message(
        self,
        queue_name: str,
        message: dict[str, Any],
    ) -> list[dict[str, Any]]:
        if message.get("task_type") not in {"cluster_recompute", "recompute_topic_clusters"}:
            return [message]

        max_task_size = self.max_task_sizes.get(queue_name)
        if max_task_size is None:
            return [message]

        topic_ids = self._topic_ids_from_message(message)
        if len(topic_ids) <= max_task_size:
            return [message]

        force_summary = bool(message.get("force_summary", False))
        cluster_workers = message.get("cluster_workers")
        workflow_fields = {
            key: message[key]
            for key in (
                "source_topic_ids",
                "workflow_date_from",
                "workflow_date_to",
                "workflow_granularity",
                "enqueue_cluster_dynamics",
            )
            if key in message
        }
        return [
            {
                "task_type": "recompute_topic_clusters",
                "topic_ids": topic_ids[index : index + max_task_size],
                "force_summary": force_summary,
                **workflow_fields,
                **(
                    {"cluster_workers": int(cluster_workers)}
                    if cluster_workers is not None
                    else {}
                ),
            }
            for index in range(0, len(topic_ids), max_task_size)
        ]

    def _is_simple_paper_indexing_message(self, message: dict[str, Any]) -> bool:
        if message.get("task_type") != "paper_indexing":
            return False
        allowed_keys = {"task_type", "paper_id", "paper_ids", "force_reindex"}
        if set(message) - allowed_keys:
            return False
        return bool(self._paper_ids_from_message(message))

    def _is_simple_keyword_extraction_message(self, message: dict[str, Any]) -> bool:
        if message.get("task_type") != "keyword_extraction":
            return False
        allowed_keys = {
            "task_type",
            "paper_id",
            "paper_ids",
            "top_k",
            "min_score",
            "skip_processed",
            "skip_non_english",
        }
        if set(message) - allowed_keys:
            return False
        return bool(self._paper_ids_from_message(message))

    def _keyword_extraction_options(
        self,
        message: dict[str, Any],
    ) -> tuple[int | None, float | None, bool, bool]:
        top_k = message.get("top_k")
        min_score = message.get("min_score")
        return (
            int(top_k) if top_k is not None else None,
            float(min_score) if min_score is not None else None,
            bool(message.get("skip_processed", True)),
            bool(message.get("skip_non_english", False)),
        )

    def _is_simple_cluster_recompute_message(self, message: dict[str, Any]) -> bool:
        if message.get("task_type") not in {"cluster_recompute", "recompute_topic_clusters"}:
            return False
        if message.get("cluster_id"):
            return False
        allowed_keys = {
            "task_type",
            "topic_id",
            "topic_ids",
            "keyword_ids",
            "paper_id",
            "paper_ids",
            "text_hash",
            "text_hashes",
            "force_summary",
        }
        if set(message) - allowed_keys:
            return False
        return bool(self._topic_ids_from_message(message))

    def _paper_ids_from_message(self, message: dict[str, Any]) -> list[int]:
        paper_ids: list[int] = []
        if "paper_id" in message:
            try:
                paper_ids.append(int(message["paper_id"]))
            except (TypeError, ValueError):
                return []
        if "paper_ids" in message:
            raw_ids = message.get("paper_ids")
            if not isinstance(raw_ids, list):
                return []
            try:
                paper_ids.extend(int(paper_id) for paper_id in raw_ids)
            except (TypeError, ValueError):
                return []
        return paper_ids

    def _topic_ids_from_message(self, message: dict[str, Any]) -> list[int]:
        topic_ids: list[int] = []
        if "topic_id" in message:
            try:
                topic_ids.append(int(message["topic_id"]))
            except (TypeError, ValueError):
                return []
        if "topic_ids" in message:
            raw_ids = message.get("topic_ids")
            if not isinstance(raw_ids, list):
                return []
            try:
                topic_ids.extend(int(topic_id) for topic_id in raw_ids)
            except (TypeError, ValueError):
                return []
        return topic_ids

    def _task_summary(
        self,
        queue_name: str,
        message: dict[str, Any],
    ) -> dict[str, Any]:
        task_type = str(message.get("task_type") or "unknown")
        item_field = None
        item_count = 1

        paper_ids = self._paper_ids_from_message(message)
        if paper_ids:
            item_field = "paper_ids"
            item_count = len(paper_ids)

        topic_ids = self._topic_ids_from_message(message)
        if topic_ids:
            item_field = "topic_ids"
            item_count = len(topic_ids)

        cluster_ids = message.get("cluster_ids")
        if isinstance(cluster_ids, list) and cluster_ids:
            item_field = "cluster_ids"
            item_count = len(cluster_ids)

        if "cluster_id" in message:
            item_field = "cluster_id"
        elif isinstance(message.get("requests"), list):
            item_field = "requests"
            item_count = len(message["requests"])
        elif "user_id" in message:
            item_field = "user_id"
        elif "limit" in message and item_field is None:
            item_field = "limit"

        return {
            "queue_name": queue_name,
            "task_type": task_type,
            "item_field": item_field,
            "item_count": item_count,
            "task_message": self._safe_message_summary(message),
        }

    def _safe_message_summary(self, message: dict[str, Any]) -> dict[str, Any]:
        return self._safe_payload_summary(message)

    def _safe_result_summary(self, result: Any) -> dict[str, Any]:
        if result is None:
            return {}
        if hasattr(result, "model_dump"):
            value = result.model_dump(mode="json")
        elif hasattr(result, "dict"):
            value = result.dict()
        elif isinstance(result, dict):
            value = result
        else:
            value = {"value": result}
        safe_value = self._safe_payload_summary(value)
        return safe_value if isinstance(safe_value, dict) else {"value": safe_value}

    def _safe_payload_summary(self, value: Any) -> Any:
        if isinstance(value, dict):
            return {
                str(key): self._safe_payload_summary(item)
                for key, item in value.items()
            }
        if isinstance(value, list):
            return {
                "count": len(value),
                "sample": [
                    self._safe_payload_summary(item)
                    for item in value[:5]
                ],
            }
        if isinstance(value, str) and len(value) > 500:
            return f"{value[:500]}..."
        return value

    def _handle_task_error(
        self,
        queue_name: str,
        message: dict,
        exc: Exception,
    ) -> None:
        error_payload = self._error_payload(exc)
        self.logger.exception(
            "ML task failed error=%s",
            error_payload,
            extra={
                "queue_name": queue_name,
                "task_type": message.get("task_type"),
                "task_error": error_payload,
            },
        )
        try:
            self.redis_adapter.enqueue(
                self.failed_queue,
                {
                    "source_queue": queue_name,
                    "message": message,
                    "error": error_payload,
                },
            )
        except Exception:
            self.logger.exception(
                "Failed to enqueue failed ML task",
                extra={"failed_queue": self.failed_queue},
            )

    def _handle_task_interrupt(
        self,
        queue_name: str,
        message: dict[str, Any],
        exc: KeyboardInterrupt,
    ) -> None:
        self.logger.warning(
            "ML task interrupted",
            extra={
                "queue_name": queue_name,
                "task_type": message.get("task_type"),
            },
        )
        try:
            self.redis_adapter.enqueue(
                self.failed_queue,
                {
                    "source_queue": queue_name,
                    "message": message,
                    "error": {
                        "code": "Interrupt",
                        "message": "Task interrupted",
                        "details": {
                            "reason": exc.__class__.__name__,
                            "source_queue": queue_name,
                        },
                    },
                },
            )
        except Exception:
            self.logger.exception(
                "Failed to enqueue interrupted ML task",
                extra={"failed_queue": self.failed_queue},
            )

    def _rollback_handler_session(self) -> None:
        session = getattr(self.task_handler, "session", None)
        if session is None:
            return
        try:
            session.rollback()
        except Exception:
            self.logger.exception("Failed to rollback interrupted ML task session")

    def _error_payload(self, exc: Exception) -> dict:
        if isinstance(exc, AppError):
            return {
                "code": exc.code,
                "message": exc.message,
                "details": exc.details or {},
            }
        return {
            "code": exc.__class__.__name__,
            "message": str(exc),
            "details": {},
        }

    def _emit_worker_event(
        self,
        event_type: str,
        task_summary: dict[str, Any],
        *,
        current: int | None = None,
        total: int | None = None,
        message: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> None:
        self.event_sink.emit(
            MLEvent(
                event_type=event_type,
                task_type=str(task_summary["task_type"]),
                entity_id=task_summary.get("item_field") or "task",
                stage="worker",
                current=current,
                total=total,
                message=message,
                payload={
                    "queue_name": task_summary["queue_name"],
                    "item_count": task_summary["item_count"],
                    "item_field": task_summary.get("item_field"),
                    **(payload or {}),
                },
            )
        )


__all__ = [
    "CLUSTER_DYNAMICS_RECOMPUTE_QUEUE",
    "CLUSTER_RECOMPUTE_QUEUE",
    "DEFAULT_QUEUE_ORDER",
    "ENTITY_INDEXING_QUEUE",
    "FAILED_TASKS_QUEUE",
    "KEYWORD_EXTRACTION_QUEUE",
    "OPENALEX_BOOTSTRAP_PAPERS_PENDING_QUEUE",
    "OPENALEX_TOPIC_STATS_QUEUE",
    "OPENALEX_BOOTSTRAP_PAPERS_QUEUE",
    "OPENALEX_RESOURCE_QUEUES",
    "OPENALEX_TOPIC_STATS_PENDING_QUEUE",
    "PAPER_INDEXING_QUEUE",
    "RedisMLWorker",
    "TOPIC_QUARTER_REPORT_QUEUE",
    "USER_PROFILE_RECOMPUTE_QUEUE",
]
