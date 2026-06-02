from __future__ import annotations

import logging
import time
from collections import defaultdict
from typing import Any, Iterable, Sequence

from adapters.redis_adapter import RedisAdapter
from core.exceptions import AppError
from core.logging import log_event
from ml.services.cluster_dynamics_tasks import release_cluster_dynamics_dedupe_keys
from ml.services.cluster_recompute_tasks import (
    build_cluster_recompute_message,
    release_cluster_recompute_dedupe_keys,
)
from ml.services.events import EventSink, MLEvent, NoopEventSink, TqdmEventSink
from ml.services.openalex_cooldown import (
    OPENALEX_BOOTSTRAP_PAPERS_PENDING_QUEUE,
    OPENALEX_COOLDOWN_KEY,
    OPENALEX_TOPIC_STATS_PENDING_QUEUE,
)
from ml.task_contracts import (
    ADMIN_COVERAGE_TASK_RESULT_KEY_PREFIX,
    CLUSTER_DYNAMICS_RECOMPUTE_QUEUE,
    CLUSTER_RECOMPUTE_QUEUE,
    ENTITY_INDEXING_QUEUE,
    FAILED_TASKS_QUEUE,
    KEYWORD_EXTRACTION_QUEUE,
    KEYWORD_EXTRACTION_TASK,
    ML_WORKER_SHUTDOWN_KEY,
    OPENALEX_BOOTSTRAP_PAPERS_QUEUE,
    OPENALEX_TOPIC_STATS_QUEUE,
    PAPER_INDEXING_QUEUE,
    PAPER_INDEXING_TASK,
    RECOMPUTE_TOPIC_CLUSTERS_TASK,
    RESUME_BOOTSTRAP_PAPERS_TASK,
    TOPIC_QUARTER_REPORT_QUEUE,
    USER_PROFILE_RECOMPUTE_QUEUE,
)
from ml.workers.task_handlers import MLTaskHandler

# TODO: вынести параметры в файл настроек
# TODO: Разработать корректную систему типов для сообщений - вместо словарей использовать контракты

ADMIN_COVERAGE_TASK_RESULT_TTL_SECONDS = 7 * 24 * 60 * 60

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
        admin_result_ttl_seconds: int = ADMIN_COVERAGE_TASK_RESULT_TTL_SECONDS,
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
        self.admin_result_ttl_seconds = max(1, int(admin_result_ttl_seconds))
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
        if self._consume_soft_shutdown_request():
            return False
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
        log_event(
            self.logger,
            "worker_batch_dequeued",
            level=logging.DEBUG,
            message_count=len(messages),
            queue=queue_name,
        )
        for task_message in self._coalesce_messages(queue_name, messages):
            for executable_message in self._split_oversized_message(
                queue_name,
                task_message,
            ):
                self._handle_task_message(queue_name, executable_message)
        return True

    def stop(self) -> None:
        self._stop_requested = True

    @property
    def stop_requested(self) -> bool:
        return self._stop_requested

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
            return {"task_type": RESUME_BOOTSTRAP_PAPERS_TASK, "page": message}
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

    def _consume_soft_shutdown_request(self) -> bool:
        try:
            payload = self.redis_adapter.consume_json(ML_WORKER_SHUTDOWN_KEY)
        except Exception:
            self.logger.exception(
                "Failed to consume worker shutdown request",
                extra={"shutdown_key": ML_WORKER_SHUTDOWN_KEY},
            )
            return False
        if payload is None:
            return False
        self._stop_requested = True
        log_event(
            self.logger,
            "worker_soft_shutdown_requested",
            shutdown_key=ML_WORKER_SHUTDOWN_KEY,
        )
        self._emit_worker_event(
            "worker_soft_shutdown_requested",
            {
                "queue_name": "worker",
                "task_type": "worker",
                "item_count": 0,
                "item_field": "shutdown",
            },
            current=0,
            total=0,
            message="Worker soft shutdown requested",
            payload={"shutdown_key": ML_WORKER_SHUTDOWN_KEY, "request": payload},
        )
        return True

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

    @staticmethod
    def _unique_preserve_order(values: Iterable[int]) -> list[int]:
        seen: set[int] = set()
        result: list[int] = []
        for value in values:
            if value in seen:
                continue
            seen.add(value)
            result.append(value)
        return result

    def _coalesce_paper_indexing_messages(
        self,
        messages: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        current_key: tuple[Any, ...] | None = None
        current_message: dict[str, Any] | None = None
        current_seen: set[int] = set()
        max_task_size = self.max_task_sizes.get(
            PAPER_INDEXING_QUEUE,
            self.batch_sizes.get(PAPER_INDEXING_QUEUE, 1),
        )

        for message in messages:
            if not self._is_simple_paper_indexing_message(message):
                result.append(message)
                current_key = None
                current_message = None
                current_seen = set()
                continue

            paper_ids = self._unique_preserve_order(
                self._paper_ids_from_message(message)
            )
            key = self._paper_indexing_coalesce_key(message)
            can_extend = (
                current_message is not None
                and current_key == key
                and len(current_message["paper_ids"])
                + sum(1 for paper_id in paper_ids if paper_id not in current_seen)
                <= max_task_size
            )
            if not can_extend:
                current_message = self._paper_indexing_coalesced_message(
                    message,
                    paper_ids=[],
                )
                result.append(current_message)
                current_key = key
                current_seen = set()

            # Typecheck guard
            if current_message is None:
                raise RuntimeError("Paper indexing coalescing invatiant violated")

            paper_ids_target = current_message["paper_ids"]
            if not isinstance(paper_ids_target, list):
                raise TypeError("paper_ids must be a list")

            for paper_id in paper_ids:
                if paper_id in current_seen:
                    continue
                paper_ids_target.append(paper_id)
                current_seen.add(paper_id)
            self._merge_paper_indexing_workflow_fields(current_message, message)

        return result

    def _coalesce_keyword_extraction_messages(
        self,
        messages: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        grouped_ids: dict[tuple[int | None, float | None, bool, bool], list[int]] = (
            defaultdict(list)
        )
        seen_by_options: dict[tuple[int | None, float | None, bool, bool], set[int]] = (
            defaultdict(set)
        )
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
                        "task_type": KEYWORD_EXTRACTION_TASK,
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
        grouped_topic_ids: dict[tuple[Any, ...], list[int]] = defaultdict(list)
        seen_by_options: dict[tuple[Any, ...], set[int]] = defaultdict(set)
        messages_by_options: dict[tuple[Any, ...], dict[str, Any]] = {}
        result: list[dict[str, Any]] = []

        for message in messages:
            if not self._is_simple_cluster_recompute_message(message):
                result.append(message)
                continue

            options = self._cluster_recompute_options(message)
            messages_by_options.setdefault(options, message)
            for topic_id in self._topic_ids_from_message(message):
                if topic_id in seen_by_options[options]:
                    continue
                grouped_topic_ids[options].append(topic_id)
                seen_by_options[options].add(topic_id)

        for options, topic_ids in grouped_topic_ids.items():
            if topic_ids:
                source = messages_by_options[options]
                result.append(
                    build_cluster_recompute_message(
                        topic_ids,
                        force_summary=bool(source.get("force_summary", False)),
                        cluster_workers=self._optional_cluster_workers(source),
                        workflow_options=source,
                    )
                )
        return result

    def _handle_task_message(
        self,
        queue_name: str,
        message: dict[str, Any],
    ) -> None:
        task_summary = self._task_summary(queue_name, message)
        log_event(
            self.logger,
            "worker_task_started",
            queue=task_summary["queue_name"],
            task_type=task_summary["task_type"],
            item_count=task_summary["item_count"],
            item_field=task_summary["item_field"],
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
            self._save_admin_coverage_task_result(message, result)
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
            log_event(
                self.logger,
                "worker_task_completed",
                queue=task_summary["queue_name"],
                task_type=task_summary["task_type"],
                item_count=task_summary["item_count"],
                elapsed_seconds=elapsed_seconds,
            )
        finally:
            if queue_name == CLUSTER_RECOMPUTE_QUEUE:
                self._release_cluster_recompute_dedupe_keys(message)
            if queue_name == CLUSTER_DYNAMICS_RECOMPUTE_QUEUE:
                self._release_cluster_dynamics_dedupe_keys(message)
            self._current_queue_name = None
            self._current_message = None

    def _save_admin_coverage_task_result(
        self,
        message: dict[str, Any],
        result: Any,
    ) -> None:
        task_id = str(message.get("client_task_id") or "").strip()
        if not task_id:
            return
        try:
            self.redis_adapter.set_json(
                f"{ADMIN_COVERAGE_TASK_RESULT_KEY_PREFIX}:{task_id}",
                self._result_payload(result),
                self.admin_result_ttl_seconds,
            )
        except Exception:
            self.logger.exception(
                "Failed to save admin coverage task result",
                extra={"client_task_id": task_id},
            )

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
        if message.get("task_type") != PAPER_INDEXING_TASK:
            return [message]
        max_task_size = self.max_task_sizes.get(queue_name)
        if max_task_size is None:
            return [message]

        paper_ids = self._paper_ids_from_message(message)
        if len(paper_ids) <= max_task_size:
            return [message]

        force_reindex = bool(message.get("force_reindex", False))
        workflow_fields = self._paper_indexing_workflow_fields(message)
        return [
            {
                "task_type": PAPER_INDEXING_TASK,
                "paper_ids": paper_ids[index : index + max_task_size],
                "force_reindex": force_reindex,
                **workflow_fields,
                **self._client_tracking_fields(message),
            }
            for index in range(0, len(paper_ids), max_task_size)
        ]

    def _split_oversized_keyword_extraction_message(
        self,
        queue_name: str,
        message: dict[str, Any],
    ) -> list[dict[str, Any]]:
        if message.get("task_type") != KEYWORD_EXTRACTION_TASK:
            return [message]
        max_task_size = self.max_task_sizes.get(queue_name)
        if max_task_size is None:
            return [message]

        paper_ids = self._paper_ids_from_message(message)
        if len(paper_ids) <= max_task_size:
            return [message]

        top_k, min_score, skip_processed, skip_non_english = (
            self._keyword_extraction_options(message)
        )
        return [
            {
                "task_type": KEYWORD_EXTRACTION_TASK,
                "paper_ids": paper_ids[index : index + max_task_size],
                "top_k": top_k,
                "min_score": min_score,
                "skip_processed": skip_processed,
                "skip_non_english": skip_non_english,
                **self._client_tracking_fields(message),
            }
            for index in range(0, len(paper_ids), max_task_size)
        ]

    def _split_oversized_cluster_recompute_message(
        self,
        queue_name: str,
        message: dict[str, Any],
    ) -> list[dict[str, Any]]:
        if message.get("task_type") != RECOMPUTE_TOPIC_CLUSTERS_TASK:
            return [message]

        max_task_size = self.max_task_sizes.get(queue_name)
        if max_task_size is None:
            return [message]

        topic_ids = self._topic_ids_from_message(message)
        if len(topic_ids) <= max_task_size:
            return [message]

        force_summary = bool(message.get("force_summary", False))
        cluster_workers = message.get("cluster_workers")
        return [
            {
                **build_cluster_recompute_message(
                    topic_ids[index : index + max_task_size],
                    force_summary=force_summary,
                    cluster_workers=(
                        int(cluster_workers) if cluster_workers is not None else None
                    ),
                    workflow_options=message,
                ),
                **self._client_tracking_fields(message),
            }
            for index in range(0, len(topic_ids), max_task_size)
        ]

    def _client_tracking_fields(self, message: dict[str, Any]) -> dict[str, Any]:
        return {
            key: message[key]
            for key in ("client_task_id", "client_workflow_id")
            if key in message
        }

    def _is_simple_paper_indexing_message(self, message: dict[str, Any]) -> bool:
        if message.get("task_type") != PAPER_INDEXING_TASK:
            return False
        allowed_keys = {
            "task_type",
            "paper_id",
            "paper_ids",
            "force_reindex",
            "source_topic_ids",
            "topic_ids",
            "workflow_date_from",
            "workflow_date_to",
            "workflow_granularity",
            "enqueue_cluster_dynamics",
        }
        if set(message) - allowed_keys:
            return False
        return bool(self._paper_ids_from_message(message))

    def _paper_indexing_coalesce_key(self, message: dict[str, Any]) -> tuple[Any, ...]:
        return (
            bool(message.get("force_reindex", False)),
            self._workflow_date_value(message, "workflow_date_from"),
            self._workflow_date_value(message, "workflow_date_to"),
            str(message.get("workflow_granularity") or "month"),
            bool(message.get("enqueue_cluster_dynamics", False)),
        )

    def _paper_indexing_coalesced_message(
        self,
        message: dict[str, Any],
        *,
        paper_ids: list[int],
    ) -> dict[str, Any]:
        return {
            "task_type": PAPER_INDEXING_TASK,
            "paper_ids": list(paper_ids),
            "force_reindex": bool(message.get("force_reindex", False)),
            **self._paper_indexing_workflow_fields(message),
        }

    def _paper_indexing_workflow_fields(
        self,
        message: dict[str, Any],
    ) -> dict[str, Any]:
        fields: dict[str, Any] = {}
        for key in (
            "workflow_date_from",
            "workflow_date_to",
            "workflow_granularity",
            "enqueue_cluster_dynamics",
        ):
            if key in message:
                fields[key] = message[key]
        source_topic_ids = self._source_topic_ids_from_message(message)
        if source_topic_ids:
            fields["source_topic_ids"] = source_topic_ids
        return fields

    def _merge_paper_indexing_workflow_fields(
        self,
        target: dict[str, Any],
        source: dict[str, Any],
    ) -> None:
        source_topic_ids = self._source_topic_ids_from_message(source)
        if source_topic_ids:
            existing = self._source_topic_ids_from_message(target)
            seen = set(existing)
            merged = list(existing)
            for topic_id in source_topic_ids:
                if topic_id in seen:
                    continue
                merged.append(topic_id)
                seen.add(topic_id)
            target["source_topic_ids"] = merged

    def _source_topic_ids_from_message(self, message: dict[str, Any]) -> list[int]:
        raw_ids = message.get("source_topic_ids")
        if raw_ids is None:
            raw_ids = message.get("topic_ids")
        if raw_ids is None:
            return []
        if not isinstance(raw_ids, list):
            return []
        result: list[int] = []
        for raw_id in raw_ids:
            try:
                topic_id = int(raw_id)
            except (TypeError, ValueError):
                return []
            if topic_id not in result:
                result.append(topic_id)
        return result

    def _workflow_date_value(self, message: dict[str, Any], key: str) -> str | None:
        value = message.get(key)
        if value is None:
            return None
        if hasattr(value, "isoformat"):
            return str(value.isoformat())
        return str(value)

    def _is_simple_keyword_extraction_message(self, message: dict[str, Any]) -> bool:
        if message.get("task_type") != KEYWORD_EXTRACTION_TASK:
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
        if message.get("task_type") != RECOMPUTE_TOPIC_CLUSTERS_TASK:
            return False
        if message.get("cluster_id"):
            return False
        allowed_keys = {
            "task_type",
            "topic_id",
            "topic_ids",
            "force_summary",
            "cluster_workers",
            "workflow_date_from",
            "workflow_date_to",
            "workflow_granularity",
            "enqueue_cluster_dynamics",
        }
        if set(message) - allowed_keys:
            return False
        return bool(self._topic_ids_from_message(message))

    def _cluster_recompute_options(self, message: dict[str, Any]) -> tuple[Any, ...]:
        return (
            bool(message.get("force_summary", False)),
            self._workflow_date_value(message, "workflow_date_from"),
            self._workflow_date_value(message, "workflow_date_to"),
            str(message.get("workflow_granularity") or "month"),
            bool(message.get("enqueue_cluster_dynamics", False)),
            self._optional_cluster_workers(message),
        )

    def _optional_cluster_workers(self, message: dict[str, Any]) -> int | None:
        value = message.get("cluster_workers")
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    def _release_cluster_recompute_dedupe_keys(
        self,
        message: dict[str, Any],
    ) -> None:
        try:
            release_cluster_recompute_dedupe_keys(self.redis_adapter, message)
        except Exception:
            self.logger.exception(
                "Failed to release cluster recompute dedupe keys",
                extra={"message": self._safe_message_summary(message)},
            )

    def _release_cluster_dynamics_dedupe_keys(
        self,
        message: dict[str, Any],
    ) -> None:
        try:
            release_cluster_dynamics_dedupe_keys(self.redis_adapter, message)
        except Exception:
            self.logger.exception(
                "Failed to release cluster dynamics dedupe keys",
                extra={"message": self._safe_message_summary(message)},
            )

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
        value = self._result_payload(result)
        safe_value = self._safe_payload_summary(value)
        return safe_value if isinstance(safe_value, dict) else {"value": safe_value}

    def _result_payload(self, result: Any) -> dict[str, Any]:
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
        return value if isinstance(value, dict) else {"value": value}

    def _safe_payload_summary(self, value: Any) -> Any:
        if isinstance(value, dict):
            return {
                str(key): self._safe_payload_summary(item)
                for key, item in value.items()
            }
        if isinstance(value, list):
            return {
                "count": len(value),
                "sample": [self._safe_payload_summary(item) for item in value[:5]],
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
        log_event(
            self.logger,
            "worker_task_failed",
            level=logging.ERROR,
            exc_info=True,
            error_code=error_payload.get("code"),
            queue=queue_name,
            task_type=message.get("task_type"),
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
    "ML_WORKER_SHUTDOWN_KEY",
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
