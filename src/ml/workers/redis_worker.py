from __future__ import annotations

from collections import defaultdict
import logging
import time
from typing import Any, Sequence

from adapters.redis_adapter import RedisAdapter
from core.exceptions import AppError
from ml.workers.task_handlers import MLTaskHandler


PAPER_INDEXING_QUEUE = "queue:paper_indexing"
ENTITY_INDEXING_QUEUE = "queue:entity_indexing"
CLUSTER_RECOMPUTE_QUEUE = "queue:cluster_recompute"
CLUSTER_DYNAMICS_RECOMPUTE_QUEUE = "queue:cluster_dynamics_recompute"
USER_PROFILE_RECOMPUTE_QUEUE = "queue:user_profile_recompute"
FAILED_TASKS_QUEUE = "queue:failed_tasks"

DEFAULT_QUEUE_ORDER = (
    PAPER_INDEXING_QUEUE,
    ENTITY_INDEXING_QUEUE,
    CLUSTER_RECOMPUTE_QUEUE,
    CLUSTER_DYNAMICS_RECOMPUTE_QUEUE,
    USER_PROFILE_RECOMPUTE_QUEUE,
)


class RedisMLWorker:
    def __init__(
        self,
        *,
        redis_adapter: RedisAdapter,
        task_handler: MLTaskHandler,
        queues: Sequence[str] = DEFAULT_QUEUE_ORDER,
        batch_sizes: dict[str, int] | None = None,
        failed_queue: str = FAILED_TASKS_QUEUE,
        dequeue_timeout_seconds: int = 1,
        idle_sleep_seconds: float = 1.0,
        logger: logging.Logger | None = None,
    ) -> None:
        self.redis_adapter = redis_adapter
        self.task_handler = task_handler
        self.queues = tuple(queues)
        self.batch_sizes = {
            queue_name: max(1, int(batch_size))
            for queue_name, batch_size in (batch_sizes or {}).items()
        }
        self.failed_queue = failed_queue
        self.dequeue_timeout_seconds = dequeue_timeout_seconds
        self.idle_sleep_seconds = idle_sleep_seconds
        self.logger = logger or logging.getLogger(__name__)
        self._stop_requested = False
        self.last_processed_message_count = 0

    def run_forever(self) -> None:
        self._stop_requested = False
        while not self._stop_requested:
            handled = self.run_once()
            if not handled:
                time.sleep(self.idle_sleep_seconds)

    def run_once(self, max_messages: int | None = None) -> bool:
        self.last_processed_message_count = 0
        if max_messages is not None and max_messages <= 0:
            return False

        for queue_name in self.queues:
            try:
                message = self.redis_adapter.dequeue(
                    queue_name,
                    timeout_seconds=self.dequeue_timeout_seconds,
                )
            except Exception:
                self.logger.exception(
                    "Failed to dequeue ML task",
                    extra={"queue_name": queue_name},
                )
                continue

            if message is None:
                continue

            messages = self._dequeue_batch(
                queue_name,
                first_message=message,
                max_messages=max_messages,
            )
            self.last_processed_message_count = len(messages)
            for task_message in self._coalesce_messages(queue_name, messages):
                self._handle_task_message(queue_name, task_message)
            return True

        return False

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
            messages.append(message)

        return messages

    def _coalesce_messages(
        self,
        queue_name: str,
        messages: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        if queue_name == PAPER_INDEXING_QUEUE:
            return self._coalesce_paper_indexing_messages(messages)
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
        try:
            self.task_handler.handle(message)
        except Exception as exc:
            self._handle_task_error(queue_name, message, exc)

    def _is_simple_paper_indexing_message(self, message: dict[str, Any]) -> bool:
        if message.get("task_type") != "paper_indexing":
            return False
        allowed_keys = {"task_type", "paper_id", "paper_ids", "force_reindex"}
        if set(message) - allowed_keys:
            return False
        return bool(self._paper_ids_from_message(message))

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

    def _handle_task_error(
        self,
        queue_name: str,
        message: dict,
        exc: Exception,
    ) -> None:
        self.logger.exception(
            "ML task failed",
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
                    "error": self._error_payload(exc),
                },
            )
        except Exception:
            self.logger.exception(
                "Failed to enqueue failed ML task",
                extra={"failed_queue": self.failed_queue},
            )

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


__all__ = [
    "CLUSTER_DYNAMICS_RECOMPUTE_QUEUE",
    "CLUSTER_RECOMPUTE_QUEUE",
    "DEFAULT_QUEUE_ORDER",
    "ENTITY_INDEXING_QUEUE",
    "FAILED_TASKS_QUEUE",
    "PAPER_INDEXING_QUEUE",
    "RedisMLWorker",
    "USER_PROFILE_RECOMPUTE_QUEUE",
]
