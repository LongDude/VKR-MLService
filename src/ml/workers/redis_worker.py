from __future__ import annotations

import logging
import time
from typing import Sequence

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
        failed_queue: str = FAILED_TASKS_QUEUE,
        dequeue_timeout_seconds: int = 1,
        idle_sleep_seconds: float = 1.0,
        logger: logging.Logger | None = None,
    ) -> None:
        self.redis_adapter = redis_adapter
        self.task_handler = task_handler
        self.queues = tuple(queues)
        self.failed_queue = failed_queue
        self.dequeue_timeout_seconds = dequeue_timeout_seconds
        self.idle_sleep_seconds = idle_sleep_seconds
        self.logger = logger or logging.getLogger(__name__)
        self._stop_requested = False

    def run_forever(self) -> None:
        self._stop_requested = False
        while not self._stop_requested:
            handled = self.run_once()
            if not handled:
                time.sleep(self.idle_sleep_seconds)

    def run_once(self) -> bool:
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

            try:
                self.task_handler.handle(message)
            except Exception as exc:
                self._handle_task_error(queue_name, message, exc)
            return True

        return False

    def stop(self) -> None:
        self._stop_requested = True

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
