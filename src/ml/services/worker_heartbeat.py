from __future__ import annotations

import logging
import threading
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from adapters.redis_adapter import RedisAdapter
from core.logging import log_event

ML_WORKER_HEARTBEAT_KEY = "ml:worker:heartbeat"


class WorkerHeartbeat:
    """Publish a short-lived worker liveness marker without touching task handling."""

    def __init__(
        self,
        redis_adapter: RedisAdapter,
        *,
        queues: list[str] | tuple[str, ...],
        interval_seconds: float = 10.0,
        ttl_seconds: int = 45,
        logger: logging.Logger | None = None,
    ) -> None:
        self.redis_adapter = redis_adapter
        self.queues = list(queues)
        self.interval_seconds = max(1.0, float(interval_seconds))
        self.ttl_seconds = max(2, int(ttl_seconds))
        self.logger = logger or logging.getLogger(__name__)
        self.worker_id = str(uuid4())
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread is not None:
            return
        self._publish()
        self._thread = threading.Thread(
            target=self._run,
            name="ml-worker-heartbeat",
            daemon=True,
        )
        self._thread.start()
        log_event(
            self.logger,
            "worker_heartbeat_started",
            interval_seconds=self.interval_seconds,
            queue_count=len(self.queues),
            ttl_seconds=self.ttl_seconds,
            worker_id=self.worker_id,
        )

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=self.interval_seconds + 1.0)
            self._thread = None
        log_event(self.logger, "worker_heartbeat_stopped", worker_id=self.worker_id)

    def _run(self) -> None:
        while not self._stop_event.wait(self.interval_seconds):
            self._publish()

    def _publish(self) -> None:
        payload: dict[str, Any] = {
            "worker_id": self.worker_id,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "queues": self.queues,
        }
        try:
            self.redis_adapter.set_json(
                ML_WORKER_HEARTBEAT_KEY,
                payload,
                ttl_seconds=self.ttl_seconds,
            )
        except Exception as exc:
            log_event(
                self.logger,
                "worker_heartbeat_publish_failed",
                level=logging.ERROR,
                exc_info=True,
                error_type=exc.__class__.__name__,
                worker_id=self.worker_id,
            )


__all__ = ["ML_WORKER_HEARTBEAT_KEY", "WorkerHeartbeat"]
