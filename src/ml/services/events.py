from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
import logging
from typing import Any, Protocol

from adapters.redis_adapter import RedisAdapter


@dataclass(frozen=True)
class MLEvent:
    """Small status event emitted by ML facades, pipelines, and workers."""

    event_type: str
    task_type: str
    entity_id: str | int | None = None
    stage: str | None = None
    current: int | None = None
    total: int | None = None
    message: str | None = None
    payload: dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable representation of the event."""
        return {
            "event_type": self.event_type,
            "task_type": self.task_type,
            "entity_id": self.entity_id,
            "stage": self.stage,
            "current": self.current,
            "total": self.total,
            "message": self.message,
            "payload": _jsonable(self.payload),
            "created_at": self.created_at.isoformat(),
        }


class EventSink(Protocol):
    """Protocol implemented by ML event receivers."""

    def emit(self, event: MLEvent) -> None:
        """Receive one ML event."""


class NoopEventSink:
    """Event sink that intentionally drops all events."""

    def emit(self, event: MLEvent) -> None:
        return None


class LoggingEventSink:
    """Write concise ML event status lines through Python logging."""

    def __init__(
        self,
        logger: logging.Logger | None = None,
        *,
        verbosity: int = 0,
    ) -> None:
        self.logger = logger or logging.getLogger("ml.events")
        self.verbosity = verbosity

    def emit(self, event: MLEvent) -> None:
        if self.verbosity <= 0 and not self._should_log_default(event):
            return

        progress = ""
        if event.current is not None and event.total is not None:
            progress = f" {event.current}/{event.total}"
        elif event.current is not None:
            progress = f" {event.current}"

        message = event.message or event.stage or event.event_type
        self.logger.info(
            "ml_event type=%s task=%s entity=%s stage=%s%s message=%s",
            event.event_type,
            event.task_type,
            event.entity_id,
            event.stage,
            progress,
            message,
        )
        if self.verbosity > 1 and event.payload:
            self.logger.debug(
                "ml_event_payload type=%s task=%s entity=%s payload=%s",
                event.event_type,
                event.task_type,
                event.entity_id,
                event.payload,
            )

    def _should_log_default(self, event: MLEvent) -> bool:
        if event.event_type.endswith("_progress"):
            return False
        return (
            event.event_type.startswith("worker_task_")
            or event.event_type.startswith("worker_queue_")
            or event.event_type.endswith("_started")
            or event.event_type.endswith("_completed")
            or event.event_type.endswith("_failed")
        )


class TqdmEventSink:
    """Render ML progress events as tqdm progress bars."""

    def __init__(self, *, leave: bool = False) -> None:
        from tqdm.auto import tqdm

        self._tqdm = tqdm
        self._bars: dict[tuple[str, str, str], Any] = {}
        self._last_current: dict[tuple[str, str, str], int] = {}
        self.leave = leave

    def emit(self, event: MLEvent) -> None:
        key = self._key(event)
        if event.total is not None:
            bar = self._bars.get(key)
            if bar is None:
                bar = self._tqdm(
                    total=max(0, int(event.total)),
                    desc=self._description(event),
                    unit="item",
                    leave=self.leave,
                    position=self._position(event),
                )
                self._bars[key] = bar
                self._last_current[key] = 0
            elif bar.total != event.total:
                bar.total = max(0, int(event.total))

            current = int(event.current or self._last_current.get(key, 0))
            delta = current - self._last_current.get(key, 0)
            if delta > 0:
                bar.update(delta)
                self._last_current[key] = current
            if event.message:
                bar.set_postfix_str(event.message[:80])

        if self._is_terminal(event):
            self._close_related(event)

    def close(self) -> None:
        """Close all active progress bars."""
        for bar in list(self._bars.values()):
            bar.close()
        self._bars.clear()
        self._last_current.clear()

    def _key(self, event: MLEvent) -> tuple[str, str, str]:
        return (
            event.task_type,
            str(event.entity_id or "run"),
            str(event.stage or event.event_type),
        )

    def _description(self, event: MLEvent) -> str:
        entity = f":{event.entity_id}" if event.entity_id is not None else ""
        stage = event.stage or event.event_type
        return f"{event.task_type}{entity} {stage}"

    def _position(self, event: MLEvent) -> int:
        if event.stage == "queue":
            return 0
        if event.stage == "worker":
            return 1
        return 2 if event.task_type == "keyword_extraction" else 1

    def _is_terminal(self, event: MLEvent) -> bool:
        return event.event_type in {
            "cluster_batch_completed",
            "cluster_completed",
            "cluster_failed",
            "cluster_dynamics_completed",
            "cluster_dynamics_failed",
            "cluster_db_sync_completed",
            "cluster_db_sync_failed",
            "entity_indexing_completed",
            "entity_indexing_failed",
            "keyword_extraction_batch_completed",
            "keyword_extraction_completed",
            "keyword_extraction_failed",
            "paper_batch_completed",
            "paper_indexing_completed",
            "paper_indexing_failed",
            "user_profile_completed",
            "user_profile_failed",
            "worker_task_completed",
            "worker_task_failed",
            "worker_task_interrupted",
            "worker_queue_completed",
        }

    def _close_related(self, event: MLEvent) -> None:
        entity = str(event.entity_id or "run")
        for key in list(self._bars):
            if key[0] != event.task_type:
                continue
            if entity in {"batch", "worker_batch"} and key[1] != entity:
                continue
            if entity not in {"run", "all", "batch", "worker_batch"} and key[1] not in {entity, "run"}:
                continue
            self._bars.pop(key).close()
            self._last_current.pop(key, None)


class RedisEventSink:
    """Store the latest ML event status in Redis with a TTL."""

    def __init__(
        self,
        redis_adapter: RedisAdapter,
        *,
        ttl_seconds: int = 24 * 60 * 60,
        key_prefix: str = "ml:task_status",
    ) -> None:
        self.redis_adapter = redis_adapter
        self.ttl_seconds = ttl_seconds
        self.key_prefix = key_prefix.rstrip(":")

    def emit(self, event: MLEvent) -> None:
        self.redis_adapter.set_json(
            self._key(event),
            event.to_dict(),
            ttl_seconds=self.ttl_seconds,
        )

    def _key(self, event: MLEvent) -> str:
        entity = event.entity_id
        if entity is None:
            entity = event.payload.get("run_id") or "run"
        safe_entity = str(entity).replace(" ", "_")
        return f"{self.key_prefix}:{event.task_type}:{safe_entity}"


class CompositeEventSink:
    """Fan out events to multiple sinks while isolating sink failures."""

    def __init__(
        self,
        sinks: list[EventSink],
        *,
        logger: logging.Logger | None = None,
    ) -> None:
        self.sinks = sinks
        self.logger = logger or logging.getLogger("ml.events")

    def emit(self, event: MLEvent) -> None:
        for sink in self.sinks:
            try:
                sink.emit(event)
            except Exception:
                self.logger.exception(
                    "ML event sink failed",
                    extra={
                        "event_type": event.event_type,
                        "task_type": event.task_type,
                        "entity_id": event.entity_id,
                    },
                )

    def close(self) -> None:
        """Close child sinks that expose a close method."""
        for sink in self.sinks:
            close = getattr(sink, "close", None)
            if callable(close):
                try:
                    close()
                except Exception:
                    self.logger.exception("ML event sink close failed")


def _jsonable(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_jsonable(item) for item in value]
    if isinstance(value, tuple):
        return [_jsonable(item) for item in value]
    return value


__all__ = [
    "CompositeEventSink",
    "EventSink",
    "LoggingEventSink",
    "MLEvent",
    "NoopEventSink",
    "RedisEventSink",
    "TqdmEventSink",
]
