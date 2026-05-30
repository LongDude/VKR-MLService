from __future__ import annotations

from datetime import datetime, timedelta, timezone
from math import ceil
from typing import Any

from adapters.redis_adapter import RedisAdapter

OPENALEX_COOLDOWN_KEY = "ml:cooldown:openalex"
OPENALEX_COOLDOWN_FALLBACK_SECONDS = 900
OPENALEX_TOPIC_STATS_PENDING_QUEUE = "queue:openalex_topic_stats_pending"
OPENALEX_BOOTSTRAP_PAPERS_PENDING_QUEUE = "queue:openalex_bootstrap_papers_pending"


def resolve_openalex_cooldown_seconds(value: float | int | None) -> int:
    if value is None or value <= 0:
        return OPENALEX_COOLDOWN_FALLBACK_SECONDS
    return max(1, int(ceil(float(value))))


def build_openalex_cooldown_payload(
    *,
    retry_after_seconds: float | int | None,
    source_queue: str | None = None,
    task_type: str | None = None,
    reason: str = "openalex_rate_limit",
) -> tuple[dict[str, Any], int]:
    ttl_seconds = resolve_openalex_cooldown_seconds(retry_after_seconds)
    until = datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)
    payload = {
        "reason": reason,
        "retry_after_seconds": ttl_seconds,
        "until": until.isoformat(),
    }
    if source_queue:
        payload["source_queue"] = source_queue
    if task_type:
        payload["task_type"] = task_type
    return payload, ttl_seconds


def set_openalex_cooldown(
    redis_adapter: RedisAdapter,
    *,
    retry_after_seconds: float | int | None,
    source_queue: str | None = None,
    task_type: str | None = None,
    reason: str = "openalex_rate_limit",
) -> dict[str, Any]:
    payload, ttl_seconds = build_openalex_cooldown_payload(
        retry_after_seconds=retry_after_seconds,
        source_queue=source_queue,
        task_type=task_type,
        reason=reason,
    )
    redis_adapter.set_json(
        OPENALEX_COOLDOWN_KEY,
        payload,
        ttl_seconds=ttl_seconds,
    )
    return payload


__all__ = [
    "OPENALEX_BOOTSTRAP_PAPERS_PENDING_QUEUE",
    "OPENALEX_COOLDOWN_FALLBACK_SECONDS",
    "OPENALEX_COOLDOWN_KEY",
    "OPENALEX_TOPIC_STATS_PENDING_QUEUE",
    "build_openalex_cooldown_payload",
    "resolve_openalex_cooldown_seconds",
    "set_openalex_cooldown",
]
