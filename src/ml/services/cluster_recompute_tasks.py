from __future__ import annotations

from datetime import date, datetime
from typing import Any

from ml.task_contracts import RECOMPUTE_TOPIC_CLUSTERS_TASK

CLUSTER_RECOMPUTE_DEDUPE_KEY_PREFIX = "ml:dedupe:cr"
DEFAULT_CLUSTER_RECOMPUTE_DEDUPE_TTL_SECONDS = 24 * 60 * 60
CLUSTER_RECOMPUTE_WORKFLOW_FIELDS = (
    "workflow_date_from",
    "workflow_date_to",
    "workflow_granularity",
    "enqueue_cluster_dynamics",
)


def cluster_recompute_dedupe_key(
    topic_id: int,
    *,
    force_summary: bool = False,
    workflow_date_from: Any | None = None,
    workflow_date_to: Any | None = None,
    workflow_granularity: Any | None = None,
    enqueue_cluster_dynamics: bool = False,
) -> str:
    return (
        f"{CLUSTER_RECOMPUTE_DEDUPE_KEY_PREFIX}:"
        f"t:{int(topic_id)}:"
        f"df:{_date_key(workflow_date_from)}:"
        f"dt:{_date_key(workflow_date_to)}:"
        f"g:{workflow_granularity or 'month'}:"
        f"fs:{1 if force_summary else 0}:"
        f"dyn:{1 if enqueue_cluster_dynamics else 0}"
    )


def acquire_cluster_recompute_topic_ids(
    redis_adapter: Any,
    topic_ids: list[int],
    *,
    force_summary: bool = False,
    workflow_options: dict[str, Any] | None = None,
    ttl_seconds: int = DEFAULT_CLUSTER_RECOMPUTE_DEDUPE_TTL_SECONDS,
) -> list[int]:
    accepted: list[int] = []
    seen: set[int] = set()
    options = workflow_options or {}
    for topic_id in topic_ids:
        normalized_id = int(topic_id)
        if normalized_id in seen:
            continue
        seen.add(normalized_id)
        key = cluster_recompute_dedupe_key(
            normalized_id,
            force_summary=force_summary,
            **_dedupe_workflow_options(options),
        )
        if redis_adapter.acquire_lock(key, ttl_seconds=ttl_seconds):
            accepted.append(normalized_id)
    return accepted


def release_cluster_recompute_dedupe_keys(
    redis_adapter: Any,
    message: dict[str, Any],
) -> None:
    force_summary = bool(message.get("force_summary", False))
    options = _dedupe_workflow_options(message)
    for topic_id in _topic_ids_from_message(message):
        redis_adapter.release_lock(
            cluster_recompute_dedupe_key(
                topic_id,
                force_summary=force_summary,
                **options,
            )
        )


def build_cluster_recompute_message(
    topic_ids: list[int],
    *,
    force_summary: bool = False,
    cluster_workers: int | None = None,
    workflow_options: dict[str, Any] | None = None,
) -> dict[str, Any]:
    message: dict[str, Any] = {
        "task_type": RECOMPUTE_TOPIC_CLUSTERS_TASK,
        "topic_ids": list(dict.fromkeys(int(topic_id) for topic_id in topic_ids)),
        "force_summary": bool(force_summary),
    }
    if cluster_workers is not None:
        message["cluster_workers"] = int(cluster_workers)
    message.update(cluster_recompute_workflow_fields(workflow_options or {}))
    return message


def cluster_recompute_workflow_fields(
    workflow_options: dict[str, Any],
) -> dict[str, Any]:
    fields: dict[str, Any] = {}
    for key in CLUSTER_RECOMPUTE_WORKFLOW_FIELDS:
        if key not in workflow_options:
            continue
        if key == "enqueue_cluster_dynamics":
            if workflow_options[key]:
                fields[key] = True
            continue
        value = workflow_options[key]
        if value is not None:
            fields[key] = _message_value(value)
    return fields


def _dedupe_workflow_options(options: dict[str, Any]) -> dict[str, Any]:
    return {
        "workflow_date_from": options.get("workflow_date_from"),
        "workflow_date_to": options.get("workflow_date_to"),
        "workflow_granularity": options.get("workflow_granularity") or "month",
        "enqueue_cluster_dynamics": bool(
            options.get("enqueue_cluster_dynamics", False)
        ),
    }


def _topic_ids_from_message(message: dict[str, Any]) -> list[int]:
    raw_ids = message.get("topic_ids")
    if not isinstance(raw_ids, list):
        return []
    topic_ids: list[int] = []
    for raw_id in raw_ids:
        try:
            topic_id = int(raw_id)
        except (TypeError, ValueError):
            continue
        if topic_id not in topic_ids:
            topic_ids.append(topic_id)
    return topic_ids


def _date_key(value: Any | None) -> str:
    if value is None:
        return "none"
    return _message_value(value)


def _message_value(value: Any) -> str:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if hasattr(value, "isoformat"):
        return str(value.isoformat())
    return str(value)


__all__ = [
    "CLUSTER_RECOMPUTE_DEDUPE_KEY_PREFIX",
    "DEFAULT_CLUSTER_RECOMPUTE_DEDUPE_TTL_SECONDS",
    "acquire_cluster_recompute_topic_ids",
    "build_cluster_recompute_message",
    "cluster_recompute_dedupe_key",
    "cluster_recompute_workflow_fields",
    "release_cluster_recompute_dedupe_keys",
]
