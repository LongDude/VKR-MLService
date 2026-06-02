from __future__ import annotations

from datetime import date, datetime
from typing import Any

from ml.task_contracts import CLUSTER_DYNAMICS_RECOMPUTE_TASK

CLUSTER_DYNAMICS_DEDUPE_KEY_PREFIX = "ml:dedupe:cd"
DEFAULT_CLUSTER_DYNAMICS_DEDUPE_TTL_SECONDS = 24 * 60 * 60


def cluster_dynamics_dedupe_key(
    cluster_id: str,
    *,
    date_from: Any,
    date_to: Any,
    granularity: str = "month",
) -> str:
    return (
        f"{CLUSTER_DYNAMICS_DEDUPE_KEY_PREFIX}:"
        f"c:{_cluster_key(cluster_id)}:"
        f"df:{_date_key(date_from)}:"
        f"dt:{_date_key(date_to)}:"
        f"g:{granularity or 'month'}"
    )


def acquire_cluster_dynamics_cluster_ids(
    redis_adapter: Any,
    cluster_ids: list[str],
    *,
    date_from: Any,
    date_to: Any,
    granularity: str = "month",
    ttl_seconds: int = DEFAULT_CLUSTER_DYNAMICS_DEDUPE_TTL_SECONDS,
) -> list[str]:
    accepted: list[str] = []
    seen: set[str] = set()
    for cluster_id in cluster_ids:
        normalized_id = str(cluster_id)
        if normalized_id in seen:
            continue
        seen.add(normalized_id)
        key = cluster_dynamics_dedupe_key(
            normalized_id,
            date_from=date_from,
            date_to=date_to,
            granularity=granularity,
        )
        if redis_adapter.acquire_lock(key, ttl_seconds=ttl_seconds):
            accepted.append(normalized_id)
    return accepted


def release_cluster_dynamics_dedupe_keys(
    redis_adapter: Any,
    message: dict[str, Any],
) -> None:
    date_from = message.get("date_from")
    date_to = message.get("date_to")
    if date_from is None or date_to is None:
        return
    granularity = str(message.get("granularity") or "month")
    for cluster_id in _cluster_ids_from_message(message):
        redis_adapter.release_lock(
            cluster_dynamics_dedupe_key(
                cluster_id,
                date_from=date_from,
                date_to=date_to,
                granularity=granularity,
            )
        )


def build_cluster_dynamics_message(
    cluster_ids: list[str],
    *,
    date_from: Any,
    date_to: Any,
    granularity: str = "month",
) -> dict[str, Any]:
    return {
        "task_type": CLUSTER_DYNAMICS_RECOMPUTE_TASK,
        "cluster_ids": list(
            dict.fromkeys(str(cluster_id) for cluster_id in cluster_ids)
        ),
        "date_from": _date_key(date_from),
        "date_to": _date_key(date_to),
        "granularity": granularity,
    }


def _cluster_ids_from_message(message: dict[str, Any]) -> list[str]:
    if "cluster_id" in message:
        return [str(message["cluster_id"])]
    raw_ids = message.get("cluster_ids")
    if not isinstance(raw_ids, list):
        return []
    cluster_ids: list[str] = []
    for raw_id in raw_ids:
        cluster_id = str(raw_id)
        if cluster_id not in cluster_ids:
            cluster_ids.append(cluster_id)
    return cluster_ids


def _cluster_key(cluster_id: str) -> str:
    return str(cluster_id).replace(" ", "_")


def _date_key(value: Any) -> str:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if hasattr(value, "isoformat"):
        return str(value.isoformat())
    return str(value)


__all__ = [
    "CLUSTER_DYNAMICS_DEDUPE_KEY_PREFIX",
    "DEFAULT_CLUSTER_DYNAMICS_DEDUPE_TTL_SECONDS",
    "acquire_cluster_dynamics_cluster_ids",
    "build_cluster_dynamics_message",
    "cluster_dynamics_dedupe_key",
    "release_cluster_dynamics_dedupe_keys",
]
