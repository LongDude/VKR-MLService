from __future__ import annotations

import json
from typing import Any

from core.exceptions import RedisOperationError


class RedisAdapter:
    def __init__(self, client: Any) -> None:
        self._client = client

    def get_json(self, key: str) -> dict[str, Any] | None:
        try:
            value = self._client.get(key)
            if value is None:
                return None
            if isinstance(value, bytes):
                value = value.decode("utf-8")
            parsed = json.loads(value)
            if not isinstance(parsed, dict):
                raise ValueError("Redis value is not a JSON object")
            return parsed
        except Exception as exc:
            raise RedisOperationError(
                f"Failed to read JSON value from Redis key {key!r}",
                details={"key": key, "reason": str(exc)},
            ) from exc

    def set_json(self, key: str, value: dict[str, Any], ttl_seconds: int) -> None:
        try:
            payload = json.dumps(value, ensure_ascii=False)
            self._client.set(key, payload, ex=ttl_seconds)
        except Exception as exc:
            raise RedisOperationError(
                f"Failed to write JSON value to Redis key {key!r}",
                details={"key": key, "reason": str(exc)},
            ) from exc

    def delete(self, key: str) -> None:
        try:
            self._client.delete(key)
        except Exception as exc:
            raise RedisOperationError(
                f"Failed to delete Redis key {key!r}",
                details={"key": key, "reason": str(exc)},
            ) from exc

    def enqueue(self, queue_name: str, message: dict[str, Any]) -> None:
        try:
            payload = json.dumps(message, ensure_ascii=False)
            self._client.rpush(queue_name, payload)
        except Exception as exc:
            raise RedisOperationError(
                f"Failed to enqueue Redis message to {queue_name!r}",
                details={"queue_name": queue_name, "reason": str(exc)},
            ) from exc

    def dequeue(
        self,
        queue_name: str,
        timeout_seconds: int = 5,
    ) -> dict[str, Any] | None:
        try:
            result = self._client.blpop(queue_name, timeout=timeout_seconds)
            if result is None:
                return None
            _, value = result
            if isinstance(value, bytes):
                value = value.decode("utf-8")
            parsed = json.loads(value)
            if not isinstance(parsed, dict):
                raise ValueError("Redis queue message is not a JSON object")
            return parsed
        except Exception as exc:
            raise RedisOperationError(
                f"Failed to dequeue Redis message from {queue_name!r}",
                details={"queue_name": queue_name, "reason": str(exc)},
            ) from exc

    def acquire_lock(self, key: str, ttl_seconds: int) -> bool:
        try:
            return bool(self._client.set(key, "1", nx=True, ex=ttl_seconds))
        except Exception as exc:
            raise RedisOperationError(
                f"Failed to acquire Redis lock {key!r}",
                details={"key": key, "reason": str(exc)},
            ) from exc

    def release_lock(self, key: str) -> None:
        self.delete(key)


__all__ = ["RedisAdapter"]
