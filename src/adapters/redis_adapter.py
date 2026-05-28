from __future__ import annotations

import json
from typing import Any, cast

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
            parsed: Any = json.loads(value)
            if not isinstance(parsed, dict):
                raise ValueError("Redis value is not a JSON object")
            return cast(dict[str, Any], parsed)
        except Exception as exc:
            raise RedisOperationError(
                f"Failed to read JSON value from Redis key {key!r}",
                details={"key": key, "reason": str(exc)},
            ) from exc

    def consume_json(self, key: str) -> dict[str, Any] | None:
        """Read a JSON object and remove the key after reading it."""
        try:
            if hasattr(self._client, "getdel"):
                value = self._client.getdel(key)
            else:
                value = self._client.get(key)
                if value is not None:
                    self._client.delete(key)
            if value is None:
                return None
            if isinstance(value, bytes):
                value = value.decode("utf-8")
            parsed = json.loads(value)
            if not isinstance(parsed, dict):
                raise ValueError("Redis value is not a JSON object")
            return cast(dict[str, Any], parsed)
        except Exception as exc:
            raise RedisOperationError(
                f"Failed to consume JSON value from Redis key {key!r}",
                details={"key": key, "reason": str(exc)},
            ) from exc

    def set_json(
        self,
        key: str,
        value: dict[str, Any],
        ttl_seconds: int | None = None,
    ) -> None:
        try:
            payload = json.dumps(value, ensure_ascii=False)
            if ttl_seconds is None:
                self._client.set(key, payload)
            else:
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
            return self._decode_queue_message(value)
        except Exception as exc:
            raise RedisOperationError(
                f"Failed to dequeue Redis message from {queue_name!r}",
                details={"queue_name": queue_name, "reason": str(exc)},
            ) from exc

    def dequeue_any(
        self,
        queue_names: list[str] | tuple[str, ...],
        timeout_seconds: int = 5,
    ) -> tuple[str, dict[str, Any]] | None:
        """Return one message from the first ready queue using Redis BLPOP."""
        if not queue_names:
            return None
        try:
            result = self._client.blpop(list(queue_names), timeout=timeout_seconds)
            if result is None:
                return None
            queue_name, value = result
            if isinstance(queue_name, bytes):
                queue_name = queue_name.decode("utf-8")
            return str(queue_name), self._decode_queue_message(value)
        except Exception as exc:
            raise RedisOperationError(
                "Failed to dequeue Redis message from multiple queues",
                details={"queue_names": list(queue_names), "reason": str(exc)},
            ) from exc

    def dequeue_nowait(self, queue_name: str) -> dict[str, Any] | None:
        """Return one queue message without blocking, or ``None`` when empty."""
        try:
            value = self._client.lpop(queue_name)
            if value is None:
                return None
            return self._decode_queue_message(value)
        except Exception as exc:
            raise RedisOperationError(
                f"Failed to dequeue Redis message from {queue_name!r}",
                details={"queue_name": queue_name, "reason": str(exc)},
            ) from exc

    def peek_queue(
        self,
        queue_name: str,
        limit: int,
    ) -> list[dict[str, Any]]:
        """Return up to ``limit`` queue messages without removing them."""
        try:
            if limit <= 0:
                return []
            values = self._client.lrange(queue_name, 0, limit - 1)
            return [self._decode_queue_message(value) for value in values]
        except Exception as exc:
            raise RedisOperationError(
                f"Failed to peek Redis messages from {queue_name!r}",
                details={"queue_name": queue_name, "reason": str(exc)},
            ) from exc

    def queue_length(self, queue_name: str) -> int:
        """Return Redis list length for a queue."""
        try:
            return int(self._client.llen(queue_name))
        except Exception as exc:
            raise RedisOperationError(
                f"Failed to read Redis queue length for {queue_name!r}",
                details={"queue_name": queue_name, "reason": str(exc)},
            ) from exc

    def exists(self, key: str) -> bool:
        """Return whether a Redis key exists."""
        try:
            return bool(self._client.exists(key))
        except Exception as exc:
            raise RedisOperationError(
                f"Failed to check Redis key {key!r}",
                details={"key": key, "reason": str(exc)},
            ) from exc

    def ttl(self, key: str) -> int:
        """Return key TTL in seconds; mirrors Redis TTL semantics."""
        try:
            return int(self._client.ttl(key))
        except Exception as exc:
            raise RedisOperationError(
                f"Failed to read Redis key TTL for {key!r}",
                details={"key": key, "reason": str(exc)},
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

    def _decode_queue_message(self, value: Any) -> dict[str, Any]:
        if isinstance(value, bytes):
            value = value.decode("utf-8")
        parsed = json.loads(value)
        if not isinstance(parsed, dict):
            raise ValueError("Redis queue message is not a JSON object")
        return cast(dict[str, Any], parsed)


__all__ = ["RedisAdapter"]
