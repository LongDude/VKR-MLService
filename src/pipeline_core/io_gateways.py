from __future__ import annotations

import io
import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

try:
    import redis as redis_module
    from redis import RedisError
except ImportError:  # pragma: no cover - allows lightweight test runs without dependencies
    redis_module = None  # type: ignore[assignment]

    class RedisError(Exception):
        pass

try:
    from minio import Minio
    from minio.error import S3Error
except ImportError:  # pragma: no cover
    Minio = Any  # type: ignore[assignment,misc]

    class S3Error(Exception):
        code = "UNKNOWN"

from pipeline_core.contracts import PipelineError
logger = logging.getLogger(__name__)


class PipelineIOOperationError(RuntimeError):
    def __init__(self, error: PipelineError) -> None:
        super().__init__(error.message)
        self.error = error


class RedisStreamGateway:
    def __init__(self, client: Any) -> None:
        self.client = client

    def xadd_json(self, stream: str, payload: Dict[str, Any]) -> str:
        try:
            stream_id = self.client.xadd(stream, {"data": json.dumps(payload, ensure_ascii=False)})
            logger.info("Redis xadd success: stream=%s stream_id=%s", stream, stream_id)
            return stream_id
        except RedisError as exc:
            raise PipelineIOOperationError(
                PipelineError(
                    code="redis_xadd_failed",
                    message=str(exc),
                    stage="io",
                    handler_id="redis_gateway",
                    recoverable=False,
                    details={"stream": stream},
                )
            ) from exc

    def read_first_json(self, stream: str) -> Optional[Tuple[str, Dict[str, Any]]]:
        try:
            entries = self.client.xrange(stream, count=1)
            if not entries:
                logger.info("Redis xrange empty: stream=%s", stream)
                return None
            stream_id, payload = entries[0]
            data = payload.get("data")
            if data is None:
                logger.warning("Redis record without data field: stream=%s stream_id=%s", stream, stream_id)
                return None
            logger.info("Redis xrange success: stream=%s stream_id=%s", stream, stream_id)
            return stream_id, json.loads(data)
        except (RedisError, json.JSONDecodeError) as exc:
            raise PipelineIOOperationError(
                PipelineError(
                    code="redis_xrange_failed",
                    message=str(exc),
                    stage="io",
                    handler_id="redis_gateway",
                    recoverable=False,
                    details={"stream": stream},
                )
            ) from exc

    def xdel(self, stream: str, stream_id: str) -> None:
        if not stream_id:
            return
        try:
            self.client.xdel(stream, stream_id)
            logger.info("Redis xdel success: stream=%s stream_id=%s", stream, stream_id)
        except RedisError as exc:
            raise PipelineIOOperationError(
                PipelineError(
                    code="redis_xdel_failed",
                    message=str(exc),
                    stage="io",
                    handler_id="redis_gateway",
                    recoverable=False,
                    details={"stream": stream, "stream_id": stream_id},
                )
            ) from exc


class ObjectStoreGateway:
    def __init__(self, client: Minio, *, buffer_size_bytes: int = 65536) -> None:
        self.client = client
        self.buffer_size_bytes = max(4096, int(buffer_size_bytes))

    def ensure_bucket(self, bucket: str) -> None:
        try:
            if not self.client.bucket_exists(bucket):
                self.client.make_bucket(bucket)
        except S3Error as exc:
            raise PipelineIOOperationError(
                PipelineError(
                    code="minio_bucket_failed",
                    message=str(exc),
                    stage="io",
                    handler_id="object_gateway",
                    recoverable=False,
                    details={"bucket": bucket},
                )
            ) from exc

    def put_bytes(self, bucket: str, object_name: str, data: bytes, content_type: str) -> None:
        try:
            stream = io.BytesIO(data)
            self.client.put_object(
                bucket_name=bucket,
                object_name=object_name,
                data=stream,
                length=len(data),
                content_type=content_type,
            )
        except S3Error as exc:
            raise PipelineIOOperationError(
                PipelineError(
                    code="minio_put_failed",
                    message=str(exc),
                    stage="io",
                    handler_id="object_gateway",
                    recoverable=False,
                    details={"bucket": bucket, "object_name": object_name},
                )
            ) from exc

    def get_bytes(self, bucket: str, object_name: str) -> bytes:
        response = None
        try:
            response = self.client.get_object(bucket, object_name)
            parts: list[bytes] = []
            while True:
                chunk = response.read(self.buffer_size_bytes)
                if not chunk:
                    break
                parts.append(chunk)
            return b"".join(parts)
        except S3Error as exc:
            raise PipelineIOOperationError(
                PipelineError(
                    code="minio_get_failed",
                    message=str(exc),
                    stage="io",
                    handler_id="object_gateway",
                    recoverable=False,
                    details={"bucket": bucket, "object_name": object_name},
                )
            ) from exc
        finally:
            if response:
                try:
                    response.close()
                    response.release_conn()
                except Exception:
                    logger.debug("Failed to close Minio response", exc_info=True)

    def exists(self, bucket: str, object_name: str) -> bool:
        try:
            self.client.stat_object(bucket, object_name)
            return True
        except S3Error as exc:
            if exc.code in {"NoSuchKey", "NoSuchObject", "NoSuchBucket"}:
                return False
            raise PipelineIOOperationError(
                PipelineError(
                    code="minio_stat_failed",
                    message=str(exc),
                    stage="io",
                    handler_id="object_gateway",
                    recoverable=False,
                    details={"bucket": bucket, "object_name": object_name},
                )
            ) from exc


class LocalDatasetGateway:
    def __init__(self, root_path: str) -> None:
        self.root_path = Path(root_path).resolve()

    def resolve(self, relative_path: str) -> Path:
        candidate = (self.root_path / relative_path).resolve()
        if not str(candidate).startswith(str(self.root_path)):
            raise ValueError(f"Path escapes dataset root: {relative_path}")
        return candidate

    def read_json(self, relative_path: str) -> Dict[str, Any]:
        path = self.resolve(relative_path)
        return json.loads(path.read_text(encoding="utf-8"))

    def read_text(self, relative_path: str) -> str:
        path = self.resolve(relative_path)
        return path.read_text(encoding="utf-8", errors="ignore")

    def exists(self, relative_path: str) -> bool:
        try:
            return self.resolve(relative_path).exists()
        except ValueError:
            return False


@dataclass
class StageCacheConfig:
    enabled: bool = False
    ttl_seconds: int = 0


class StageCache:
    def __init__(
        self,
        store: ObjectStoreGateway,
        cache_bucket: str,
        stage_config: Dict[str, Dict[str, Any]],
    ) -> None:
        self.store = store
        self.cache_bucket = cache_bucket
        self.stage_config = stage_config
        if cache_bucket:
            self.store.ensure_bucket(cache_bucket)

    def enabled(self, boundary: str) -> bool:
        cfg = self.stage_config.get(boundary, {})
        return bool(cfg.get("enabled", False))

    def write_bytes(self, boundary: str, key: str, data: bytes, content_type: str) -> Optional[str]:
        if not self.enabled(boundary):
            return None
        object_name = f"{boundary}/{key}"
        self.store.put_bytes(self.cache_bucket, object_name, data, content_type)
        return object_name

    def read_bytes(self, boundary: str, key: str) -> Optional[bytes]:
        if not self.enabled(boundary):
            return None
        object_name = f"{boundary}/{key}"
        if not self.store.exists(self.cache_bucket, object_name):
            return None
        return self.store.get_bytes(self.cache_bucket, object_name)


class PipelineIO:
    def __init__(
        self,
        *,
        redis_gateway: RedisStreamGateway,
        hot_store: ObjectStoreGateway,
        cold_store: Optional[ObjectStoreGateway],
        dataset_gateway: Optional[LocalDatasetGateway],
        ingress_stream: str,
        keywords_stream: str,
        error_stream: str,
        cache_bucket: str,
        cache_config: Dict[str, Dict[str, Any]],
    ) -> None:
        self.redis = redis_gateway
        self.hot_store = hot_store
        self.cold_store = cold_store
        self.dataset = dataset_gateway
        self.ingress_stream = ingress_stream
        self.keywords_stream = keywords_stream
        self.error_stream = error_stream
        self.cache = StageCache(hot_store, cache_bucket, cache_config)

    @classmethod
    def from_env(
        cls,
        *,
        ingress_stream: str,
        keywords_stream: str,
        error_stream: str,
        cache_bucket: str,
        cache_config: Dict[str, Dict[str, Any]],
        buffer_size_bytes: int,
    ) -> "PipelineIO":
        from service_lib import ServiceConnectionFactory

        redis_client = ServiceConnectionFactory.getRedisClient()
        hot_minio_client = ServiceConnectionFactory.getMinioClient()

        if redis_module is None:
            raise RuntimeError("redis dependency is required for PipelineIO.from_env")
        logger.info(
            "PipelineIO.from_env hot endpoints ready: ingress=%s keywords=%s errors=%s cache_bucket=%s buffer=%s",
            ingress_stream,
            keywords_stream,
            error_stream,
            cache_bucket,
            buffer_size_bytes,
        )

        cold_endpoint = os.getenv("COLD_MINIO_ENDPOINT", "")
        cold_store = None
        if cold_endpoint:
            cold_minio = Minio(
                cold_endpoint,
                access_key=os.getenv("COLD_MINIO_ACCESS_KEY") or os.getenv("COLD_MINIO_USER"),
                secret_key=os.getenv("COLD_MINIO_SECRET_KEY") or os.getenv("COLD_MINIO_PASSWORD"),
                secure=str(os.getenv("COLD_MINIO_SECURE", "false")).lower() == "true",
            )
            cold_store = ObjectStoreGateway(cold_minio, buffer_size_bytes=buffer_size_bytes)

        dataset_root = os.getenv("COLD_DATASET_ROOT", "").strip()
        dataset = LocalDatasetGateway(dataset_root) if dataset_root else None

        return cls(
            redis_gateway=RedisStreamGateway(redis_client),
            hot_store=ObjectStoreGateway(hot_minio_client, buffer_size_bytes=buffer_size_bytes),
            cold_store=cold_store,
            dataset_gateway=dataset,
            ingress_stream=ingress_stream,
            keywords_stream=keywords_stream,
            error_stream=error_stream,
            cache_bucket=cache_bucket,
            cache_config=cache_config,
        )

    def poll_ingress(self) -> Optional[Tuple[str, Dict[str, Any]]]:
        logger.info("Polling ingress stream: %s", self.ingress_stream)
        return self.redis.read_first_json(self.ingress_stream)

    def ack_ingress(self, stream_id: str) -> None:
        logger.info("Acknowledging ingress stream_id=%s", stream_id)
        self.redis.xdel(self.ingress_stream, stream_id)

    def publish_keywords(self, payload: Dict[str, Any]) -> None:
        logger.info("Publishing keywords payload for article_id=%s", payload.get("article_id"))
        self.redis.xadd_json(self.keywords_stream, payload)

    def publish_error(self, payload: Dict[str, Any]) -> None:
        logger.info(
            "Publishing error payload for article_id=%s code=%s",
            payload.get("article_id"),
            (payload.get("error") or {}).get("code") if isinstance(payload.get("error"), dict) else None,
        )
        self.redis.xadd_json(self.error_stream, payload)
