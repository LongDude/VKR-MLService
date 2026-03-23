from __future__ import annotations

import mimetypes
from typing import Any, List, Optional, Tuple
from urllib.parse import urlparse

import requests

from pipeline_core.contracts import ArtifactRef, PipelineContext, StageResult
from pipeline_handlers.base import BaseAcquireHandler


def _parse_bucket_ref(source_ref: str, default_bucket: str) -> Tuple[str, str]:
    ref = (source_ref or "").strip()
    if not ref:
        return default_bucket, ""
    if ref.startswith("s3://"):
        parsed = urlparse(ref)
        return parsed.netloc, parsed.path.lstrip("/")
    if "/" in ref:
        first, rest = ref.split("/", 1)
        if "." not in first and first:
            return first, rest
    return default_bucket, ref


def _guess_content_type(source_ref: str) -> str:
    content_type, _ = mimetypes.guess_type(source_ref)
    return content_type or "application/octet-stream"


class ColdObjectReadHandler(BaseAcquireHandler):
    handler_id = "cold_object_read"
    priority = 10

    def can_handle(self, ctx: PipelineContext) -> bool:
        return (ctx.source_type or "").lower() == "cold_object"

    def recoverable(self, err: Exception) -> bool:
        return True

    def handle(self, ctx: PipelineContext, io: Any) -> StageResult:
        if not io.cold_store:
            return StageResult.recoverable_error(
                ctx,
                code="cold_store_missing",
                message="Cold object store is not configured",
                stage=self.stage.value,
                handler_id=self.handler_id,
            )
        bucket_default = str(self.config.get("cold_bucket", "cold-raw"))
        bucket, object_name = _parse_bucket_ref(ctx.source_ref, bucket_default)
        if not object_name:
            return StageResult.fatal_error(
                ctx,
                code="cold_object_missing",
                message="Cold source_ref does not contain object name",
                stage=self.stage.value,
                handler_id=self.handler_id,
            )

        cache_key = f"{ctx.article_id or object_name}/raw.bin"
        cached = io.cache.read_bytes("raw_object", cache_key)
        if cached is not None:
            raw_data = cached
        else:
            raw_data = io.cold_store.get_bytes(bucket, object_name)
            io.cache.write_bytes("raw_object", cache_key, raw_data, _guess_content_type(object_name))

        ctx.runtime_data["raw_bytes"] = raw_data
        ctx.runtime_data["raw_source_bucket"] = bucket
        ctx.runtime_data["raw_source_object"] = object_name
        ctx.register_artifact(
            ArtifactRef(
                name="raw_object",
                bucket=bucket,
                object_name=object_name,
                source="cold_object_store",
                content_type=_guess_content_type(object_name),
            )
        )
        return StageResult.success(ctx)


class HotObjectReadHandler(BaseAcquireHandler):
    handler_id = "hot_object_read"
    priority = 20

    def can_handle(self, ctx: PipelineContext) -> bool:
        source = (ctx.source_type or "hot_arxiv").lower()
        return source in {"hot_arxiv", "arxiv", "hot_object", ""}

    def recoverable(self, err: Exception) -> bool:
        return True

    def _download_http(self, url: str, timeout_seconds: int) -> Tuple[bytes, str]:
        response = requests.get(url, timeout=timeout_seconds)
        response.raise_for_status()
        return response.content, response.headers.get("content-type", "application/octet-stream")

    def _resolve_object_ref(self, ctx: PipelineContext) -> Tuple[str, str]:
        default_bucket = str(self.config.get("source_bucket", "pdf-raw"))
        source_ref = ctx.source_ref or str(ctx.metadata.get("source_ref", ""))
        return _parse_bucket_ref(source_ref, default_bucket)

    def handle(self, ctx: PipelineContext, io: Any) -> StageResult:
        source_ref = ctx.source_ref.strip() if ctx.source_ref else ""
        timeout_seconds = int(self.config.get("http_timeout_seconds", 60))

        if source_ref.startswith("http://") or source_ref.startswith("https://"):
            cache_key = f"{ctx.article_id or 'article'}/raw.http"
            cached = io.cache.read_bytes("raw_object", cache_key)
            if cached is not None:
                raw_data = cached
                content_type = _guess_content_type(source_ref)
            else:
                raw_data, content_type = self._download_http(source_ref, timeout_seconds)
                io.cache.write_bytes("raw_object", cache_key, raw_data, content_type)
            ctx.runtime_data["raw_bytes"] = raw_data
            ctx.register_artifact(
                ArtifactRef(
                    name="raw_object",
                    uri=source_ref,
                    source="http",
                    content_type=content_type,
                )
            )
            return StageResult.success(ctx)

        bucket, object_name = self._resolve_object_ref(ctx)
        if not object_name:
            return StageResult.fatal_error(
                ctx,
                code="hot_object_missing",
                message="Unable to resolve object reference for hot source",
                stage=self.stage.value,
                handler_id=self.handler_id,
            )

        cache_key = f"{ctx.article_id or object_name}/raw.bin"
        cached = io.cache.read_bytes("raw_object", cache_key)
        if cached is not None:
            raw_data = cached
        else:
            raw_data = io.hot_store.get_bytes(bucket, object_name)
            io.cache.write_bytes("raw_object", cache_key, raw_data, _guess_content_type(object_name))

        ctx.runtime_data["raw_bytes"] = raw_data
        ctx.runtime_data["raw_source_bucket"] = bucket
        ctx.runtime_data["raw_source_object"] = object_name
        ctx.register_artifact(
            ArtifactRef(
                name="raw_object",
                bucket=bucket,
                object_name=object_name,
                source="hot_object_store",
                content_type=_guess_content_type(object_name),
            )
        )
        return StageResult.success(ctx)


def get_handlers() -> List[type[BaseAcquireHandler]]:
    return [ColdObjectReadHandler, HotObjectReadHandler]

