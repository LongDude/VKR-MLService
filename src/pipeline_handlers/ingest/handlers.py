from __future__ import annotations

import json
from typing import Any, Dict, List

from pipeline_core.contracts import ArtifactRef, PipelineContext, StageResult
from pipeline_handlers.base import BaseIngestHandler


class ColdDatasetIngestHandler(BaseIngestHandler):
    handler_id = "cold_dataset_ingest"
    priority = 10

    def can_handle(self, ctx: PipelineContext) -> bool:
        return (ctx.source_type or "").lower() == "cold_dataset"

    def recoverable(self, err: Exception) -> bool:
        return True

    def handle(self, ctx: PipelineContext, io: Any) -> StageResult:
        if not io.dataset:
            return StageResult.fatal_error(
                ctx,
                code="dataset_unavailable",
                message="Cold dataset gateway is not configured",
                stage=self.stage.value,
                handler_id=self.handler_id,
            )

        if not ctx.source_ref:
            return StageResult.fatal_error(
                ctx,
                code="dataset_ref_missing",
                message="source_ref is required for cold_dataset source",
                stage=self.stage.value,
                handler_id=self.handler_id,
            )

        payload = io.dataset.read_json(ctx.source_ref)
        metadata = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
        ctx.metadata.update(metadata)
        ctx.article_id = ctx.article_id or payload.get("article_id") or metadata.get("article_id")
        ctx.db_id = ctx.db_id or payload.get("db_id") or metadata.get("db_id")
        text = payload.get("clean_text") or payload.get("text")
        if text:
            ctx.runtime_data["clean_text"] = str(text)
            ctx.register_artifact(
                ArtifactRef(
                    name="clean_text",
                    path=ctx.source_ref,
                    source="cold_dataset",
                    content_type="text/plain",
                )
            )
        io.cache.write_bytes(
            "metadata",
            self._cache_key(ctx, suffix="metadata.json"),
            json.dumps(ctx.metadata, ensure_ascii=False).encode("utf-8"),
            "application/json",
        )
        return StageResult.success(ctx)

    @staticmethod
    def _cache_key(ctx: PipelineContext, suffix: str) -> str:
        anchor = ctx.article_id or "dataset-item"
        return f"{anchor}/{suffix}"


class ColdObjectIngestHandler(BaseIngestHandler):
    handler_id = "cold_object_ingest"
    priority = 20

    def can_handle(self, ctx: PipelineContext) -> bool:
        return (ctx.source_type or "").lower() == "cold_object"

    def recoverable(self, err: Exception) -> bool:
        return True

    def handle(self, ctx: PipelineContext, io: Any) -> StageResult:
        source_ref = ctx.source_ref or str(ctx.metadata.get("source_ref", ""))
        if not source_ref:
            return StageResult.fatal_error(
                ctx,
                code="cold_source_ref_missing",
                message="source_ref is required for cold_object source",
                stage=self.stage.value,
                handler_id=self.handler_id,
            )
        ctx.source_ref = source_ref
        ctx.metadata.setdefault("cold_source_ref", source_ref)
        return StageResult.success(ctx)


class ArxivIngestHandler(BaseIngestHandler):
    handler_id = "arxiv_ingest"
    priority = 30

    def can_handle(self, ctx: PipelineContext) -> bool:
        source = (ctx.source_type or "hot_arxiv").lower()
        return source in {"hot_arxiv", "arxiv", "hot_object", ""}

    def recoverable(self, err: Exception) -> bool:
        return True

    def handle(self, ctx: PipelineContext, io: Any) -> StageResult:
        identifier = ctx.article_id or ctx.metadata.get("id") or ctx.metadata.get("article_id")
        if identifier:
            ctx.article_id = str(identifier)
        if not ctx.source_ref:
            ctx.source_ref = (
                str(ctx.metadata.get("source_ref", ""))
                or str(ctx.metadata.get("pdf_path", ""))
                or str(ctx.metadata.get("object_name", ""))
            )
        if not ctx.source_ref and isinstance(ctx.metadata.get("download_preferences"), dict):
            prefs = ctx.metadata["download_preferences"]
            preferred = str(ctx.metadata.get("preferred_format") or "source")
            entry = prefs.get(preferred) or prefs.get("source") or prefs.get("pdf")
            if isinstance(entry, dict) and entry.get("url"):
                ctx.source_ref = str(entry["url"])
            elif isinstance(entry, str):
                ctx.source_ref = entry
        if not ctx.source_ref:
            return StageResult.fatal_error(
                ctx,
                code="source_ref_missing",
                message="Unable to resolve source_ref for hot_arxiv document",
                stage=self.stage.value,
                handler_id=self.handler_id,
            )
        return StageResult.success(ctx)


def get_handlers() -> List[type[BaseIngestHandler]]:
    return [ColdDatasetIngestHandler, ColdObjectIngestHandler, ArxivIngestHandler]

