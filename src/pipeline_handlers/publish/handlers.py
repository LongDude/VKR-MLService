from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List

from pipeline_core.contracts import PipelineContext, PipelineError, StageResult
from pipeline_handlers.base import BasePublishHandler


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class KeywordPublishHandler(BasePublishHandler):
    handler_id = "keyword_publish"
    priority = 10

    def can_handle(self, ctx: PipelineContext) -> bool:
        keywords = ctx.runtime_data.get("keywords")
        return isinstance(keywords, list) and len(keywords) > 0

    def recoverable(self, err: Exception) -> bool:
        return False

    def handle(self, ctx: PipelineContext, io: Any) -> StageResult:
        keywords = ctx.runtime_data.get("keywords")
        if not isinstance(keywords, list) or not keywords:
            return StageResult.fatal_error(
                ctx,
                code="keywords_missing",
                message="Keywords payload is missing",
                stage=self.stage.value,
                handler_id=self.handler_id,
            )
        payload: Dict[str, Any] = {
            "article_id": ctx.article_id,
            "db_id": ctx.db_id,
            "source_type": ctx.source_type,
            "source_ref": ctx.source_ref,
            "keywords": keywords,
            "keyword_count": len(keywords),
            "artifacts": {key: ref.to_dict() for key, ref in ctx.artifacts.items()},
            "trace": [entry.to_dict() for entry in ctx.trace],
            "published_at": _utc_now(),
        }
        io.publish_keywords(payload)
        return StageResult.success(ctx)


class ErrorPublishHandler(BasePublishHandler):
    handler_id = "error_publish"
    priority = 1000

    def can_handle(self, ctx: PipelineContext) -> bool:
        return False

    def recoverable(self, err: Exception) -> bool:
        return False

    def handle(self, ctx: PipelineContext, io: Any) -> StageResult:
        return StageResult.skip(ctx)

    def publish_error(self, ctx: PipelineContext, error: PipelineError, io: Any) -> None:
        payload = {
            "article_id": ctx.article_id,
            "db_id": ctx.db_id,
            "source_type": ctx.source_type,
            "source_ref": ctx.source_ref,
            "error": error.to_dict(),
            "trace": [entry.to_dict() for entry in ctx.trace],
            "failed_at": _utc_now(),
            "profile": ctx.profile,
        }
        io.publish_error(payload)


def get_handlers() -> List[type[BasePublishHandler]]:
    return [KeywordPublishHandler, ErrorPublishHandler]

