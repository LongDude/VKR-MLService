from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import yaml

from pipeline_core.contracts import (
    PipelineContext,
    PipelineError,
    StageName,
    StageResult,
    StageStatus,
    StageTrace,
)
from pipeline_core.io_gateways import PipelineIO
from pipeline_core.registry import HandlerRegistry
from pipeline_handlers.base import BaseFallbackAwareHandler, BaseHandler

logger = logging.getLogger(__name__)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class PipelineStageFailure(RuntimeError):
    def __init__(self, stage: StageName, error: PipelineError, ctx: PipelineContext) -> None:
        super().__init__(error.message)
        self.stage = stage
        self.error = error
        self.ctx = ctx


class PipelineManager:
    stage_order = [
        StageName.INGEST,
        StageName.ACQUIRE,
        StageName.EXTRACT,
        StageName.KEYWORDS,
        StageName.PUBLISH,
    ]

    def __init__(
        self,
        *,
        config_path: str = "core/pipeline.yaml",
        profile: Optional[str] = None,
        registry: Optional[HandlerRegistry] = None,
        io: Optional[PipelineIO] = None,
    ) -> None:
        self.config_path = config_path
        self.config = self._load_config(config_path)
        self.profile = profile or self.config.get("default_profile", "dev_cold")
        self.profile_cfg = self.config.get("profiles", {}).get(self.profile, {})
        if not self.profile_cfg:
            raise ValueError(f"Unknown pipeline profile '{self.profile}' in {config_path}")
        logger.info("PipelineManager profile selected: %s", self.profile)

        self.registry = registry or HandlerRegistry()
        if registry is None:
            from pipeline_handlers import load_builtin_handlers

            load_builtin_handlers(self.registry)
            logger.info("Builtin handlers loaded")
        else:
            logger.info("Custom handler registry provided")

        streams_cfg = self.profile_cfg.get("streams", {})
        io_cfg = self.config.get("io", {})
        cache_cfg = self.profile_cfg.get("cache", {})
        self.io = io or PipelineIO.from_env(
            ingress_stream=streams_cfg.get("ingress", "pipeline_ingress_v2"),
            keywords_stream=streams_cfg.get("keywords", "pipeline_keywords_v2"),
            error_stream=streams_cfg.get("errors", "pipeline_errors_v2"),
            cache_bucket=self.profile_cfg.get("cache_bucket", "pipeline-cache-v2"),
            cache_config=cache_cfg,
            buffer_size_bytes=int(io_cfg.get("buffer_size_bytes", 65536)),
        )

        self.handler_configs: Dict[str, Dict[str, Any]] = self.profile_cfg.get("handler_configs", {})
        self.handlers_by_stage: Dict[StageName, list[BaseHandler]] = {}
        for stage in self.stage_order:
            stage_key = stage.value
            enabled = self.profile_cfg.get("handlers", {}).get(stage_key, [])
            self.handlers_by_stage[stage] = self.registry.resolve_chain(
                stage,
                enabled_handlers=enabled,
                handler_configs=self.handler_configs,
            )
            logger.info(
                "Stage chain configured: stage=%s handlers=%s",
                stage.value,
                [handler.handler_id for handler in self.handlers_by_stage[stage]],
            )
        from pipeline_handlers.publish import ErrorPublishHandler

        self.error_handler = ErrorPublishHandler(config=self.profile_cfg.get("error_publish", {}))

    @staticmethod
    def _load_config(config_path: str) -> Dict[str, Any]:
        path = Path(config_path)
        if not path.exists():
            raise FileNotFoundError(f"Pipeline config not found: {config_path}")
        content = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        if not isinstance(content, dict):
            raise ValueError(f"Pipeline config must be YAML object: {config_path}")
        return content

    @staticmethod
    def _build_context(record: Dict[str, Any], stream_id: str, profile: str) -> PipelineContext:
        metadata = record.get("metadata") if isinstance(record.get("metadata"), dict) else {}
        ctx = PipelineContext(
            source_type=str(record.get("source_type") or metadata.get("source_type") or "hot_arxiv"),
            source_ref=str(record.get("source_ref") or metadata.get("source_ref") or ""),
            article_id=record.get("article_id") or metadata.get("article_id") or metadata.get("id"),
            db_id=record.get("db_id") or metadata.get("db_id"),
            metadata={**metadata, **{k: v for k, v in record.items() if k not in {"metadata"}}},
            profile=profile,
            ingress_stream_id=stream_id,
        )
        return ctx

    def _run_stage(self, stage: StageName, ctx: PipelineContext) -> PipelineContext:
        chain = self.handlers_by_stage.get(stage, [])
        if not chain:
            logger.info("Stage skipped (no handlers configured): %s", stage.value)
            return ctx

        attempted = False
        recoverable_errors: list[PipelineError] = []
        logger.info(
            "Stage started: stage=%s article_id=%s",
            stage.value,
            ctx.article_id or "unknown",
        )
        for handler in chain:
            if not handler.can_handle(ctx):
                logger.info(
                    "Handler skipped by can_handle: stage=%s handler=%s",
                    stage.value,
                    handler.handler_id,
                )
                continue
            attempted = True
            started_at = _utc_now()
            logger.info("Handler started: stage=%s handler=%s", stage.value, handler.handler_id)
            try:
                result = handler.handle(ctx, self.io)
            except Exception as exc:
                is_recoverable = isinstance(handler, BaseFallbackAwareHandler) and handler.recoverable(exc)
                if is_recoverable:
                    result = StageResult.recoverable_error(
                        ctx,
                        code="handler_exception",
                        message=str(exc),
                        stage=stage.value,
                        handler_id=handler.handler_id,
                    )
                    logger.warning(
                        "Handler failed recoverably: stage=%s handler=%s error=%s",
                        stage.value,
                        handler.handler_id,
                        exc,
                    )
                else:
                    result = StageResult.fatal_error(
                        ctx,
                        code="handler_exception",
                        message=str(exc),
                        stage=stage.value,
                        handler_id=handler.handler_id,
                    )
                    logger.exception(
                        "Handler failed fatally: stage=%s handler=%s",
                        stage.value,
                        handler.handler_id,
                    )
            finished_at = _utc_now()
            ctx = result.ctx
            ctx.add_trace(
                StageTrace(
                    stage=stage.value,
                    handler_id=handler.handler_id,
                    status=result.status.value,
                    started_at=started_at,
                    finished_at=finished_at,
                    error=result.error.to_dict() if result.error else None,
                )
            )
            logger.info(
                "Handler finished: stage=%s handler=%s status=%s",
                stage.value,
                handler.handler_id,
                result.status.value,
            )

            if result.status == StageStatus.SUCCESS:
                return ctx
            if result.status == StageStatus.SKIP:
                continue
            if result.status == StageStatus.RECOVERABLE_ERROR:
                if result.error:
                    recoverable_errors.append(result.error)
                continue
            if result.error:
                raise PipelineStageFailure(stage, result.error, ctx)
            raise PipelineStageFailure(
                stage,
                PipelineError(
                    code="stage_failed",
                    message=f"Stage {stage.value} failed",
                    stage=stage.value,
                    handler_id=handler.handler_id,
                    recoverable=False,
                ),
                ctx,
            )

        if not attempted:
            logger.warning("Stage had handlers but none accepted context: stage=%s", stage.value)
            return ctx

        if recoverable_errors:
            final_error = recoverable_errors[-1]
            final_error.code = "stage_fallback_exhausted"
            final_error.recoverable = False
            logger.error(
                "Stage fallback exhausted: stage=%s last_error=%s",
                stage.value,
                final_error.message,
            )
            raise PipelineStageFailure(stage, final_error, ctx)

        raise PipelineStageFailure(
            stage,
            PipelineError(
                code="stage_not_handled",
                message=f"No handler completed stage {stage.value}",
                stage=stage.value,
                handler_id="pipeline_manager",
                recoverable=False,
            ),
            ctx,
        )

    def run_once(self) -> bool:
        logger.info("Polling ingress stream for next record")
        record_info = self.io.poll_ingress()
        if not record_info:
            logger.info("No ingress records available")
            return False
        stream_id, payload = record_info
        logger.info("Ingress record received: stream_id=%s keys=%s", stream_id, sorted(payload.keys()))
        ctx = self._build_context(payload, stream_id, self.profile)
        try:
            for stage in self.stage_order:
                ctx = self._run_stage(stage, ctx)
            if ctx.ingress_stream_id:
                self.io.ack_ingress(ctx.ingress_stream_id)
                logger.info("Ingress record acknowledged: stream_id=%s", ctx.ingress_stream_id)
            return True
        except PipelineStageFailure as failure:
            try:
                self.error_handler.publish_error(ctx=failure.ctx, error=failure.error, io=self.io)
                logger.info(
                    "Error record published: stage=%s code=%s",
                    failure.stage.value,
                    failure.error.code,
                )
            finally:
                if ctx.ingress_stream_id:
                    self.io.ack_ingress(ctx.ingress_stream_id)
                    logger.info("Failed ingress record acknowledged: stream_id=%s", ctx.ingress_stream_id)
            logger.error("Pipeline failed at stage %s: %s", failure.stage.value, failure.error.message)
            return True

    def run_continuous(self) -> None:
        logger.info("PipelineManager started with profile=%s", self.profile)
        while self.run_once():
            continue
        logger.info("PipelineManager finished")
