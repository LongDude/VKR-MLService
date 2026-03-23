from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict

from pipeline_core.contracts import PipelineContext, StageName, StageResult


class BaseHandler(ABC):
    handler_id: str = "base_handler"
    stage: StageName = StageName.INGEST
    priority: int = 100

    def __init__(self, config: Dict[str, Any] | None = None) -> None:
        self.config = config or {}

    @abstractmethod
    def can_handle(self, ctx: PipelineContext) -> bool:
        raise NotImplementedError

    @abstractmethod
    def handle(self, ctx: PipelineContext, io: Any) -> StageResult:
        raise NotImplementedError


class BaseFallbackAwareHandler(BaseHandler, ABC):
    @abstractmethod
    def recoverable(self, err: Exception) -> bool:
        raise NotImplementedError


class BaseIngestHandler(BaseFallbackAwareHandler, ABC):
    stage = StageName.INGEST


class BaseAcquireHandler(BaseFallbackAwareHandler, ABC):
    stage = StageName.ACQUIRE


class BaseExtractHandler(BaseFallbackAwareHandler, ABC):
    stage = StageName.EXTRACT


class BaseKeywordHandler(BaseFallbackAwareHandler, ABC):
    stage = StageName.KEYWORDS


class BasePublishHandler(BaseFallbackAwareHandler, ABC):
    stage = StageName.PUBLISH

