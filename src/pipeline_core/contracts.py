from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, Optional


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class StageName(str, Enum):
    INGEST = "ingest"
    ACQUIRE = "acquire"
    EXTRACT = "extract"
    KEYWORDS = "keywords"
    PUBLISH = "publish"


class StageStatus(str, Enum):
    SUCCESS = "success"
    SKIP = "skip"
    RECOVERABLE_ERROR = "recoverable_error"
    FATAL_ERROR = "fatal_error"


@dataclass
class PipelineError:
    code: str
    message: str
    stage: str
    handler_id: str
    recoverable: bool = False
    details: Dict[str, Any] = field(default_factory=dict)
    timestamp: str = field(default_factory=utc_now_iso)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "stage": self.stage,
            "handler_id": self.handler_id,
            "recoverable": self.recoverable,
            "details": self.details,
            "timestamp": self.timestamp,
        }

    @classmethod
    def from_exception(
        cls,
        exc: Exception,
        *,
        stage: str,
        handler_id: str,
        recoverable: bool,
        code: str = "handler_exception",
        details: Optional[Dict[str, Any]] = None,
    ) -> "PipelineError":
        return cls(
            code=code,
            message=str(exc),
            stage=stage,
            handler_id=handler_id,
            recoverable=recoverable,
            details=details or {},
        )


@dataclass
class ArtifactRef:
    name: str
    bucket: Optional[str] = None
    object_name: Optional[str] = None
    path: Optional[str] = None
    uri: Optional[str] = None
    content_type: Optional[str] = None
    source: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "bucket": self.bucket,
            "object_name": self.object_name,
            "path": self.path,
            "uri": self.uri,
            "content_type": self.content_type,
            "source": self.source,
            "metadata": self.metadata,
        }


@dataclass
class StageTrace:
    stage: str
    handler_id: str
    status: str
    started_at: str
    finished_at: str
    error: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "stage": self.stage,
            "handler_id": self.handler_id,
            "status": self.status,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "error": self.error,
        }


@dataclass
class PipelineContext:
    source_type: str = ""
    source_ref: str = ""
    article_id: Optional[str] = None
    db_id: Optional[int] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    artifacts: Dict[str, ArtifactRef] = field(default_factory=dict)
    trace: list[StageTrace] = field(default_factory=list)
    profile: str = "default"
    ingress_stream_id: Optional[str] = None
    runtime_data: Dict[str, Any] = field(default_factory=dict)

    def register_artifact(self, artifact: ArtifactRef) -> None:
        self.artifacts[artifact.name] = artifact

    def add_trace(self, trace: StageTrace) -> None:
        self.trace.append(trace)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "source_type": self.source_type,
            "source_ref": self.source_ref,
            "article_id": self.article_id,
            "db_id": self.db_id,
            "metadata": self.metadata,
            "artifacts": {name: ref.to_dict() for name, ref in self.artifacts.items()},
            "trace": [entry.to_dict() for entry in self.trace],
            "profile": self.profile,
        }


@dataclass
class StageResult:
    status: StageStatus
    ctx: PipelineContext
    error: Optional[PipelineError] = None

    @classmethod
    def success(cls, ctx: PipelineContext) -> "StageResult":
        return cls(status=StageStatus.SUCCESS, ctx=ctx)

    @classmethod
    def skip(cls, ctx: PipelineContext) -> "StageResult":
        return cls(status=StageStatus.SKIP, ctx=ctx)

    @classmethod
    def recoverable_error(
        cls,
        ctx: PipelineContext,
        *,
        code: str,
        message: str,
        stage: str,
        handler_id: str,
        details: Optional[Dict[str, Any]] = None,
    ) -> "StageResult":
        error = PipelineError(
            code=code,
            message=message,
            stage=stage,
            handler_id=handler_id,
            recoverable=True,
            details=details or {},
        )
        return cls(status=StageStatus.RECOVERABLE_ERROR, ctx=ctx, error=error)

    @classmethod
    def fatal_error(
        cls,
        ctx: PipelineContext,
        *,
        code: str,
        message: str,
        stage: str,
        handler_id: str,
        details: Optional[Dict[str, Any]] = None,
    ) -> "StageResult":
        error = PipelineError(
            code=code,
            message=message,
            stage=stage,
            handler_id=handler_id,
            recoverable=False,
            details=details or {},
        )
        return cls(status=StageStatus.FATAL_ERROR, ctx=ctx, error=error)

