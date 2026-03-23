from .contracts import (
    ArtifactRef,
    PipelineContext,
    PipelineError,
    StageName,
    StageResult,
    StageStatus,
    StageTrace,
)
from .registry import HandlerRegistry

try:
    from .io_gateways import PipelineIO
except Exception:  # pragma: no cover - optional in lightweight test envs
    PipelineIO = None  # type: ignore[assignment]

try:
    from .manager import PipelineManager
except Exception:  # pragma: no cover - optional in lightweight test envs
    PipelineManager = None  # type: ignore[assignment]

__all__ = [
    "ArtifactRef",
    "PipelineContext",
    "PipelineError",
    "PipelineIO",
    "PipelineManager",
    "HandlerRegistry",
    "StageName",
    "StageResult",
    "StageStatus",
    "StageTrace",
]
