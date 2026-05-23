from __future__ import annotations

from typing import Any

from ml.services.openalex_paper_plan import OPENALEX_MAX_SAMPLE, OpenAlexPaperPlanService


class OpenAlexLoadPlanBuilder(OpenAlexPaperPlanService):
    """Compatibility wrapper for the new one-pass plan service."""

    def __init__(
        self,
        *args: Any,
        monthly_counts_loader: Any | None = None,
        redis_adapter: Any | None = None,
        **kwargs: Any,
    ) -> None:
        kwargs.pop("stats_collector", None)
        super().__init__(**kwargs)

    def build(
        self,
        request: Any,
        *args: Any,
        target_new_count: int | None = None,
        seed: int = 42,
        **kwargs: Any,
    ) -> Any:
        if target_new_count is not None and hasattr(request, "model_copy"):
            request = request.model_copy(update={"target_count": target_new_count})
        return super().build(request, seed=seed)


__all__ = ["OPENALEX_MAX_SAMPLE", "OpenAlexLoadPlanBuilder", "OpenAlexPaperPlanService"]
