from __future__ import annotations

from typing import Any

from dto.openalex import OpenAlexBootstrapReportDTO, OpenAlexBootstrapRequestDTO
from ml.facades.openalex_papers import OpenAlexPapersFacade


class OpenAlexPaperLoadingPipeline:
    """Pipeline wrapper for OpenAlex paper loading CLI scenarios."""

    def __init__(self, facade: OpenAlexPapersFacade) -> None:
        self.facade = facade

    async def bootstrap_papers(
        self,
        request: OpenAlexBootstrapRequestDTO,
    ) -> OpenAlexBootstrapReportDTO:
        return await self.facade.bootstrap(request)

    async def resume_pages(
        self,
        pages: list[Any],
        *,
        db_workers: int,
        skip_existing: bool,
        enqueue_indexing: bool,
        show_progress: bool,
    ) -> dict[str, Any]:
        return await self.facade.resume(
            pages,
            db_workers=db_workers,
            skip_existing=skip_existing,
            enqueue_indexing=enqueue_indexing,
            show_progress=show_progress,
        )


__all__ = ["OpenAlexPaperLoadingPipeline"]
