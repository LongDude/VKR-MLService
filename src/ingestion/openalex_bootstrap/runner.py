from __future__ import annotations

from typing import Any

from dto.openalex import OpenAlexBootstrapReportDTO, OpenAlexBootstrapRequestDTO
from ml.facades.openalex_papers import OpenAlexPapersFacade
from ml.services.openalex_paper_downloader import OpenAlexPaperDownloader
from ml.services.openalex_paper_importer import OpenAlexPaperImporter
from ml.services.openalex_paper_plan import OpenAlexPaperPlanService


class OpenAlexBootstrapRunner:
    """Compatibility wrapper for the new OpenAlexPapersFacade."""

    def __init__(
        self,
        *,
        request: OpenAlexBootstrapRequestDTO,
        session_factory: Any,
        load_plan_builder: OpenAlexPaperPlanService | None = None,
        downloader: OpenAlexPaperDownloader,
        importer: OpenAlexPaperImporter,
        redis_adapter: Any | None = None,
        pending_redis_key: str | None = None,
        **_unused: Any,
    ) -> None:
        self.request = request
        self.facade = OpenAlexPapersFacade(
            session_factory=session_factory,
            plan_service=load_plan_builder or OpenAlexPaperPlanService(),
            downloader=downloader,
            importer=importer,
            redis_adapter=redis_adapter,
            pending_redis_key=pending_redis_key,
        )

    async def run(self) -> OpenAlexBootstrapReportDTO:
        return await self.facade.bootstrap(self.request)


__all__ = ["OpenAlexBootstrapRunner"]

