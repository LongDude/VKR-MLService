from __future__ import annotations

import logging
import time
from typing import Any

from adapters.redis_adapter import RedisAdapter
from dto.external import ExternalPaperDTO
from dto.openalex import (
    OpenAlexBootstrapReportDTO,
    OpenAlexBootstrapRequestDTO,
    OpenAlexPendingPageDTO,
    OpenAlexUnitSummaryDTO,
)
from ml.services.openalex_paper_downloader import (
    OpenAlexDownloadResult,
    OpenAlexPaperDownloader,
)
from ml.services.openalex_paper_importer import OpenAlexPaperImporter
from ml.services.openalex_paper_plan import OpenAlexPaperPlanService
from ml.services.openalex_cooldown import set_openalex_cooldown
from repositories.papers import PaperRepository


PAPER_INDEXING_QUEUE = "queue:paper_indexing"


class OpenAlexPapersFacade:
    """Coordinate OpenAlex paper planning, downloading, importing, and queues."""

    def __init__(
        self,
        *,
        session_factory: Any,
        plan_service: OpenAlexPaperPlanService,
        downloader: OpenAlexPaperDownloader,
        importer: OpenAlexPaperImporter,
        redis_adapter: RedisAdapter | None = None,
        pending_redis_key: str | None = None,
        pending_task_options: dict[str, Any] | None = None,
        cooldown_source_queue: str | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self.session_factory = session_factory
        self.plan_service = plan_service
        self.downloader = downloader
        self.importer = importer
        self.redis_adapter = redis_adapter
        self.pending_redis_key = pending_redis_key
        self.pending_task_options = dict(pending_task_options or {})
        self.cooldown_source_queue = cooldown_source_queue
        self.logger = logger or logging.getLogger(__name__)

    async def bootstrap(
        self,
        request: OpenAlexBootstrapRequestDTO,
    ) -> OpenAlexBootstrapReportDTO:
        started_at = time.monotonic()
        plan = self.plan_service.build(request, seed=request.seed)
        initial_total_count = self._paper_count()
        report = self._base_report(
            request,
            initial_total_count=initial_total_count,
            planned_sample_count=plan.total_sample_count,
            plan_items=len(plan.items),
            quota_units=len(plan.units),
        )
        if request.dry_run or not plan.items:
            report.openalex_requests = plan.estimated_requests if request.dry_run else 0
            report.unit_summaries = [
                self._unit_summary_payload(
                    OpenAlexUnitSummaryDTO(
                        unit_key=unit.unit_key,
                        period=unit.period,
                        topic_id=unit.topic_id,
                        requested=unit.requested,
                    )
                )
                for unit in plan.units
            ]
            report.elapsed_seconds = time.monotonic() - started_at
            return report

        self.logger.info(
            "OpenAlex paper loading: quota_units=%s plan_items=%s sample=%s",
            len(plan.units),
            len(plan.items),
            plan.total_sample_count,
        )
        download = await self.downloader.fetch_plan(
            plan,
            sample=True,
            per_page=request.per_page,
            show_progress=request.show_progress,
        )
        self._merge_download(report, download)
        self._classify_unit_imports(download, request.skip_existing)
        self._assign_filter_primary_topics(download)

        imported = await self.importer.import_papers(
            download.papers,
            db_workers=request.db_workers,
            skip_existing=request.skip_existing,
            show_progress=request.show_progress,
        )
        self._merge_import(report, imported)
        enqueued_indexing = 0
        if request.enqueue_indexing:
            enqueued_indexing = self._enqueue_indexing(imported.paper_ids)

        if download.deferred:
            report.deferred_pages = self._enqueue_pending_pages(download.pending_pages)
            report.deferred = True
            report.pending_redis_key = self.pending_redis_key
            report.retry_after_seconds = download.retry_after_seconds
            self._set_cooldown(
                download.retry_after_seconds,
                task_type="bootstrap_papers",
            )

        report.final_count = self._paper_count()
        report.final_total_count = report.final_count
        self._finalize_unit_report(
            report,
            download,
            enqueued_indexing=enqueued_indexing,
        )
        report.elapsed_seconds = time.monotonic() - started_at
        return report

    async def resume(
        self,
        pages: list[OpenAlexPendingPageDTO],
        *,
        db_workers: int,
        skip_existing: bool,
        enqueue_indexing: bool,
        show_progress: bool,
    ) -> dict[str, Any]:
        if not pages:
            return {
                "loaded_pages": 0,
                "deferred": False,
                "deferred_pages": 0,
                "retry_after_seconds": None,
            }
        download = await self.downloader.fetch_pages(pages, show_progress=show_progress)
        self._classify_unit_imports(download, skip_existing)
        self._assign_filter_primary_topics(download)
        imported = await self.importer.import_papers(
            download.papers,
            db_workers=db_workers,
            skip_existing=skip_existing,
            show_progress=show_progress,
        )
        enqueued = self._enqueue_indexing(imported.paper_ids) if enqueue_indexing else 0
        deferred_pages = (
            self._enqueue_pending_pages(download.pending_pages) if download.deferred else 0
        )
        if download.deferred:
            self._set_cooldown(
                download.retry_after_seconds,
                task_type="resume_bootstrap_papers",
            )
        unit_summaries = [
            self._unit_summary_payload(summary)
            for summary in download.unit_summaries.values()
        ]
        return {
            "loaded_pages": len(pages),
            "fetched": download.fetched,
            "normalized": download.normalized,
            "created": imported.created,
            "updated": imported.updated,
            "existing": imported.existing,
            "skipped_empty_title": (
                download.skipped_empty_title + imported.skipped_empty_title
            ),
            "skipped_empty_abstract": (
                download.skipped_empty_abstract + imported.skipped_empty_abstract
            ),
            "skipped_duplicates": imported.skipped_duplicates,
            "failed": download.failed + imported.failed,
            "openalex_requests": download.openalex_requests,
            "enqueued_indexing": enqueued,
            "deferred": download.deferred,
            "deferred_pages": deferred_pages,
            "retry_after_seconds": download.retry_after_seconds,
            "unit_summaries": unit_summaries,
            "errors": [*download.errors, *imported.errors],
        }

    def _base_report(
        self,
        request: OpenAlexBootstrapRequestDTO,
        *,
        initial_total_count: int,
        planned_sample_count: int,
        plan_items: int,
        quota_units: int,
    ) -> OpenAlexBootstrapReportDTO:
        return OpenAlexBootstrapReportDTO(
            target_count=request.target_count,
            target_count_scope=request.target_count_scope,
            target_count_unit=request.target_count_unit,
            target_scope_units=quota_units,
            target_topic_count=len(request.topic_targets)
            if request.target_count_unit == "topic"
            else 0,
            target_goal_count=planned_sample_count,
            initial_count=initial_total_count,
            final_count=initial_total_count,
            initial_total_count=initial_total_count,
            final_total_count=initial_total_count,
            required_new_count=planned_sample_count,
            planned_sample_count=planned_sample_count,
            plan_items=plan_items,
            quota_units=quota_units,
        )

    def _merge_download(
        self,
        report: OpenAlexBootstrapReportDTO,
        download: OpenAlexDownloadResult,
    ) -> None:
        report.fetched += download.fetched
        report.normalized += download.normalized
        report.skipped_empty_title += download.skipped_empty_title
        report.skipped_empty_abstract += download.skipped_empty_abstract
        report.failed += download.failed
        report.openalex_requests += download.openalex_requests
        report.errors.extend(download.errors)

    def _merge_import(self, report: OpenAlexBootstrapReportDTO, imported: Any) -> None:
        report.created += imported.created
        report.updated += imported.updated
        report.existing += imported.existing
        report.skipped_empty_title += imported.skipped_empty_title
        report.skipped_empty_abstract += imported.skipped_empty_abstract
        report.skipped_duplicates += imported.skipped_duplicates
        report.failed += imported.failed
        report.errors.extend(imported.errors)
        report.duplicate_shortfall = (
            imported.updated + imported.existing + imported.skipped_duplicates
        )

    def _classify_unit_imports(
        self,
        download: OpenAlexDownloadResult,
        skip_existing: bool,
    ) -> None:
        if not download.papers:
            return
        with self.session_factory() as session:
            existing_keys = PaperRepository(session).existing_external_paper_keys(
                download.papers,
                source_name="openalex",
            )
        seen_unit_pairs: set[tuple[str, str]] = set()
        for paper in download.papers:
            paper_key = self._paper_key(paper)
            unit_keys = download.paper_unit_keys.get(paper_key, [])
            is_existing = bool(self._paper_keys(paper) & existing_keys)
            for unit_key in unit_keys:
                pair = (paper_key, unit_key)
                if pair in seen_unit_pairs:
                    continue
                seen_unit_pairs.add(pair)
                summary = download.unit_summaries.setdefault(
                    unit_key,
                    OpenAlexUnitSummaryDTO(unit_key=unit_key),
                )
                if is_existing:
                    if not skip_existing:
                        summary.updated += 1
                else:
                    summary.created += 1

    def _assign_filter_primary_topics(self, download: OpenAlexDownloadResult) -> None:
        if not download.papers or not download.paper_unit_keys:
            return
        topic_by_unit = {
            unit_key: summary.topic_id
            for unit_key, summary in download.unit_summaries.items()
            if summary.topic_id is not None
        }
        if not topic_by_unit:
            return

        papers: list[ExternalPaperDTO] = []
        for paper in download.papers:
            if paper.primary_topic_id is not None:
                papers.append(paper)
                continue
            topic_id = self._primary_topic_from_units(
                download.paper_unit_keys.get(self._paper_key(paper), []),
                topic_by_unit,
            )
            papers.append(
                paper.model_copy(update={"primary_topic_id": topic_id})
                if topic_id is not None
                else paper
            )
        download.papers = papers

    def _primary_topic_from_units(
        self,
        unit_keys: list[str],
        topic_by_unit: dict[str, int | None],
    ) -> int | None:
        for unit_key in unit_keys:
            topic_id = topic_by_unit.get(unit_key)
            if topic_id is not None:
                return int(topic_id)
        return None

    def _finalize_unit_report(
        self,
        report: OpenAlexBootstrapReportDTO,
        download: OpenAlexDownloadResult,
        *,
        enqueued_indexing: int,
    ) -> None:
        summaries = list(download.unit_summaries.values())
        report.exhausted_units = sum(1 for summary in summaries if summary.exhausted)
        report.exhausted_missing_count = sum(
            summary.missing_count for summary in summaries if summary.exhausted
        )
        report.unit_summaries = [
            self._unit_summary_payload(summary) for summary in summaries
        ]
        report.rounds.append(
            {
                "round": 1,
                "plan_items": report.plan_items,
                "planned_sample_count": report.planned_sample_count,
                "openalex_requests": report.openalex_requests,
                "created": report.created,
                "updated": report.updated,
                "exhausted_units": report.exhausted_units,
                "duplicate_shortfall": report.duplicate_shortfall,
                "enqueued_indexing": enqueued_indexing,
                "final_count": report.final_count,
            }
        )

    def _enqueue_indexing(self, paper_ids: list[int]) -> int:
        if self.redis_adapter is None:
            return 0
        enqueued = 0
        for paper_id in paper_ids:
            message = {
                "task_type": "paper_indexing",
                "paper_id": paper_id,
                "force_reindex": False,
            }
            workflow_options = self._indexing_workflow_options()
            if workflow_options:
                message.update(workflow_options)
            self.redis_adapter.enqueue(
                PAPER_INDEXING_QUEUE,
                message,
            )
            enqueued += 1
        return enqueued

    def _enqueue_pending_pages(self, pages: list[OpenAlexPendingPageDTO]) -> int:
        if not pages:
            return 0
        if self.redis_adapter is None or not self.pending_redis_key:
            raise ValueError("Redis adapter and pending_redis_key are required.")
        enqueued = 0
        for page in pages:
            payload = page.model_dump(mode="json")
            if self.pending_task_options:
                payload = {
                    "task_type": "resume_bootstrap_papers",
                    "page": payload,
                    **self.pending_task_options,
                }
            self.redis_adapter.enqueue(
                self.pending_redis_key,
                payload,
            )
            enqueued += 1
        return enqueued

    def _indexing_workflow_options(self) -> dict[str, Any]:
        allowed = {
            "source_topic_ids",
            "workflow_date_from",
            "workflow_date_to",
            "workflow_granularity",
            "enqueue_cluster_dynamics",
        }
        return {
            key: value
            for key, value in self.pending_task_options.items()
            if key in allowed and value is not None
        }

    def _set_cooldown(
        self,
        retry_after_seconds: float | None,
        *,
        task_type: str,
    ) -> None:
        if self.redis_adapter is None:
            return
        set_openalex_cooldown(
            self.redis_adapter,
            retry_after_seconds=retry_after_seconds,
            source_queue=self.cooldown_source_queue,
            task_type=task_type,
        )

    def _paper_count(self) -> int:
        with self.session_factory() as session:
            return PaperRepository(session).count_all()

    def _unit_summary_payload(self, summary: OpenAlexUnitSummaryDTO) -> dict[str, Any]:
        payload = summary.model_dump(mode="json")
        payload["missing_count"] = summary.missing_count
        return payload

    def _paper_key(self, paper: ExternalPaperDTO) -> str:
        if not paper.external_id or not paper.external_id.strip():
            raise ValueError("OpenAlex paper external_id is required")
        return f"external:{paper.external_id.strip()}"

    def _paper_keys(self, paper: ExternalPaperDTO) -> set[str]:
        return {self._paper_key(paper)}


__all__ = ["OpenAlexPapersFacade"]
