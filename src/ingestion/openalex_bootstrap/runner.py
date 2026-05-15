from __future__ import annotations

import logging
import time
from typing import Any

from adapters.redis_adapter import RedisAdapter
from ingestion.openalex_bootstrap.downloader import OpenAlexBootstrapDownloader
from ingestion.openalex_bootstrap.dto import (
    OpenAlexBootstrapReportDTO,
    OpenAlexBootstrapRequestDTO,
)
from ingestion.openalex_bootstrap.importer import OpenAlexBatchImporter
from ingestion.openalex_bootstrap.load_plan import OpenAlexLoadPlanBuilder
from ingestion.openalex_bootstrap.monthly_counts import MonthlyCountsLoader
from ingestion.openalex_bootstrap.stats import OpenAlexMonthlyStatsCollector
from ml.workers.redis_worker import PAPER_INDEXING_QUEUE
from repositories.papers import PaperRepository


class OpenAlexBootstrapRunner:
    """Coordinate planning, downloading, importing, and optional indexing enqueue."""

    def __init__(
        self,
        *,
        request: OpenAlexBootstrapRequestDTO,
        session_factory: Any,
        load_plan_builder: OpenAlexLoadPlanBuilder,
        downloader: OpenAlexBootstrapDownloader,
        importer: OpenAlexBatchImporter,
        redis_adapter: RedisAdapter | None = None,
        stats_collector: OpenAlexMonthlyStatsCollector | None = None,
        monthly_counts_loader: MonthlyCountsLoader | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self.request = request
        self.session_factory = session_factory
        self.load_plan_builder = load_plan_builder
        self.downloader = downloader
        self.importer = importer
        self.redis_adapter = redis_adapter
        self.stats_collector = stats_collector
        self.monthly_counts_loader = monthly_counts_loader or MonthlyCountsLoader()
        self.logger = logger or logging.getLogger(__name__)

    async def run(self) -> OpenAlexBootstrapReportDTO:
        """Run bootstrap until target_count is reached or max_rounds is exhausted."""
        started_at = time.monotonic()
        await self._ensure_monthly_stats()
        initial_count = self._target_scope_paper_count()
        initial_total_count = self._paper_count()
        required_new_count = max(0, self.request.target_count - initial_count)
        report = OpenAlexBootstrapReportDTO(
            target_count=self.request.target_count,
            target_count_scope=self.request.target_count_scope,
            initial_count=initial_count,
            final_count=initial_count,
            initial_total_count=initial_total_count,
            final_total_count=initial_total_count,
            required_new_count=required_new_count,
        )

        if required_new_count <= 0:
            report.elapsed_seconds = time.monotonic() - started_at
            self.logger.info(
                "OpenAlex bootstrap skipped: initial_count=%s target_count=%s",
                initial_count,
                self.request.target_count,
            )
            return report

        for round_index in range(self.request.max_rounds):
            current_count = self._target_scope_paper_count()
            missing_count = max(0, self.request.target_count - current_count)
            if missing_count <= 0:
                break

            round_seed = self.request.seed + round_index
            plan = self.load_plan_builder.build(
                self.request,
                target_new_count=missing_count,
                seed=round_seed,
            )
            report.planned_sample_count += plan.total_sample_count
            round_payload: dict[str, Any] = {
                "round": round_index + 1,
                "seed": round_seed,
                "required": missing_count,
                "plan_items": len(plan.items),
                "planned_sample_count": plan.total_sample_count,
                "estimated_requests": plan.estimated_requests,
            }

            self.logger.info(
                "OpenAlex bootstrap round %s: required=%s plan_items=%s sample=%s",
                round_index + 1,
                missing_count,
                len(plan.items),
                plan.total_sample_count,
            )

            if self.request.dry_run:
                round_payload["dry_run"] = True
                report.openalex_requests += plan.estimated_requests
                report.rounds.append(round_payload)
                break

            download = await self.downloader.fetch_plan(
                plan,
                sample=self.request.sample,
                per_page=self.request.per_page,
                show_progress=self.request.show_progress,
            )
            report.fetched += download.fetched
            report.normalized += download.normalized
            report.skipped_empty_title += download.skipped_empty_title
            report.failed += download.failed
            report.openalex_requests += download.openalex_requests
            report.errors.extend(download.errors)

            imported = await self.importer.import_papers(
                download.papers,
                db_workers=self.request.db_workers,
                skip_existing=self.request.skip_existing,
                show_progress=self.request.show_progress,
            )
            report.created += imported.created
            report.updated += imported.updated
            report.existing += imported.existing
            report.skipped_empty_title += imported.skipped_empty_title
            report.skipped_duplicates += imported.skipped_duplicates
            report.failed += imported.failed
            report.errors.extend(imported.errors)

            enqueued = 0
            if self.request.enqueue_indexing:
                enqueued = self._enqueue_indexing(imported.paper_ids)

            final_count = self._target_scope_paper_count()
            report.final_count = final_count
            report.final_total_count = self._paper_count()
            round_payload.update(
                {
                    "fetched": download.fetched,
                    "normalized": download.normalized,
                    "created": imported.created,
                    "updated": imported.updated,
                    "existing": imported.existing,
                    "skipped_empty_title": (
                        download.skipped_empty_title + imported.skipped_empty_title
                    ),
                    "skipped_duplicates": imported.skipped_duplicates,
                    "failed": download.failed + imported.failed,
                    "openalex_requests": download.openalex_requests,
                    "final_count": final_count,
                    "final_total_count": report.final_total_count,
                    "enqueued_indexing": enqueued,
                }
            )
            report.rounds.append(round_payload)

            self.logger.info(
                "OpenAlex bootstrap round %s done: final_count=%s created=%s updated=%s",
                round_index + 1,
                final_count,
                imported.created,
                imported.updated,
            )

        report.final_count = (
            self._target_scope_paper_count()
            if not self.request.dry_run
            else initial_count
        )
        report.final_total_count = (
            self._paper_count()
            if not self.request.dry_run
            else initial_total_count
        )
        report.elapsed_seconds = time.monotonic() - started_at
        return report

    def _paper_count(self) -> int:
        with self.session_factory() as session:
            return PaperRepository(session).count_all()

    def _target_scope_paper_count(self) -> int:
        with self.session_factory() as session:
            repository = PaperRepository(session)
            if self.request.target_count_scope == "period":
                return repository.count_by_period(
                    self.request.date_from,
                    self.request.date_to,
                )
            return repository.count_all()

    async def _ensure_monthly_stats(self) -> None:
        if self.request.normalize == "none":
            return
        if self.request.monthly_stats_source != "redis":
            return
        if self.redis_adapter is None:
            raise ValueError("Redis adapter is required for Redis monthly stats.")
        if not self.request.monthly_counts_redis_key:
            raise ValueError("monthly_counts_redis_key is required for Redis monthly stats.")

        missing_months = self.monthly_counts_loader.missing_redis_months(
            self.redis_adapter,
            self.request.monthly_counts_redis_key,
            date_from=self.request.date_from,
            date_to=self.request.date_to,
        )
        if not missing_months:
            return
        if self.request.missing_stats_policy == "error":
            missing_periods = [item.period for item in missing_months]
            raise ValueError(
                "OpenAlex monthly stats are missing in Redis for periods: "
                + ", ".join(missing_periods[:20])
            )
        if self.request.dry_run:
            raise ValueError(
                "--dry-run cannot fetch missing OpenAlex stats; run collect-stats first."
            )
        if self.stats_collector is None:
            raise ValueError("Stats collector is required when missing_stats_policy=fetch.")

        await self.stats_collector.collect_and_store(
            date_from=min(item.date_from for item in missing_months),
            date_to=max(item.date_to for item in missing_months),
            languages=self.request.languages,
            types=self.request.types,
            missing_only=True,
            show_progress=self.request.show_progress,
        )

    def _enqueue_indexing(self, paper_ids: list[int]) -> int:
        if self.redis_adapter is None:
            return 0
        enqueued = 0
        for paper_id in paper_ids:
            self.redis_adapter.enqueue(
                PAPER_INDEXING_QUEUE,
                {
                    "task_type": "paper_indexing",
                    "paper_id": paper_id,
                    "force_reindex": False,
                },
            )
            enqueued += 1
        return enqueued


__all__ = ["OpenAlexBootstrapRunner"]
