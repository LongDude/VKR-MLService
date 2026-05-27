from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent
SRC_DIR = BASE_DIR.parent
PROJECT_DIR = SRC_DIR.parent

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from adapters import QdrantAdapter, RedisAdapter
from core.config import Settings
from core.exceptions import AppError
from ml.constants import PAPERS_COLLECTION
from ml.services.quarter_periods import QuarterPeriodService
from ml.services.openalex_cooldown import OPENALEX_COOLDOWN_KEY
from ml.services.cluster_recompute_tasks import (
    acquire_cluster_recompute_topic_ids,
    build_cluster_recompute_message,
    release_cluster_recompute_dedupe_keys,
)
from ml.services.cluster_dynamics_tasks import (
    acquire_cluster_dynamics_cluster_ids,
    build_cluster_dynamics_message,
    release_cluster_dynamics_dedupe_keys,
)
from ml.workers.redis_worker import (
    CLUSTER_DYNAMICS_RECOMPUTE_QUEUE,
    CLUSTER_RECOMPUTE_QUEUE,
    FAILED_TASKS_QUEUE,
    ML_WORKER_SHUTDOWN_KEY,
    OPENALEX_BOOTSTRAP_PAPERS_QUEUE,
    OPENALEX_TOPIC_STATS_QUEUE,
    PAPER_INDEXING_QUEUE,
    TOPIC_QUARTER_REPORT_QUEUE,
)
from models.session import create_db_engine, create_session_factory
from repositories import PaperRepository, TaxonomyRepository


DEFAULT_PAGE_SIZE = 1000
DEFAULT_QDRANT_RETRIEVE_SIZE = 256


class ClusterRecomputeTaskEnqueuer:
    """Create Redis tasks for static topic cluster recomputation."""

    def __init__(
        self,
        *,
        redis_adapter: RedisAdapter | None,
        queue_name: str = CLUSTER_RECOMPUTE_QUEUE,
    ) -> None:
        self.redis_adapter = redis_adapter
        self.queue_name = queue_name

    def enqueue(
        self,
        *,
        topic_ids: list[int],
        batch_size: int,
        force_summary: bool = False,
        cluster_workers: int = 1,
        dry_run: bool = False,
        show_progress: bool = True,
    ) -> dict[str, Any]:
        """Build static cluster recompute messages and optionally enqueue them."""
        if not topic_ids:
            raise ValueError("At least one topic id is required.")
        if batch_size <= 0:
            raise ValueError("--batch-size must be a positive integer.")
        if cluster_workers <= 0:
            raise ValueError("--cluster-workers must be a positive integer.")
        unique_topic_ids = list(dict.fromkeys(int(topic_id) for topic_id in topic_ids))
        accepted_topic_ids = unique_topic_ids
        if not dry_run:
            if self.redis_adapter is None:
                raise ValueError("Redis adapter is required unless --dry-run is used.")
            accepted_topic_ids = acquire_cluster_recompute_topic_ids(
                self.redis_adapter,
                unique_topic_ids,
                force_summary=bool(force_summary),
            )
        messages = [
            build_cluster_recompute_message(
                chunk,
                force_summary=bool(force_summary),
                cluster_workers=(
                    int(cluster_workers) if int(cluster_workers) != 1 else None
                ),
            )
            for chunk in chunked(accepted_topic_ids, batch_size)
        ]
        try:
            self._enqueue_messages(messages, dry_run=dry_run, show_progress=show_progress)
        except Exception:
            if not dry_run and self.redis_adapter is not None:
                for message in messages:
                    release_cluster_recompute_dedupe_keys(self.redis_adapter, message)
            raise
        return {
            "queue": self.queue_name,
            "dry_run": dry_run,
            "topic_count": len(set(unique_topic_ids)),
            "dedupe_skipped": len(unique_topic_ids) - len(accepted_topic_ids),
            "messages": len(messages),
            "batch_size": batch_size,
            "force_summary": bool(force_summary),
            "cluster_workers": int(cluster_workers),
            "sample": messages[:3],
        }

    def _enqueue_messages(
        self,
        messages: list[dict[str, Any]],
        *,
        dry_run: bool,
        show_progress: bool,
    ) -> None:
        if dry_run:
            return
        if self.redis_adapter is None:
            raise ValueError("Redis adapter is required unless --dry-run is used.")
        progress = None
        if show_progress:
            from tqdm.auto import tqdm

            progress = tqdm(total=len(messages), desc="Enqueue cluster recompute", unit="batch")
        try:
            for message in messages:
                self.redis_adapter.enqueue(self.queue_name, message)
                if progress is not None:
                    progress.update(1)
        finally:
            if progress is not None:
                progress.close()


class ClusterDynamicsTaskEnqueuer:
    """Create Redis tasks for time-sliced topic cluster dynamics recomputation."""

    def __init__(
        self,
        *,
        redis_adapter: RedisAdapter | None,
        queue_name: str = CLUSTER_DYNAMICS_RECOMPUTE_QUEUE,
    ) -> None:
        self.redis_adapter = redis_adapter
        self.queue_name = queue_name

    def enqueue(
        self,
        *,
        cluster_ids: list[str],
        date_from: date,
        date_to: date,
        granularity: str,
        batch_size: int,
        dry_run: bool = False,
        show_progress: bool = True,
    ) -> dict[str, Any]:
        """Build cluster dynamics recompute messages and optionally enqueue them."""
        if not cluster_ids:
            raise ValueError("At least one cluster id is required.")
        if date_from > date_to:
            raise ValueError("--date-from must be before or equal to --date-to.")
        if granularity not in {"week", "month"}:
            raise ValueError("--granularity must be 'week' or 'month'.")
        if batch_size <= 0:
            raise ValueError("--batch-size must be a positive integer.")
        unique_cluster_ids = list(dict.fromkeys(cluster_ids))
        accepted_cluster_ids = unique_cluster_ids
        if not dry_run:
            if self.redis_adapter is None:
                raise ValueError("Redis adapter is required unless --dry-run is used.")
            accepted_cluster_ids = acquire_cluster_dynamics_cluster_ids(
                self.redis_adapter,
                unique_cluster_ids,
                date_from=date_from,
                date_to=date_to,
                granularity=granularity,
            )
        messages = [
            build_cluster_dynamics_message(
                chunk,
                date_from=date_from,
                date_to=date_to,
                granularity=granularity,
            )
            for chunk in chunked_strings(accepted_cluster_ids, batch_size)
        ]
        try:
            self._enqueue_messages(messages, dry_run=dry_run, show_progress=show_progress)
        except Exception:
            if not dry_run and self.redis_adapter is not None:
                for message in messages:
                    release_cluster_dynamics_dedupe_keys(
                        self.redis_adapter,
                        message,
                    )
            raise
        return {
            "queue": self.queue_name,
            "dry_run": dry_run,
            "cluster_count": len(unique_cluster_ids),
            "dedupe_skipped": len(unique_cluster_ids) - len(accepted_cluster_ids),
            "messages": len(messages),
            "batch_size": batch_size,
            "date_from": date_from,
            "date_to": date_to,
            "granularity": granularity,
            "sample": messages[:3],
        }

    def _enqueue_messages(
        self,
        messages: list[dict[str, Any]],
        *,
        dry_run: bool,
        show_progress: bool,
    ) -> None:
        if dry_run:
            return
        if self.redis_adapter is None:
            raise ValueError("Redis adapter is required unless --dry-run is used.")
        progress = None
        if show_progress:
            from tqdm.auto import tqdm

            progress = tqdm(total=len(messages), desc="Enqueue cluster dynamics", unit="batch")
        try:
            for message in messages:
                self.redis_adapter.enqueue(self.queue_name, message)
                if progress is not None:
                    progress.update(1)
        finally:
            if progress is not None:
                progress.close()


class TopicQuarterReportTaskEnqueuer:
    """Create Redis tasks for quarterly topic report generation."""

    def __init__(
        self,
        *,
        redis_adapter: RedisAdapter | None,
        queue_name: str = TOPIC_QUARTER_REPORT_QUEUE,
        quarter_period_service: QuarterPeriodService | None = None,
    ) -> None:
        self.redis_adapter = redis_adapter
        self.queue_name = queue_name
        self.quarter_period_service = quarter_period_service or QuarterPeriodService()

    def enqueue(
        self,
        *,
        topic_ids: list[int],
        date_from: date,
        date_to: date,
        batch_size: int,
        force: bool = False,
        report_language: str = "ru",
        dry_run: bool = False,
        show_progress: bool = True,
    ) -> dict[str, Any]:
        """Build topic-quarter report task messages and optionally enqueue them."""
        if not topic_ids:
            raise ValueError("At least one topic id is required.")
        if batch_size <= 0:
            raise ValueError("--batch-size must be a positive integer.")
        periods = self.quarter_period_service.quarter_periods(date_from, date_to)
        requests = [
            {
                "topic_id": int(topic_id),
                "period_start": period.date_from.isoformat(),
                "period_end": period.date_to.isoformat(),
            }
            for topic_id in list(dict.fromkeys(topic_ids))
            for period in periods
        ]
        messages = [
            {
                "task_type": "topic_quarter_report",
                "requests": chunk,
                "force": bool(force),
                "report_language": report_language,
            }
            for chunk in chunked_dicts(requests, batch_size)
        ]
        if not dry_run:
            if self.redis_adapter is None:
                raise ValueError("Redis adapter is required unless --dry-run is used.")
            iterator = messages
            progress = None
            if show_progress:
                from tqdm.auto import tqdm

                progress = tqdm(total=len(messages), desc="Enqueue topic reports", unit="batch")
            try:
                for message in iterator:
                    self.redis_adapter.enqueue(self.queue_name, message)
                    if progress is not None:
                        progress.update(1)
            finally:
                if progress is not None:
                    progress.close()
        return {
            "queue": self.queue_name,
            "dry_run": dry_run,
            "topic_count": len(set(topic_ids)),
            "periods": [period.key for period in periods],
            "request_count": len(requests),
            "messages": len(messages),
            "batch_size": batch_size,
            "force": bool(force),
            "report_language": report_language,
            "sample": messages[:3],
        }


class OpenAlexTopicStatsTaskEnqueuer:
    """Create high-priority Redis tasks for OpenAlex topic stats collection."""

    def __init__(
        self,
        *,
        redis_adapter: RedisAdapter | None,
        queue_name: str = OPENALEX_TOPIC_STATS_QUEUE,
    ) -> None:
        self.redis_adapter = redis_adapter
        self.queue_name = queue_name

    def enqueue(
        self,
        *,
        date_from: date,
        date_to: date,
        topic_ids: list[int] | None = None,
        field_ids: list[int] | None = None,
        subfield_ids: list[int] | None = None,
        taxonomy_scope: str = "topic",
        languages: list[str] | None = None,
        types: list[str] | None = None,
        batch_size: int = 500,
        task_batch_size: int = 100,
        request_workers: int = 8,
        rate_limit_rps: float = 10.0,
        max_retries: int = 5,
        group_by_page_size: int = 200,
        normalize_january_first: bool = True,
        primary_topic_only: bool = True,
        openalex_url: str | None = None,
        openalex_api_key: str | None = None,
        openalex_mailto: str | None = None,
        dry_run: bool = False,
        show_progress: bool = True,
    ) -> dict[str, Any]:
        """Build collect-topic-stats worker messages and optionally enqueue them."""
        self._validate(
            date_from=date_from,
            date_to=date_to,
            topic_ids=topic_ids,
            field_ids=field_ids,
            subfield_ids=subfield_ids,
            taxonomy_scope=taxonomy_scope,
            batch_size=batch_size,
            task_batch_size=task_batch_size,
            request_workers=request_workers,
            rate_limit_rps=rate_limit_rps,
            max_retries=max_retries,
            group_by_page_size=group_by_page_size,
        )
        messages = self._messages(
            date_from=date_from,
            date_to=date_to,
            topic_ids=topic_ids or [],
            field_ids=field_ids or [],
            subfield_ids=subfield_ids or [],
            taxonomy_scope=taxonomy_scope,
            languages=languages or ["en", "ru"],
            types=types or ["article"],
            batch_size=batch_size,
            task_batch_size=task_batch_size,
            request_workers=request_workers,
            rate_limit_rps=rate_limit_rps,
            max_retries=max_retries,
            group_by_page_size=group_by_page_size,
            normalize_january_first=normalize_january_first,
            primary_topic_only=primary_topic_only,
            openalex_url=openalex_url,
            openalex_api_key=openalex_api_key,
            openalex_mailto=openalex_mailto,
            show_progress=show_progress,
        )
        if not dry_run:
            if self.redis_adapter is None:
                raise ValueError("Redis adapter is required unless --dry-run is used.")
            progress = None
            if show_progress:
                from tqdm.auto import tqdm

                progress = tqdm(total=len(messages), desc="Enqueue topic stats", unit="task")
            try:
                for message in messages:
                    self.redis_adapter.enqueue(self.queue_name, message)
                    if progress is not None:
                        progress.update(1)
            finally:
                if progress is not None:
                    progress.close()
        return {
            "queue": self.queue_name,
            "priority": "max",
            "dry_run": dry_run,
            "taxonomy_scope": taxonomy_scope,
            "topic_count": len(set(topic_ids or [])),
            "field_ids": sorted(set(field_ids or [])),
            "subfield_ids": sorted(set(subfield_ids or [])),
            "messages": len(messages),
            "task_batch_size": task_batch_size,
            "date_from": date_from,
            "date_to": date_to,
            "sample": messages[:3],
        }

    def _messages(
        self,
        *,
        date_from: date,
        date_to: date,
        topic_ids: list[int],
        field_ids: list[int],
        subfield_ids: list[int],
        taxonomy_scope: str,
        languages: list[str],
        types: list[str],
        batch_size: int,
        task_batch_size: int,
        request_workers: int,
        rate_limit_rps: float,
        max_retries: int,
        group_by_page_size: int,
        normalize_january_first: bool,
        primary_topic_only: bool,
        openalex_url: str | None,
        openalex_api_key: str | None,
        openalex_mailto: str | None,
        show_progress: bool,
    ) -> list[dict[str, Any]]:
        base = {
            "task_type": "collect_topic_stats",
            "date_from": date_from.isoformat(),
            "date_to": date_to.isoformat(),
            "taxonomy_scope": taxonomy_scope,
            "languages": languages,
            "types": types,
            "batch_size": batch_size,
            "request_workers": request_workers,
            "rate_limit_rps": rate_limit_rps,
            "max_retries": max_retries,
            "group_by_page_size": group_by_page_size,
            "normalize_january_first": bool(normalize_january_first),
            "primary_topic_only": bool(primary_topic_only),
            "show_progress": bool(show_progress),
            **({"openalex_url": openalex_url} if openalex_url else {}),
            **({"openalex_api_key": openalex_api_key} if openalex_api_key else {}),
            **({"openalex_mailto": openalex_mailto} if openalex_mailto else {}),
        }
        if taxonomy_scope == "topic":
            return [
                {
                    **base,
                    "topic_ids": chunk,
                }
                for chunk in chunked(topic_ids, task_batch_size)
            ]
        return [
            {
                **base,
                **({"field_ids": field_ids} if field_ids else {}),
                **({"subfield_ids": subfield_ids} if subfield_ids else {}),
            }
        ]

    def _validate(
        self,
        *,
        date_from: date,
        date_to: date,
        topic_ids: list[int] | None,
        field_ids: list[int] | None,
        subfield_ids: list[int] | None,
        taxonomy_scope: str,
        batch_size: int,
        task_batch_size: int,
        request_workers: int,
        rate_limit_rps: float,
        max_retries: int,
        group_by_page_size: int,
    ) -> None:
        if date_from > date_to:
            raise ValueError("--date-from must be before or equal to --date-to.")
        if taxonomy_scope not in {"topic", "field", "subfield"}:
            raise ValueError("--taxonomy-scope must be one of: topic, field, subfield.")
        if batch_size <= 0 or batch_size > 500:
            raise ValueError("--batch-size must be between 1 and 500.")
        if task_batch_size <= 0:
            raise ValueError("--task-batch-size must be a positive integer.")
        if request_workers <= 0:
            raise ValueError("--request-workers must be a positive integer.")
        if rate_limit_rps <= 0:
            raise ValueError("--rate-limit-rps must be positive.")
        if max_retries < 0:
            raise ValueError("--max-retries must be non-negative.")
        if group_by_page_size <= 0 or group_by_page_size > 200:
            raise ValueError("--group-by-page-size must be between 1 and 200.")
        scopes = sum(bool(value) for value in (topic_ids, field_ids, subfield_ids))
        if scopes != 1:
            raise ValueError("Provide exactly one of --topic-ids, --field-ids, --subfield-ids.")
        if taxonomy_scope == "topic" and not topic_ids:
            raise ValueError("--taxonomy-scope topic requires --topic-ids.")
        if taxonomy_scope == "field" and not field_ids:
            raise ValueError("--taxonomy-scope field requires --field-ids.")
        if taxonomy_scope == "subfield" and not subfield_ids:
            raise ValueError("--taxonomy-scope subfield requires --subfield-ids.")


class PaperIndexingTaskEnqueuer:
    """Create Redis paper indexing tasks without running embedding generation."""

    def __init__(
        self,
        *,
        paper_repository: PaperRepository | None = None,
        redis_adapter: RedisAdapter,
        qdrant_adapter: QdrantAdapter | None = None,
        queue_name: str = PAPER_INDEXING_QUEUE,
        papers_collection: str = PAPERS_COLLECTION,
    ) -> None:
        self.paper_repository = paper_repository
        self.redis_adapter = redis_adapter
        self.qdrant_adapter = qdrant_adapter
        self.queue_name = queue_name
        self.papers_collection = papers_collection

    def enqueue(
        self,
        *,
        date_from: date | None = None,
        date_to: date | None = None,
        paper_ids: list[int] | None = None,
        missing_only: bool = False,
        force_reindex: bool = False,
    ) -> dict[str, Any]:
        """Resolve paper ids and enqueue one Redis message per paper."""
        if date_from is None and date_to is None and not paper_ids:
            raise ValueError("Provide --paper-ids or at least one date boundary.")
        if date_from is not None and date_to is not None and date_from > date_to:
            raise ValueError("--date-from must be before or equal to --date-to.")
        if missing_only and self.qdrant_adapter is None:
            raise ValueError("--missing-only requires Qdrant connection settings.")

        candidate_ids = self._resolve_candidate_ids(
            date_from=date_from,
            date_to=date_to,
            paper_ids=paper_ids or [],
        )
        skipped_existing: list[int] = []
        enqueue_ids = candidate_ids

        if missing_only:
            enqueue_ids, skipped_existing = self._filter_missing(candidate_ids)

        for paper_id in enqueue_ids:
            self.redis_adapter.enqueue(
                self.queue_name,
                {
                    "task_type": "paper_indexing",
                    "paper_id": paper_id,
                    "force_reindex": force_reindex,
                },
            )

        return {
            "candidate_count": len(candidate_ids),
            "enqueued": len(enqueue_ids),
            "skipped_existing": len(skipped_existing),
            "queue": self.queue_name,
            "missing_only": missing_only,
            "force_reindex": force_reindex,
            "sample_enqueued_ids": enqueue_ids[:20],
            "sample_skipped_existing_ids": skipped_existing[:20],
        }

    def _resolve_candidate_ids(
        self,
        *,
        date_from: date | None,
        date_to: date | None,
        paper_ids: list[int],
    ) -> list[int]:
        ordered_ids: list[int] = []
        seen: set[int] = set()

        for paper_id in paper_ids:
            if paper_id not in seen:
                ordered_ids.append(paper_id)
                seen.add(paper_id)

        if date_from is not None or date_to is not None:
            if self.paper_repository is None:
                raise ValueError("Date-based enqueueing requires PostgreSQL access.")
            offset = 0
            while True:
                page_ids = self.paper_repository.list_ids_by_period(
                    date_from=date_from,
                    date_to=date_to,
                    limit=DEFAULT_PAGE_SIZE,
                    offset=offset,
                )
                if not page_ids:
                    break
                offset += len(page_ids)
                for paper_id in page_ids:
                    paper_id = int(paper_id)
                    if paper_id not in seen:
                        ordered_ids.append(paper_id)
                        seen.add(paper_id)

        return ordered_ids

    def _filter_missing(self, paper_ids: list[int]) -> tuple[list[int], list[int]]:
        if not paper_ids:
            return [], []
        missing_ids: list[int] = []
        existing_ids: list[int] = []

        for chunk in chunked(paper_ids, DEFAULT_QDRANT_RETRIEVE_SIZE):
            points = self.qdrant_adapter.retrieve(  # type: ignore[union-attr]
                self.papers_collection,
                chunk,
                with_vectors=False,
            )
            existing = {int(point.id) for point in points}
            for paper_id in chunk:
                if paper_id in existing:
                    existing_ids.append(paper_id)
                else:
                    missing_ids.append(paper_id)

        return missing_ids, existing_ids


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse task CLI command and command-specific arguments."""
    parser = argparse.ArgumentParser(description="Redis task enqueueing CLI utilities.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    enqueue_parser = subparsers.add_parser(
        "enqueue-indexing",
        help="Enqueue paper indexing tasks into Redis without running ML locally.",
    )
    enqueue_parser.add_argument(
        "--date-from",
        type=parse_iso_date,
        default=None,
        help="Publication date lower bound in YYYY-MM-DD format.",
    )
    enqueue_parser.add_argument(
        "--date-to",
        type=parse_iso_date,
        default=None,
        help="Publication date upper bound in YYYY-MM-DD format.",
    )
    enqueue_parser.add_argument(
        "--paper-ids",
        default=None,
        help="Path to a text file with paper ids separated by whitespace, comma, or newline.",
    )
    enqueue_parser.add_argument(
        "--missing-only",
        action="store_true",
        help="Enqueue only papers missing from Qdrant papers_content_v1.",
    )
    enqueue_parser.add_argument(
        "--force-reindex",
        action="store_true",
        help="Set force_reindex=true in enqueued messages.",
    )
    add_database_args(enqueue_parser)
    add_redis_args(enqueue_parser)
    add_qdrant_args(enqueue_parser)

    stats_parser = subparsers.add_parser(
        "enqueue-topic-stats",
        aliases=["enqueue-collect-topic-stats"],
        help="Enqueue high-priority OpenAlex collect-topic-stats worker tasks.",
    )
    stats_scope = stats_parser.add_mutually_exclusive_group(required=True)
    stats_scope.add_argument(
        "--topic-ids",
        default=None,
        help="Comma-separated local topic ids to collect with taxonomy-scope topic.",
    )
    stats_scope.add_argument(
        "--field-ids",
        default=None,
        help="Comma-separated local field ids to collect with taxonomy-scope field.",
    )
    stats_scope.add_argument(
        "--subfield-ids",
        default=None,
        help="Comma-separated local subfield ids to collect with taxonomy-scope subfield.",
    )
    stats_parser.add_argument(
        "--date-from",
        type=parse_iso_date,
        required=True,
        help="Publication period lower bound in YYYY-MM-DD format.",
    )
    stats_parser.add_argument(
        "--date-to",
        type=parse_iso_date,
        required=True,
        help="Publication period upper bound in YYYY-MM-DD format.",
    )
    stats_parser.add_argument(
        "--taxonomy-scope",
        choices=["topic", "field", "subfield"],
        default=None,
        help="OpenAlex collection strategy. Defaults to the provided id scope.",
    )
    stats_parser.add_argument(
        "--languages",
        default="en,ru",
        help="Comma-separated OpenAlex languages. Defaults to en,ru.",
    )
    stats_parser.add_argument(
        "--types",
        default="article",
        help="Comma-separated OpenAlex work types. Defaults to article.",
    )
    stats_parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="PostgreSQL upsert batch size used by worker. Defaults to 500.",
    )
    stats_parser.add_argument(
        "--task-batch-size",
        type=int,
        default=100,
        help="Maximum topic ids per Redis task for topic scope. Defaults to 100.",
    )
    stats_parser.add_argument(
        "--request-workers",
        type=int,
        default=8,
        help="Concurrent OpenAlex request workers used by worker. Defaults to 8.",
    )
    stats_parser.add_argument(
        "--rate-limit-rps",
        type=float,
        default=10.0,
        help="OpenAlex request rate used by worker. Defaults to 10.",
    )
    stats_parser.add_argument(
        "--max-retries",
        type=int,
        default=5,
        help="Retries for rate-limit/service/network errors. Defaults to 5.",
    )
    stats_parser.add_argument(
        "--group-by-page-size",
        type=int,
        default=200,
        help="OpenAlex group_by page size for field/subfield scope, max 200.",
    )
    stats_parser.add_argument(
        "--normalize-january-first",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "Estimate and remove January-1 artificial publication dates. "
            "Enabled by default; use --no-normalize-january-first to disable."
        ),
    )
    stats_parser.add_argument(
        "--primary-topic-only",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "Use primary_topic.id instead of topics.id filter. "
            "Enabled by default; use --no-primary-topic-only to disable."
        ),
    )
    stats_parser.add_argument(
        "--openalex-url",
        default=None,
        help="OpenAlex base URL. Defaults to worker OPENALEX_BASE_URL.",
    )
    stats_parser.add_argument(
        "--openalex-api-key",
        default=None,
        help="OpenAlex API key stored in task. Defaults to worker OPENALEX_API_KEY.",
    )
    stats_parser.add_argument(
        "--openalex-mailto",
        default=None,
        help="OpenAlex polite-pool email stored in task. Defaults to worker OPENALEX_MAILTO.",
    )
    stats_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview messages without modifying Redis.",
    )
    stats_parser.add_argument(
        "--no-progress",
        action="store_true",
        help="Disable enqueue progress bar and worker task progress.",
    )
    add_database_args(stats_parser)
    add_redis_args(stats_parser)

    bootstrap_parser = subparsers.add_parser(
        "enqueue-bootstrap-papers",
        help="Enqueue OpenAlex bootstrap_papers worker tasks without running OpenAlex.",
    )
    bootstrap_parser.add_argument(
        "--topic-ids",
        required=True,
        help="Comma-separated local topic ids to sample with primary_topic filters.",
    )
    bootstrap_parser.add_argument(
        "--date-from",
        type=parse_iso_date,
        required=True,
        help="Publication period lower bound in YYYY-MM-DD format.",
    )
    bootstrap_parser.add_argument(
        "--date-to",
        type=parse_iso_date,
        required=True,
        help="Publication period upper bound in YYYY-MM-DD format.",
    )
    bootstrap_parser.add_argument(
        "--target-count",
        type=int,
        default=20,
        help="Target sample papers per topic/month. Defaults to 20.",
    )
    bootstrap_parser.add_argument(
        "--task-batch-size",
        type=int,
        default=100,
        help="Maximum topic ids per Redis task. Defaults to 100.",
    )
    bootstrap_parser.add_argument("--languages", default="en,ru")
    bootstrap_parser.add_argument("--types", default="article")
    bootstrap_parser.add_argument("--batch-size", type=int, default=500)
    bootstrap_parser.add_argument("--request-workers", type=int, default=8)
    bootstrap_parser.add_argument("--db-workers", type=int, default=2)
    bootstrap_parser.add_argument("--rate-limit-rps", type=float, default=70.0)
    bootstrap_parser.add_argument("--seed", type=int, default=42)
    bootstrap_parser.add_argument("--per-page", type=int, default=100)
    bootstrap_parser.add_argument("--max-retries", type=int, default=5)
    bootstrap_parser.add_argument("--skip-existing", action="store_true")
    bootstrap_parser.add_argument(
        "--no-enqueue-indexing",
        action="store_true",
        help="Do not enqueue imported papers for indexing.",
    )
    bootstrap_parser.add_argument(
        "--no-enqueue-cluster-dynamics",
        action="store_true",
        help="Do not enqueue cluster dynamics through indexing workflow.",
    )
    bootstrap_parser.add_argument(
        "--primary-topic-only",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    bootstrap_parser.add_argument("--openalex-url", default=None)
    bootstrap_parser.add_argument("--openalex-api-key", default=None)
    bootstrap_parser.add_argument("--openalex-mailto", default=None)
    bootstrap_parser.add_argument("--dry-run", action="store_true")
    bootstrap_parser.add_argument(
        "--env-file",
        default=str(PROJECT_DIR / ".env"),
        help="Path to .env file. Defaults to project .env.",
    )
    add_redis_args(bootstrap_parser)

    cooldown_parser = subparsers.add_parser(
        "openalex-cooldown-status",
        help="Show OpenAlex worker cooldown status stored in Redis.",
    )
    cooldown_parser.add_argument(
        "--env-file",
        default=str(PROJECT_DIR / ".env"),
        help="Path to .env file. Defaults to project .env.",
    )
    add_redis_args(cooldown_parser)

    shutdown_parser = subparsers.add_parser(
        "request-worker-shutdown",
        help="Publish a soft shutdown request for one Redis ML worker.",
    )
    shutdown_parser.add_argument(
        "--reason",
        default="operator_request",
        help="Reason stored in the shutdown request payload.",
    )
    shutdown_parser.add_argument(
        "--ttl-seconds",
        type=int,
        default=3600,
        help="How long the shutdown request remains valid. Defaults to 3600.",
    )
    shutdown_parser.add_argument(
        "--env-file",
        default=str(PROJECT_DIR / ".env"),
        help="Path to .env file. Defaults to project .env.",
    )
    add_redis_args(shutdown_parser)

    cluster_parser = subparsers.add_parser(
        "enqueue-cluster-recompute",
        help="Enqueue static topic cluster recomputation tasks.",
    )
    add_topic_scope_args(cluster_parser)
    cluster_parser.add_argument(
        "--batch-size",
        type=int,
        default=50,
        help="Maximum topic ids per Redis message. Defaults to 50.",
    )
    cluster_parser.add_argument(
        "--force-summary",
        action="store_true",
        help="Regenerate LLM cluster summaries.",
    )
    cluster_parser.add_argument(
        "--cluster-workers",
        type=int,
        default=1,
        help="Worker-side parallel cluster recompute workers. Defaults to 1.",
    )
    cluster_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview messages without modifying Redis.",
    )
    cluster_parser.add_argument(
        "--no-progress",
        action="store_true",
        help="Disable enqueue progress bar.",
    )
    add_database_args(cluster_parser)
    add_redis_args(cluster_parser)

    dynamics_parser = subparsers.add_parser(
        "enqueue-cluster-dynamics",
        help="Enqueue time-sliced cluster dynamics recomputation tasks.",
    )
    dynamics_scope = dynamics_parser.add_mutually_exclusive_group(required=True)
    dynamics_scope.add_argument(
        "--cluster-ids",
        default=None,
        help="Comma-separated cluster ids, for example topic:1,topic:2.",
    )
    dynamics_scope.add_argument(
        "--topic-ids",
        default=None,
        help="Comma-separated topic ids to schedule as topic:{id} clusters.",
    )
    dynamics_scope.add_argument(
        "--field-id",
        type=int,
        default=None,
        help="Schedule dynamics for all topics belonging to one field.",
    )
    dynamics_scope.add_argument(
        "--all-topics",
        action="store_true",
        help="Schedule dynamics for all topics, optionally constrained by --limit/--offset.",
    )
    dynamics_parser.add_argument(
        "--date-from",
        type=parse_iso_date,
        required=True,
        help="Dynamics period lower bound in YYYY-MM-DD format.",
    )
    dynamics_parser.add_argument(
        "--date-to",
        type=parse_iso_date,
        required=True,
        help="Dynamics period upper bound in YYYY-MM-DD format.",
    )
    dynamics_parser.add_argument(
        "--granularity",
        choices=["month", "week"],
        default="month",
        help="Dynamics period granularity. Defaults to month.",
    )
    dynamics_parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum topics to resolve for --field-id/--all-topics.",
    )
    dynamics_parser.add_argument(
        "--offset",
        type=int,
        default=0,
        help="Topic offset for --field-id/--all-topics. Defaults to 0.",
    )
    dynamics_parser.add_argument(
        "--batch-size",
        type=int,
        default=50,
        help="Maximum cluster ids per Redis message. Defaults to 50.",
    )
    dynamics_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview messages without modifying Redis.",
    )
    dynamics_parser.add_argument(
        "--no-progress",
        action="store_true",
        help="Disable enqueue progress bar.",
    )
    add_database_args(dynamics_parser)
    add_redis_args(dynamics_parser)

    report_parser = subparsers.add_parser(
        "enqueue-topic-reports",
        help="Enqueue quarterly topic report generation tasks.",
    )
    report_scope = report_parser.add_mutually_exclusive_group(required=True)
    report_scope.add_argument(
        "--topic-ids",
        default=None,
        help="Comma-separated topic ids to schedule.",
    )
    report_scope.add_argument(
        "--field-id",
        type=int,
        default=None,
        help="Schedule reports for all topics belonging to one field.",
    )
    report_parser.add_argument(
        "--date-from",
        type=parse_iso_date,
        required=True,
        help="Report period lower bound in YYYY-MM-DD format.",
    )
    report_parser.add_argument(
        "--date-to",
        type=parse_iso_date,
        required=True,
        help="Report period upper bound in YYYY-MM-DD format.",
    )
    report_parser.add_argument(
        "--batch-size",
        type=int,
        default=50,
        help="Maximum topic-quarter requests per Redis message. Defaults to 50.",
    )
    report_parser.add_argument(
        "--force",
        action="store_true",
        help="Set force=true for report generation tasks.",
    )
    report_parser.add_argument(
        "--report-language",
        default="ru",
        help="Narrative language for generated report text. Defaults to ru.",
    )
    report_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview messages without modifying Redis.",
    )
    report_parser.add_argument(
        "--no-progress",
        action="store_true",
        help="Disable enqueue progress bar.",
    )
    add_database_args(report_parser)
    add_redis_args(report_parser)

    restore_parser = subparsers.add_parser(
        "restore-failed",
        help="Move failed worker messages back from queue:failed_tasks to their source queues.",
    )
    restore_parser.add_argument(
        "--failed-queue",
        default=FAILED_TASKS_QUEUE,
        help=f"Queue with failed task wrappers. Defaults to {FAILED_TASKS_QUEUE}.",
    )
    restore_parser.add_argument(
        "--target-queue",
        default=None,
        help="Override target queue for all restored messages. Defaults to wrapper source_queue.",
    )
    restore_parser.add_argument(
        "--task-type",
        default=None,
        help="Restore only failed messages whose inner message.task_type matches this value.",
    )
    restore_parser.add_argument(
        "--limit",
        type=int,
        default=1000,
        help="Maximum failed wrappers to inspect/process. Defaults to 1000.",
    )
    restore_parser.add_argument(
        "--split-paper-indexing-batch-size",
        "--split-batch-size",
        dest="split_paper_indexing_batch_size",
        type=int,
        default=128,
        help=(
            "Maximum paper_ids per restored paper_indexing task. "
            "Defaults to 128."
        ),
    )
    restore_parser.add_argument(
        "--no-split-paper-indexing",
        action="store_true",
        help="Restore paper_indexing messages exactly as stored in failed queue.",
    )
    restore_parser.add_argument(
        "--split-cluster-recompute-batch-size",
        "--split-cluster-batch-size",
        dest="split_cluster_recompute_batch_size",
        type=int,
        default=50,
        help=(
            "Maximum topic_ids per restored cluster recompute task. "
            "Defaults to 50."
        ),
    )
    restore_parser.add_argument(
        "--no-split-cluster-recompute",
        action="store_true",
        help="Restore cluster recompute messages exactly as stored in failed queue.",
    )
    restore_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview what would be restored without modifying Redis.",
    )
    add_database_args(restore_parser)
    add_redis_args(restore_parser)

    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Run the requested task CLI command."""
    args = parse_args(argv)
    load_dotenv(args.env_file)

    try:
        if args.command == "enqueue-indexing":
            payload = run_enqueue_indexing(args)
        elif args.command in {"enqueue-topic-stats", "enqueue-collect-topic-stats"}:
            payload = run_enqueue_topic_stats_collection(args)
        elif args.command == "enqueue-bootstrap-papers":
            payload = run_enqueue_bootstrap_papers(args)
        elif args.command == "openalex-cooldown-status":
            payload = run_openalex_cooldown_status(args)
        elif args.command == "request-worker-shutdown":
            payload = run_request_worker_shutdown(args)
        elif args.command == "enqueue-cluster-recompute":
            payload = run_enqueue_cluster_recompute(args)
        elif args.command == "enqueue-cluster-dynamics":
            payload = run_enqueue_cluster_dynamics(args)
        elif args.command == "enqueue-topic-reports":
            payload = run_enqueue_topic_reports(args)
        elif args.command == "restore-failed":
            payload = run_restore_failed(args)
        else:
            raise AssertionError(f"Unhandled command: {args.command}")
    except AppError as exc:
        print_json(exc.to_dict(), stream=sys.stderr)
        return 1
    except Exception as exc:
        print_json(
            {
                "error": {
                    "code": exc.__class__.__name__,
                    "message": str(exc),
                    "details": {},
                }
            },
            stream=sys.stderr,
        )
        return 1

    print_json(payload)
    return 0


def run_enqueue_indexing(args: argparse.Namespace) -> dict[str, Any]:
    """Enqueue paper indexing tasks into Redis."""
    redis_adapter = RedisAdapter(build_redis_client(args))
    qdrant_adapter = build_qdrant_adapter(args) if args.missing_only else None
    file_paper_ids = read_paper_ids_file(args.paper_ids) if args.paper_ids else []

    if args.date_from is not None or args.date_to is not None:
        database_url = args.database_url or Settings.from_env().database_url
        engine = create_db_engine(database_url, echo=False)
        SessionLocal = create_session_factory(engine, expire_on_commit=False)
        try:
            with SessionLocal() as session:
                enqueuer = PaperIndexingTaskEnqueuer(
                    paper_repository=PaperRepository(session),
                    redis_adapter=redis_adapter,
                    qdrant_adapter=qdrant_adapter,
                )
                result = enqueuer.enqueue(
                    date_from=args.date_from,
                    date_to=args.date_to,
                    paper_ids=file_paper_ids,
                    missing_only=args.missing_only,
                    force_reindex=args.force_reindex,
                )
        finally:
            engine.dispose()
    else:
        enqueuer = PaperIndexingTaskEnqueuer(
            redis_adapter=redis_adapter,
            qdrant_adapter=qdrant_adapter,
        )
        result = enqueuer.enqueue(
            paper_ids=file_paper_ids,
            missing_only=args.missing_only,
            force_reindex=args.force_reindex,
        )

    return {
        "command": "enqueue-indexing",
        "date_from": args.date_from,
        "date_to": args.date_to,
        "paper_ids_file": args.paper_ids,
        "result": result,
    }


def run_enqueue_topic_stats_collection(args: argparse.Namespace) -> dict[str, Any]:
    """Enqueue OpenAlex topic publication stats collection tasks."""
    taxonomy_scope = resolve_topic_stats_taxonomy_scope(args)
    topic_ids = parse_int_csv_arg(args.topic_ids) if args.topic_ids else None
    field_ids = parse_int_csv_arg(args.field_ids) if args.field_ids else None
    subfield_ids = parse_int_csv_arg(args.subfield_ids) if args.subfield_ids else None
    redis_adapter = None if args.dry_run else RedisAdapter(build_redis_client(args))
    result = OpenAlexTopicStatsTaskEnqueuer(
        redis_adapter=redis_adapter,
    ).enqueue(
        date_from=args.date_from,
        date_to=args.date_to,
        topic_ids=topic_ids,
        field_ids=field_ids,
        subfield_ids=subfield_ids,
        taxonomy_scope=taxonomy_scope,
        languages=parse_csv_arg(args.languages),
        types=parse_csv_arg(args.types),
        batch_size=args.batch_size,
        task_batch_size=args.task_batch_size,
        request_workers=args.request_workers,
        rate_limit_rps=args.rate_limit_rps,
        max_retries=args.max_retries,
        group_by_page_size=args.group_by_page_size,
        normalize_january_first=bool(args.normalize_january_first),
        primary_topic_only=bool(args.primary_topic_only),
        openalex_url=args.openalex_url,
        openalex_api_key=args.openalex_api_key,
        openalex_mailto=args.openalex_mailto,
        dry_run=bool(args.dry_run),
        show_progress=not args.no_progress,
    )
    return {
        "command": "enqueue-topic-stats",
        "result": result,
    }


def run_enqueue_bootstrap_papers(args: argparse.Namespace) -> dict[str, Any]:
    """Enqueue OpenAlex paper bootstrap tasks into Redis."""
    if args.date_from > args.date_to:
        raise ValueError("--date-from must be before or equal to --date-to.")
    if args.target_count < 0:
        raise ValueError("--target-count must be non-negative.")
    if args.task_batch_size <= 0:
        raise ValueError("--task-batch-size must be a positive integer.")
    topic_ids = parse_int_csv_arg(args.topic_ids)
    messages: list[dict[str, Any]] = []
    for chunk in chunked(topic_ids, args.task_batch_size):
        messages.append(
            {
                "task_type": "bootstrap_papers",
                "date_from": args.date_from.isoformat(),
                "date_to": args.date_to.isoformat(),
                "topic_ids": chunk,
                "source_topic_ids": chunk,
                "target_count": args.target_count,
                "target_scope": "month",
                "target_unit": "topic",
                "languages": parse_csv_arg(args.languages),
                "types": parse_csv_arg(args.types),
                "batch_size": args.batch_size,
                "request_workers": args.request_workers,
                "db_workers": args.db_workers,
                "rate_limit_rps": args.rate_limit_rps,
                "seed": args.seed,
                "per_page": args.per_page,
                "max_retries": args.max_retries,
                "skip_existing": bool(args.skip_existing),
                "enqueue_indexing": not bool(args.no_enqueue_indexing),
                "enqueue_cluster_dynamics": not bool(args.no_enqueue_cluster_dynamics),
                "primary_topic_only": bool(args.primary_topic_only),
                "workflow_date_from": args.date_from.isoformat(),
                "workflow_date_to": args.date_to.isoformat(),
                "workflow_granularity": "month",
                "show_progress": False,
                **({"openalex_url": args.openalex_url} if args.openalex_url else {}),
                **({"openalex_api_key": args.openalex_api_key} if args.openalex_api_key else {}),
                **({"openalex_mailto": args.openalex_mailto} if args.openalex_mailto else {}),
            }
        )
    if not args.dry_run:
        redis_adapter = RedisAdapter(build_redis_client(args))
        for message in messages:
            redis_adapter.enqueue(OPENALEX_BOOTSTRAP_PAPERS_QUEUE, message)
    return {
        "command": "enqueue-bootstrap-papers",
        "queue": OPENALEX_BOOTSTRAP_PAPERS_QUEUE,
        "dry_run": bool(args.dry_run),
        "topic_count": len(set(topic_ids)),
        "messages": len(messages),
        "target_count": args.target_count,
        "sample": messages[:3],
    }


def run_openalex_cooldown_status(args: argparse.Namespace) -> dict[str, Any]:
    """Return OpenAlex cooldown key status from Redis."""
    redis_adapter = RedisAdapter(build_redis_client(args))
    exists = redis_adapter.exists(OPENALEX_COOLDOWN_KEY)
    return {
        "command": "openalex-cooldown-status",
        "key": OPENALEX_COOLDOWN_KEY,
        "exists": exists,
        "ttl_seconds": redis_adapter.ttl(OPENALEX_COOLDOWN_KEY) if exists else -2,
        "payload": redis_adapter.get_json(OPENALEX_COOLDOWN_KEY) if exists else None,
    }


def run_request_worker_shutdown(args: argparse.Namespace) -> dict[str, Any]:
    """Publish a soft shutdown request consumed by one worker."""
    if args.ttl_seconds <= 0:
        raise ValueError("--ttl-seconds must be a positive integer.")
    payload = {
        "reason": str(args.reason),
        "requested_at": datetime.now(timezone.utc).isoformat(),
    }
    redis_adapter = RedisAdapter(build_redis_client(args))
    redis_adapter.set_json(
        ML_WORKER_SHUTDOWN_KEY,
        payload,
        ttl_seconds=int(args.ttl_seconds),
    )
    return {
        "command": "request-worker-shutdown",
        "key": ML_WORKER_SHUTDOWN_KEY,
        "ttl_seconds": int(args.ttl_seconds),
        "payload": payload,
    }


def run_enqueue_cluster_recompute(args: argparse.Namespace) -> dict[str, Any]:
    """Enqueue static topic cluster recomputation tasks into Redis."""
    if args.batch_size <= 0:
        raise ValueError("--batch-size must be a positive integer.")
    if args.cluster_workers <= 0:
        raise ValueError("--cluster-workers must be a positive integer.")
    topic_ids = resolve_topic_ids_for_scope(args)
    redis_adapter = None if args.dry_run else RedisAdapter(build_redis_client(args))
    result = ClusterRecomputeTaskEnqueuer(redis_adapter=redis_adapter).enqueue(
        topic_ids=topic_ids,
        batch_size=args.batch_size,
        force_summary=bool(args.force_summary),
        cluster_workers=args.cluster_workers,
        dry_run=bool(args.dry_run),
        show_progress=not args.no_progress,
    )
    return {
        "command": "enqueue-cluster-recompute",
        "field_id": getattr(args, "field_id", None),
        "all_topics": bool(getattr(args, "all_topics", False)),
        "topic_ids": topic_ids[:20],
        "topic_count": len(topic_ids),
        "result": result,
    }


def run_enqueue_cluster_dynamics(args: argparse.Namespace) -> dict[str, Any]:
    """Enqueue time-sliced cluster dynamics recomputation tasks into Redis."""
    if args.batch_size <= 0:
        raise ValueError("--batch-size must be a positive integer.")
    if args.date_from > args.date_to:
        raise ValueError("--date-from must be before or equal to --date-to.")
    if args.cluster_ids:
        cluster_ids = parse_csv_arg(args.cluster_ids)
    else:
        cluster_ids = [
            f"topic:{topic_id}"
            for topic_id in resolve_topic_ids_for_scope(args)
        ]
    redis_adapter = None if args.dry_run else RedisAdapter(build_redis_client(args))
    result = ClusterDynamicsTaskEnqueuer(redis_adapter=redis_adapter).enqueue(
        cluster_ids=cluster_ids,
        date_from=args.date_from,
        date_to=args.date_to,
        granularity=args.granularity,
        batch_size=args.batch_size,
        dry_run=bool(args.dry_run),
        show_progress=not args.no_progress,
    )
    return {
        "command": "enqueue-cluster-dynamics",
        "field_id": getattr(args, "field_id", None),
        "all_topics": bool(getattr(args, "all_topics", False)),
        "cluster_ids": cluster_ids[:20],
        "cluster_count": len(cluster_ids),
        "date_from": args.date_from,
        "date_to": args.date_to,
        "granularity": args.granularity,
        "result": result,
    }


def run_enqueue_topic_reports(args: argparse.Namespace) -> dict[str, Any]:
    """Enqueue quarterly topic report generation tasks into Redis."""
    if args.batch_size <= 0:
        raise ValueError("--batch-size must be a positive integer.")
    if args.date_from > args.date_to:
        raise ValueError("--date-from must be before or equal to --date-to.")
    topic_ids = resolve_report_topic_ids(args)
    redis_adapter = None if args.dry_run else RedisAdapter(build_redis_client(args))
    result = TopicQuarterReportTaskEnqueuer(
        redis_adapter=redis_adapter,
    ).enqueue(
        topic_ids=topic_ids,
        date_from=args.date_from,
        date_to=args.date_to,
        batch_size=args.batch_size,
        force=bool(args.force),
        report_language=args.report_language,
        dry_run=bool(args.dry_run),
        show_progress=not args.no_progress,
    )
    return {
        "command": "enqueue-topic-reports",
        "field_id": args.field_id,
        "topic_ids": topic_ids[:20],
        "topic_count": len(topic_ids),
        "date_from": args.date_from,
        "date_to": args.date_to,
        "result": result,
    }


def resolve_report_topic_ids(args: argparse.Namespace) -> list[int]:
    """Resolve topic ids for topic report task enqueueing."""
    return resolve_topic_ids_for_scope(args)


def resolve_topic_stats_taxonomy_scope(args: argparse.Namespace) -> str:
    """Infer collect-topic-stats taxonomy scope from CLI arguments."""
    if args.taxonomy_scope:
        return str(args.taxonomy_scope)
    if args.field_ids:
        return "field"
    if args.subfield_ids:
        return "subfield"
    return "topic"


def resolve_topic_ids_for_scope(args: argparse.Namespace) -> list[int]:
    """Resolve topic ids from CLI scope arguments."""
    if getattr(args, "topic_ids", None):
        topic_ids = parse_int_csv_arg(args.topic_ids)
        if not topic_ids:
            raise ValueError("--topic-ids must contain at least one id.")
        return topic_ids

    field_id = getattr(args, "field_id", None)
    all_topics = bool(getattr(args, "all_topics", False))
    if field_id is not None and field_id <= 0:
        raise ValueError("--field-id must be a positive integer.")
    if not all_topics and field_id is None:
        raise ValueError("Provide --topic-ids, --field-id, or --all-topics.")

    limit = getattr(args, "limit", None)
    offset = int(getattr(args, "offset", 0) or 0)
    if limit is not None and limit < 0:
        raise ValueError("--limit must be non-negative.")
    if offset < 0:
        raise ValueError("--offset must be non-negative.")
    database_url = args.database_url or Settings.from_env().database_url
    engine = create_db_engine(database_url, echo=False)
    SessionLocal = create_session_factory(engine, expire_on_commit=False)
    try:
        with SessionLocal() as session:
            topics = TaxonomyRepository(session).list_topics_for_stats(
                field_ids=[field_id] if field_id is not None else None,
                limit=limit,
                offset=offset,
            )
            return [int(topic.id) for topic in topics]
    finally:
        engine.dispose()


class FailedTaskRestorer:
    """Restore failed worker task wrappers to executable Redis queues."""

    def __init__(
        self,
        redis_adapter: RedisAdapter,
        *,
        failed_queue: str = FAILED_TASKS_QUEUE,
    ) -> None:
        self.redis_adapter = redis_adapter
        self.failed_queue = failed_queue

    def restore(
        self,
        *,
        limit: int,
        target_queue: str | None = None,
        task_type: str | None = None,
        dry_run: bool = False,
        split_paper_indexing: bool = True,
        split_paper_indexing_batch_size: int = 128,
        split_cluster_recompute: bool = True,
        split_cluster_recompute_batch_size: int = 50,
    ) -> dict[str, Any]:
        """Restore failed tasks and return operation counters."""
        if limit <= 0:
            raise ValueError("--limit must be a positive integer.")
        if split_paper_indexing and split_paper_indexing_batch_size <= 0:
            raise ValueError("--split-paper-indexing-batch-size must be positive.")
        if split_cluster_recompute and split_cluster_recompute_batch_size <= 0:
            raise ValueError("--split-cluster-recompute-batch-size must be positive.")

        if dry_run:
            wrappers = self.redis_adapter.peek_queue(self.failed_queue, limit)
            return self._preview(
                wrappers,
                target_queue=target_queue,
                task_type=task_type,
                split_paper_indexing=split_paper_indexing,
                split_paper_indexing_batch_size=split_paper_indexing_batch_size,
                split_cluster_recompute=split_cluster_recompute,
                split_cluster_recompute_batch_size=split_cluster_recompute_batch_size,
            )

        restored = 0
        processed_failed_wrappers = 0
        skipped = 0
        invalid = 0
        restored_by_queue: dict[str, int] = {}
        restored_chunk_sizes: list[int] = []
        skipped_samples: list[dict[str, Any]] = []
        invalid_samples: list[dict[str, Any]] = []

        for _ in range(limit):
            wrapper = self.redis_adapter.dequeue_nowait(self.failed_queue)
            if wrapper is None:
                break

            extracted = self._extract_restore_target(
                wrapper,
                target_queue=target_queue,
            )
            if extracted is None:
                invalid += 1
                invalid_samples.append(self._sample(wrapper))
                continue

            queue_name, message = extracted
            if task_type and message.get("task_type") != task_type:
                skipped += 1
                skipped_samples.append(self._sample(wrapper))
                self.redis_adapter.enqueue(self.failed_queue, wrapper)
                continue

            messages = self._restore_messages(
                message,
                split_paper_indexing=split_paper_indexing,
                split_paper_indexing_batch_size=split_paper_indexing_batch_size,
                split_cluster_recompute=split_cluster_recompute,
                split_cluster_recompute_batch_size=split_cluster_recompute_batch_size,
            )
            try:
                for restore_message in messages:
                    self.redis_adapter.enqueue(queue_name, restore_message)
            except Exception:
                self.redis_adapter.enqueue(self.failed_queue, wrapper)
                raise

            processed_failed_wrappers += 1
            restored += len(messages)
            restored_by_queue[queue_name] = (
                restored_by_queue.get(queue_name, 0) + len(messages)
            )
            restored_chunk_sizes.extend(
                self._message_chunk_size(restore_message)
                for restore_message in messages
            )

        return {
            "failed_queue": self.failed_queue,
            "dry_run": False,
            "limit": limit,
            "processed_failed_wrappers": processed_failed_wrappers,
            "restored": restored,
            "skipped": skipped,
            "invalid": invalid,
            "restored_by_queue": restored_by_queue,
            "sample_chunk_sizes": restored_chunk_sizes[:20],
            "split_paper_indexing": split_paper_indexing,
            "split_paper_indexing_batch_size": (
                split_paper_indexing_batch_size if split_paper_indexing else None
            ),
            "split_cluster_recompute": split_cluster_recompute,
            "split_cluster_recompute_batch_size": (
                split_cluster_recompute_batch_size
                if split_cluster_recompute
                else None
            ),
            "skipped_samples": skipped_samples[:20],
            "invalid_samples": invalid_samples[:20],
        }

    def _preview(
        self,
        wrappers: list[dict[str, Any]],
        *,
        target_queue: str | None,
        task_type: str | None,
        split_paper_indexing: bool,
        split_paper_indexing_batch_size: int,
        split_cluster_recompute: bool,
        split_cluster_recompute_batch_size: int,
    ) -> dict[str, Any]:
        restorable = 0
        output_messages = 0
        skipped = 0
        invalid = 0
        by_queue: dict[str, int] = {}
        chunk_sizes: list[int] = []
        samples: list[dict[str, Any]] = []

        for wrapper in wrappers:
            extracted = self._extract_restore_target(
                wrapper,
                target_queue=target_queue,
            )
            if extracted is None:
                invalid += 1
                continue
            queue_name, message = extracted
            if task_type and message.get("task_type") != task_type:
                skipped += 1
                continue
            restorable += 1
            messages = self._restore_messages(
                message,
                split_paper_indexing=split_paper_indexing,
                split_paper_indexing_batch_size=split_paper_indexing_batch_size,
                split_cluster_recompute=split_cluster_recompute,
                split_cluster_recompute_batch_size=split_cluster_recompute_batch_size,
            )
            output_messages += len(messages)
            by_queue[queue_name] = by_queue.get(queue_name, 0) + len(messages)
            chunk_sizes.extend(
                self._message_chunk_size(restore_message)
                for restore_message in messages
            )
            samples.append(
                {
                    "target_queue": queue_name,
                    "task_type": message.get("task_type"),
                    "output_messages": len(messages),
                    "chunk_sizes": [
                        self._message_chunk_size(restore_message)
                        for restore_message in messages[:20]
                    ],
                    "message": messages[0] if len(messages) == 1 else None,
                }
            )

        return {
            "failed_queue": self.failed_queue,
            "dry_run": True,
            "inspected": len(wrappers),
            "restorable": restorable,
            "output_messages": output_messages,
            "skipped": skipped,
            "invalid": invalid,
            "restorable_by_queue": by_queue,
            "sample_chunk_sizes": chunk_sizes[:20],
            "split_paper_indexing": split_paper_indexing,
            "split_paper_indexing_batch_size": (
                split_paper_indexing_batch_size if split_paper_indexing else None
            ),
            "split_cluster_recompute": split_cluster_recompute,
            "split_cluster_recompute_batch_size": (
                split_cluster_recompute_batch_size
                if split_cluster_recompute
                else None
            ),
            "samples": samples[:20],
        }

    def _extract_restore_target(
        self,
        wrapper: dict[str, Any],
        *,
        target_queue: str | None,
    ) -> tuple[str, dict[str, Any]] | None:
        message = wrapper.get("message")
        if not isinstance(message, dict):
            return None

        queue_name = target_queue or wrapper.get("source_queue")
        if not isinstance(queue_name, str) or not queue_name.startswith("queue:"):
            return None
        return queue_name, message

    def _restore_messages(
        self,
        message: dict[str, Any],
        *,
        split_paper_indexing: bool,
        split_paper_indexing_batch_size: int,
        split_cluster_recompute: bool,
        split_cluster_recompute_batch_size: int,
    ) -> list[dict[str, Any]]:
        if split_paper_indexing and message.get("task_type") == "paper_indexing":
            paper_ids = message.get("paper_ids")
            if (
                isinstance(paper_ids, list)
                and len(paper_ids) > split_paper_indexing_batch_size
            ):
                force_reindex = bool(message.get("force_reindex", False))
                return [
                    {
                        "task_type": "paper_indexing",
                        "paper_ids": chunk,
                        "force_reindex": force_reindex,
                    }
                    for chunk in chunked(paper_ids, split_paper_indexing_batch_size)
                ]

        if (
            split_cluster_recompute
            and message.get("task_type") in {"cluster_recompute", "recompute_topic_clusters"}
        ):
            topic_ids = message.get("topic_ids")
            if (
                isinstance(topic_ids, list)
                and len(topic_ids) > split_cluster_recompute_batch_size
            ):
                force_summary = bool(message.get("force_summary", False))
                cluster_workers = message.get("cluster_workers")
                return [
                    build_cluster_recompute_message(
                        chunk,
                        force_summary=force_summary,
                        cluster_workers=(
                            int(cluster_workers) if cluster_workers is not None else None
                        ),
                        workflow_options=message,
                    )
                    for chunk in chunked(topic_ids, split_cluster_recompute_batch_size)
                ]

        return [message]

    def _message_chunk_size(self, message: dict[str, Any]) -> int:
        paper_ids = message.get("paper_ids")
        if isinstance(paper_ids, list):
            return len(paper_ids)
        topic_ids = message.get("topic_ids")
        if isinstance(topic_ids, list):
            return len(topic_ids)
        return 1

    def _sample(self, wrapper: dict[str, Any]) -> dict[str, Any]:
        return {
            "source_queue": wrapper.get("source_queue"),
            "task_type": (
                wrapper.get("message", {}).get("task_type")
                if isinstance(wrapper.get("message"), dict)
                else None
            ),
            "error": wrapper.get("error"),
        }


def run_restore_failed(args: argparse.Namespace) -> dict[str, Any]:
    """Restore failed ML task wrappers back to Redis work queues."""
    redis_adapter = RedisAdapter(build_redis_client(args))
    result = FailedTaskRestorer(
        redis_adapter,
        failed_queue=args.failed_queue,
    ).restore(
        limit=args.limit,
        target_queue=args.target_queue,
        task_type=args.task_type,
        dry_run=args.dry_run,
        split_paper_indexing=not args.no_split_paper_indexing,
        split_paper_indexing_batch_size=args.split_paper_indexing_batch_size,
        split_cluster_recompute=not args.no_split_cluster_recompute,
        split_cluster_recompute_batch_size=args.split_cluster_recompute_batch_size,
    )

    return {
        "command": "restore-failed",
        "result": result,
    }


def add_database_args(parser: argparse.ArgumentParser) -> None:
    """Add shared environment and database connection arguments."""
    parser.add_argument(
        "--env-file",
        default=str(PROJECT_DIR / ".env"),
        help="Path to .env file. Defaults to project .env.",
    )
    parser.add_argument(
        "--database-url",
        default=None,
        help="SQLAlchemy database URL. Defaults to DATABASE_URL or POSTGRES_* envs.",
    )


def add_topic_scope_args(parser: argparse.ArgumentParser) -> None:
    """Add shared topic scope arguments for cluster task enqueueing."""
    scope = parser.add_mutually_exclusive_group(required=True)
    scope.add_argument(
        "--topic-ids",
        default=None,
        help="Comma-separated topic ids to schedule.",
    )
    scope.add_argument(
        "--field-id",
        type=int,
        default=None,
        help="Schedule all topics belonging to one field.",
    )
    scope.add_argument(
        "--all-topics",
        action="store_true",
        help="Schedule all topics, optionally constrained by --limit/--offset.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum topics to resolve for --field-id/--all-topics.",
    )
    parser.add_argument(
        "--offset",
        type=int,
        default=0,
        help="Topic offset for --field-id/--all-topics. Defaults to 0.",
    )


def add_redis_args(parser: argparse.ArgumentParser) -> None:
    """Add Redis connection arguments."""
    parser.add_argument(
        "--redis-url",
        default=None,
        help="Redis URL. Defaults to REDIS_URL when set.",
    )
    parser.add_argument(
        "--redis-host",
        default=None,
        help="Redis host. Defaults to REDIS_HOST or localhost.",
    )
    parser.add_argument(
        "--redis-port",
        type=int,
        default=None,
        help="Redis port. Defaults to REDIS_PORT or 6379.",
    )
    parser.add_argument(
        "--redis-db",
        type=int,
        default=None,
        help="Redis database number. Defaults to REDIS_DB or 0.",
    )


def add_qdrant_args(parser: argparse.ArgumentParser) -> None:
    """Add Qdrant arguments used only by --missing-only."""
    parser.add_argument(
        "--qdrant-url",
        default=None,
        help="Qdrant URL. Defaults to QDRANT_URL when set.",
    )
    parser.add_argument(
        "--qdrant-host",
        default=None,
        help="Qdrant host. Defaults to QDRANT_HOST or localhost.",
    )
    parser.add_argument(
        "--qdrant-port",
        type=int,
        default=None,
        help="Qdrant port. Defaults to QDRANT_PORT or 6333.",
    )
    parser.add_argument(
        "--qdrant-api-key",
        default=None,
        help="Qdrant API key. Defaults to QDRANT_API_KEY when set.",
    )


def read_paper_ids_file(path: str) -> list[int]:
    """Read paper ids from a text file and preserve first occurrence order."""
    raw_text = Path(path).read_text(encoding="utf-8")
    values = raw_text.replace(",", " ").split()
    ids: list[int] = []
    seen: set[int] = set()
    for value in values:
        paper_id = int(value)
        if paper_id <= 0:
            raise ValueError(f"Paper ids must be positive integers, got {paper_id}.")
        if paper_id not in seen:
            ids.append(paper_id)
            seen.add(paper_id)
    return ids


def parse_int_csv_arg(value: str) -> list[int]:
    """Parse comma-separated integer ids preserving first occurrence order."""
    ids: list[int] = []
    seen: set[int] = set()
    for item in value.split(","):
        clean = item.strip()
        if not clean:
            continue
        item_id = int(clean)
        if item_id <= 0:
            raise ValueError(f"Ids must be positive integers, got {item_id}.")
        if item_id not in seen:
            ids.append(item_id)
            seen.add(item_id)
    return ids


def parse_csv_arg(value: str) -> list[str]:
    """Parse comma-separated strings preserving first occurrence order."""
    values: list[str] = []
    seen: set[str] = set()
    for item in value.split(","):
        clean = item.strip()
        if not clean or clean in seen:
            continue
        values.append(clean)
        seen.add(clean)
    return values


def build_redis_client(args: argparse.Namespace) -> Any:
    """Build a redis-py client from CLI arguments or environment variables."""
    try:
        from redis import Redis
    except ImportError as exc:
        raise RuntimeError(
            "redis package is not installed. Install dependencies from requirements.txt."
        ) from exc

    redis_url = args.redis_url or os.getenv("REDIS_URL")
    if redis_url:
        return Redis.from_url(redis_url)
    return Redis(
        host=args.redis_host or os.getenv("REDIS_HOST") or "localhost",
        port=args.redis_port or _optional_int_env("REDIS_PORT") or 6379,
        db=args.redis_db if args.redis_db is not None else _optional_int_env("REDIS_DB") or 0,
        password=os.getenv("REDIS_PASSWORD") or None,
    )


def build_qdrant_adapter(args: argparse.Namespace) -> QdrantAdapter:
    """Build QdrantAdapter from CLI arguments or environment variables."""
    qdrant_url = args.qdrant_url or os.getenv("QDRANT_URL")
    api_key = args.qdrant_api_key or os.getenv("QDRANT_API_KEY")
    if qdrant_url:
        return QdrantAdapter(url=qdrant_url, api_key=api_key)

    return QdrantAdapter(
        host=args.qdrant_host or os.getenv("QDRANT_HOST") or "localhost",
        port=args.qdrant_port or _optional_int_env("QDRANT_PORT") or 6333,
        api_key=api_key,
    )


def parse_iso_date(value: str) -> date:
    """Parse an ISO date for argparse."""
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Expected YYYY-MM-DD date, got {value!r}."
        ) from exc


def chunked(values: list[int], size: int) -> list[list[int]]:
    """Split integer values into fixed-size chunks."""
    return [values[index : index + size] for index in range(0, len(values), size)]


def chunked_dicts(values: list[dict[str, Any]], size: int) -> list[list[dict[str, Any]]]:
    """Split dictionary values into fixed-size chunks."""
    return [values[index : index + size] for index in range(0, len(values), size)]


def chunked_strings(values: list[str], size: int) -> list[list[str]]:
    """Split string values into fixed-size chunks."""
    return [values[index : index + size] for index in range(0, len(values), size)]


def _optional_int_env(name: str) -> int | None:
    value = os.getenv(name)
    if value is None or not value.strip():
        return None
    return int(value)


def print_json(payload: dict[str, Any], *, stream: Any = sys.stdout) -> None:
    """Print JSON with stable UTF-8 output."""
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str), file=stream)


if __name__ == "__main__":
    raise SystemExit(main())
