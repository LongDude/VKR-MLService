from __future__ import annotations

import calendar
import logging
import threading
import time
from collections import deque
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from email.utils import parsedate_to_datetime
from typing import Callable, Iterator, Literal

import httpx
from tqdm.auto import tqdm

from adapters.openalex_adapter import OpenAlexAdapter
from core.exceptions import (
    AppError,
    ExternalServiceRateLimitError,
    ExternalServiceUnavailableError,
)
from ml.services.openalex_rate_limiter import SyncRateLimiter
from dto.external import OpenAlexSearchFiltersDTO
from models import Topic
from repositories.openalex_topic_stats import (
    OpenAlexTopicMonthlyCount,
    OpenAlexTopicStatsRepository,
)
from repositories.openalex_yearly_topic_stats import (
    OpenAlexTopicYearlyArtificialEstimate,
    OpenAlexYearlyTopicStatsRepository,
)
from repositories.taxonomy import TaxonomyRepository


logger = logging.getLogger(__name__)
TaxonomyStatsScope = Literal["topic", "field", "subfield"]


@dataclass(frozen=True)
class MonthPeriod:
    """Calendar month boundaries used by OpenAlex monthly topic statistics."""

    period_start: date
    period_end: date


@dataclass
class OpenAlexTopicStatsCollectionResult:
    """Summary of OpenAlex topic/month statistics collection."""

    date_from: date
    date_to: date
    taxonomy_scope: str = "topic"
    total_topics: int = 0
    topics_with_openalex_id: int = 0
    skipped_topics_without_openalex_id: int = 0
    periods: int = 0
    planned_requests: int = 0
    openalex_requests: int = 0
    yearly_artificial_estimates: int = 0
    collected: int = 0
    created: int = 0
    updated: int = 0
    failed: int = 0
    deferred: bool = False
    retry_after_seconds: float | None = None
    pending_tasks: list[dict[str, object]] = field(default_factory=list)
    dry_run: bool = False
    errors: list[dict[str, object]] = field(default_factory=list)


@dataclass(frozen=True)
class _TopicInfo:
    id: int
    openalex_id: str
    name: str


@dataclass(frozen=True)
class _FetchResult:
    item: OpenAlexTopicMonthlyCount | None
    openalex_requests: int
    errors: list[dict[str, object]]
    yearly_item: OpenAlexTopicYearlyArtificialEstimate | None = None


@dataclass(frozen=True)
class _GroupedFetchResult:
    items: list[OpenAlexTopicMonthlyCount]
    yearly_items: list[OpenAlexTopicYearlyArtificialEstimate]
    openalex_requests: int
    errors: list[dict[str, object]]


@dataclass(frozen=True)
class _JanuaryNormalizationResult:
    expected_real_count: int
    estimated_artificial_count: int

class OpenAlexTopicStatsCollector:
    """Collect topic/month OpenAlex work counts and upsert them into PostgreSQL."""

    def __init__(
        self,
        *,
        taxonomy_repository: TaxonomyRepository,
        stats_repository: OpenAlexTopicStatsRepository,
        openalex_adapter_factory: Callable[[], OpenAlexAdapter],
        yearly_stats_repository: OpenAlexYearlyTopicStatsRepository | None = None,
        request_workers: int = 8,
        rate_limiter: SyncRateLimiter | None = None,
        max_retries: int = 5,
        rate_limit_defer_after_seconds: float = 120.0,
        primary_topic_only: bool = False,
        commit_each_batch: bool = True,
    ) -> None:
        self.taxonomy_repository = taxonomy_repository
        self.stats_repository = stats_repository
        self.yearly_stats_repository = yearly_stats_repository
        self.openalex_adapter_factory = openalex_adapter_factory
        self.request_workers = max(1, request_workers)
        self.rate_limiter = rate_limiter or SyncRateLimiter(70)
        self.max_retries = max(0, max_retries)
        self.rate_limit_defer_after_seconds = max(0.0, rate_limit_defer_after_seconds)
        self.primary_topic_only = primary_topic_only
        self.commit_each_batch = commit_each_batch
        self._thread_local = threading.local()
        self._adapters: list[OpenAlexAdapter] = []
        self._adapters_lock = threading.Lock()

    def collect_and_store(
        self,
        *,
        date_from: date,
        date_to: date,
        topic_ids: list[int] | None = None,
        field_ids: list[int] | None = None,
        subfield_ids: list[int] | None = None,
        taxonomy_scope: TaxonomyStatsScope = "topic",
        limit: int | None = None,
        offset: int = 0,
        languages: list[str] | None = None,
        types: list[str] | None = None,
        batch_size: int = 500,
        group_by_page_size: int = 200,
        normalize_january_first: bool = False,
        dry_run: bool = False,
        show_progress: bool = True,
    ) -> OpenAlexTopicStatsCollectionResult:
        """Collect OpenAlex monthly counts for DB topics and upsert stats.

        ``date_from`` and ``date_to`` are expanded to full calendar months because
        ``openalex_montly_topic_stats`` stores one row per month.
        """
        if date_from > date_to:
            raise ValueError("date_from must be before or equal to date_to")
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        if taxonomy_scope not in {"topic", "field", "subfield"}:
            raise ValueError("taxonomy_scope must be one of: topic, field, subfield")
        if normalize_january_first and self.yearly_stats_repository is None:
            raise ValueError("Yearly stats repository is required for January normalization")

        periods = list(iter_month_periods(date_from, date_to))
        group_filter_parts, scope_errors = self._group_filter_parts(
            taxonomy_scope,
            field_ids=field_ids,
            subfield_ids=subfield_ids,
        )
        topics, missing_topic_ids = self._load_topics(
            topic_ids,
            limit,
            offset,
            taxonomy_scope=taxonomy_scope,
            field_ids=field_ids,
            subfield_ids=subfield_ids,
        )
        topic_infos, skipped_without_openalex = self._topic_infos(topics)
        language_filter = _or_filter_value(languages or ["en", "ru"])
        type_filter = _or_filter_value(types or ["article"])
        january_period_count = (
            sum(1 for period in periods if period.period_start.month == 1)
            if normalize_january_first
            else 0
        )
        planned_requests = (
            len(topic_infos) * (len(periods) + january_period_count)
            if taxonomy_scope == "topic"
            else len(periods) + january_period_count
        )

        result = OpenAlexTopicStatsCollectionResult(
            date_from=date_from,
            date_to=date_to,
            taxonomy_scope=taxonomy_scope,
            total_topics=len(topics),
            topics_with_openalex_id=len(topic_infos),
            skipped_topics_without_openalex_id=skipped_without_openalex,
            periods=len(periods),
            planned_requests=planned_requests,
            dry_run=dry_run,
        )
        for topic_id in missing_topic_ids:
            result.errors.append({"topic_id": topic_id, "message": "Topic not found"})
        result.errors.extend(scope_errors)
        if dry_run or not topic_infos or not periods:
            return result

        pending_items: list[OpenAlexTopicMonthlyCount] = []
        pending_yearly_items: list[OpenAlexTopicYearlyArtificialEstimate] = []
        logger.info(
            "OpenAlex topic stats collection started: topics=%s periods=%s planned_requests=%s workers=%s batch_size=%s",
            len(topic_infos),
            len(periods),
            result.planned_requests,
            self.request_workers,
            batch_size,
        )
        progress_bar = (
            tqdm(
                total=(
                    len(topic_infos) * len(periods)
                    if taxonomy_scope == "topic"
                    else len(periods)
                ),
                desc="OpenAlex topic stats",
                unit="month",
            )
            if show_progress
            else None
        )
        try:
            if taxonomy_scope == "topic":
                completed_topic_periods: set[tuple[int, date]] = set()
                for fetch_result in self._iter_fetch_results(
                    topics=topic_infos,
                    periods=periods,
                    language_filter=language_filter,
                    type_filter=type_filter,
                    normalize_january_first=normalize_january_first,
                ):
                    result.openalex_requests += fetch_result.openalex_requests
                    deferred_error = self._deferred_error(fetch_result.errors)
                    if deferred_error is not None:
                        result.deferred = True
                        result.retry_after_seconds = self._error_retry_after_seconds(
                            deferred_error
                        )
                        result.errors.append(deferred_error)
                        result.pending_tasks = self._topic_pending_tasks(
                            topic_infos=topic_infos,
                            periods=periods,
                            completed=completed_topic_periods,
                            languages=languages or ["en", "ru"],
                            types=types or ["article"],
                            batch_size=batch_size,
                            request_workers=self.request_workers,
                            group_by_page_size=group_by_page_size,
                            normalize_january_first=normalize_january_first,
                        )
                        break
                    if fetch_result.errors:
                        result.failed += 1
                        result.errors.extend(fetch_result.errors)
                    if fetch_result.item is not None:
                        pending_items.append(fetch_result.item)
                        completed_topic_periods.add(
                            (
                                int(fetch_result.item.topic_id),
                                fetch_result.item.period_start,
                            )
                        )
                    if fetch_result.yearly_item is not None:
                        pending_yearly_items.append(fetch_result.yearly_item)
                    if progress_bar is not None:
                        progress_bar.update(1)
                    if len(pending_items) >= batch_size:
                        self._flush_batch(pending_items, pending_yearly_items, result)
            else:
                completed_periods: set[date] = set()
                topic_by_external_id = {
                    _normalize_external_id(item.openalex_id): item
                    for item in topic_infos
                }
                for grouped_result in self._iter_grouped_fetch_results(
                    periods=periods,
                    topic_by_external_id=topic_by_external_id,
                    group_filter_parts=group_filter_parts,
                    language_filter=language_filter,
                    type_filter=type_filter,
                    group_by_page_size=group_by_page_size,
                    normalize_january_first=normalize_january_first,
                ):
                    result.openalex_requests += grouped_result.openalex_requests
                    deferred_error = self._deferred_error(grouped_result.errors)
                    if deferred_error is not None:
                        result.deferred = True
                        result.retry_after_seconds = self._error_retry_after_seconds(
                            deferred_error
                        )
                        result.errors.append(deferred_error)
                        result.pending_tasks = self._grouped_pending_tasks(
                            periods=periods,
                            completed=completed_periods,
                            field_ids=field_ids,
                            subfield_ids=subfield_ids,
                            taxonomy_scope=taxonomy_scope,
                            languages=languages or ["en", "ru"],
                            types=types or ["article"],
                            batch_size=batch_size,
                            request_workers=self.request_workers,
                            group_by_page_size=group_by_page_size,
                            normalize_january_first=normalize_january_first,
                        )
                        break
                    if grouped_result.errors:
                        result.failed += 1
                        result.errors.extend(grouped_result.errors)
                    pending_items.extend(grouped_result.items)
                    pending_yearly_items.extend(grouped_result.yearly_items)
                    for item in grouped_result.items:
                        completed_periods.add(item.period_start)
                    if progress_bar is not None:
                        progress_bar.update(1)
                    if len(pending_items) >= batch_size:
                        self._flush_batch(pending_items, pending_yearly_items, result)

            if pending_items:
                self._flush_batch(pending_items, pending_yearly_items, result)
            elif pending_yearly_items:
                self._flush_batch(pending_items, pending_yearly_items, result)
        finally:
            if progress_bar is not None:
                progress_bar.close()
            self._close_thread_adapters()
        logger.info(
            "OpenAlex topic stats collection completed: collected=%s created=%s updated=%s failed=%s requests=%s",
            result.collected,
            result.created,
            result.updated,
            result.failed,
            result.openalex_requests,
        )
        return result

    def _topic_pending_tasks(
        self,
        *,
        topic_infos: list[_TopicInfo],
        periods: list[MonthPeriod],
        completed: set[tuple[int, date]],
        languages: list[str],
        types: list[str],
        batch_size: int,
        request_workers: int,
        group_by_page_size: int,
        normalize_january_first: bool,
    ) -> list[dict[str, object]]:
        return [
            self._pending_task_payload(
                period=period,
                taxonomy_scope="topic",
                topic_ids=[topic.id],
                field_ids=None,
                subfield_ids=None,
                languages=languages,
                types=types,
                batch_size=batch_size,
                request_workers=request_workers,
                group_by_page_size=group_by_page_size,
                normalize_january_first=normalize_january_first,
            )
            for topic in topic_infos
            for period in periods
            if (topic.id, period.period_start) not in completed
        ]

    def _grouped_pending_tasks(
        self,
        *,
        periods: list[MonthPeriod],
        completed: set[date],
        field_ids: list[int] | None,
        subfield_ids: list[int] | None,
        taxonomy_scope: TaxonomyStatsScope,
        languages: list[str],
        types: list[str],
        batch_size: int,
        request_workers: int,
        group_by_page_size: int,
        normalize_january_first: bool,
    ) -> list[dict[str, object]]:
        return [
            self._pending_task_payload(
                period=period,
                taxonomy_scope=taxonomy_scope,
                topic_ids=None,
                field_ids=field_ids,
                subfield_ids=subfield_ids,
                languages=languages,
                types=types,
                batch_size=batch_size,
                request_workers=request_workers,
                group_by_page_size=group_by_page_size,
                normalize_january_first=normalize_january_first,
            )
            for period in periods
            if period.period_start not in completed
        ]

    def _pending_task_payload(
        self,
        *,
        period: MonthPeriod,
        taxonomy_scope: TaxonomyStatsScope,
        topic_ids: list[int] | None,
        field_ids: list[int] | None,
        subfield_ids: list[int] | None,
        languages: list[str],
        types: list[str],
        batch_size: int,
        request_workers: int,
        group_by_page_size: int,
        normalize_january_first: bool,
    ) -> dict[str, object]:
        payload: dict[str, object] = {
            "task_type": "collect_topic_stats",
            "date_from": period.period_start.isoformat(),
            "date_to": period.period_end.isoformat(),
            "taxonomy_scope": taxonomy_scope,
            "languages": languages,
            "types": types,
            "batch_size": batch_size,
            "request_workers": request_workers,
            "group_by_page_size": group_by_page_size,
            "max_retries": self.max_retries,
            "normalize_january_first": normalize_january_first,
            "primary_topic_only": self.primary_topic_only,
            "show_progress": False,
        }
        if topic_ids:
            payload["topic_ids"] = topic_ids
        if field_ids:
            payload["field_ids"] = field_ids
        if subfield_ids:
            payload["subfield_ids"] = subfield_ids
        rate_limit = getattr(self.rate_limiter, "requests_per_second", None)
        if rate_limit is not None:
            payload["rate_limit_rps"] = float(rate_limit)
        return payload

    def _deferred_error(
        self,
        errors: list[dict[str, object]],
    ) -> dict[str, object] | None:
        for error in errors:
            if bool(error.get("deferred")):
                return error
        return None

    def _error_retry_after_seconds(self, error: dict[str, object]) -> float | None:
        value = error.get("retry_after_seconds")
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _flush_batch(
        self,
        pending_items: list[OpenAlexTopicMonthlyCount],
        pending_yearly_items: list[OpenAlexTopicYearlyArtificialEstimate],
        result: OpenAlexTopicStatsCollectionResult,
    ) -> None:
        batch_size = len(pending_items)
        yearly_batch_size = len(pending_yearly_items)
        logger.info(
            "Upserting OpenAlex topic stats batch: rows=%s yearly_artificial=%s",
            batch_size,
            yearly_batch_size,
        )
        started_at = time.monotonic()
        created, updated = self.stats_repository.upsert_many(pending_items)
        if pending_yearly_items:
            if self.yearly_stats_repository is None:
                raise ValueError("Yearly stats repository is required for yearly estimates")
            self.yearly_stats_repository.upsert_artificial_estimates(pending_yearly_items)
        if self.commit_each_batch:
            self.stats_repository.session.commit()
        result.created += created
        result.updated += updated
        result.collected += batch_size
        result.yearly_artificial_estimates += yearly_batch_size
        pending_items.clear()
        pending_yearly_items.clear()
        logger.info(
            "OpenAlex topic stats batch upserted: rows=%s created=%s updated=%s yearly_artificial=%s elapsed=%.2fs",
            batch_size,
            created,
            updated,
            yearly_batch_size,
            time.monotonic() - started_at,
        )

    def _iter_fetch_results(
        self,
        *,
        topics: list[_TopicInfo],
        periods: list[MonthPeriod],
        language_filter: str,
        type_filter: str,
        normalize_january_first: bool,
    ) -> Iterator[_FetchResult]:
        iterator = iter((topic, period) for topic in topics for period in periods)
        max_pending = max(1, self.request_workers)
        with ThreadPoolExecutor(max_workers=self.request_workers) as executor:
            pending: set[Future[_FetchResult]] = set()

            def submit_next() -> None:
                try:
                    topic, period = next(iterator)
                except StopIteration:
                    return
                pending.add(
                    executor.submit(
                        self._fetch_topic_month,
                        topic,
                        period,
                        language_filter,
                        type_filter,
                        normalize_january_first,
                    )
                )

            for _ in range(max_pending):
                submit_next()
            while pending:
                completed, pending = wait(pending, return_when=FIRST_COMPLETED)
                for future in completed:
                    submit_next()
                    yield future.result()

    def _fetch_topic_month(
        self,
        topic: _TopicInfo,
        period: MonthPeriod,
        language_filter: str,
        type_filter: str,
        normalize_january_first: bool,
    ) -> _FetchResult:
        count, requests, error = self._count_with_retries(
            topic=topic,
            period=period,
            language=language_filter,
            publication_type=type_filter,
        )
        if error is not None:
            return _FetchResult(item=None, openalex_requests=requests, errors=[error])
        yearly_item: OpenAlexTopicYearlyArtificialEstimate | None = None
        if normalize_january_first and period.period_start.month == 1:
            jan1_count, jan1_requests, jan1_error = self._count_with_retries(
                topic=topic,
                period=MonthPeriod(period.period_start, period.period_start),
                language=language_filter,
                publication_type=type_filter,
            )
            requests += jan1_requests
            if jan1_error is not None:
                return _FetchResult(item=None, openalex_requests=requests, errors=[jan1_error])
            normalized = normalize_january_publication_count(count, jan1_count)
            count = normalized.expected_real_count
            yearly_item = OpenAlexTopicYearlyArtificialEstimate(
                topic_id=topic.id,
                stat_year=date(period.period_start.year, 1, 1),
                artifical_pubdates_estimation=normalized.estimated_artificial_count,
            )
        return _FetchResult(
            item=OpenAlexTopicMonthlyCount(
                topic_id=topic.id,
                period_start=period.period_start,
                works_count=count,
            ),
            openalex_requests=requests,
            errors=[],
            yearly_item=yearly_item,
        )

    def _iter_grouped_fetch_results(
        self,
        *,
        periods: list[MonthPeriod],
        topic_by_external_id: dict[str, _TopicInfo],
        group_filter_parts: list[str],
        language_filter: str,
        type_filter: str,
        group_by_page_size: int,
        normalize_january_first: bool,
    ) -> Iterator[_GroupedFetchResult]:
        iterator = iter(periods)
        max_pending = max(1, self.request_workers)
        with ThreadPoolExecutor(max_workers=self.request_workers) as executor:
            pending: set[Future[_GroupedFetchResult]] = set()

            def submit_next() -> None:
                try:
                    period = next(iterator)
                except StopIteration:
                    return
                pending.add(
                    executor.submit(
                        self._fetch_grouped_month,
                        period,
                        topic_by_external_id,
                        group_filter_parts,
                        language_filter,
                        type_filter,
                        group_by_page_size,
                        normalize_january_first,
                    )
                )

            for _ in range(max_pending):
                submit_next()
            while pending:
                completed, pending = wait(pending, return_when=FIRST_COMPLETED)
                for future in completed:
                    submit_next()
                    yield future.result()

    def _fetch_grouped_month(
        self,
        period: MonthPeriod,
        topic_by_external_id: dict[str, _TopicInfo],
        group_filter_parts: list[str],
        language_filter: str,
        type_filter: str,
        group_by_page_size: int,
        normalize_january_first: bool,
    ) -> _GroupedFetchResult:
        group_by = "primary_topic.id" if self.primary_topic_only else "topics.id"
        counts, requests, error = self._group_counts_with_retries(
            period=period,
            group_by=group_by,
            group_filter_parts=group_filter_parts,
            language=language_filter,
            publication_type=type_filter,
            group_by_page_size=group_by_page_size,
        )
        if error is not None:
            return _GroupedFetchResult([], [], requests, [error])

        jan1_counts: dict[str, int] = {}
        if normalize_january_first and period.period_start.month == 1:
            jan1_counts, jan1_requests, jan1_error = self._group_counts_with_retries(
                period=MonthPeriod(period.period_start, period.period_start),
                group_by=group_by,
                group_filter_parts=group_filter_parts,
                language=language_filter,
                publication_type=type_filter,
                group_by_page_size=group_by_page_size,
            )
            requests += jan1_requests
            if jan1_error is not None:
                return _GroupedFetchResult([], [], requests, [jan1_error])

        items: list[OpenAlexTopicMonthlyCount] = []
        yearly_items: list[OpenAlexTopicYearlyArtificialEstimate] = []
        for external_id, count in counts.items():
            topic = topic_by_external_id.get(external_id)
            if topic is None:
                continue
            works_count = count
            if normalize_january_first and period.period_start.month == 1:
                normalized = normalize_january_publication_count(
                    count,
                    jan1_counts.get(external_id, 0),
                )
                works_count = normalized.expected_real_count
                yearly_items.append(
                    OpenAlexTopicYearlyArtificialEstimate(
                        topic_id=topic.id,
                        stat_year=date(period.period_start.year, 1, 1),
                        artifical_pubdates_estimation=(
                            normalized.estimated_artificial_count
                        ),
                    )
                )
            items.append(
                OpenAlexTopicMonthlyCount(
                    topic_id=topic.id,
                    period_start=period.period_start,
                    works_count=works_count,
                )
            )
        return _GroupedFetchResult(items, yearly_items, requests, [])

    def _group_counts_with_retries(
        self,
        *,
        period: MonthPeriod,
        group_by: str,
        group_filter_parts: list[str],
        language: str,
        publication_type: str,
        group_by_page_size: int,
    ) -> tuple[dict[str, int], int, dict[str, object] | None]:
        requests = 0
        for attempt in range(self.max_retries + 1):
            try:
                counts: dict[str, int] = {}
                cursor: str | None = "*"
                seen_cursors: set[str] = set()
                while cursor:
                    self.rate_limiter.acquire()
                    requests += 1
                    groups, next_cursor = self._adapter().group_works(
                        OpenAlexSearchFiltersDTO(
                            date_from=period.period_start,
                            date_to=period.period_end,
                            type=publication_type,
                            language=language,
                        ),
                        group_by=group_by,
                        extra_filter_parts=group_filter_parts,
                        cursor=cursor,
                        per_page=group_by_page_size,
                    )
                    for group in groups:
                        key = _normalize_external_id(group.get("key") or group.get("id"))
                        if not key:
                            continue
                        counts[key] = int(group.get("count") or 0)
                    if (
                        not next_cursor
                        or next_cursor == cursor
                        or next_cursor in seen_cursors
                    ):
                        break
                    seen_cursors.add(cursor)
                    cursor = next_cursor
                return counts, requests, None
            except ExternalServiceRateLimitError as exc:
                retry_after = self._retry_after_seconds(exc)
                if (
                    retry_after is None
                    or retry_after > self.rate_limit_defer_after_seconds
                ):
                    error = self._group_error_payload(
                        period,
                        language,
                        publication_type,
                        group_by,
                        exc,
                    )
                    error["deferred"] = True
                    error["retry_after_seconds"] = retry_after
                    return {}, requests, error
                if attempt >= self.max_retries:
                    return {}, requests, self._group_error_payload(
                        period,
                        language,
                        publication_type,
                        group_by,
                        exc,
                    )
                logger.warning(
                    "OpenAlex group retry: period=%s language=%s type=%s group_by=%s attempt=%s/%s delay=%.2fs reason=%s",
                    period.period_start.isoformat(),
                    language,
                    publication_type,
                    group_by,
                    attempt + 1,
                    self.max_retries,
                    retry_after,
                    exc.__class__.__name__,
                )
                time.sleep(retry_after)
            except (
                ExternalServiceUnavailableError,
                httpx.HTTPError,
            ) as exc:
                if attempt >= self.max_retries:
                    return {}, requests, self._group_error_payload(
                        period,
                        language,
                        publication_type,
                        group_by,
                        exc,
                    )
                delay = self._retry_delay(exc, attempt)
                logger.warning(
                    "OpenAlex group retry: period=%s language=%s type=%s group_by=%s attempt=%s/%s delay=%.2fs reason=%s",
                    period.period_start.isoformat(),
                    language,
                    publication_type,
                    group_by,
                    attempt + 1,
                    self.max_retries,
                    delay,
                    exc.__class__.__name__,
                )
                time.sleep(delay)
            except AppError as exc:
                return {}, requests, self._group_error_payload(
                    period,
                    language,
                    publication_type,
                    group_by,
                    exc,
                )
        return {}, requests, None

    def _count_with_retries(
        self,
        *,
        topic: _TopicInfo,
        period: MonthPeriod,
        language: str,
        publication_type: str,
    ) -> tuple[int, int, dict[str, object] | None]:
        requests = 0
        for attempt in range(self.max_retries + 1):
            self.rate_limiter.acquire()
            requests += 1
            try:
                count = self._adapter().count_works(
                    OpenAlexSearchFiltersDTO(
                        date_from=period.period_start,
                        date_to=period.period_end,
                        type=publication_type,
                        language=language,
                    ),
                    topic_external_id=topic.openalex_id,
                    primary_topic_only=self.primary_topic_only,
                )
                return count, requests, None
            except ExternalServiceRateLimitError as exc:
                retry_after = self._retry_after_seconds(exc)
                if (
                    retry_after is None
                    or retry_after > self.rate_limit_defer_after_seconds
                ):
                    error = self._error_payload(
                        topic,
                        period,
                        language,
                        publication_type,
                        exc,
                    )
                    error["deferred"] = True
                    error["retry_after_seconds"] = retry_after
                    return 0, requests, error
                if attempt >= self.max_retries:
                    return 0, requests, self._error_payload(
                        topic,
                        period,
                        language,
                        publication_type,
                        exc,
                    )
                logger.warning(
                    "OpenAlex count retry: topic_id=%s period=%s language=%s type=%s attempt=%s/%s delay=%.2fs reason=%s",
                    topic.id,
                    period.period_start.isoformat(),
                    language,
                    publication_type,
                    attempt + 1,
                    self.max_retries,
                    retry_after,
                    exc.__class__.__name__,
                )
                time.sleep(retry_after)
            except (
                ExternalServiceUnavailableError,
                httpx.HTTPError,
            ) as exc:
                if attempt >= self.max_retries:
                    return 0, requests, self._error_payload(
                        topic,
                        period,
                        language,
                        publication_type,
                        exc,
                    )
                delay = self._retry_delay(exc, attempt)
                logger.warning(
                    "OpenAlex count retry: topic_id=%s period=%s language=%s type=%s attempt=%s/%s delay=%.2fs reason=%s",
                    topic.id,
                    period.period_start.isoformat(),
                    language,
                    publication_type,
                    attempt + 1,
                    self.max_retries,
                    delay,
                    exc.__class__.__name__,
                )
                time.sleep(delay)
            except AppError as exc:
                return 0, requests, self._error_payload(
                    topic,
                    period,
                    language,
                    publication_type,
                    exc,
                )
        return 0, requests, None

    def _adapter(self) -> OpenAlexAdapter:
        adapter = getattr(self._thread_local, "adapter", None)
        if adapter is None:
            adapter = self.openalex_adapter_factory()
            self._thread_local.adapter = adapter
            with self._adapters_lock:
                self._adapters.append(adapter)
        return adapter

    def _close_thread_adapters(self) -> None:
        with self._adapters_lock:
            adapters = list(self._adapters)
            self._adapters.clear()
        for adapter in adapters:
            client = getattr(adapter, "_client", None)
            close = getattr(client, "close", None)
            if callable(close):
                close()

    def _load_topics(
        self,
        topic_ids: list[int] | None,
        limit: int | None,
        offset: int,
        *,
        taxonomy_scope: TaxonomyStatsScope,
        field_ids: list[int] | None,
        subfield_ids: list[int] | None,
    ) -> tuple[list[Topic], list[int]]:
        if taxonomy_scope != "topic":
            return (
                self.taxonomy_repository.list_topics_for_stats(
                    limit=limit,
                    offset=offset,
                    field_ids=field_ids if taxonomy_scope == "field" else None,
                    subfield_ids=subfield_ids if taxonomy_scope == "subfield" else None,
                ),
                [],
            )
        if topic_ids:
            requested = list(dict.fromkeys(int(topic_id) for topic_id in topic_ids))
            topics = self.taxonomy_repository.list_topics_by_ids(requested)
            found = {topic.id for topic in topics}
            missing = [topic_id for topic_id in requested if topic_id not in found]
            return topics, missing
        return (
            self.taxonomy_repository.list_topics_for_stats(limit=limit, offset=offset),
            [],
        )

    def _group_filter_parts(
        self,
        taxonomy_scope: TaxonomyStatsScope,
        *,
        field_ids: list[int] | None,
        subfield_ids: list[int] | None,
    ) -> tuple[list[str], list[dict[str, object]]]:
        group_prefix = "primary_topic" if self.primary_topic_only else "topics"
        if taxonomy_scope == "topic":
            return [], []
        if taxonomy_scope == "field" and field_ids:
            fields = self.taxonomy_repository.list_fields_by_ids(field_ids)
            found = {int(field.id) for field in fields}
            missing = [field_id for field_id in field_ids if field_id not in found]
            openalex_ids = [
                _normalize_external_id(field.openalex_id)
                for field in fields
                if field.openalex_id
            ]
            errors: list[dict[str, object]] = [
                {"field_id": field_id, "message": "Field not found"}
                for field_id in missing
            ]
            if openalex_ids:
                return [f"{group_prefix}.field.id:{'|'.join(openalex_ids)}"], errors
            return [], errors
        if taxonomy_scope == "subfield" and subfield_ids:
            subfields = self.taxonomy_repository.list_subfields_by_ids(subfield_ids)
            found = {int(subfield.id) for subfield in subfields}
            missing = [
                subfield_id
                for subfield_id in subfield_ids
                if subfield_id not in found
            ]
            openalex_ids = [
                _normalize_external_id(subfield.openalex_id)
                for subfield in subfields
                if subfield.openalex_id
            ]
            errors = [
                {"subfield_id": subfield_id, "message": "Subfield not found"}
                for subfield_id in missing
            ]
            if openalex_ids:
                return [f"{group_prefix}.subfield.id:{'|'.join(openalex_ids)}"], errors
            return [], errors
        return [], []

    def _topic_infos(self, topics: list[Topic]) -> tuple[list[_TopicInfo], int]:
        topic_infos: list[_TopicInfo] = []
        skipped = 0
        for topic in topics:
            if not topic.openalex_id:
                skipped += 1
                continue
            topic_infos.append(
                _TopicInfo(
                    id=int(topic.id),
                    openalex_id=str(topic.openalex_id),
                    name=topic.name,
                )
            )
        return topic_infos, skipped

    def _error_payload(
        self,
        topic: _TopicInfo,
        period: MonthPeriod,
        language: str,
        publication_type: str,
        exc: Exception,
    ) -> dict[str, object]:
        details = getattr(exc, "details", None)
        return {
            "topic_id": topic.id,
            "topic_openalex_id": topic.openalex_id,
            "period_start": period.period_start.isoformat(),
            "period_end": period.period_end.isoformat(),
            "language": language,
            "type": publication_type,
            "code": exc.__class__.__name__,
            "message": str(exc),
            "details": details or {},
        }

    def _group_error_payload(
        self,
        period: MonthPeriod,
        language: str,
        publication_type: str,
        group_by: str,
        exc: Exception,
    ) -> dict[str, object]:
        details = getattr(exc, "details", None)
        return {
            "period_start": period.period_start.isoformat(),
            "period_end": period.period_end.isoformat(),
            "language": language,
            "type": publication_type,
            "group_by": group_by,
            "code": exc.__class__.__name__,
            "message": str(exc),
            "details": details or {},
        }

    def _backoff_seconds(self, attempt: int) -> float:
        return min(30.0, 0.5 * (2**attempt))

    def _retry_after_seconds(self, exc: Exception) -> float | None:
        details = getattr(exc, "details", None)
        retry_after = (
            details.get("retry_after")
            if isinstance(details, dict)
            else None
        )
        if isinstance(retry_after, str):
            return self._parse_retry_after(retry_after)
        if isinstance(retry_after, (int, float)):
            return max(0.0, float(retry_after))
        return None

    def _retry_delay(self, exc: Exception, attempt: int) -> float:
        parsed = self._retry_after_seconds(exc)
        if parsed is not None:
            return parsed
        return self._backoff_seconds(attempt)

    def _parse_retry_after(self, value: str) -> float | None:
        value = value.strip()
        if not value:
            return None
        try:
            return max(0.0, float(value))
        except ValueError:
            pass
        try:
            parsed = parsedate_to_datetime(value)
        except (TypeError, ValueError):
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return max(0.0, (parsed - datetime.now(timezone.utc)).total_seconds())


def iter_month_periods(date_from: date, date_to: date) -> Iterator[MonthPeriod]:
    """Yield full calendar months overlapping the supplied date range."""
    current = date(date_from.year, date_from.month, 1)
    final = date(date_to.year, date_to.month, 1)
    while current <= final:
        last_day = calendar.monthrange(current.year, current.month)[1]
        yield MonthPeriod(
            period_start=current,
            period_end=date(current.year, current.month, last_day),
        )
        if current.month == 12:
            current = date(current.year + 1, 1, 1)
        else:
            current = date(current.year, current.month + 1, 1)


def _clean_list(values: list[str]) -> list[str]:
    return [value.strip() for value in values if value and value.strip()]


def _or_filter_value(values: list[str]) -> str:
    cleaned = list(dict.fromkeys(_clean_list(values)))
    if not cleaned:
        raise ValueError("At least one OpenAlex filter value is required")
    return "|".join(cleaned)


def _normalize_external_id(value: object) -> str:
    text = str(value or "").strip().rstrip("/")
    if "/" in text:
        text = text.rsplit("/", 1)[-1]
    return text


def normalize_january_publication_count(
    jan_pubcount: int,
    jan1_pubcount: int,
) -> _JanuaryNormalizationResult:
    """Estimate real January publications after removing January-1 artifacts."""
    jan_pubcount = max(0, int(jan_pubcount))
    jan1_pubcount = max(0, int(jan1_pubcount))
    jan_guarantee = max(0, jan_pubcount - jan1_pubcount)
    jan_expected = Decimal(jan_guarantee) + (Decimal(jan_guarantee) / Decimal(30))
    expected_real_count = int(
        jan_expected.quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    )
    expected_real_count = min(jan_pubcount, max(0, expected_real_count))
    estimated_artificial_count = max(0, jan_pubcount - expected_real_count)
    return _JanuaryNormalizationResult(
        expected_real_count=expected_real_count,
        estimated_artificial_count=estimated_artificial_count,
    )


__all__ = [
    "MonthPeriod",
    "OpenAlexTopicStatsCollectionResult",
    "OpenAlexTopicStatsCollector",
    "SyncRateLimiter",
    "iter_month_periods",
    "normalize_january_publication_count",
]
