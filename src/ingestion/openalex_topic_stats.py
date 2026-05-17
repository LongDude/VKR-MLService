from __future__ import annotations

import calendar
import logging
import threading
import time
from collections import deque
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Callable, Iterator

import httpx
from tqdm.auto import tqdm

from adapters.openalex_adapter import OpenAlexAdapter
from core.exceptions import (
    AppError,
    ExternalServiceRateLimitError,
    ExternalServiceUnavailableError,
)
from dto.external import OpenAlexSearchFiltersDTO
from models import Topic
from repositories.openalex_topic_stats import (
    OpenAlexTopicMonthlyCount,
    OpenAlexTopicStatsRepository,
)
from repositories.taxonomy import TaxonomyRepository


logger = logging.getLogger(__name__)


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
    total_topics: int = 0
    topics_with_openalex_id: int = 0
    skipped_topics_without_openalex_id: int = 0
    periods: int = 0
    planned_requests: int = 0
    openalex_requests: int = 0
    collected: int = 0
    created: int = 0
    updated: int = 0
    failed: int = 0
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


class SyncRateLimiter:
    """Thread-safe sliding-window request limiter for synchronous OpenAlex calls."""

    def __init__(self, requests_per_second: float) -> None:
        if requests_per_second <= 0:
            raise ValueError("requests_per_second must be positive")
        self.requests_per_second = requests_per_second
        self._timestamps: deque[float] = deque()
        self._lock = threading.Lock()

    def acquire(self) -> None:
        """Block until a request slot is available."""
        while True:
            with self._lock:
                now = time.monotonic()
                while self._timestamps and now - self._timestamps[0] >= 1.0:
                    self._timestamps.popleft()
                if len(self._timestamps) < self.requests_per_second:
                    self._timestamps.append(now)
                    return
                sleep_for = max(0.0, 1.0 - (now - self._timestamps[0]))
            time.sleep(sleep_for)


class OpenAlexTopicStatsCollector:
    """Collect topic/month OpenAlex work counts and upsert them into PostgreSQL."""

    def __init__(
        self,
        *,
        taxonomy_repository: TaxonomyRepository,
        stats_repository: OpenAlexTopicStatsRepository,
        openalex_adapter_factory: Callable[[], OpenAlexAdapter],
        request_workers: int = 8,
        rate_limiter: SyncRateLimiter | None = None,
        max_retries: int = 5,
        primary_topic_only: bool = False,
        commit_each_batch: bool = True,
    ) -> None:
        self.taxonomy_repository = taxonomy_repository
        self.stats_repository = stats_repository
        self.openalex_adapter_factory = openalex_adapter_factory
        self.request_workers = max(1, request_workers)
        self.rate_limiter = rate_limiter or SyncRateLimiter(70)
        self.max_retries = max(0, max_retries)
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
        limit: int | None = None,
        offset: int = 0,
        languages: list[str] | None = None,
        types: list[str] | None = None,
        batch_size: int = 500,
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

        periods = list(iter_month_periods(date_from, date_to))
        topics, missing_topic_ids = self._load_topics(topic_ids, limit, offset)
        topic_infos, skipped_without_openalex = self._topic_infos(topics)
        languages = _clean_list(languages or ["en", "ru"])
        types = _clean_list(types or ["article"])

        result = OpenAlexTopicStatsCollectionResult(
            date_from=date_from,
            date_to=date_to,
            total_topics=len(topics),
            topics_with_openalex_id=len(topic_infos),
            skipped_topics_without_openalex_id=skipped_without_openalex,
            periods=len(periods),
            planned_requests=len(topic_infos) * len(periods) * len(languages) * len(types),
            dry_run=dry_run,
        )
        for topic_id in missing_topic_ids:
            result.errors.append({"topic_id": topic_id, "message": "Topic not found"})
        if dry_run or not topic_infos or not periods:
            return result

        pending_items: list[OpenAlexTopicMonthlyCount] = []
        logger.info(
            "OpenAlex topic stats collection started: topics=%s periods=%s planned_requests=%s workers=%s batch_size=%s",
            len(topic_infos),
            len(periods),
            result.planned_requests,
            self.request_workers,
            batch_size,
        )
        progress_bar = (
            tqdm(total=len(topic_infos) * len(periods), desc="OpenAlex topic stats", unit="month")
            if show_progress
            else None
        )
        try:
            for fetch_result in self._iter_fetch_results(
                topics=topic_infos,
                periods=periods,
                languages=languages,
                types=types,
            ):
                result.openalex_requests += fetch_result.openalex_requests
                if fetch_result.errors:
                    result.failed += 1
                    result.errors.extend(fetch_result.errors)
                if fetch_result.item is not None:
                    pending_items.append(fetch_result.item)
                if progress_bar is not None:
                    progress_bar.update(1)
                if len(pending_items) >= batch_size:
                    self._flush_batch(pending_items, result)

            if pending_items:
                self._flush_batch(pending_items, result)
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

    def _flush_batch(
        self,
        pending_items: list[OpenAlexTopicMonthlyCount],
        result: OpenAlexTopicStatsCollectionResult,
    ) -> None:
        batch_size = len(pending_items)
        logger.info("Upserting OpenAlex topic stats batch: rows=%s", batch_size)
        started_at = time.monotonic()
        created, updated = self.stats_repository.upsert_many(pending_items)
        if self.commit_each_batch:
            self.stats_repository.session.commit()
        result.created += created
        result.updated += updated
        result.collected += batch_size
        pending_items.clear()
        logger.info(
            "OpenAlex topic stats batch upserted: rows=%s created=%s updated=%s elapsed=%.2fs",
            batch_size,
            created,
            updated,
            time.monotonic() - started_at,
        )

    def _iter_fetch_results(
        self,
        *,
        topics: list[_TopicInfo],
        periods: list[MonthPeriod],
        languages: list[str],
        types: list[str],
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
                        languages,
                        types,
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
        languages: list[str],
        types: list[str],
    ) -> _FetchResult:
        count = 0
        requests = 0
        errors: list[dict[str, object]] = []
        for language in languages:
            for publication_type in types:
                value, request_count, error = self._count_with_retries(
                    topic=topic,
                    period=period,
                    language=language,
                    publication_type=publication_type,
                )
                requests += request_count
                if error is not None:
                    errors.append(error)
                    continue
                count += value
        if errors:
            return _FetchResult(item=None, openalex_requests=requests, errors=errors)
        return _FetchResult(
            item=OpenAlexTopicMonthlyCount(
                topic_id=topic.id,
                period_start=period.period_start,
                works_count=count,
            ),
            openalex_requests=requests,
            errors=[],
        )

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
            except (
                ExternalServiceRateLimitError,
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
    ) -> tuple[list[Topic], list[int]]:
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
            "language": language,
            "type": publication_type,
            "code": exc.__class__.__name__,
            "message": str(exc),
            "details": details or {},
        }

    def _backoff_seconds(self, attempt: int) -> float:
        return min(30.0, 0.5 * (2**attempt))

    def _retry_delay(self, exc: Exception, attempt: int) -> float:
        details = getattr(exc, "details", None)
        retry_after = (
            details.get("retry_after")
            if isinstance(details, dict)
            else None
        )
        if isinstance(retry_after, str):
            parsed = self._parse_retry_after(retry_after)
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


__all__ = [
    "MonthPeriod",
    "OpenAlexTopicStatsCollectionResult",
    "OpenAlexTopicStatsCollector",
    "SyncRateLimiter",
    "iter_month_periods",
]
