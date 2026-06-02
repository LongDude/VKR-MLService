from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import logging
from math import ceil
from typing import Any

import httpx
from tqdm.auto import tqdm

from adapters.openalex_adapter import OpenAlexAdapter
from core.logging import get_logger, log_event, logged_call
from dto.external import ExternalPaperDTO
from dto.openalex import (
    OpenAlexLoadPlanDTO,
    OpenAlexLoadPlanItemDTO,
    OpenAlexPendingPageDTO,
    OpenAlexUnitSummaryDTO,
)
from ml.services.openalex_rate_limiter import AsyncRateLimiter

logger = get_logger(__name__)

OPENALEX_WORKS_SELECT = ",".join(
    [
        "id",
        "doi",
        "title",
        "display_name",
        "publication_year",
        "publication_date",
        "type",
        "language",
        "abstract_inverted_index",
        "open_access",
        "cited_by_count",
        "referenced_works_count",
        "authorships",
        "topics",
        "keywords",
        "locations",
        "primary_location",
    ]
)


@dataclass
class OpenAlexDownloadResult:
    papers: list[ExternalPaperDTO] = field(default_factory=list[ExternalPaperDTO])
    fetched: int = 0
    normalized: int = 0
    skipped_empty_title: int = 0
    skipped_empty_abstract: int = 0
    failed: int = 0
    openalex_requests: int = 0
    deferred: bool = False
    pending_pages: list[OpenAlexPendingPageDTO] = field(
        default_factory=list[OpenAlexPendingPageDTO]
    )
    retry_after_seconds: float | None = None
    unit_summaries: dict[str, OpenAlexUnitSummaryDTO] = field(
        default_factory=dict[str, OpenAlexUnitSummaryDTO]
    )
    paper_unit_keys: dict[str, list[str]] = field(default_factory=dict[str, list[str]])
    errors: list[dict[str, Any]] = field(default_factory=list[dict[str, Any]])


class OpenAlexPaperDownloader:
    """Download OpenAlex works concurrently and detect exhausted quota units."""

    def __init__(
        self,
        *,
        base_url: str = "https://api.openalex.org",
        timeout_seconds: float = 30.0,
        request_workers: int = 8,
        rate_limiter: AsyncRateLimiter | None = None,
        max_retries: int = 5,
        api_key: str | None = None,
        mailto: str | None = None,
        rate_limit_defer_after_seconds: float = 120.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.request_workers = max(1, request_workers)
        self.rate_limiter = rate_limiter or AsyncRateLimiter(70)
        self.max_retries = max(0, max_retries)
        self.api_key = api_key.strip() if api_key and api_key.strip() else None
        self.mailto = mailto.strip() if mailto and mailto.strip() else None
        self.rate_limit_defer_after_seconds = max(0.0, rate_limit_defer_after_seconds)
        self.transport = transport

    @logged_call(logger, "openalex_download_plan")
    async def fetch_plan(
        self,
        plan: OpenAlexLoadPlanDTO,
        *,
        sample: bool = True,
        per_page: int = 100,
        show_progress: bool = False,
    ) -> OpenAlexDownloadResult:
        pages = self._plan_pages(plan, sample=sample, per_page=per_page)
        result = await self.fetch_pages(pages, show_progress=show_progress)
        for unit in plan.units:
            result.unit_summaries.setdefault(
                unit.unit_key,
                OpenAlexUnitSummaryDTO(
                    unit_key=unit.unit_key,
                    period=unit.period,
                    topic_id=unit.topic_id,
                    requested=unit.requested,
                ),
            )
        return result

    @logged_call(logger, "openalex_download_pages")
    async def fetch_pages(
        self,
        pages: list[OpenAlexPendingPageDTO],
        *,
        show_progress: bool = False,
    ) -> OpenAlexDownloadResult:
        result = OpenAlexDownloadResult()
        if not pages:
            return result

        semaphore = asyncio.Semaphore(self.request_workers)
        progress_bar = (
            tqdm(total=len(pages), desc="OpenAlex download", unit="page")
            if show_progress
            else None
        )
        queue: asyncio.Queue[OpenAlexPendingPageDTO] = asyncio.Queue()
        for page in pages:
            queue.put_nowait(page)
            self._ensure_unit_summary(result, page)

        deferred_event = asyncio.Event()
        page_results: list[dict[str, Any]] = []
        pending_pages: list[OpenAlexPendingPageDTO] = []

        async with httpx.AsyncClient(
            base_url=self.base_url,
            timeout=self.timeout_seconds,
            transport=self.transport,
        ) as client:
            try:
                workers = [
                    asyncio.create_task(
                        self._fetch_page_worker(
                            client,
                            queue=queue,
                            semaphore=semaphore,
                            deferred_event=deferred_event,
                            page_results=page_results,
                            pending_pages=pending_pages,
                            progress_bar=progress_bar,
                        )
                    )
                    for _ in range(self.request_workers)
                ]
                await asyncio.gather(*workers)
                if deferred_event.is_set():
                    while True:
                        try:
                            pending_pages.append(queue.get_nowait())
                        except asyncio.QueueEmpty:
                            break
                    result.deferred = True
                    result.pending_pages = pending_pages
                    retry_values = [
                        float(page_result["retry_after_seconds"])
                        for page_result in page_results
                        if page_result.get("deferred")
                        and page_result.get("retry_after_seconds") is not None
                    ]
                    result.retry_after_seconds = (
                        max(retry_values) if retry_values else None
                    )
            finally:
                if progress_bar is not None:
                    progress_bar.close()

        self._merge_page_results(result, page_results)
        if result.deferred:
            log_event(
                logger,
                "openalex_download_deferred",
                level=logging.WARNING,
                page_count=len(pages),
                pending_page_count=len(result.pending_pages),
                retry_after_seconds=result.retry_after_seconds,
            )
        return result

    async def _fetch_page_worker(
        self,
        client: httpx.AsyncClient,
        *,
        queue: asyncio.Queue[OpenAlexPendingPageDTO],
        semaphore: asyncio.Semaphore,
        deferred_event: asyncio.Event,
        page_results: list[dict[str, Any]],
        pending_pages: list[OpenAlexPendingPageDTO],
        progress_bar: Any,
    ) -> None:
        while not deferred_event.is_set():
            try:
                pending_page = queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            page_result = await self._fetch_page(
                client,
                pending_page,
                semaphore=semaphore,
            )
            page_results.append(page_result)
            if page_result.get("deferred"):
                pending_pages.append(pending_page)
                deferred_event.set()
            if progress_bar is not None:
                progress_bar.update(1)

    async def _fetch_page(
        self,
        client: httpx.AsyncClient,
        pending_page: OpenAlexPendingPageDTO,
        *,
        semaphore: asyncio.Semaphore,
    ) -> dict[str, Any]:
        item = pending_page.to_plan_item()
        params: dict[str, Any] = {
            "filter": self._filter_param(item),
            "page": pending_page.page,
            "per-page": pending_page.per_page,
            "select": OPENALEX_WORKS_SELECT,
        }
        if pending_page.sample:
            params["sample"] = item.sample_size
            params["seed"] = item.effective_seed
        params = self._with_auth_params(params)

        errors: list[dict[str, Any]] = []
        requests = 0
        async with semaphore:
            for attempt in range(self.max_retries + 1):
                await self.rate_limiter.acquire()
                requests += 1
                try:
                    response = await client.get("/works", params=params)
                except httpx.HTTPError as exc:
                    if attempt >= self.max_retries:
                        return self._failed_page(pending_page, requests, exc)
                    await asyncio.sleep(self._backoff_seconds(attempt))
                    continue

                if response.status_code == 429:
                    retry_delay = self._retry_delay(response, attempt)
                    if retry_delay > self.rate_limit_defer_after_seconds:
                        return {
                            **self._page_identity(pending_page),
                            "items": [],
                            "requests": requests,
                            "failed": 0,
                            "errors": [
                                {
                                    **self._page_identity(pending_page),
                                    "status_code": response.status_code,
                                    "retry_after_seconds": retry_delay,
                                    "body": response.text[:500],
                                }
                            ],
                            "deferred": True,
                            "retry_after_seconds": retry_delay,
                        }
                    if attempt >= self.max_retries:
                        errors.append(
                            {
                                **self._page_identity(pending_page),
                                "status_code": response.status_code,
                                "body": response.text[:500],
                            }
                        )
                        return {
                            **self._page_identity(pending_page),
                            "items": [],
                            "requests": requests,
                            "failed": 1,
                            "errors": errors,
                        }
                    await asyncio.sleep(retry_delay)
                    continue

                if response.status_code >= 500:
                    if attempt >= self.max_retries:
                        errors.append(
                            {
                                **self._page_identity(pending_page),
                                "status_code": response.status_code,
                                "body": response.text[:500],
                            }
                        )
                        return {
                            **self._page_identity(pending_page),
                            "items": [],
                            "requests": requests,
                            "failed": 1,
                            "errors": errors,
                        }
                    await asyncio.sleep(self._retry_delay(response, attempt))
                    continue

                if response.status_code >= 400:
                    return {
                        **self._page_identity(pending_page),
                        "items": [],
                        "requests": requests,
                        "failed": 1,
                        "errors": [
                            {
                                **self._page_identity(pending_page),
                                "status_code": response.status_code,
                                "body": response.text[:500],
                            }
                        ],
                    }

                try:
                    payload = response.json()
                except ValueError as exc:
                    return {
                        **self._page_identity(pending_page),
                        "items": [],
                        "requests": requests,
                        "failed": 1,
                        "errors": [
                            {
                                **self._page_identity(pending_page),
                                "code": "invalid_json",
                                "message": str(exc),
                            }
                        ],
                    }

                raw_results = (
                    payload.get("results") if isinstance(payload, dict) else []
                )
                if not isinstance(raw_results, list):
                    return {
                        **self._page_identity(pending_page),
                        "items": [],
                        "requests": requests,
                        "failed": 1,
                        "errors": [
                            {
                                **self._page_identity(pending_page),
                                "code": "invalid_response",
                                "message": "OpenAlex results payload is not a list",
                            }
                        ],
                    }

                page_offset = (pending_page.page - 1) * pending_page.per_page
                remaining = max(0, pending_page.sample_size - page_offset)
                expected_count = min(pending_page.per_page, remaining)
                raw_results = raw_results[:expected_count]
                valid_items = [
                    value for value in raw_results if isinstance(value, dict)
                ]
                exhausted = len(valid_items) < expected_count
                return {
                    **self._page_identity(pending_page),
                    "items": valid_items,
                    "requests": requests,
                    "failed": 0,
                    "errors": [],
                    "expected_count": expected_count,
                    "fetched_count": len(valid_items),
                    "exhausted": exhausted,
                    "exhausted_reason": "empty_page"
                    if exhausted and not valid_items
                    else "short_page"
                    if exhausted
                    else None,
                }

        return {
            **self._page_identity(pending_page),
            "items": [],
            "requests": requests,
            "failed": 0,
            "errors": [],
        }

    def _merge_page_results(
        self,
        result: OpenAlexDownloadResult,
        page_results: list[dict[str, Any]],
    ) -> None:
        for page_result in page_results:
            unit_key = str(page_result.get("unit_key") or "aggregate")
            summary = result.unit_summaries.setdefault(
                unit_key,
                OpenAlexUnitSummaryDTO(
                    unit_key=unit_key,
                    period=page_result.get("period"),
                    topic_id=page_result.get("topic_id"),
                    requested=int(page_result.get("requested_count") or 0),
                ),
            )
            fetched_count = int(
                page_result.get("fetched_count") or len(page_result["items"])
            )
            summary.fetched += fetched_count
            if page_result.get("exhausted") and not summary.exhausted:
                summary.exhausted = True
                summary.reason = str(
                    page_result.get("exhausted_reason") or "short_page"
                )

            result.openalex_requests += int(page_result["requests"])
            result.errors.extend(page_result["errors"])
            result.failed += int(page_result["failed"])
            for raw_item in page_result["items"]:
                result.fetched += 1
                try:
                    paper = OpenAlexAdapter.normalize_work(raw_item)
                except Exception as exc:
                    result.failed += 1
                    result.errors.append(
                        {
                            "external_id": raw_item.get("id"),
                            "code": exc.__class__.__name__,
                            "message": str(exc),
                        }
                    )
                    continue
                if not paper.title or not paper.title.strip():
                    result.skipped_empty_title += 1
                    continue
                if not paper.abstract or not paper.abstract.strip():
                    result.skipped_empty_abstract += 1
                    continue
                result.normalized += 1
                result.papers.append(paper)
                paper_key = self.paper_key(paper)
                unit_keys = result.paper_unit_keys.setdefault(paper_key, [])
                if unit_key not in unit_keys:
                    unit_keys.append(unit_key)

    def _ensure_unit_summary(
        self,
        result: OpenAlexDownloadResult,
        page: OpenAlexPendingPageDTO,
    ) -> None:
        result.unit_summaries.setdefault(
            page.unit_key,
            OpenAlexUnitSummaryDTO(
                unit_key=page.unit_key,
                period=page.period,
                topic_id=page.topic_id,
                requested=page.requested_count or page.sample_size,
            ),
        )

    def _plan_pages(
        self,
        plan: OpenAlexLoadPlanDTO,
        *,
        sample: bool,
        per_page: int,
    ) -> list[OpenAlexPendingPageDTO]:
        per_page = max(1, min(100, per_page))
        pages: list[OpenAlexPendingPageDTO] = []
        for item in plan.items:
            if item.sample_size <= 0:
                continue
            for page in range(1, ceil(item.sample_size / per_page) + 1):
                pages.append(
                    OpenAlexPendingPageDTO(
                        date_from=item.date_from,
                        date_to=item.date_to,
                        sample_size=item.sample_size,
                        language=item.language,
                        type=item.type,
                        filter_parts=item.filter_parts,
                        seed=item.seed,
                        seed_offset=item.seed_offset,
                        page=page,
                        per_page=per_page,
                        sample=sample,
                        unit_key=item.unit_key,
                        period=item.period,
                        topic_id=item.topic_id,
                        requested_count=item.requested_count or item.sample_size,
                    )
                )
        return pages

    def _filter_param(self, item: OpenAlexLoadPlanItemDTO) -> str:
        return ",".join(
            dict.fromkeys(
                [
                    f"type:{item.type}",
                    f"from_publication_date:{item.date_from.isoformat()}",
                    f"to_publication_date:{item.date_to.isoformat()}",
                    f"language:{item.language}",
                    "has_abstract:true",
                    *item.filter_parts,
                ]
            )
        )

    def _with_auth_params(self, params: dict[str, Any]) -> dict[str, Any]:
        result = dict(params)
        if self.api_key:
            result.setdefault("api_key", self.api_key)
        if self.mailto:
            result.setdefault("mailto", self.mailto)
        return result

    def _failed_page(
        self,
        page: OpenAlexPendingPageDTO,
        requests: int,
        exc: Exception,
    ) -> dict[str, Any]:
        return {
            **self._page_identity(page),
            "items": [],
            "requests": requests,
            "failed": 1,
            "errors": [
                {
                    **self._page_identity(page),
                    "code": exc.__class__.__name__,
                    "message": str(exc),
                }
            ],
        }

    def _page_identity(self, page: OpenAlexPendingPageDTO) -> dict[str, Any]:
        return {
            "date_from": page.date_from.isoformat(),
            "date_to": page.date_to.isoformat(),
            "language": page.language,
            "type": page.type,
            "page": page.page,
            "unit_key": page.unit_key,
            "period": page.period,
            "topic_id": page.topic_id,
            "requested_count": page.requested_count,
        }

    def _retry_delay(self, response: httpx.Response, attempt: int) -> float:
        retry_after = response.headers.get("Retry-After")
        if retry_after:
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

    def _backoff_seconds(self, attempt: int) -> float:
        return min(30.0, 0.5 * (2**attempt))

    def paper_key(self, paper: ExternalPaperDTO) -> str:
        if paper.external_id:
            return f"external:{paper.external_id}"
        if paper.doi:
            return f"doi:{paper.doi}"
        return f"title:{' '.join(paper.title.strip().lower().split())}:{paper.publication_year or ''}"


OpenAlexBootstrapDownloader = OpenAlexPaperDownloader


__all__ = [
    "OPENALEX_WORKS_SELECT",
    "OpenAlexBootstrapDownloader",
    "OpenAlexDownloadResult",
    "OpenAlexPaperDownloader",
]
