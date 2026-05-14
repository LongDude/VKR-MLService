from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from math import ceil
from typing import Any

import httpx

from adapters.openalex_adapter import OpenAlexAdapter
from dto.external import ExternalPaperDTO
from ingestion.openalex_bootstrap.dto import OpenAlexLoadPlanDTO, OpenAlexLoadPlanItemDTO
from ingestion.openalex_bootstrap.rate_limiter import AsyncRateLimiter


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
        "authorships",
        "topics",
        "keywords",
        "locations",
        "primary_location",
    ]
)


@dataclass
class OpenAlexDownloadResult:
    papers: list[ExternalPaperDTO] = field(default_factory=list)
    fetched: int = 0
    normalized: int = 0
    skipped_empty_title: int = 0
    failed: int = 0
    openalex_requests: int = 0
    errors: list[dict[str, Any]] = field(default_factory=list)


class _UnusedClient:
    def get(self, *_args: Any, **_kwargs: Any) -> Any:
        raise RuntimeError("Synchronous OpenAlex client is not used by bootstrap")


class OpenAlexBootstrapDownloader:
    """Download OpenAlex works concurrently and normalize them to ExternalPaperDTO."""

    def __init__(
        self,
        *,
        base_url: str = "https://api.openalex.org",
        timeout_seconds: float = 30.0,
        request_workers: int = 8,
        rate_limiter: AsyncRateLimiter | None = None,
        max_retries: int = 5,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.request_workers = max(1, request_workers)
        self.rate_limiter = rate_limiter or AsyncRateLimiter(70)
        self.max_retries = max(0, max_retries)
        self.transport = transport
        self._normalizer = OpenAlexAdapter(base_url=base_url, client=_UnusedClient())

    async def fetch_plan(
        self,
        plan: OpenAlexLoadPlanDTO,
        *,
        sample: bool,
        per_page: int = 100,
    ) -> OpenAlexDownloadResult:
        """Fetch all plan items with one shared AsyncClient."""
        result = OpenAlexDownloadResult()
        semaphore = asyncio.Semaphore(self.request_workers)
        per_page = max(1, min(100, per_page))

        async with httpx.AsyncClient(
            base_url=self.base_url,
            timeout=self.timeout_seconds,
            transport=self.transport,
        ) as client:
            item_results = await asyncio.gather(
                *[
                    self._fetch_item(
                        client,
                        item,
                        sample=sample,
                        per_page=per_page,
                        semaphore=semaphore,
                    )
                    for item in plan.items
                    if item.sample_size > 0
                ]
            )

        for item_result in item_results:
            result.papers.extend(item_result.papers)
            result.fetched += item_result.fetched
            result.normalized += item_result.normalized
            result.skipped_empty_title += item_result.skipped_empty_title
            result.failed += item_result.failed
            result.openalex_requests += item_result.openalex_requests
            result.errors.extend(item_result.errors)
        return result

    async def _fetch_item(
        self,
        client: httpx.AsyncClient,
        item: OpenAlexLoadPlanItemDTO,
        *,
        sample: bool,
        per_page: int,
        semaphore: asyncio.Semaphore,
    ) -> OpenAlexDownloadResult:
        pages = ceil(item.sample_size / per_page)
        page_results = await asyncio.gather(
            *[
                self._fetch_page(
                    client,
                    item,
                    page=page,
                    per_page=per_page,
                    sample=sample,
                    semaphore=semaphore,
                )
                for page in range(1, pages + 1)
            ]
        )

        result = OpenAlexDownloadResult()
        raw_items: list[dict[str, Any]] = []
        for page_result in page_results:
            raw_items.extend(page_result["items"])
            result.openalex_requests += int(page_result["requests"])
            result.errors.extend(page_result["errors"])
            result.failed += int(page_result["failed"])

        for raw_item in raw_items[: item.sample_size]:
            result.fetched += 1
            try:
                paper = self._normalizer._normalize_work(raw_item)
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
            result.normalized += 1
            result.papers.append(paper)
        return result

    async def _fetch_page(
        self,
        client: httpx.AsyncClient,
        item: OpenAlexLoadPlanItemDTO,
        *,
        page: int,
        per_page: int,
        sample: bool,
        semaphore: asyncio.Semaphore,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "filter": self._filter_param(item),
            "page": page,
            "per-page": per_page,
            "select": OPENALEX_WORKS_SELECT,
        }
        if sample:
            params["sample"] = item.sample_size
            params["seed"] = item.effective_seed

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
                        return {
                            "items": [],
                            "requests": requests,
                            "failed": 1,
                            "errors": [self._request_error(item, page, exc)],
                        }
                    await asyncio.sleep(self._backoff_seconds(attempt))
                    continue

                if response.status_code == 429 or response.status_code >= 500:
                    if attempt >= self.max_retries:
                        errors.append(
                            {
                                "date_from": item.date_from.isoformat(),
                                "date_to": item.date_to.isoformat(),
                                "language": item.language,
                                "type": item.type,
                                "page": page,
                                "status_code": response.status_code,
                                "body": response.text[:500],
                            }
                        )
                        return {
                            "items": [],
                            "requests": requests,
                            "failed": 1,
                            "errors": errors,
                        }
                    await asyncio.sleep(self._retry_delay(response, attempt))
                    continue

                if response.status_code >= 400:
                    return {
                        "items": [],
                        "requests": requests,
                        "failed": 1,
                        "errors": [
                            {
                                "date_from": item.date_from.isoformat(),
                                "date_to": item.date_to.isoformat(),
                                "language": item.language,
                                "type": item.type,
                                "page": page,
                                "status_code": response.status_code,
                                "body": response.text[:500],
                            }
                        ],
                    }

                try:
                    payload = response.json()
                except ValueError as exc:
                    return {
                        "items": [],
                        "requests": requests,
                        "failed": 1,
                        "errors": [
                            {
                                "date_from": item.date_from.isoformat(),
                                "date_to": item.date_to.isoformat(),
                                "language": item.language,
                                "type": item.type,
                                "page": page,
                                "code": "invalid_json",
                                "message": str(exc),
                            }
                        ],
                    }
                raw_results = payload.get("results") if isinstance(payload, dict) else []
                if not isinstance(raw_results, list):
                    return {
                        "items": [],
                        "requests": requests,
                        "failed": 1,
                        "errors": [
                            {
                                "date_from": item.date_from.isoformat(),
                                "date_to": item.date_to.isoformat(),
                                "language": item.language,
                                "type": item.type,
                                "page": page,
                                "code": "invalid_response",
                                "message": "OpenAlex results payload is not a list",
                            }
                        ],
                    }
                return {
                    "items": [value for value in raw_results if isinstance(value, dict)],
                    "requests": requests,
                    "failed": 0,
                    "errors": [],
                }

        return {"items": [], "requests": requests, "failed": 0, "errors": []}

    def _filter_param(self, item: OpenAlexLoadPlanItemDTO) -> str:
        return ",".join(
            [
                f"type:{item.type}",
                f"from_publication_date:{item.date_from.isoformat()}",
                f"to_publication_date:{item.date_to.isoformat()}",
                f"language:{item.language}",
            ]
        )

    def _request_error(
        self,
        item: OpenAlexLoadPlanItemDTO,
        page: int,
        exc: Exception,
    ) -> dict[str, Any]:
        return {
            "date_from": item.date_from.isoformat(),
            "date_to": item.date_to.isoformat(),
            "language": item.language,
            "type": item.type,
            "page": page,
            "code": exc.__class__.__name__,
            "message": str(exc),
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
        return min(30.0, 0.5 * (2 ** attempt))


__all__ = [
    "OPENALEX_WORKS_SELECT",
    "OpenAlexBootstrapDownloader",
    "OpenAlexDownloadResult",
]
