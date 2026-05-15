from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any

import httpx
from tqdm.auto import tqdm

from adapters.redis_adapter import RedisAdapter
from ingestion.openalex_bootstrap.monthly_counts import MonthlyCount, MonthlyCountsLoader
from ingestion.openalex_bootstrap.rate_limiter import AsyncRateLimiter


@dataclass
class OpenAlexStatsCollectionResult:
    redis_key: str
    date_from: date
    date_to: date
    collected_months: int = 0
    openalex_requests: int = 0
    failed: int = 0
    errors: list[dict[str, Any]] = field(default_factory=list)
    months: list[MonthlyCount] = field(default_factory=list)


class OpenAlexMonthlyStatsCollector:
    """Collect monthly OpenAlex work counts and persist them in Redis."""

    def __init__(
        self,
        *,
        redis_adapter: RedisAdapter,
        redis_key: str,
        base_url: str = "https://api.openalex.org",
        request_workers: int = 8,
        rate_limiter: AsyncRateLimiter | None = None,
        max_retries: int = 5,
        timeout_seconds: float = 30.0,
        ttl_seconds: int | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
        monthly_counts_loader: MonthlyCountsLoader | None = None,
    ) -> None:
        self.redis_adapter = redis_adapter
        self.redis_key = redis_key
        self.base_url = base_url.rstrip("/")
        self.request_workers = max(1, request_workers)
        self.rate_limiter = rate_limiter or AsyncRateLimiter(70)
        self.max_retries = max(0, max_retries)
        self.timeout_seconds = timeout_seconds
        self.ttl_seconds = ttl_seconds
        self.transport = transport
        self.monthly_counts_loader = monthly_counts_loader or MonthlyCountsLoader()

    async def collect_and_store(
        self,
        *,
        date_from: date,
        date_to: date,
        languages: list[str],
        types: list[str],
        missing_only: bool = False,
        show_progress: bool = True,
    ) -> OpenAlexStatsCollectionResult:
        """Collect full calendar month counts for the period and store them in Redis."""
        months = self.monthly_counts_loader.iter_months(date_from, date_to)
        if missing_only:
            missing_periods = {
                item.period
                for item in self.monthly_counts_loader.missing_redis_months(
                    self.redis_adapter,
                    self.redis_key,
                    date_from=date_from,
                    date_to=date_to,
                )
            }
            months = [item for item in months if item.period in missing_periods]

        result = OpenAlexStatsCollectionResult(
            redis_key=self.redis_key,
            date_from=date_from,
            date_to=date_to,
        )
        if not months:
            return result

        combos = [
            (language.strip(), publication_type.strip())
            for language in languages
            for publication_type in types
            if language.strip() and publication_type.strip()
        ]
        semaphore = asyncio.Semaphore(self.request_workers)
        progress_bar = (
            tqdm(total=len(months) * len(combos), desc="OpenAlex stats", unit="count")
            if show_progress
            else None
        )

        async with httpx.AsyncClient(
            base_url=self.base_url,
            timeout=self.timeout_seconds,
            transport=self.transport,
        ) as client:
            try:
                tasks = [
                    self._collect_month(client, month, combos, semaphore, progress_bar)
                    for month in months
                ]
                month_results = await asyncio.gather(*tasks)
            finally:
                if progress_bar is not None:
                    progress_bar.close()

        collected_months: list[MonthlyCount] = []
        for month, payload in month_results:
            result.openalex_requests += int(payload["requests"])
            result.failed += int(payload["failed"])
            result.errors.extend(payload["errors"])
            if int(payload["failed"]):
                continue
            collected_months.append(
                MonthlyCount(
                    period=month.period,
                    date_from=month.date_from,
                    date_to=month.date_to,
                    count=int(payload["count"]),
                )
            )

        self.monthly_counts_loader.merge_into_redis(
            self.redis_adapter,
            self.redis_key,
            collected_months,
            metadata={
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "languages": languages,
                "types": types,
                "source": "openalex",
            },
            ttl_seconds=self.ttl_seconds,
        )
        result.months = collected_months
        result.collected_months = len(collected_months)
        return result

    async def _collect_month(
        self,
        client: httpx.AsyncClient,
        month: MonthlyCount,
        combos: list[tuple[str, str]],
        semaphore: asyncio.Semaphore,
        progress_bar: Any,
    ) -> tuple[MonthlyCount, dict[str, Any]]:
        requests = 0
        failed = 0
        total_count = 0
        errors: list[dict[str, Any]] = []
        async with semaphore:
            for language, publication_type in combos:
                count_payload = await self._fetch_count(
                    client,
                    month,
                    language,
                    publication_type,
                )
                requests += int(count_payload["requests"])
                failed += int(count_payload["failed"])
                total_count += int(count_payload["count"])
                errors.extend(count_payload["errors"])
                if progress_bar is not None:
                    progress_bar.update(1)
        return month, {
            "count": total_count,
            "requests": requests,
            "failed": failed,
            "errors": errors,
        }

    async def _fetch_count(
        self,
        client: httpx.AsyncClient,
        month: MonthlyCount,
        language: str,
        publication_type: str,
    ) -> dict[str, Any]:
        params = {
            "filter": ",".join(
                [
                    f"type:{publication_type}",
                    f"from_publication_date:{month.date_from.isoformat()}",
                    f"to_publication_date:{month.date_to.isoformat()}",
                    f"language:{language}",
                ]
            ),
            "per-page": 1,
            "select": "id",
        }
        requests = 0
        for attempt in range(self.max_retries + 1):
            await self.rate_limiter.acquire()
            requests += 1
            try:
                response = await client.get("/works", params=params)
            except httpx.HTTPError as exc:
                if attempt >= self.max_retries:
                    return self._failed_count(month, language, publication_type, requests, exc)
                await asyncio.sleep(self._backoff_seconds(attempt))
                continue

            if response.status_code == 429 or response.status_code >= 500:
                if attempt >= self.max_retries:
                    return {
                        "count": 0,
                        "requests": requests,
                        "failed": 1,
                        "errors": [
                            {
                                "period": month.period,
                                "language": language,
                                "type": publication_type,
                                "status_code": response.status_code,
                                "body": response.text[:500],
                            }
                        ],
                    }
                await asyncio.sleep(self._retry_delay(response, attempt))
                continue

            if response.status_code >= 400:
                return {
                    "count": 0,
                    "requests": requests,
                    "failed": 1,
                    "errors": [
                        {
                            "period": month.period,
                            "language": language,
                            "type": publication_type,
                            "status_code": response.status_code,
                            "body": response.text[:500],
                        }
                    ],
                }

            payload = response.json()
            meta = payload.get("meta") if isinstance(payload, dict) else {}
            return {
                "count": int(meta.get("count") or 0) if isinstance(meta, dict) else 0,
                "requests": requests,
                "failed": 0,
                "errors": [],
            }

        return {"count": 0, "requests": requests, "failed": 0, "errors": []}

    def _failed_count(
        self,
        month: MonthlyCount,
        language: str,
        publication_type: str,
        requests: int,
        exc: Exception,
    ) -> dict[str, Any]:
        return {
            "count": 0,
            "requests": requests,
            "failed": 1,
            "errors": [
                {
                    "period": month.period,
                    "language": language,
                    "type": publication_type,
                    "code": exc.__class__.__name__,
                    "message": str(exc),
                }
            ],
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


__all__ = ["OpenAlexMonthlyStatsCollector", "OpenAlexStatsCollectionResult"]
