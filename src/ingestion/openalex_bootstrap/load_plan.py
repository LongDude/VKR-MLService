from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from math import ceil, floor

from core.exceptions import InvalidRequestError
from adapters.redis_adapter import RedisAdapter
from ingestion.openalex_bootstrap.dto import (
    NormalizeMode,
    OpenAlexBootstrapRequestDTO,
    OpenAlexLoadPlanDTO,
    OpenAlexLoadPlanItemDTO,
)
from ingestion.openalex_bootstrap.monthly_counts import MonthlyCount, MonthlyCountsLoader


OPENALEX_MAX_SAMPLE = 10_000


@dataclass(frozen=True)
class _WeightedPeriod:
    date_from: date
    date_to: date
    count: int


class OpenAlexLoadPlanBuilder:
    """Build OpenAlex sampling plans from target counts and monthly statistics."""

    def __init__(
        self,
        *,
        monthly_counts_loader: MonthlyCountsLoader | None = None,
        redis_adapter: RedisAdapter | None = None,
        max_sample_size: int = OPENALEX_MAX_SAMPLE,
    ) -> None:
        self.monthly_counts_loader = monthly_counts_loader or MonthlyCountsLoader()
        self.redis_adapter = redis_adapter
        self.max_sample_size = max_sample_size

    def build(
        self,
        request: OpenAlexBootstrapRequestDTO,
        *,
        target_new_count: int,
        seed: int,
    ) -> OpenAlexLoadPlanDTO:
        """Create a concrete request plan for one bootstrap round."""
        if request.date_from > request.date_to:
            raise InvalidRequestError(
                "date_from must be less than or equal to date_to",
                details={"date_from": request.date_from, "date_to": request.date_to},
            )
        if target_new_count <= 0:
            return OpenAlexLoadPlanDTO(normalize=request.normalize)

        base_periods = self._base_periods(request, target_new_count)
        items: list[OpenAlexLoadPlanItemDTO] = []
        seed_offset = 0
        max_item_size = self.max_sample_size if request.sample else target_new_count
        language_filter = self._or_filter_value(request.languages, "languages")
        type_filter = self._or_filter_value(request.types, "types")
        for period, quota in base_periods:
            if quota <= 0:
                continue
            pieces, seed_offset = self._split_period(
                period.date_from,
                period.date_to,
                quota,
                language_filter,
                type_filter,
                seed,
                seed_offset,
                max_item_size,
            )
            items.extend(pieces)

        total_sample_count = sum(item.sample_size for item in items)
        return OpenAlexLoadPlanDTO(
            items=items,
            total_sample_count=total_sample_count,
            estimated_requests=sum(ceil(item.sample_size / request.per_page) for item in items),
            normalize=request.normalize,
        )

    def _base_periods(
        self,
        request: OpenAlexBootstrapRequestDTO,
        target_new_count: int,
    ) -> list[tuple[_WeightedPeriod, int]]:
        if request.normalize == "none":
            return [
                (
                    _WeightedPeriod(request.date_from, request.date_to, target_new_count),
                    target_new_count,
                )
            ]

        months: list[MonthlyCount]
        if request.monthly_stats_source == "redis":
            if self.redis_adapter is None:
                raise InvalidRequestError(
                    "Redis connection is required for normalized OpenAlex bootstrap",
                    details={"normalize": request.normalize},
                )
            if not request.monthly_counts_redis_key:
                raise InvalidRequestError(
                    "monthly_counts_redis_key is required for Redis monthly stats",
                    details={"normalize": request.normalize},
                )
            months = self.monthly_counts_loader.load_from_redis(
                self.redis_adapter,
                request.monthly_counts_redis_key,
                date_from=request.date_from,
                date_to=request.date_to,
            )
        elif request.monthly_stats_source == "csv":
            if not request.monthly_counts_csv:
                raise InvalidRequestError(
                    "monthly_counts_csv is required for normalized OpenAlex bootstrap",
                    details={"normalize": request.normalize},
                )
            months = self.monthly_counts_loader.load(
                request.monthly_counts_csv,
                date_from=request.date_from,
                date_to=request.date_to,
            )
        else:
            raise InvalidRequestError(
                "Unsupported monthly stats source",
                details={"source": request.monthly_stats_source},
            )
        if not months:
            raise InvalidRequestError(
                "No monthly count rows overlap the requested date range",
                details={
                    "date_from": request.date_from,
                    "date_to": request.date_to,
                    "monthly_stats_source": request.monthly_stats_source,
                    "monthly_counts_csv": request.monthly_counts_csv,
                    "monthly_counts_redis_key": request.monthly_counts_redis_key,
                },
            )
        if request.normalize == "month":
            weighted = [
                _WeightedPeriod(item.date_from, item.date_to, item.count)
                for item in months
            ]
        elif request.normalize == "year":
            weighted = self._year_periods(months)
        else:
            raise InvalidRequestError(
                "Unsupported normalization mode",
                details={"normalize": request.normalize},
            )
        return list(zip(weighted, self._weighted_quotas(weighted, target_new_count)))

    def _year_periods(self, months: list[MonthlyCount]) -> list[_WeightedPeriod]:
        grouped: dict[int, list[MonthlyCount]] = defaultdict(list)
        for month in months:
            grouped[month.date_from.year].append(month)
        result: list[_WeightedPeriod] = []
        for year in sorted(grouped):
            values = grouped[year]
            result.append(
                _WeightedPeriod(
                    date_from=min(item.date_from for item in values),
                    date_to=max(item.date_to for item in values),
                    count=sum(item.count for item in values),
                )
            )
        return result

    def _weighted_quotas(
        self,
        periods: list[_WeightedPeriod],
        target_new_count: int,
    ) -> list[int]:
        total_count = sum(max(0, item.count) for item in periods)
        if total_count <= 0:
            return self._equal_quotas(target_new_count, len(periods))

        raw_values = [
            (target_new_count * max(0, item.count)) / total_count
            for item in periods
        ]
        quotas = [floor(value) for value in raw_values]
        remainder = target_new_count - sum(quotas)
        fractions = sorted(
            enumerate(raw_values),
            key=lambda pair: (pair[1] - floor(pair[1]), -pair[0]),
            reverse=True,
        )
        for index, _ in fractions[:remainder]:
            quotas[index] += 1
        return quotas

    def _or_filter_value(
        self,
        values: list[str],
        field_name: str,
    ) -> str:
        cleaned = list(dict.fromkeys(value.strip() for value in values if value.strip()))
        if not cleaned:
            raise InvalidRequestError(
                f"At least one OpenAlex {field_name} value is required",
                details={field_name: values},
            )
        return "|".join(cleaned)

    def _equal_quotas(self, total: int, parts: int) -> list[int]:
        if parts <= 0:
            return []
        base = total // parts
        remainder = total % parts
        return [base + (1 if index < remainder else 0) for index in range(parts)]

    def _split_period(
        self,
        date_from: date,
        date_to: date,
        sample_size: int,
        language: str,
        publication_type: str,
        seed: int,
        seed_offset: int,
        max_item_size: int,
    ) -> tuple[list[OpenAlexLoadPlanItemDTO], int]:
        if sample_size <= max_item_size:
            return [
                OpenAlexLoadPlanItemDTO(
                    date_from=date_from,
                    date_to=date_to,
                    sample_size=sample_size,
                    language=language,
                    type=publication_type,
                    seed=seed,
                    seed_offset=seed_offset,
                )
            ], seed_offset + 1

        days = (date_to - date_from).days + 1
        if days <= 1:
            quotas = self._equal_quotas(
                sample_size,
                ceil(sample_size / max_item_size),
            )
            items: list[OpenAlexLoadPlanItemDTO] = []
            for quota in quotas:
                items.append(
                    OpenAlexLoadPlanItemDTO(
                        date_from=date_from,
                        date_to=date_to,
                        sample_size=quota,
                        language=language,
                        type=publication_type,
                        seed=seed,
                        seed_offset=seed_offset,
                    )
                )
                seed_offset += 1
            return items, seed_offset

        quotas = self._equal_quotas(sample_size, days)
        items = []
        current = date_from
        for quota in quotas:
            split_items, seed_offset = self._split_period(
                current,
                current,
                quota,
                language,
                publication_type,
                seed,
                seed_offset,
                max_item_size,
            )
            items.extend(split_items)
            current += timedelta(days=1)
        return items, seed_offset


__all__ = ["OPENALEX_MAX_SAMPLE", "OpenAlexLoadPlanBuilder"]
