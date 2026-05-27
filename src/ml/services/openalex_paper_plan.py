from __future__ import annotations

from math import ceil

from core.exceptions import InvalidRequestError
from dto.openalex import (
    OpenAlexBootstrapRequestDTO,
    OpenAlexLoadPlanDTO,
    OpenAlexLoadPlanItemDTO,
    OpenAlexPlanUnitDTO,
)
from ml.services.openalex_periods import OpenAlexPeriod, OpenAlexPeriodService


OPENALEX_MAX_SAMPLE = 10_000


class OpenAlexPaperPlanService:
    """Build one-pass OpenAlex paper loading plans."""

    def __init__(
        self,
        *,
        period_service: OpenAlexPeriodService | None = None,
        max_sample_size: int = OPENALEX_MAX_SAMPLE,
    ) -> None:
        self.period_service = period_service or OpenAlexPeriodService()
        self.max_sample_size = max_sample_size

    def build(
        self,
        request: OpenAlexBootstrapRequestDTO,
        *,
        seed: int,
    ) -> OpenAlexLoadPlanDTO:
        if request.date_from > request.date_to:
            raise InvalidRequestError(
                "date_from must be less than or equal to date_to",
                details={"date_from": request.date_from, "date_to": request.date_to},
            )
        if request.target_count <= 0:
            return OpenAlexLoadPlanDTO()
        if request.target_count > self.max_sample_size:
            raise self._quota_error(request.target_count)

        language_filter = self._or_filter_value(request.languages, "languages")
        type_filter = self._or_filter_value(request.types, "types")
        units = self._quota_units(request)

        items: list[OpenAlexLoadPlanItemDTO] = []
        seed_offset = 0
        for unit in units:
            subperiods = self._request_periods(request, unit)
            quotas = self._equal_quotas(unit.requested, len(subperiods))
            for period, quota in zip(subperiods, quotas, strict=True):
                if quota <= 0:
                    continue
                items.append(
                    OpenAlexLoadPlanItemDTO(
                        date_from=period.date_from,
                        date_to=period.date_to,
                        sample_size=quota,
                        language=language_filter,
                        type=type_filter,
                        filter_parts=unit.filter_parts,
                        seed=seed,
                        seed_offset=seed_offset,
                        unit_key=unit.unit_key,
                        period=unit.period,
                        topic_id=unit.topic_id,
                        requested_count=unit.requested,
                    )
                )
                seed_offset += 1

        return OpenAlexLoadPlanDTO(
            items=items,
            units=units,
            total_sample_count=sum(item.sample_size for item in items),
            estimated_requests=sum(
                ceil(item.sample_size / request.per_page) for item in items
            ),
            normalize="none",
        )

    def _quota_units(
        self,
        request: OpenAlexBootstrapRequestDTO,
    ) -> list[OpenAlexPlanUnitDTO]:
        periods = self.period_service.scope_periods(
            date_from=request.date_from,
            date_to=request.date_to,
            target_scope=request.target_count_scope,
        )
        if request.target_count_scope in {"total", "period"}:
            periods = [
                OpenAlexPeriod(
                    key="period",
                    date_from=min(period.date_from for period in periods),
                    date_to=max(period.date_to for period in periods),
                )
            ] if periods else []

        if request.target_count_unit == "topic":
            if not request.topic_targets:
                raise InvalidRequestError("topic targets are required for topic quota units")
            units: list[OpenAlexPlanUnitDTO] = []
            for period in periods:
                for target in request.topic_targets:
                    filter_parts = list(
                        dict.fromkeys([*request.openalex_filter_parts, target.filter_part])
                    )
                    units.append(
                        OpenAlexPlanUnitDTO(
                            unit_key=f"{period.key}:topic:{target.topic_id}",
                            period=period.key,
                            date_from=period.date_from,
                            date_to=period.date_to,
                            requested=request.target_count,
                            topic_id=target.topic_id,
                            filter_parts=filter_parts,
                        )
                    )
            return units

        return [
            OpenAlexPlanUnitDTO(
                unit_key=period.key,
                period=period.key,
                date_from=period.date_from,
                date_to=period.date_to,
                requested=request.target_count,
                filter_parts=list(request.openalex_filter_parts),
            )
            for period in periods
        ]

    def _request_periods(
        self,
        request: OpenAlexBootstrapRequestDTO,
        unit: OpenAlexPlanUnitDTO,
    ) -> list[OpenAlexPeriod]:
        if request.target_count_scope in {"total", "period"}:
            return self.period_service.month_periods(unit.date_from, unit.date_to)
        return [
            OpenAlexPeriod(
                key=unit.period,
                date_from=unit.date_from,
                date_to=unit.date_to,
            )
        ]

    def _quota_error(self, value: int) -> InvalidRequestError:
        return InvalidRequestError(
            "OpenAlex paper quota exceeds the maximum sample size for one quota unit",
            details={
                "target_count": value,
                "max_sample_size": self.max_sample_size,
                "hint": (
                    "Lower --target-count, use --target-scope month/year, "
                    "or narrow the taxonomy/date filters."
                ),
            },
        )

    def _or_filter_value(self, values: list[str], field_name: str) -> str:
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


__all__ = ["OPENALEX_MAX_SAMPLE", "OpenAlexPaperPlanService"]
