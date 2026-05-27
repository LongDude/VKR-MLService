from __future__ import annotations

from calendar import monthrange
from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True)
class OpenAlexPeriod:
    key: str
    date_from: date
    date_to: date


class OpenAlexPeriodService:
    """Build month-aligned OpenAlex loading periods."""

    def scope_periods(
        self,
        *,
        date_from: date,
        date_to: date,
        target_scope: str,
    ) -> list[OpenAlexPeriod]:
        normalized_from = date(date_from.year, date_from.month, 1)
        normalized_to = date(
            date_to.year,
            date_to.month,
            monthrange(date_to.year, date_to.month)[1],
        )
        if normalized_from > normalized_to:
            return []
        if target_scope == "year":
            return self._year_periods(normalized_from, normalized_to)
        if target_scope == "month":
            return self.month_periods(normalized_from, normalized_to)
        return self.month_periods(normalized_from, normalized_to)

    def month_periods(self, date_from: date, date_to: date) -> list[OpenAlexPeriod]:
        result: list[OpenAlexPeriod] = []
        year = date_from.year
        month = date_from.month
        while (year, month) <= (date_to.year, date_to.month):
            month_start = date(year, month, 1)
            month_end = date(year, month, monthrange(year, month)[1])
            period_start = max(date_from, month_start)
            period_end = min(date_to, month_end)
            if month == 1:
                period_start = max(period_start, date(year, 1, 2))
            if period_start <= period_end:
                result.append(
                    OpenAlexPeriod(
                        key=f"{year:04d}-{month:02d}",
                        date_from=period_start,
                        date_to=period_end,
                    )
                )
            if month == 12:
                year += 1
                month = 1
            else:
                month += 1
        return result

    def _year_periods(self, date_from: date, date_to: date) -> list[OpenAlexPeriod]:
        result: list[OpenAlexPeriod] = []
        for year in range(date_from.year, date_to.year + 1):
            year_start = date(year, 1, 1)
            year_end = date(year, 12, 31)
            period_start = max(date_from, year_start)
            period_end = min(date_to, year_end)
            if period_start <= period_end:
                result.append(
                    OpenAlexPeriod(
                        key=str(year),
                        date_from=period_start,
                        date_to=period_end,
                    )
                )
        return result


__all__ = ["OpenAlexPeriod", "OpenAlexPeriodService"]
