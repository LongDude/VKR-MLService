from __future__ import annotations

from calendar import monthrange
from dataclasses import dataclass
from datetime import date

from core.exceptions import InvalidRequestError


@dataclass(frozen=True)
class QuarterPeriod:
    """Calendar quarter period with a stable report key."""

    key: str
    date_from: date
    date_to: date


class QuarterPeriodService:
    """Build full calendar quarters from arbitrary date ranges."""

    def quarter_periods(self, date_from: date, date_to: date) -> list[QuarterPeriod]:
        """Return all full quarters intersecting the supplied date range."""
        if date_from > date_to:
            raise InvalidRequestError(
                "date_from must be before or equal to date_to",
                details={"date_from": date_from, "date_to": date_to},
            )

        current = self.quarter_start(date_from)
        end = self.quarter_start(date_to)
        periods: list[QuarterPeriod] = []
        while current <= end:
            periods.append(
                QuarterPeriod(
                    key=self.period_key(current),
                    date_from=current,
                    date_to=self.quarter_end(current),
                )
            )
            current = self.next_quarter_start(current)
        return periods

    def period_key(self, period_start: date) -> str:
        """Return the YYYY-QN key for a quarter start date."""
        quarter = (period_start.month - 1) // 3 + 1
        return f"{period_start.year:04d}-Q{quarter}"

    def quarter_start(self, value: date) -> date:
        """Return the first day of the quarter containing value."""
        month = ((value.month - 1) // 3) * 3 + 1
        return date(value.year, month, 1)

    def quarter_end(self, quarter_start: date) -> date:
        """Return the last day of the supplied quarter."""
        end_month = quarter_start.month + 2
        return date(
            quarter_start.year,
            end_month,
            monthrange(quarter_start.year, end_month)[1],
        )

    def next_quarter_start(self, quarter_start: date) -> date:
        """Return the first day of the next quarter."""
        if quarter_start.month == 10:
            return date(quarter_start.year + 1, 1, 1)
        return date(quarter_start.year, quarter_start.month + 3, 1)


__all__ = ["QuarterPeriod", "QuarterPeriodService"]
