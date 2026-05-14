from __future__ import annotations

import csv
from calendar import monthrange
from dataclasses import dataclass
from datetime import date
from pathlib import Path


@dataclass(frozen=True)
class MonthlyCount:
    period: str
    date_from: date
    date_to: date
    count: int


class MonthlyCountsLoader:
    """Load OpenAlex publication counts from the project CSV formats."""

    def load(
        self,
        csv_path: str | Path,
        *,
        date_from: date,
        date_to: date,
    ) -> list[MonthlyCount]:
        """Read monthly counts and return rows overlapping the requested period."""
        rows: list[MonthlyCount] = []
        with Path(csv_path).open("r", encoding="utf-8", newline="") as file:
            reader = csv.DictReader(file)
            for raw_row in reader:
                item = self._parse_row(raw_row)
                if item is None:
                    continue
                if item.date_to < date_from or item.date_from > date_to:
                    continue
                rows.append(
                    MonthlyCount(
                        period=item.period,
                        date_from=max(item.date_from, date_from),
                        date_to=min(item.date_to, date_to),
                        count=item.count,
                    )
                )
        return rows

    def _parse_row(self, row: dict[str, str]) -> MonthlyCount | None:
        count = self._int_value(row.get("count"))
        if count is None or count < 0:
            return None

        period_value = (row.get("period") or "").strip()
        if period_value:
            year_text, month_text = period_value.split("-", 1)
            year = int(year_text)
            month = int(month_text)
        else:
            year = int((row.get("year") or "").strip())
            month = int((row.get("month") or "").strip())

        month_start = date(year, month, 1)
        month_end = date(year, month, monthrange(year, month)[1])
        date_from = self._date_value(row.get("date_from")) or month_start
        date_to = self._date_value(row.get("date_to")) or month_end
        return MonthlyCount(
            period=f"{year:04d}-{month:02d}",
            date_from=date_from,
            date_to=date_to,
            count=count,
        )

    def _int_value(self, value: str | None) -> int | None:
        if value is None or not value.strip():
            return None
        return int(value)

    def _date_value(self, value: str | None) -> date | None:
        if value is None or not value.strip():
            return None
        return date.fromisoformat(value.strip())


__all__ = ["MonthlyCount", "MonthlyCountsLoader"]
