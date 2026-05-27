from __future__ import annotations

import csv
from calendar import monthrange
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

from adapters.redis_adapter import RedisAdapter


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

    def load_from_redis(
        self,
        redis_adapter: RedisAdapter,
        redis_key: str,
        *,
        date_from: date,
        date_to: date,
    ) -> list[MonthlyCount]:
        """Read monthly counts from Redis and return rows overlapping the period."""
        payload = redis_adapter.get_json(redis_key) or {}
        rows: list[MonthlyCount] = []
        for raw_item in self._redis_month_items(payload):
            item = self._parse_redis_item(raw_item)
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

    def missing_redis_months(
        self,
        redis_adapter: RedisAdapter,
        redis_key: str,
        *,
        date_from: date,
        date_to: date,
    ) -> list[MonthlyCount]:
        """Return full month periods missing from Redis for the date range."""
        payload = redis_adapter.get_json(redis_key) or {}
        existing_periods = {
            item.period
            for raw_item in self._redis_month_items(payload)
            if (item := self._parse_redis_item(raw_item)) is not None
        }
        return [
            item
            for item in self.iter_months(date_from, date_to)
            if item.period not in existing_periods
        ]

    def merge_into_redis(
        self,
        redis_adapter: RedisAdapter,
        redis_key: str,
        months: list[MonthlyCount],
        *,
        metadata: dict[str, Any] | None = None,
        ttl_seconds: int | None = None,
    ) -> None:
        """Merge monthly counts into the Redis stats document."""
        payload = redis_adapter.get_json(redis_key) or {}
        month_map: dict[str, dict[str, Any]] = {}
        for raw_item in self._redis_month_items(payload):
            item = self._parse_redis_item(raw_item)
            if item is None:
                continue
            month_map[item.period] = self._month_to_dict(item)
        for item in months:
            month_map[item.period] = self._month_to_dict(item)

        redis_adapter.set_json(
            redis_key,
            {
                "schema_version": 1,
                "metadata": metadata or payload.get("metadata") or {},
                "months": dict(sorted(month_map.items())),
            },
            ttl_seconds=ttl_seconds,
        )

    def iter_months(self, date_from: date, date_to: date) -> list[MonthlyCount]:
        """Return full calendar months overlapping the requested date range."""
        result: list[MonthlyCount] = []
        year = date_from.year
        month = date_from.month
        while (year, month) <= (date_to.year, date_to.month):
            month_start = date(year, month, 1)
            month_end = date(year, month, monthrange(year, month)[1])
            result.append(
                MonthlyCount(
                    period=f"{year:04d}-{month:02d}",
                    date_from=month_start,
                    date_to=month_end,
                    count=0,
                )
            )
            if month == 12:
                year += 1
                month = 1
            else:
                month += 1
        return result

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

    def _redis_month_items(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        months = payload.get("months")
        if isinstance(months, dict):
            return [
                {"period": period, **value}
                for period, value in months.items()
                if isinstance(value, dict)
            ]
        if isinstance(months, list):
            return [value for value in months if isinstance(value, dict)]
        return []

    def _parse_redis_item(self, item: dict[str, Any]) -> MonthlyCount | None:
        period = str(item.get("period") or "").strip()
        if not period:
            return None
        count = self._int_value(str(item.get("count"))) if item.get("count") is not None else None
        if count is None or count < 0:
            return None
        year_text, month_text = period.split("-", 1)
        year = int(year_text)
        month = int(month_text)
        month_start = date(year, month, 1)
        month_end = date(year, month, monthrange(year, month)[1])
        return MonthlyCount(
            period=period,
            date_from=self._date_value(str(item.get("date_from") or "")) or month_start,
            date_to=self._date_value(str(item.get("date_to") or "")) or month_end,
            count=count,
        )

    def _month_to_dict(self, item: MonthlyCount) -> dict[str, Any]:
        return {
            "period": item.period,
            "date_from": item.date_from.isoformat(),
            "date_to": item.date_to.isoformat(),
            "count": item.count,
        }


__all__ = ["MonthlyCount", "MonthlyCountsLoader"]
