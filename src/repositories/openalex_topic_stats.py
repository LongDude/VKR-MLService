from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

from sqlalchemy import bindparam, func, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from models import OpenAlexMonthlyTopicStat

from .base import BaseRepository


@dataclass(frozen=True)
class OpenAlexTopicMonthlyCount:
    """Count of OpenAlex works for one local topic and one calendar month."""

    topic_id: int
    period_start: date
    works_count: int


class OpenAlexTopicStatsRepository(BaseRepository):
    """Persist OpenAlex monthly topic publication counters."""

    def load_period_stats(
        self,
        *,
        date_from: date,
        date_to: date,
        granularity: str = "month",
        topic_ids: list[int] | None = None,
    ) -> list[dict[str, Any]]:
        """Return aggregated OpenAlex topic counters for charts.

        ``granularity`` controls calendar aggregation and must be one of
        ``month``, ``quarter`` or ``year``. The repository performs read-only
        SELECT queries and returns plain dictionaries suitable for pandas.
        """
        period_unit = self._validate_granularity(granularity)
        params: dict[str, Any] = {
            "date_from": date_from,
            "date_to": date_to,
        }
        topic_filter = ""
        if topic_ids:
            topic_filter = "AND s.topic_id IN :topic_ids"
            params["topic_ids"] = sorted(set(topic_ids))

        stmt = text(
            f"""
            SELECT
                CONCAT('topic:', t.id) AS cluster_id,
                t.id AS topic_id,
                t.name AS topic_name,
                d.id AS domain_id,
                d.name AS domain_name,
                f.id AS field_id,
                f.name AS field_name,
                sf.id AS subfield_id,
                sf.name AS subfield_name,
                DATE_TRUNC('{period_unit}', s.period_start)::date AS period_start,
                SUM(s.works_count)::bigint AS works_count
            FROM openalex_montly_topic_stats s
            JOIN topics t ON t.id = s.topic_id
            LEFT JOIN subfields sf ON sf.id = t.subfield_id
            LEFT JOIN fields f ON f.id = sf.field_id
            LEFT JOIN domains d ON d.id = f.domain_id
            WHERE s.period_start BETWEEN :date_from AND :date_to
              {topic_filter}
            GROUP BY
                t.id, t.name,
                d.id, d.name,
                f.id, f.name,
                sf.id, sf.name,
                DATE_TRUNC('{period_unit}', s.period_start)
            ORDER BY period_start, topic_name
            """
        )
        if topic_ids:
            stmt = stmt.bindparams(bindparam("topic_ids", expanding=True))
        return [dict(row) for row in self.session.execute(stmt, params).mappings()]

    def load_topic_totals(
        self,
        *,
        date_from: date,
        date_to: date,
        topic_ids: list[int] | None = None,
    ) -> list[dict[str, Any]]:
        """Return total OpenAlex work counters by topic for a date range."""
        params: dict[str, Any] = {
            "date_from": date_from,
            "date_to": date_to,
        }
        topic_filter = ""
        if topic_ids:
            topic_filter = "AND s.topic_id IN :topic_ids"
            params["topic_ids"] = sorted(set(topic_ids))

        stmt = text(
            f"""
            SELECT
                CONCAT('topic:', t.id) AS cluster_id,
                t.id AS topic_id,
                t.name AS topic_name,
                SUM(s.works_count)::bigint AS total_works_count,
                MIN(s.period_start)::date AS first_period,
                MAX(s.period_start)::date AS last_period
            FROM openalex_montly_topic_stats s
            JOIN topics t ON t.id = s.topic_id
            WHERE s.period_start BETWEEN :date_from AND :date_to
              {topic_filter}
            GROUP BY t.id, t.name
            ORDER BY total_works_count DESC, topic_name
            """
        )
        if topic_ids:
            stmt = stmt.bindparams(bindparam("topic_ids", expanding=True))
        return [dict(row) for row in self.session.execute(stmt, params).mappings()]

    def upsert_many(
        self,
        items: list[OpenAlexTopicMonthlyCount],
    ) -> tuple[int, int]:
        """Insert or update topic/month counters.

        Returns ``(created, updated)`` estimated from rows that already existed
        before the upsert. The caller owns transaction commit/rollback.

        Примечание: так как подсчитать количество перезаписанных строк (UPDATE) 
        можно только доп запросом, то обновления также входят в число created
        """
        if not items:
            return 0, 0

        deduplicated = self._deduplicate(items)
        # existing_keys = self._existing_keys(deduplicated)
        # created = len(deduplicated) - len(existing_keys)
        # updated = len(existing_keys)

        updated = 0
        created = len(deduplicated)

        if self._is_postgresql():
            values = [
                {
                    "topic_id": item.topic_id,
                    "period_start": item.period_start,
                    "works_count": item.works_count,
                }
                for item in deduplicated
            ]
            stmt = pg_insert(OpenAlexMonthlyTopicStat).values(values)
            self.session.execute(
                stmt.on_conflict_do_update(
                    index_elements=[
                        OpenAlexMonthlyTopicStat.topic_id,
                        OpenAlexMonthlyTopicStat.period_start,
                    ],
                    set_={
                        "works_count": stmt.excluded.works_count,
                        "collected_at": func.now(),
                    },
                )
            )
            return created, updated

        for item in deduplicated:
            stat = self.session.scalar(
                select(OpenAlexMonthlyTopicStat).where(
                    OpenAlexMonthlyTopicStat.topic_id == item.topic_id,
                    OpenAlexMonthlyTopicStat.period_start == item.period_start,
                )
            )
            if stat is None:
                self.session.add(
                    OpenAlexMonthlyTopicStat(
                        topic_id=item.topic_id,
                        period_start=item.period_start,
                        works_count=item.works_count,
                    )
                )
                continue
            stat.works_count = item.works_count
            stat.collected_at = datetime.now(timezone.utc)
        return created, updated

    def _deduplicate(
        self,
        items: list[OpenAlexTopicMonthlyCount],
    ) -> list[OpenAlexTopicMonthlyCount]:
        by_key: dict[tuple[int, date], OpenAlexTopicMonthlyCount] = {}
        for item in items:
            by_key[(item.topic_id, item.period_start)] = item
        return list(by_key.values())

    def _existing_keys(
        self,
        items: list[OpenAlexTopicMonthlyCount],
    ) -> set[tuple[int, date]]:
        if not items:
            return set()
        topic_ids = sorted({item.topic_id for item in items})
        period_starts = sorted({item.period_start for item in items})
        stmt = select(
            OpenAlexMonthlyTopicStat.topic_id,
            OpenAlexMonthlyTopicStat.period_start,
        ).where(
            OpenAlexMonthlyTopicStat.topic_id.in_(topic_ids),
            OpenAlexMonthlyTopicStat.period_start.in_(period_starts),
        )
        return {
            (int(topic_id), period_start)
            for topic_id, period_start in self.session.execute(stmt)
            if topic_id is not None
        }

    def _validate_granularity(self, granularity: str) -> str:
        allowed = {"month", "quarter", "year"}
        if granularity not in allowed:
            raise ValueError(
                f"Unsupported OpenAlex topic stats granularity: {granularity!r}"
            )
        return granularity


__all__ = ["OpenAlexTopicMonthlyCount", "OpenAlexTopicStatsRepository"]
