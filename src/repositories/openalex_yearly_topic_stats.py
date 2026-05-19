from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from models import OpenAlexMonthlyTopicStat, OpenAlexYearlyTopicStat

from .base import BaseRepository


@dataclass(frozen=True)
class OpenAlexTopicYearlyCount:
    """Count of OpenAlex works for one local topic and one calendar year."""

    topic_id: int
    stat_year: date
    works_count: int
    artifical_pubdates_estimation: int = 0


@dataclass(frozen=True)
class OpenAlexTopicYearlyArtificialEstimate:
    """Estimated artificial January-1 publications for one topic/year."""

    topic_id: int
    stat_year: date
    artifical_pubdates_estimation: int


class OpenAlexYearlyTopicStatsRepository(BaseRepository):
    """Persist yearly OpenAlex topic counters and January artifact estimates."""

    def upsert_many(self, items: list[OpenAlexTopicYearlyCount]) -> tuple[int, int]:
        """Insert or update yearly topic counters."""
        if not items:
            return 0, 0
        deduplicated = self._deduplicate_counts(items)
        if self._is_postgresql():
            values = [
                {
                    "topic_id": item.topic_id,
                    "stat_year": item.stat_year,
                    "works_count": item.works_count,
                    "artifical_pubdates_estimation": item.artifical_pubdates_estimation,
                }
                for item in deduplicated
            ]
            stmt = pg_insert(OpenAlexYearlyTopicStat).values(values)
            self.session.execute(
                stmt.on_conflict_do_update(
                    index_elements=[
                        OpenAlexYearlyTopicStat.topic_id,
                        OpenAlexYearlyTopicStat.stat_year,
                    ],
                    set_={
                        "works_count": stmt.excluded.works_count,
                        "artifical_pubdates_estimation": (
                            stmt.excluded.artifical_pubdates_estimation
                        ),
                        "collected_at": func.now(),
                    },
                )
            )
            return len(deduplicated), 0

        for item in deduplicated:
            stat = self._get(item.topic_id, item.stat_year)
            if stat is None:
                self.session.add(
                    OpenAlexYearlyTopicStat(
                        topic_id=item.topic_id,
                        stat_year=item.stat_year,
                        works_count=item.works_count,
                        artifical_pubdates_estimation=item.artifical_pubdates_estimation,
                    )
                )
                continue
            stat.works_count = item.works_count
            stat.artifical_pubdates_estimation = item.artifical_pubdates_estimation
            stat.collected_at = datetime.now(timezone.utc)
        return len(deduplicated), 0

    def upsert_artificial_estimates(
        self,
        items: list[OpenAlexTopicYearlyArtificialEstimate],
    ) -> tuple[int, int]:
        """Insert or update only the January artifact estimate for topic/year rows."""
        if not items:
            return 0, 0
        deduplicated = self._deduplicate_estimates(items)
        if self._is_postgresql():
            values = [
                {
                    "topic_id": item.topic_id,
                    "stat_year": item.stat_year,
                    "works_count": 0,
                    "artifical_pubdates_estimation": item.artifical_pubdates_estimation,
                }
                for item in deduplicated
            ]
            stmt = pg_insert(OpenAlexYearlyTopicStat).values(values)
            self.session.execute(
                stmt.on_conflict_do_update(
                    index_elements=[
                        OpenAlexYearlyTopicStat.topic_id,
                        OpenAlexYearlyTopicStat.stat_year,
                    ],
                    set_={
                        "artifical_pubdates_estimation": (
                            stmt.excluded.artifical_pubdates_estimation
                        ),
                        "collected_at": func.now(),
                    },
                )
            )
            return len(deduplicated), 0

        for item in deduplicated:
            stat = self._get(item.topic_id, item.stat_year)
            if stat is None:
                self.session.add(
                    OpenAlexYearlyTopicStat(
                        topic_id=item.topic_id,
                        stat_year=item.stat_year,
                        works_count=0,
                        artifical_pubdates_estimation=item.artifical_pubdates_estimation,
                    )
                )
                continue
            stat.artifical_pubdates_estimation = item.artifical_pubdates_estimation
            stat.collected_at = datetime.now(timezone.utc)
        return len(deduplicated), 0

    def rebuild_from_monthly(
        self,
        *,
        date_from: date,
        date_to: date,
        topic_ids: list[int] | None = None,
        include_artificial: bool = True,
    ) -> list[OpenAlexTopicYearlyCount]:
        """Build and upsert yearly stats from monthly rows for the supplied period."""
        monthly_stmt = (
            select(
                OpenAlexMonthlyTopicStat.topic_id,
                OpenAlexMonthlyTopicStat.period_start,
                OpenAlexMonthlyTopicStat.works_count,
            )
            .where(
                OpenAlexMonthlyTopicStat.topic_id.is_not(None),
                OpenAlexMonthlyTopicStat.period_start >= date_from,
                OpenAlexMonthlyTopicStat.period_start <= date_to,
            )
            .order_by(OpenAlexMonthlyTopicStat.topic_id, OpenAlexMonthlyTopicStat.period_start)
        )
        if topic_ids:
            monthly_stmt = monthly_stmt.where(
                OpenAlexMonthlyTopicStat.topic_id.in_(sorted(set(topic_ids)))
            )

        yearly_counts: dict[tuple[int, date], int] = defaultdict(int)
        for topic_id, period_start, works_count in self.session.execute(monthly_stmt):
            if topic_id is None:
                continue
            stat_year = date(period_start.year, 1, 1)
            yearly_counts[(int(topic_id), stat_year)] += int(works_count or 0)

        artificial_by_key = self._artificial_estimates(set(yearly_counts)) if yearly_counts else {}
        items = [
            OpenAlexTopicYearlyCount(
                topic_id=topic_id,
                stat_year=stat_year,
                works_count=monthly_count
                + (
                    artificial_by_key.get((topic_id, stat_year), 0)
                    if include_artificial
                    else 0
                ),
                artifical_pubdates_estimation=artificial_by_key.get(
                    (topic_id, stat_year),
                    0,
                ),
            )
            for (topic_id, stat_year), monthly_count in sorted(yearly_counts.items())
        ]
        self.upsert_many(items)
        return items

    def list_january_artifact_estimates(
        self,
        *,
        date_from: date,
        date_to: date,
        topic_ids: list[int] | None = None,
        min_estimate: int = 1,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """List yearly rows with non-trivial January artificial publication estimates."""
        stmt = (
            select(OpenAlexYearlyTopicStat)
            .where(
                OpenAlexYearlyTopicStat.stat_year >= date(date_from.year, 1, 1),
                OpenAlexYearlyTopicStat.stat_year <= date(date_to.year, 1, 1),
                OpenAlexYearlyTopicStat.artifical_pubdates_estimation >= min_estimate,
            )
            .order_by(
                OpenAlexYearlyTopicStat.artifical_pubdates_estimation.desc(),
                OpenAlexYearlyTopicStat.stat_year.asc(),
                OpenAlexYearlyTopicStat.topic_id.asc(),
            )
            .limit(limit)
        )
        if topic_ids:
            stmt = stmt.where(OpenAlexYearlyTopicStat.topic_id.in_(sorted(set(topic_ids))))
        return [
            {
                "topic_id": stat.topic_id,
                "stat_year": stat.stat_year,
                "works_count": stat.works_count,
                "artifical_pubdates_estimation": stat.artifical_pubdates_estimation,
            }
            for stat in self.session.scalars(stmt).all()
        ]

    def _get(self, topic_id: int, stat_year: date) -> OpenAlexYearlyTopicStat | None:
        return self.session.scalar(
            select(OpenAlexYearlyTopicStat).where(
                OpenAlexYearlyTopicStat.topic_id == topic_id,
                OpenAlexYearlyTopicStat.stat_year == stat_year,
            )
        )

    def _artificial_estimates(
        self,
        keys: set[tuple[int, date]],
    ) -> dict[tuple[int, date], int]:
        topic_ids = sorted({topic_id for topic_id, _ in keys})
        stat_years = sorted({stat_year for _, stat_year in keys})
        stmt = select(
            OpenAlexYearlyTopicStat.topic_id,
            OpenAlexYearlyTopicStat.stat_year,
            OpenAlexYearlyTopicStat.artifical_pubdates_estimation,
        ).where(
            OpenAlexYearlyTopicStat.topic_id.in_(topic_ids),
            OpenAlexYearlyTopicStat.stat_year.in_(stat_years),
        )
        return {
            (int(topic_id), stat_year): int(value or 0)
            for topic_id, stat_year, value in self.session.execute(stmt)
            if topic_id is not None
        }

    def _deduplicate_counts(
        self,
        items: list[OpenAlexTopicYearlyCount],
    ) -> list[OpenAlexTopicYearlyCount]:
        by_key: dict[tuple[int, date], OpenAlexTopicYearlyCount] = {}
        for item in items:
            by_key[(item.topic_id, item.stat_year)] = item
        return list(by_key.values())

    def _deduplicate_estimates(
        self,
        items: list[OpenAlexTopicYearlyArtificialEstimate],
    ) -> list[OpenAlexTopicYearlyArtificialEstimate]:
        by_key: dict[tuple[int, date], OpenAlexTopicYearlyArtificialEstimate] = {}
        for item in items:
            by_key[(item.topic_id, item.stat_year)] = item
        return list(by_key.values())


__all__ = [
    "OpenAlexTopicYearlyArtificialEstimate",
    "OpenAlexTopicYearlyCount",
    "OpenAlexYearlyTopicStatsRepository",
]
