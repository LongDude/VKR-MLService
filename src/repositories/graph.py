from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Literal

from sqlalchemy import func, literal, select

from core.exceptions import InvalidRequestError
from dto.charts import PeriodCountDTO
from models import Paper, PaperTopic, Topic

from .base import BaseRepository


class PaperGraphRepository(BaseRepository):
    def count_papers_by_topic_and_period(
        self,
        topic_id: int,
        date_from: date,
        date_to: date,
        granularity: Literal["week", "month"],
        *,
        topic_match: Literal["soft", "strict"] = "soft",
    ) -> list[PeriodCountDTO]:
        """Count papers for one topic grouped by week or month."""
        self._validate_topic_match(topic_match)
        period = self._period_expr(granularity)
        stmt = select(period, func.count(Paper.id)).select_from(Paper)
        if topic_match == "strict":
            stmt = stmt.where(Paper.primary_topic_id == topic_id)
        else:
            stmt = stmt.join(PaperTopic, PaperTopic.paper_id == Paper.id).where(
                PaperTopic.topic_id == topic_id,
            )
        stmt = (
            stmt.where(
                Paper.publication_date >= date_from,
                Paper.publication_date <= date_to,
            )
            .group_by(period)
            .order_by(period.asc())
        )
        return [
            PeriodCountDTO(period_start=self._to_date(period_start), count=int(count))
            for period_start, count in self.session.execute(stmt)
        ]

    def count_papers_by_topics_heatmap(
        self,
        topic_ids: list[int],
        date_from: date,
        date_to: date,
        granularity: Literal["week", "month"],
        *,
        topic_match: Literal["soft", "strict"] = "soft",
    ) -> dict:
        """Count papers for topics and periods as heatmap-compatible data."""
        self._validate_topic_match(topic_match)
        if not topic_ids:
            return {"granularity": granularity, "items": []}
        period = self._period_expr(granularity)
        if topic_match == "strict":
            topic_column = Paper.primary_topic_id
            stmt = select(topic_column, period, func.count(Paper.id)).select_from(Paper)
        else:
            topic_column = PaperTopic.topic_id
            stmt = (
                select(topic_column, period, func.count(Paper.id))
                .select_from(PaperTopic)
                .join(Paper, Paper.id == PaperTopic.paper_id)
            )
        stmt = (
            stmt.where(
                topic_column.in_(topic_ids),
                Paper.publication_date >= date_from,
                Paper.publication_date <= date_to,
            )
            .group_by(topic_column, period)
            .order_by(topic_column.asc(), period.asc())
        )
        return {
            "granularity": granularity,
            "items": [
                {
                    "topic_id": topic_id,
                    "period_start": self._to_date(period_start),
                    "count": int(count),
                }
                for topic_id, period_start, count in self.session.execute(stmt)
            ],
        }

    def get_top_topics_by_recent_growth(
        self,
        date_from: date,
        date_to: date,
        limit: int,
        *,
        topic_match: Literal["soft", "strict"] = "soft",
    ) -> list[dict]:
        """Return topics ordered by recent paper-count growth."""
        self._validate_topic_match(topic_match)
        period_days = (date_to - date_from).days + 1
        previous_to = date_from - timedelta(days=1)
        previous_from = date_from - timedelta(days=period_days)

        current_counts = self._topic_counts_subquery(
            date_from,
            date_to,
            "paper_count",
            topic_match=topic_match,
        )
        previous_counts = self._topic_counts_subquery(
            previous_from,
            previous_to,
            "previous_paper_count",
            topic_match=topic_match,
        )
        current_count = func.coalesce(current_counts.c.paper_count, 0)
        previous_count = func.coalesce(previous_counts.c.previous_paper_count, 0)
        growth_rate = (
            (current_count - previous_count)
            / func.nullif(previous_count, 0)
        )

        stmt = (
            select(
                Topic.id,
                Topic.name,
                current_count.label("paper_count"),
                previous_count.label("previous_paper_count"),
                func.coalesce(growth_rate, literal(0)).label("growth_rate"),
            )
            .join(current_counts, current_counts.c.topic_id == Topic.id)
            .outerjoin(previous_counts, previous_counts.c.topic_id == Topic.id)
            .order_by(
                func.coalesce(growth_rate, literal(0)).desc(),
                current_count.desc(),
            )
            .limit(limit)
        )
        return [
            {
                "topic_id": topic_id,
                "topic_name": topic_name,
                "paper_count": int(paper_count),
                "previous_paper_count": int(previous_paper_count),
                "growth_rate": float(growth_rate),
            }
            for (
                topic_id,
                topic_name,
                paper_count,
                previous_paper_count,
                growth_rate,
            ) in self.session.execute(stmt)
        ]

    def _period_expr(self, granularity: Literal["week", "month"]):
        if granularity not in {"week", "month"}:
            raise ValueError("granularity must be 'week' or 'month'")
        return func.date_trunc(granularity, Paper.publication_date).label("period_start")

    def _topic_counts_subquery(
        self,
        date_from: date,
        date_to: date,
        count_label: str,
        *,
        topic_match: Literal["soft", "strict"],
    ):
        if topic_match == "strict":
            topic_column = Paper.primary_topic_id.label("topic_id")
            stmt = select(
                topic_column,
                func.count(Paper.id).label(count_label),
            ).select_from(Paper)
        else:
            topic_column = PaperTopic.topic_id.label("topic_id")
            stmt = (
                select(
                    topic_column,
                    func.count(Paper.id).label(count_label),
                )
                .select_from(PaperTopic)
                .join(Paper, Paper.id == PaperTopic.paper_id)
            )
        return (
            stmt.where(
                topic_column.is_not(None),
                Paper.publication_date >= date_from,
                Paper.publication_date <= date_to,
            )
            .group_by(topic_column)
            .subquery()
        )

    def _validate_topic_match(self, value: Literal["soft", "strict"]) -> None:
        if value not in {"soft", "strict"}:
            raise InvalidRequestError(
                "Topic match mode must be 'soft' or 'strict'",
                details={"topic_match": value},
            )

    def _to_date(self, value) -> date:
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        return value.date()


__all__ = ["PaperGraphRepository"]
