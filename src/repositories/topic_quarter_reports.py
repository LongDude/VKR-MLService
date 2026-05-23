from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from sqlalchemy import delete, select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from core.exceptions import InvalidRequestError
from dto.topic_reports import (
    TopicQuarterReportDTO,
    TopicQuarterReportItemDTO,
    TopicQuarterReportItemInputDTO,
    TopicQuarterReportPaperDTO,
    TopicQuarterReportPaperInputDTO,
)
from models import (
    TopicQuarterReport,
    TopicQuarterReportItem,
    TopicQuarterReportPaper,
)

from .base import BaseRepository


class TopicQuarterReportRepository(BaseRepository):
    """Persist quarterly topic reports and their evidence rows."""

    def get_by_topic_period(
        self,
        topic_id: int,
        period_key: str,
    ) -> TopicQuarterReport | None:
        """Return one report by topic and quarter key."""
        stmt = select(TopicQuarterReport).where(
            TopicQuarterReport.topic_id == int(topic_id),
            TopicQuarterReport.period_key == period_key,
        )
        return self.session.scalar(stmt)

    def upsert_report(
        self,
        *,
        topic_id: int,
        period_start: date,
        period_end: date,
        period_key: str,
        title: str | None,
        summary: str | None,
        definition: str | None,
        dynamics_summary: str | None,
        future_dynamics: str | None,
        metrics: dict[str, Any],
        keyword_dynamics: dict[str, Any],
    ) -> tuple[TopicQuarterReport, bool]:
        """Create or update a report row and return it with a created flag."""
        values = {
            "topic_id": int(topic_id),
            "period_start": period_start,
            "period_end": period_end,
            "period_key": period_key,
            "title": title,
            "summary": summary,
            "definition": definition,
            "dynamics_summary": dynamics_summary,
            "future_dynamics": future_dynamics,
            "metrics": metrics,
            "keyword_dynamics": keyword_dynamics,
            "updated_at": datetime.now(timezone.utc),
        }
        existing = self.get_by_topic_period(topic_id, period_key)
        created = existing is None

        if self._is_postgresql():
            stmt = pg_insert(TopicQuarterReport).values(values)
            report_id = self.session.scalar(
                stmt.on_conflict_do_update(
                    index_elements=[
                        TopicQuarterReport.topic_id,
                        TopicQuarterReport.period_key,
                    ],
                    set_={
                        "period_start": stmt.excluded.period_start,
                        "period_end": stmt.excluded.period_end,
                        "title": stmt.excluded.title,
                        "summary": stmt.excluded.summary,
                        "definition": stmt.excluded.definition,
                        "dynamics_summary": stmt.excluded.dynamics_summary,
                        "future_dynamics": stmt.excluded.future_dynamics,
                        "metrics": stmt.excluded.metrics,
                        "keyword_dynamics": stmt.excluded.keyword_dynamics,
                        "updated_at": stmt.excluded.updated_at,
                    },
                ).returning(TopicQuarterReport.id)
            )
            report = self.session.get(TopicQuarterReport, int(report_id))
            if report is None:
                raise InvalidRequestError(
                    "Topic quarter report upsert did not return a row",
                    details={"topic_id": topic_id, "period_key": period_key},
                )
            return report, created

        if existing is None:
            report = TopicQuarterReport(**values)
            self.session.add(report)
            self.session.flush()
            return report, True

        for field, value in values.items():
            setattr(existing, field, value)
        self.session.flush()
        return existing, False

    def replace_items(
        self,
        report_id: int,
        items: list[TopicQuarterReportItemInputDTO],
    ) -> list[TopicQuarterReportItem]:
        """Replace structured report items for one report."""
        self.session.execute(
            delete(TopicQuarterReportItem).where(
                TopicQuarterReportItem.report_id == int(report_id)
            )
        )
        rows = [
            TopicQuarterReportItem(
                report_id=int(report_id),
                item_type=item.item_type,
                title=item.title.strip(),
                description=item.description,
                maturity=item.maturity,
                evidence=item.evidence,
                sort_order=item.sort_order,
            )
            for item in items
            if item.title.strip()
        ]
        self.session.add_all(rows)
        self.session.flush()
        return rows

    def replace_papers(
        self,
        report_id: int,
        papers: list[TopicQuarterReportPaperInputDTO],
    ) -> list[TopicQuarterReportPaper]:
        """Replace paper evidence links for one report."""
        self.session.execute(
            delete(TopicQuarterReportPaper).where(
                TopicQuarterReportPaper.report_id == int(report_id)
            )
        )
        rows = [
            TopicQuarterReportPaper(
                report_id=int(report_id),
                paper_id=int(paper.paper_id),
                role=paper.role,
                score=self._decimal_or_none(paper.score),
                note=paper.note,
            )
            for paper in papers
        ]
        self.session.add_all(rows)
        self.session.flush()
        return rows

    def list_existing_keys(
        self,
        topic_ids: list[int],
        period_keys: list[str],
    ) -> set[tuple[int, str]]:
        """Return topic and period keys that already have reports."""
        if not topic_ids or not period_keys:
            return set()
        stmt = select(
            TopicQuarterReport.topic_id,
            TopicQuarterReport.period_key,
        ).where(
            TopicQuarterReport.topic_id.in_(sorted(set(int(item) for item in topic_ids))),
            TopicQuarterReport.period_key.in_(sorted(set(period_keys))),
        )
        return {
            (int(topic_id), str(period_key))
            for topic_id, period_key in self.session.execute(stmt)
        }

    def to_dto(self, report: TopicQuarterReport) -> TopicQuarterReportDTO:
        """Convert a report ORM instance into a DTO."""
        return TopicQuarterReportDTO(
            id=int(report.id),
            topic_id=int(report.topic_id),
            period_start=report.period_start,
            period_end=report.period_end,
            period_key=report.period_key,
            title=report.title,
            summary=report.summary,
            definition=report.definition,
            dynamics_summary=report.dynamics_summary,
            future_dynamics=report.future_dynamics,
            metrics=dict(report.metrics or {}),
            keyword_dynamics=dict(report.keyword_dynamics or {}),
            created_at=report.created_at,
            updated_at=report.updated_at,
            items=[
                TopicQuarterReportItemDTO(
                    id=int(item.id),
                    report_id=int(item.report_id),
                    item_type=item.item_type,
                    title=item.title,
                    description=item.description,
                    maturity=item.maturity,
                    evidence=dict(item.evidence or {}),
                    sort_order=int(item.sort_order or 0),
                    created_at=item.created_at,
                )
                for item in sorted(report.items, key=lambda row: row.sort_order)
            ],
            papers=[
                TopicQuarterReportPaperDTO(
                    report_id=int(link.report_id),
                    paper_id=int(link.paper_id),
                    role=link.role,
                    score=link.score,
                    note=link.note,
                )
                for link in report.paper_links
            ],
        )

    def _decimal_or_none(self, value: Any) -> Decimal | None:
        if value is None:
            return None
        try:
            return Decimal(str(value))
        except (InvalidOperation, ValueError) as exc:
            raise InvalidRequestError(
                "Topic report paper score is invalid",
                details={"value": value},
            ) from exc


__all__ = ["TopicQuarterReportRepository"]
