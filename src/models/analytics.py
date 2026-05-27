from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any, TYPE_CHECKING

from sqlalchemy import (
    BigInteger,
    Date,
    DateTime,
    ForeignKey,
    Integer,
    Numeric,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base

if TYPE_CHECKING:
    from .paper import Paper
    from .topic import Topic


class ResearchCluster(Base):
    __tablename__ = "research_clusters"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    cluster_key: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    cluster_type: Mapped[str] = mapped_column(Text, nullable=False)
    source_topic_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("topics.id", ondelete="SET NULL")
    )
    name: Mapped[str] = mapped_column(Text, nullable=False)
    summary: Mapped[str | None] = mapped_column(Text)
    status: Mapped[str | None] = mapped_column(Text)
    trend_score: Mapped[Decimal | None] = mapped_column(Numeric(8, 5))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )

    source_topic: Mapped["Topic | None"] = relationship(back_populates="research_clusters")
    period_stats: Mapped[list["ResearchClusterPeriodStat"]] = relationship(
        back_populates="cluster", cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return f"ResearchCluster(id={self.id!r}, cluster_key={self.cluster_key!r})"


class ResearchClusterPeriodStat(Base):
    __tablename__ = "research_cluster_period_stats"
    __table_args__ = (
        UniqueConstraint("cluster_id", "period_start", "period_end"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    cluster_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("research_clusters.id", ondelete="CASCADE"), nullable=False
    )
    period_start: Mapped[date] = mapped_column(Date, nullable=False)
    period_end: Mapped[date] = mapped_column(Date, nullable=False)
    paper_count: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default=text("0")
    )
    previous_paper_count: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default=text("0")
    )
    growth_rate: Mapped[Decimal | None] = mapped_column(Numeric(10, 5))
    trend_score: Mapped[Decimal | None] = mapped_column(Numeric(10, 5))
    semantic_drift: Mapped[Decimal | None] = mapped_column(Numeric(10, 5))
    citation_count_sum: Mapped[int | None] = mapped_column(
        Integer, server_default=text("0")
    )
    avg_cited_by_count: Mapped[Decimal | None] = mapped_column(Numeric(10, 3))
    top_keywords: Mapped[Any | None] = mapped_column(JSONB)
    representative_paper_ids: Mapped[Any | None] = mapped_column(JSONB)
    summary: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )

    cluster: Mapped["ResearchCluster"] = relationship(back_populates="period_stats")

    def __repr__(self) -> str:
        return f"ResearchClusterPeriodStat(id={self.id!r}, cluster_id={self.cluster_id!r})"


class OpenAlexMonthlyTopicStat(Base):
    __tablename__ = "openalex_montly_topic_stats"
    __table_args__ = (
        UniqueConstraint("topic_id", "period_start"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    topic_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("topics.id", ondelete="SET NULL")
    )
    period_start: Mapped[date] = mapped_column(Date, nullable=False)
    works_count: Mapped[int] = mapped_column(Integer, nullable=False)
    collected_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )

    topic: Mapped["Topic | None"] = relationship(back_populates="openalex_monthly_stats")

    def __repr__(self) -> str:
        return (
            "OpenAlexMonthlyTopicStat("
            f"topic_id={self.topic_id!r}, period_start={self.period_start!r})"
        )


class OpenAlexYearlyTopicStat(Base):
    __tablename__ = "openalex_yearly_topic_stats"
    __table_args__ = (
        UniqueConstraint("topic_id", "stat_year"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    topic_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("topics.id", ondelete="SET NULL")
    )
    stat_year: Mapped[date] = mapped_column(Date, nullable=False)
    works_count: Mapped[int] = mapped_column(Integer, nullable=False)
    artifical_pubdates_estimation: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default=text("0")
    )
    collected_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )

    topic: Mapped["Topic | None"] = relationship(back_populates="openalex_yearly_stats")

    def __repr__(self) -> str:
        return (
            "OpenAlexYearlyTopicStat("
            f"topic_id={self.topic_id!r}, stat_year={self.stat_year!r})"
        )


class TopicQuarterReport(Base):
    __tablename__ = "topic_quarter_reports"
    __table_args__ = (
        UniqueConstraint("topic_id", "period_key"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    topic_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("topics.id", ondelete="CASCADE"), nullable=False
    )
    period_start: Mapped[date] = mapped_column(Date, nullable=False)
    period_end: Mapped[date] = mapped_column(Date, nullable=False)
    period_key: Mapped[str] = mapped_column(Text, nullable=False)
    summary: Mapped[str | None] = mapped_column(Text)
    period_characterization: Mapped[str | None] = mapped_column(Text)
    dynamics_summary: Mapped[str | None] = mapped_column(Text)
    future_dynamics: Mapped[str | None] = mapped_column(Text)
    metrics: Mapped[Any] = mapped_column(
        JSONB, nullable=False, server_default=text("'{}'::jsonb")
    )
    keyword_dynamics: Mapped[Any] = mapped_column(
        JSONB, nullable=False, server_default=text("'{}'::jsonb")
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )

    topic: Mapped["Topic"] = relationship(back_populates="quarter_reports")
    items: Mapped[list["TopicQuarterReportItem"]] = relationship(
        back_populates="report", cascade="all, delete-orphan"
    )
    paper_links: Mapped[list["TopicQuarterReportPaper"]] = relationship(
        back_populates="report", cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return (
            "TopicQuarterReport("
            f"id={self.id!r}, topic_id={self.topic_id!r}, period_key={self.period_key!r})"
        )


class TopicQuarterReportItem(Base):
    __tablename__ = "topic_quarter_report_items"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    report_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("topic_quarter_reports.id", ondelete="CASCADE"),
        nullable=False,
    )
    item_type: Mapped[str] = mapped_column(Text, nullable=False)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    description: Mapped[str | None] = mapped_column(Text)
    maturity: Mapped[str | None] = mapped_column(Text)
    evidence: Mapped[Any] = mapped_column(
        JSONB, nullable=False, server_default=text("'{}'::jsonb")
    )
    sort_order: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default=text("0")
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )

    report: Mapped["TopicQuarterReport"] = relationship(back_populates="items")

    def __repr__(self) -> str:
        return (
            "TopicQuarterReportItem("
            f"id={self.id!r}, report_id={self.report_id!r}, item_type={self.item_type!r})"
        )


class TopicQuarterReportPaper(Base):
    __tablename__ = "topic_quarter_report_papers"

    report_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("topic_quarter_reports.id", ondelete="CASCADE"),
        primary_key=True,
    )
    paper_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("papers.id", ondelete="CASCADE"), primary_key=True
    )
    role: Mapped[str] = mapped_column(Text, primary_key=True)
    score: Mapped[Decimal | None] = mapped_column(Numeric(10, 5))
    note: Mapped[str | None] = mapped_column(Text)

    report: Mapped["TopicQuarterReport"] = relationship(back_populates="paper_links")
    paper: Mapped["Paper"] = relationship(back_populates="quarter_report_links")

    def __repr__(self) -> str:
        return (
            "TopicQuarterReportPaper("
            f"report_id={self.report_id!r}, paper_id={self.paper_id!r}, role={self.role!r})"
        )
