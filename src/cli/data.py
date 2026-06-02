from __future__ import annotations

import argparse
import json
import logging
import sys
from calendar import monthrange
from collections import defaultdict
from datetime import date
from pathlib import Path
from typing import Any, Callable, cast

from sqlalchemy import CursorResult, delete, exists, func, or_, select, update
from sqlalchemy.orm import Session

BASE_DIR = Path(__file__).resolve().parent
SRC_DIR = BASE_DIR.parent
PROJECT_DIR = SRC_DIR.parent

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from adapters import QdrantAdapter
from adapters.redis_adapter import RedisAdapter
from core.config import Settings, load_settings
from core.dependencies import create_qdrant_adapter, create_redis_client
from core.logging import configure_logging, get_logger, log_event
from ml.facades.cluster_db_sync import ClusterDbSyncFacade
from ml.services.events import NoopEventSink
from ml.services.qdrant_collections import QdrantCollectionInitializer
from ml.services.cluster_dynamics_tasks import (
    acquire_cluster_dynamics_cluster_ids,
    build_cluster_dynamics_message,
    release_cluster_dynamics_dedupe_keys,
)
from ml.services.quarter_periods import QuarterPeriodService
from ml.task_contracts import (
    BOOTSTRAP_PAPERS_TASK,
    CLUSTER_DYNAMICS_RECOMPUTE_QUEUE,
    COLLECT_TOPIC_STATS_TASK,
    OPENALEX_BOOTSTRAP_PAPERS_QUEUE,
    OPENALEX_TOPIC_STATS_QUEUE,
)

logger = get_logger(__name__)
from models import (
    Domain,
    Field,
    Keyword,
    OpenAlexMonthlyTopicStat,
    OpenAlexYearlyTopicStat,
    Paper,
    PaperTopic,
    ResearchCluster,
    ResearchClusterPeriodStat,
    Subfield,
    Topic,
    TopicQuarterReport,
)
from models.session import create_db_engine, create_session_factory
from repositories.openalex_yearly_topic_stats import OpenAlexYearlyTopicStatsRepository
from repositories.research_clusters import ResearchClusterRepository

SAMPLE_LIMIT = 20
class LocalDataValidator:
    """Validate local PostgreSQL data quality before ML indexing."""

    def __init__(self, session: Session, *, sample_limit: int = SAMPLE_LIMIT) -> None:
        self.session = session
        self.sample_limit = sample_limit

    def validate(self) -> list[dict[str, Any]]:
        """Run all local data quality checks and return report items."""
        checks = [
            self._check_papers_empty_title(),
            self._check_papers_without_publication_date(),
            self._check_papers_without_doi_and_external_id(),
            self._check_papers_without_topics(),
            self._check_papers_without_abstract(),
            self._check_topics_without_subfield(),
            self._check_subfields_without_field(),
            self._check_fields_without_domain(),
            self._check_duplicate_doi(),
            self._check_duplicate_openalex_external_id(),
        ]
        return checks

    def fix_safe(self) -> dict[str, Any]:
        """Apply conservative safe fixes that do not merge semantic entities."""
        fixes: dict[str, list[dict[str, Any]]] = {
            "applied": [],
            "skipped": [],
        }
        self._trim_non_unique_strings(fixes)
        self._normalize_doi_values(fixes)
        self._delete_empty_keywords(fixes)
        self._normalize_keyword_values(fixes)
        self._trim_unique_strings(fixes)
        return fixes

    def _check_papers_empty_title(self) -> dict[str, Any]:
        stmt = select(
            Paper.id.label("paper_id"),
            Paper.title.label("title"),
        ).where(self._is_blank(Paper.title))
        return self._query_check(
            "papers_empty_title",
            "Papers with empty title.",
            stmt,
        )

    def _check_papers_without_publication_date(self) -> dict[str, Any]:
        stmt = select(
            Paper.id.label("paper_id"),
            Paper.title.label("title"),
        ).where(Paper.publication_date.is_(None))
        return self._query_check(
            "papers_without_publication_date",
            "Papers without publication_date.",
            stmt,
        )

    def _check_papers_without_doi_and_external_id(self) -> dict[str, Any]:
        stmt = select(
            Paper.id.label("paper_id"),
            Paper.title.label("title"),
        ).where(self._is_blank(Paper.doi), self._is_blank(Paper.openalex_id))
        return self._query_check(
            "papers_without_doi_and_external_id",
            "Papers without DOI and external_id.",
            stmt,
        )

    def _check_papers_without_topics(self) -> dict[str, Any]:
        has_topic = exists().where(PaperTopic.paper_id == Paper.id)
        stmt = select(
            Paper.id.label("paper_id"),
            Paper.title.label("title"),
        ).where(~has_topic)
        return self._query_check(
            "papers_without_topics",
            "Papers without linked topics.",
            stmt,
        )

    def _check_papers_without_abstract(self) -> dict[str, Any]:
        stmt = select(
            Paper.id.label("paper_id"),
            Paper.title.label("title"),
        ).where(self._is_blank(Paper.abstract))
        return self._query_check(
            "papers_without_abstract",
            "Papers without abstract.",
            stmt,
        )

    def _check_topics_without_subfield(self) -> dict[str, Any]:
        stmt = select(
            Topic.id.label("topic_id"),
            Topic.name.label("name"),
        ).where(Topic.subfield_id.is_(None))
        return self._query_check(
            "topics_without_subfield",
            "Topics without subfield.",
            stmt,
        )

    def _check_subfields_without_field(self) -> dict[str, Any]:
        stmt = select(
            Subfield.id.label("subfield_id"),
            Subfield.name.label("name"),
        ).where(Subfield.field_id.is_(None))
        return self._query_check(
            "subfields_without_field",
            "Subfields without field.",
            stmt,
        )

    def _check_fields_without_domain(self) -> dict[str, Any]:
        stmt = select(
            Field.id.label("field_id"),
            Field.name.label("name"),
        ).where(Field.domain_id.is_(None))
        return self._query_check(
            "fields_without_domain",
            "Fields without domain.",
            stmt,
        )

    def _check_duplicate_doi(self) -> dict[str, Any]:
        groups: dict[str, list[int]] = defaultdict(list)
        stmt = select(Paper.id, Paper.doi).where(self._is_not_blank(Paper.doi))
        for paper_id, doi in self.session.execute(stmt):
            normalized = normalize_doi(doi)
            if normalized:
                groups[normalized].append(int(paper_id))

        duplicate_groups = {
            doi: paper_ids for doi, paper_ids in groups.items() if len(paper_ids) > 1
        }
        samples = [
            {
                "normalized_doi": doi,
                "paper_ids": paper_ids[: self.sample_limit],
                "count": len(paper_ids),
            }
            for doi, paper_ids in list(duplicate_groups.items())[: self.sample_limit]
        ]
        return {
            "code": "duplicate_doi",
            "description": "Duplicate DOI after safe normalization.",
            "count": sum(len(paper_ids) for paper_ids in duplicate_groups.values()),
            "duplicate_groups": len(duplicate_groups),
            "sample": samples,
        }

    def _check_duplicate_openalex_external_id(self) -> dict[str, Any]:
        groups: dict[str, list[int]] = defaultdict(list)
        stmt = select(Paper.id, Paper.openalex_id).where(
            self._is_not_blank(Paper.openalex_id),
        )
        for paper_id, openalex_id in self.session.execute(stmt):
            normalized = normalize_external_id(openalex_id)
            if normalized:
                groups[normalized].append(int(paper_id))

        duplicate_groups = {
            external_id: paper_ids
            for external_id, paper_ids in groups.items()
            if len(paper_ids) > 1
        }
        samples = [
            {
                "external_id": external_id,
                "paper_ids": paper_ids[: self.sample_limit],
                "count": len(paper_ids),
            }
            for external_id, paper_ids in list(duplicate_groups.items())[
                : self.sample_limit
            ]
        ]
        return {
            "code": "duplicate_openalex_external_id",
            "description": "Duplicate OpenAlex external_id after safe normalization.",
            "count": sum(len(paper_ids) for paper_ids in duplicate_groups.values()),
            "duplicate_groups": len(duplicate_groups),
            "sample": samples,
        }

    def _trim_non_unique_strings(self, fixes: dict[str, list[dict[str, Any]]]) -> None:
        columns = [
            (Paper, Paper.title, "papers.title"),
            (Paper, Paper.language, "papers.language"),
            (Paper, Paper.abstract, "papers.abstract"),
            (Paper, Paper.openalex_id, "papers.openalex_id"),
            (Field, Field.name, "fields.name"),
            (Subfield, Subfield.name, "subfields.name"),
            (Topic, Topic.name, "topics.name"),
        ]
        for model, column, label in columns:
            result = self.session.execute(
                update(model)
                .where(column.is_not(None), column != func.trim(column))
                .values({column.key: func.trim(column)})
            )
            cursor_result = cast(CursorResult[Any], result)

            self._append_applied(
                fixes,
                "trim_strings",
                label,
                int(cursor_result.rowcount or 0),
            )

    def _normalize_doi_values(
        self,
        fixes: dict[str, list[dict[str, Any]]],
    ) -> None:
        rows = list(self.session.execute(select(Paper.id, Paper.doi)))
        normalized_groups: dict[str, list[int]] = defaultdict(list)
        normalized_by_id: dict[int, str | None] = {}

        for paper_id, doi in rows:
            normalized = normalize_doi(doi)
            normalized_by_id[int(paper_id)] = normalized
            if normalized:
                normalized_groups[normalized].append(int(paper_id))

        updated = 0
        skipped = 0
        for paper_id, doi in rows:
            paper_id = int(paper_id)
            normalized = normalized_by_id[paper_id]
            current = doi if doi is not None else None
            new_value = normalized or None
            if current == new_value:
                continue
            if normalized and len(normalized_groups[normalized]) > 1:
                skipped += 1
                continue
            self.session.execute(
                update(Paper).where(Paper.id == paper_id).values(doi=new_value)
            )
            updated += 1

        self._append_applied(fixes, "normalize_doi", "papers.doi", updated)
        if skipped:
            fixes["skipped"].append(
                {
                    "action": "normalize_doi",
                    "target": "papers.doi",
                    "count": skipped,
                    "reason": "normalized DOI would conflict with another paper",
                }
            )

    def _delete_empty_keywords(
        self,
        fixes: dict[str, list[dict[str, Any]]],
    ) -> None:
        result = self.session.execute(
            delete(Keyword).where(self._is_blank(Keyword.value))
        )
        cursor_result = cast(CursorResult[Any], result)

        self._append_applied(
            fixes,
            "delete_empty_keywords",
            "keywords",
            int(cursor_result.rowcount or 0),
        )

    def _normalize_keyword_values(
        self,
        fixes: dict[str, list[dict[str, Any]]],
    ) -> None:
        rows = list(
            self.session.execute(
                select(Keyword.id, Keyword.value).where(
                    self._is_not_blank(Keyword.value)
                )
            )
        )
        normalized_groups: dict[str, list[int]] = defaultdict(list)
        for keyword_id, value in rows:
            normalized_groups[normalize_keyword(value)].append(int(keyword_id))

        updated = 0
        skipped = 0
        for keyword_id, value in rows:
            keyword_id = int(keyword_id)
            normalized = normalize_keyword(value)
            if normalized == value:
                continue
            if len(normalized_groups[normalized]) > 1:
                skipped += 1
                continue
            self.session.execute(
                update(Keyword).where(Keyword.id == keyword_id).values(value=normalized)
            )
            updated += 1

        self._append_applied(fixes, "lowercase_keywords", "keywords.value", updated)
        if skipped:
            fixes["skipped"].append(
                {
                    "action": "lowercase_keywords",
                    "target": "keywords.value",
                    "count": skipped,
                    "reason": "normalized keyword would conflict with another keyword",
                }
            )

    def _trim_unique_strings(self, fixes: dict[str, list[dict[str, Any]]]) -> None:
        specs = [
            (Domain, Domain.id, Domain.name, "domains.name"),
            (Domain, Domain.id, Domain.openalex_id, "domains.openalex_id"),
            (Field, Field.id, Field.openalex_id, "fields.openalex_id"),
            (Subfield, Subfield.id, Subfield.openalex_id, "subfields.openalex_id"),
            (Topic, Topic.id, Topic.openalex_id, "topics.openalex_id"),
        ]
        for model, id_column, value_column, label in specs:
            self._apply_unique_string_normalization(
                fixes,
                model=model,
                id_column=id_column,
                value_column=value_column,
                normalize=normalize_nullable_trim,
                action="trim_strings",
                target=label,
            )

    def _apply_unique_string_normalization(
        self,
        fixes: dict[str, list[dict[str, Any]]],
        *,
        model: type[Any],
        id_column: Any,
        value_column: Any,
        normalize: Callable[[str | None], str | None],
        action: str,
        target: str,
    ) -> None:
        rows = list(self.session.execute(select(id_column, value_column)))
        groups: dict[str, list[int]] = defaultdict(list)
        normalized_by_id: dict[int, str | None] = {}

        for row_id, value in rows:
            row_id = int(row_id)
            normalized = normalize(value)
            normalized_by_id[row_id] = normalized
            if normalized:
                groups[normalized].append(row_id)

        updated = 0
        skipped = 0
        for row_id, value in rows:
            row_id = int(row_id)
            normalized = normalized_by_id[row_id]
            if normalized == value:
                continue
            if not normalized:
                skipped += 1
                continue
            if len(groups[normalized]) > 1:
                skipped += 1
                continue
            self.session.execute(
                update(model)
                .where(id_column == row_id)
                .values({value_column.key: normalized})
            )
            updated += 1

        self._append_applied(fixes, action, target, updated)
        if skipped:
            fixes["skipped"].append(
                {
                    "action": action,
                    "target": target,
                    "count": skipped,
                    "reason": "normalized value would be empty or conflict with another row",
                }
            )

    def _query_check(
        self,
        code: str,
        description: str,
        stmt: Any,
    ) -> dict[str, Any]:
        count = int(
            self.session.scalar(
                select(func.count()).select_from(stmt.order_by(None).subquery())
            )
            or 0
        )
        sample = [
            dict(row)
            for row in self.session.execute(stmt.limit(self.sample_limit)).mappings()
        ]
        return {
            "code": code,
            "description": description,
            "count": count,
            "sample": sample,
        }

    def _is_blank(self, column: Any) -> Any:
        return or_(column.is_(None), func.length(func.trim(column)) == 0)

    def _is_not_blank(self, column: Any) -> Any:
        return ~self._is_blank(column)

    def _append_applied(
        self,
        fixes: dict[str, list[dict[str, Any]]],
        action: str,
        target: str,
        count: int,
    ) -> None:
        fixes["applied"].append(
            {
                "action": action,
                "target": target,
                "count": count,
            }
        )


class DataCoverageAnalyzer:
    """Analyze database coverage for topic statistics, samples, and reports."""

    def __init__(self, session: Session, *, sample_limit: int = SAMPLE_LIMIT) -> None:
        self.session = session
        self.sample_limit = max(0, sample_limit)

    def analyze(
        self,
        *,
        date_from: date,
        date_to: date,
        field_ids: list[int] | None = None,
        topic_ids: list[int] | None = None,
        include_missing_topic_ids: bool = False,
    ) -> dict[str, Any]:
        """Return a coverage report for selected fields or topics."""
        if date_from > date_to:
            raise ValueError("--date-from must be before or equal to --date-to.")
        if field_ids and topic_ids:
            raise ValueError("--field-ids and --topic-ids are mutually exclusive.")

        requested_topic_ids = sorted(set(topic_ids or []))
        topics = self._load_topics(field_ids=field_ids, topic_ids=topic_ids)
        if not topics:
            raise ValueError("No topics found for selected coverage scope.")

        months = self._month_periods(date_from, date_to)
        quarters = [
            {
                "key": quarter.key,
                "date_from": quarter.date_from,
                "date_to": quarter.date_to,
            }
            for quarter in QuarterPeriodService().quarter_periods(date_from, date_to)
        ]
        topic_ids = [int(topic["topic_id"]) for topic in topics]

        publication_stats = self._publication_stats_coverage(
            topics,
            months,
            include_missing_topic_ids=include_missing_topic_ids,
        )
        samples = self._sample_coverage(topics, months)
        cluster_dynamics = self._cluster_dynamics_coverage(
            topics,
            months,
            include_missing_topic_ids=include_missing_topic_ids,
        )
        quarter_reports = self._quarter_report_coverage(topics, quarters)

        return {
            "command": "analyze-coverage",
            "date_from": date_from,
            "date_to": date_to,
            "normalized_month_from": months[0]["date_from"] if months else None,
            "normalized_month_to": months[-1]["date_to"] if months else None,
            "scope": {
                "type": "topic"
                if requested_topic_ids
                else "field"
                if field_ids
                else "all",
                "field_ids": sorted(set(field_ids or [])),
                "topic_ids": sorted(set(topic_ids)),
                "topics_count": len(topics),
                "fields": self._field_summary(topics),
            },
            "summary": {
                "months": len(months),
                "quarters": len(quarters),
                "publication_stats_missing": publication_stats["missing_topic_periods"],
                "sample_missing": samples["missing_topic_periods"],
                "sample_empty_months": len(samples["empty_periods"]),
                "cluster_dynamics_missing": cluster_dynamics["missing_topic_periods"],
                "quarter_reports_missing": quarter_reports["missing_topic_periods"],
            },
            "checks": {
                "publication_stats": publication_stats,
                "samples": samples,
                "cluster_dynamics": cluster_dynamics,
                "quarter_reports": quarter_reports,
            },
        }

    def analyze_sample_month_coverage(
        self,
        *,
        date_from: date,
        date_to: date,
        field_id: int,
        topic_ids: list[int] | None = None,
        include_missing_topic_ids: bool = False,
    ) -> dict[str, Any]:
        """Return local paper sample coverage by month for topics in one field."""
        if date_from > date_to:
            raise ValueError("--date-from must be before or equal to --date-to.")

        topics = self._load_topics(field_ids=[field_id], topic_ids=topic_ids)
        if not topics:
            raise ValueError("No topics found for selected field scope.")

        months = self._month_periods(date_from, date_to)
        samples = self._sample_coverage(
            topics,
            months,
            include_missing_topic_ids=include_missing_topic_ids,
        )

        return {
            "command": "analyze-sample-month-coverage",
            "date_from": date_from,
            "date_to": date_to,
            "normalized_month_from": months[0]["date_from"] if months else None,
            "normalized_month_to": months[-1]["date_to"] if months else None,
            "scope": {
                "type": "field",
                "field_id": field_id,
                "topic_ids": sorted(set(topic_ids or [])),
                "topics_count": len(topics),
                "fields": self._field_summary(topics),
            },
            "summary": {
                "months": len(months),
                "sample_missing": samples["missing_topic_periods"],
                "sample_empty_months": len(samples["empty_periods"]),
            },
            "checks": {
                "samples": samples,
            },
        }

    def _publication_stats_coverage(
        self,
        topics: list[dict[str, Any]],
        months: list[dict[str, Any]],
        *,
        include_missing_topic_ids: bool = False,
    ) -> dict[str, Any]:
        topic_ids = [int(topic["topic_id"]) for topic in topics]
        month_starts = [period["date_from"] for period in months]
        existing: set[tuple[int, str]] = set()
        if topic_ids and month_starts:
            stmt = select(
                OpenAlexMonthlyTopicStat.topic_id,
                OpenAlexMonthlyTopicStat.period_start,
            ).where(
                OpenAlexMonthlyTopicStat.topic_id.in_(topic_ids),
                OpenAlexMonthlyTopicStat.period_start.in_(month_starts),
            )
            existing = {
                (int(topic_id), self._month_key(period_start))
                for topic_id, period_start in self.session.execute(stmt)
                if topic_id is not None
            }
        return self._topic_period_coverage(
            code="publication_stats",
            description="OpenAlex monthly publication statistics coverage.",
            period_unit="month",
            topics=topics,
            periods=months,
            existing=existing,
            include_missing_topic_ids=include_missing_topic_ids,
        )

    def _sample_coverage(
        self,
        topics: list[dict[str, Any]],
        months: list[dict[str, Any]],
        *,
        include_missing_topic_ids: bool = False,
    ) -> dict[str, Any]:
        topic_ids = [int(topic["topic_id"]) for topic in topics]
        counts = self._load_sample_counts(topic_ids, months)
        existing = {key for key, count in counts.items() if count > 0}
        coverage = self._topic_period_coverage(
            code="samples",
            description="Local paper sample coverage by publication month.",
            period_unit="month",
            topics=topics,
            periods=months,
            existing=existing,
            include_missing_topic_ids=include_missing_topic_ids,
        )
        coverage["empty_periods"] = [
            {
                "period": period["key"],
                "period_start": period["date_from"],
                "period_end": period["date_to"],
                "sample_count": 0,
            }
            for period in months
            if sum(
                counts.get((int(topic["topic_id"]), period["key"]), 0)
                for topic in topics
            )
            == 0
        ]
        return coverage

    def _cluster_dynamics_coverage(
        self,
        topics: list[dict[str, Any]],
        months: list[dict[str, Any]],
        *,
        include_missing_topic_ids: bool = False,
    ) -> dict[str, Any]:
        topic_ids = [int(topic["topic_id"]) for topic in topics]
        cluster_keys = [f"topic:{topic_id}" for topic_id in topic_ids]
        month_starts = [period["date_from"] for period in months]
        existing: set[tuple[int, str]] = set()
        if cluster_keys and month_starts:
            stmt = (
                select(
                    ResearchCluster.cluster_key,
                    ResearchCluster.source_topic_id,
                    ResearchClusterPeriodStat.period_start,
                )
                .join(
                    ResearchClusterPeriodStat,
                    ResearchClusterPeriodStat.cluster_id == ResearchCluster.id,
                )
                .where(
                    ResearchCluster.cluster_key.in_(cluster_keys),
                    ResearchClusterPeriodStat.period_start.in_(month_starts),
                )
            )
            for cluster_key, source_topic_id, period_start in self.session.execute(
                stmt
            ):
                topic_id = (
                    int(source_topic_id)
                    if source_topic_id is not None
                    else self._topic_id_from_cluster_key(str(cluster_key))
                )
                if topic_id in topic_ids:
                    existing.add((topic_id, self._month_key(period_start)))
        return self._topic_period_coverage(
            code="cluster_dynamics",
            description="Research cluster monthly dynamics coverage.",
            period_unit="month",
            topics=topics,
            periods=months,
            existing=existing,
            include_missing_topic_ids=include_missing_topic_ids,
        )

    def _quarter_report_coverage(
        self,
        topics: list[dict[str, Any]],
        quarters: list[dict[str, Any]],
    ) -> dict[str, Any]:
        topic_ids = [int(topic["topic_id"]) for topic in topics]
        period_keys = [period["key"] for period in quarters]
        existing: set[tuple[int, str]] = set()
        if topic_ids and period_keys:
            stmt = select(
                TopicQuarterReport.topic_id,
                TopicQuarterReport.period_key,
            ).where(
                TopicQuarterReport.topic_id.in_(topic_ids),
                TopicQuarterReport.period_key.in_(period_keys),
            )
            existing = {
                (int(topic_id), str(period_key))
                for topic_id, period_key in self.session.execute(stmt)
            }
        return self._topic_period_coverage(
            code="quarter_reports",
            description="Topic quarterly report coverage.",
            period_unit="quarter",
            topics=topics,
            periods=quarters,
            existing=existing,
        )

    def _topic_period_coverage(
        self,
        *,
        code: str,
        description: str,
        period_unit: str,
        topics: list[dict[str, Any]],
        periods: list[dict[str, Any]],
        existing: set[tuple[int, str]],
        include_missing_topic_ids: bool = False,
    ) -> dict[str, Any]:
        expected = {
            (int(topic["topic_id"]), period["key"])
            for topic in topics
            for period in periods
        }
        covered = existing & expected
        missing = expected - covered
        missing_by_period: list[dict[str, Any]] = []
        missing_by_topic: list[dict[str, Any]] = []

        for period in periods:
            missing_topics = [
                topic
                for topic in topics
                if (int(topic["topic_id"]), period["key"]) in missing
            ]
            if missing_topics:
                item = {
                    "period": period["key"],
                    "period_start": period["date_from"],
                    "period_end": period["date_to"],
                    "missing_topics": len(missing_topics),
                    "topic_sample": self._topic_sample(missing_topics),
                }
                if include_missing_topic_ids:
                    item["topic_ids"] = [
                        int(topic["topic_id"]) for topic in missing_topics
                    ]
                missing_by_period.append(item)

        for topic in topics:
            missing_periods = [
                period["key"]
                for period in periods
                if (int(topic["topic_id"]), period["key"]) in missing
            ]
            if missing_periods:
                missing_by_topic.append(
                    {
                        **self._topic_identity(topic),
                        "missing_periods": len(missing_periods),
                        "period_sample": missing_periods[: self.sample_limit],
                    }
                )

        return {
            "code": code,
            "description": description,
            "period_unit": period_unit,
            "periods": len(periods),
            "topics": len(topics),
            "expected_topic_periods": len(expected),
            "covered_topic_periods": len(covered),
            "missing_topic_periods": len(missing),
            "missing_by_period": missing_by_period,
            "missing_by_topic_sample": missing_by_topic[: self.sample_limit],
        }

    def _load_topics(
        self,
        *,
        field_ids: list[int] | None,
        topic_ids: list[int] | None,
    ) -> list[dict[str, Any]]:
        stmt = (
            select(
                Topic.id.label("topic_id"),
                Topic.name.label("topic_name"),
                Subfield.id.label("subfield_id"),
                Subfield.name.label("subfield_name"),
                Field.id.label("field_id"),
                Field.name.label("field_name"),
            )
            .outerjoin(Subfield, Subfield.id == Topic.subfield_id)
            .outerjoin(Field, Field.id == Subfield.field_id)
            .order_by(Field.name.asc(), Subfield.name.asc(), Topic.name.asc())
        )
        if topic_ids:
            stmt = stmt.where(Topic.id.in_(sorted(set(topic_ids))))
        if field_ids:
            stmt = stmt.where(Field.id.in_(sorted(set(field_ids))))
        return [dict(row) for row in self.session.execute(stmt).mappings()]

    def _load_sample_counts(
        self,
        topic_ids: list[int],
        months: list[dict[str, Any]],
    ) -> dict[tuple[int, str], int]:
        if not topic_ids or not months:
            return {}
        year_expr = func.extract("year", Paper.publication_date)
        month_expr = func.extract("month", Paper.publication_date)
        stmt = (
            select(
                PaperTopic.topic_id,
                year_expr.label("year"),
                month_expr.label("month"),
                func.count(Paper.id).label("sample_count"),
            )
            .join(Paper, Paper.id == PaperTopic.paper_id)
            .where(
                PaperTopic.topic_id.in_(topic_ids),
                Paper.publication_date >= months[0]["date_from"],
                Paper.publication_date <= months[-1]["date_to"],
            )
            .group_by(PaperTopic.topic_id, year_expr, month_expr)
        )
        result: dict[tuple[int, str], int] = {}
        for topic_id, year, month, sample_count in self.session.execute(stmt):
            key = f"{int(year):04d}-{int(month):02d}"
            result[(int(topic_id), key)] = int(sample_count or 0)
        return result

    def _field_summary(self, topics: list[dict[str, Any]]) -> list[dict[str, Any]]:
        fields: dict[int | None, dict[str, Any]] = {}
        for topic in topics:
            field_id = topic.get("field_id")
            item = fields.setdefault(
                field_id,
                {
                    "field_id": field_id,
                    "field_name": topic.get("field_name"),
                    "topics_count": 0,
                },
            )
            item["topics_count"] += 1
        return sorted(
            fields.values(),
            key=lambda item: (
                item["field_name"] is None,
                str(item["field_name"] or ""),
            ),
        )

    def _topic_sample(self, topics: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [self._topic_identity(topic) for topic in topics[: self.sample_limit]]

    def _topic_identity(self, topic: dict[str, Any]) -> dict[str, Any]:
        return {
            "topic_id": topic.get("topic_id"),
            "topic_name": topic.get("topic_name"),
            "field_id": topic.get("field_id"),
            "field_name": topic.get("field_name"),
        }

    def _month_periods(self, date_from: date, date_to: date) -> list[dict[str, Any]]:
        current = date(date_from.year, date_from.month, 1)
        end = date(date_to.year, date_to.month, 1)
        periods: list[dict[str, Any]] = []
        while current <= end:
            period_end = date(
                current.year,
                current.month,
                monthrange(current.year, current.month)[1],
            )
            periods.append(
                {
                    "key": self._month_key(current),
                    "date_from": current,
                    "date_to": period_end,
                }
            )
            current = self._next_month_start(current)
        return periods

    def _next_month_start(self, value: date) -> date:
        if value.month == 12:
            return date(value.year + 1, 1, 1)
        return date(value.year, value.month + 1, 1)

    def _month_key(self, value: date) -> str:
        return f"{value.year:04d}-{value.month:02d}"

    def _topic_id_from_cluster_key(self, cluster_key: str) -> int:
        prefix = "topic:"
        if not cluster_key.startswith(prefix):
            return -1
        try:
            return int(cluster_key.removeprefix(prefix))
        except ValueError:
            return -1


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse data CLI command and command-specific arguments."""
    settings = _preload_settings(argv)
    parser = argparse.ArgumentParser(description="Data maintenance CLI utilities.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    validate_parser = subparsers.add_parser(
        "validate-local-data",
        help="Validate local PostgreSQL data quality before indexing.",
    )
    validate_parser.add_argument(
        "--fix-safe",
        action="store_true",
        help="Apply safe string, DOI, and keyword normalizations.",
    )
    validate_parser.add_argument(
        "--report-json",
        default=None,
        help="Optional path to write JSON report.",
    )
    validate_parser.add_argument(
        "--env-file",
        default=None,
        help="Optional .env path. Defaults to ML_ENV_FILE or project .env.",
    )
    validate_parser.add_argument(
        "--database-url",
        default=None,
        help="SQLAlchemy database URL. Defaults to DATABASE_URL or POSTGRES_* envs.",
    )

    yearly_parser = subparsers.add_parser(
        "rebuild-openalex-yearly-topic-stats",
        help="Rebuild yearly OpenAlex topic stats from monthly rows.",
    )
    yearly_parser.add_argument(
        "--date-from",
        type=parse_iso_date,
        required=True,
        help="Monthly period lower bound in YYYY-MM-DD format.",
    )
    yearly_parser.add_argument(
        "--date-to",
        type=parse_iso_date,
        required=True,
        help="Monthly period upper bound in YYYY-MM-DD format.",
    )
    yearly_parser.add_argument(
        "--topic-ids",
        default=None,
        help="Comma-separated local topic ids. Defaults to all topics.",
    )
    yearly_parser.add_argument(
        "--exclude-artificial",
        action="store_true",
        help="Do not add artifical_pubdates_estimation to yearly works_count.",
    )
    yearly_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Build yearly rows but roll back instead of writing to PostgreSQL.",
    )
    yearly_parser.add_argument(
        "--report-json",
        default=None,
        help="Optional path to write JSON report.",
    )
    yearly_parser.add_argument(
        "--env-file",
        default=None,
        help="Optional .env path. Defaults to ML_ENV_FILE or project .env.",
    )
    yearly_parser.add_argument(
        "--database-url",
        default=None,
        help="SQLAlchemy database URL. Defaults to DATABASE_URL or POSTGRES_* envs.",
    )

    jan_parser = subparsers.add_parser(
        "check-openalex-jan1-anomalies",
        help="Check January-1 artificial publication estimates by year and taxonomy area.",
    )
    jan_parser.add_argument(
        "--date-from",
        type=parse_iso_date,
        required=True,
        help="Year lower bound in YYYY-MM-DD format.",
    )
    jan_parser.add_argument(
        "--date-to",
        type=parse_iso_date,
        required=True,
        help="Year upper bound in YYYY-MM-DD format.",
    )
    jan_parser.add_argument(
        "--topic-ids",
        default=None,
        help="Comma-separated local topic ids. Defaults to all topics.",
    )
    jan_parser.add_argument(
        "--field-ids",
        default=None,
        help="Comma-separated local field ids.",
    )
    jan_parser.add_argument(
        "--subfield-ids",
        default=None,
        help="Comma-separated local subfield ids.",
    )
    jan_parser.add_argument(
        "--min-estimate",
        type=int,
        default=1,
        help="Minimum artificial estimate to include. Defaults to 1.",
    )
    jan_parser.add_argument(
        "--sample-limit",
        type=int,
        default=settings.data.sample_limit,
        help="Maximum detailed topic rows in the report.",
    )
    jan_parser.add_argument(
        "--report-json",
        default=None,
        help="Optional path to write JSON report.",
    )
    jan_parser.add_argument(
        "--env-file",
        default=None,
        help="Optional .env path. Defaults to ML_ENV_FILE or project .env.",
    )
    jan_parser.add_argument(
        "--database-url",
        default=None,
        help="SQLAlchemy database URL. Defaults to DATABASE_URL or POSTGRES_* envs.",
    )

    coverage_parser = subparsers.add_parser(
        "analyze-coverage",
        help=("Analyze monthly and quarterly database coverage for fields or topics."),
    )
    coverage_parser.add_argument(
        "--date-from",
        type=parse_iso_date,
        required=True,
        help="Coverage period lower bound in YYYY-MM-DD format.",
    )
    coverage_parser.add_argument(
        "--date-to",
        type=parse_iso_date,
        required=True,
        help="Coverage period upper bound in YYYY-MM-DD format.",
    )
    coverage_parser.add_argument(
        "--field-ids",
        default=None,
        help="Comma-separated local field ids. Mutually exclusive with --topic-ids.",
    )
    coverage_parser.add_argument(
        "--topic-ids",
        default=None,
        help="Comma-separated local topic ids. Mutually exclusive with --field-ids.",
    )
    coverage_parser.add_argument(
        "--sample-limit",
        type=int,
        default=settings.data.sample_limit,
        help="Maximum detailed topics/periods per coverage section. Defaults to 20.",
    )
    coverage_parser.add_argument(
        "--include-missing-topic-ids",
        action="store_true",
        help="Include full missing topic id lists in missing_by_period sections.",
    )
    coverage_parser.add_argument(
        "--enqueue-missing-topic-stats",
        action="store_true",
        help="Enqueue high-priority worker collect_topic_stats tasks for missing publication stats.",
    )
    coverage_parser.add_argument(
        "--enqueue-missing-cluster-dynamics",
        action="store_true",
        help="Enqueue cluster_dynamics_recompute tasks for missing monthly cluster dynamics.",
    )
    coverage_parser.add_argument(
        "--enqueue-task-batch-size",
        type=int,
        default=settings.openalex.stats_task_batch_size,
        help="Maximum topic ids per enqueued collect_topic_stats task. Defaults to 100.",
    )
    coverage_parser.add_argument(
        "--enqueue-queue",
        default=OPENALEX_TOPIC_STATS_QUEUE,
        help=f"Redis queue for enqueued worker tasks. Defaults to {OPENALEX_TOPIC_STATS_QUEUE}.",
    )
    coverage_parser.add_argument(
        "--cluster-dynamics-queue",
        default=CLUSTER_DYNAMICS_RECOMPUTE_QUEUE,
        help=f"Redis queue for cluster dynamics tasks. Defaults to {CLUSTER_DYNAMICS_RECOMPUTE_QUEUE}.",
    )
    coverage_parser.add_argument(
        "--cluster-dynamics-batch-size",
        type=int,
        default=settings.operations.cluster_dynamics_batch_size,
        help="Maximum cluster ids per enqueued cluster dynamics task. Defaults to 50.",
    )
    coverage_parser.add_argument(
        "--languages",
        default=",".join(settings.openalex.languages),
        help="Comma-separated OpenAlex languages for enqueued stats tasks. Defaults to en,ru.",
    )
    coverage_parser.add_argument(
        "--types",
        default=",".join(settings.openalex.types),
        help="Comma-separated OpenAlex work types for enqueued stats tasks. Defaults to article.",
    )
    coverage_parser.add_argument(
        "--collect-batch-size",
        type=int,
        default=settings.openalex.stats_batch_size,
        help="PostgreSQL upsert batch size for enqueued stats tasks. Defaults to 500.",
    )
    coverage_parser.add_argument(
        "--request-workers",
        type=int,
        default=settings.openalex.stats_request_workers,
        help="OpenAlex request workers for enqueued stats tasks. Defaults to 8.",
    )
    coverage_parser.add_argument(
        "--rate-limit-rps",
        type=float,
        default=settings.openalex.stats_rate_limit_rps,
        help="OpenAlex request rate for enqueued stats tasks. Defaults to 10.",
    )
    coverage_parser.add_argument(
        "--max-retries",
        type=int,
        default=settings.openalex.stats_max_retries,
        help="OpenAlex retry count for enqueued stats tasks. Defaults to 5.",
    )
    coverage_parser.add_argument(
        "--normalize-january-first",
        action=argparse.BooleanOptionalAction,
        default=settings.openalex.normalize_january_first,
        help=(
            "Estimate and remove January-1 artificial publication dates in "
            "enqueued stats tasks. Enabled by default."
        ),
    )
    coverage_parser.add_argument(
        "--primary-topic-only",
        action=argparse.BooleanOptionalAction,
        default=settings.openalex.primary_topic_only,
        help=(
            "Use primary_topic.id instead of topics.id filter in enqueued stats tasks. "
            "Enabled by default."
        ),
    )
    coverage_parser.add_argument(
        "--openalex-url",
        default=None,
        help="OpenAlex base URL stored in enqueued stats tasks.",
    )
    coverage_parser.add_argument(
        "--openalex-api-key",
        default=None,
        help="OpenAlex API key stored in enqueued stats tasks.",
    )
    coverage_parser.add_argument(
        "--openalex-mailto",
        default=None,
        help="OpenAlex polite-pool email stored in enqueued stats tasks.",
    )
    coverage_parser.add_argument(
        "--report-json",
        default=None,
        help="Optional path to write JSON report.",
    )
    coverage_parser.add_argument(
        "--env-file",
        default=None,
        help="Optional .env path. Defaults to ML_ENV_FILE or project .env.",
    )
    coverage_parser.add_argument(
        "--database-url",
        default=None,
        help="SQLAlchemy database URL. Defaults to DATABASE_URL or POSTGRES_* envs.",
    )
    add_redis_args(coverage_parser)

    sample_coverage_parser = subparsers.add_parser(
        "analyze-sample-month-coverage",
        help="Analyze local monthly paper sample coverage for topics in one field.",
    )
    sample_coverage_parser.add_argument(
        "--field-id",
        type=int,
        required=True,
        help="Local field id whose topics should be analyzed.",
    )
    sample_coverage_parser.add_argument(
        "--date-from",
        type=parse_iso_date,
        required=True,
        help="Coverage period lower bound in YYYY-MM-DD format.",
    )
    sample_coverage_parser.add_argument(
        "--date-to",
        type=parse_iso_date,
        required=True,
        help="Coverage period upper bound in YYYY-MM-DD format.",
    )
    sample_coverage_parser.add_argument(
        "--topic-ids",
        default=None,
        help="Optional comma-separated local topic ids within the selected field.",
    )
    sample_coverage_parser.add_argument(
        "--sample-limit",
        type=int,
        default=settings.data.sample_limit,
        help="Maximum detailed topics/periods in the report. Defaults to 20.",
    )
    sample_coverage_parser.add_argument(
        "--include-missing-topic-ids",
        action="store_true",
        help="Include full missing topic id lists in missing_by_period sections.",
    )
    sample_coverage_parser.add_argument(
        "--enqueue-missing-samples",
        action="store_true",
        help="Enqueue OpenAlex bootstrap_papers tasks for missing sample months.",
    )
    sample_coverage_parser.add_argument(
        "--enqueue-task-batch-size",
        type=int,
        default=settings.openalex.bootstrap_task_batch_size,
        help="Maximum topic ids per enqueued bootstrap_papers task. Defaults to 100.",
    )
    sample_coverage_parser.add_argument(
        "--enqueue-queue",
        default=OPENALEX_BOOTSTRAP_PAPERS_QUEUE,
        help=f"Redis queue for enqueued bootstrap tasks. Defaults to {OPENALEX_BOOTSTRAP_PAPERS_QUEUE}.",
    )
    sample_coverage_parser.add_argument(
        "--target-count",
        type=int,
        default=settings.openalex.bootstrap_target_count,
        help="Target sample papers per enqueued topic/month batch. Defaults to 20.",
    )
    sample_coverage_parser.add_argument(
        "--languages",
        default=",".join(settings.openalex.languages),
        help="Comma-separated OpenAlex languages for enqueued bootstrap tasks. Defaults to en,ru.",
    )
    sample_coverage_parser.add_argument(
        "--types",
        default=",".join(settings.openalex.types),
        help="Comma-separated OpenAlex work types for enqueued bootstrap tasks. Defaults to article.",
    )
    sample_coverage_parser.add_argument(
        "--batch-size",
        type=int,
        default=settings.openalex.bootstrap_batch_size,
        help="PostgreSQL import batch size for enqueued bootstrap tasks. Defaults to 500.",
    )
    sample_coverage_parser.add_argument(
        "--request-workers",
        type=int,
        default=settings.openalex.bootstrap_request_workers,
        help="OpenAlex request workers for enqueued bootstrap tasks. Defaults to 8.",
    )
    sample_coverage_parser.add_argument(
        "--db-workers",
        type=int,
        default=settings.openalex.bootstrap_db_workers,
        help="PostgreSQL import workers for enqueued bootstrap tasks. Defaults to 2.",
    )
    sample_coverage_parser.add_argument(
        "--rate-limit-rps",
        type=float,
        default=settings.openalex.bootstrap_rate_limit_rps,
        help="OpenAlex request rate for enqueued bootstrap tasks. Defaults to 70.",
    )
    sample_coverage_parser.add_argument(
        "--seed",
        type=int,
        default=settings.openalex.bootstrap_seed,
        help="Base OpenAlex sample seed for enqueued bootstrap tasks. Defaults to 42.",
    )
    sample_coverage_parser.add_argument(
        "--per-page",
        type=int,
        default=settings.openalex.bootstrap_per_page,
        help="OpenAlex per-page value for enqueued bootstrap tasks. Defaults to 100.",
    )
    sample_coverage_parser.add_argument(
        "--max-retries",
        type=int,
        default=settings.openalex.bootstrap_max_retries,
        help="OpenAlex retry count for enqueued bootstrap tasks. Defaults to 5.",
    )
    sample_coverage_parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Ask bootstrap task processor not to update existing papers.",
    )
    sample_coverage_parser.add_argument(
        "--enqueue-indexing",
        action="store_true",
        help="Ask bootstrap task processor to enqueue imported paper ids for indexing.",
    )
    sample_coverage_parser.add_argument(
        "--primary-topic-only",
        action=argparse.BooleanOptionalAction,
        default=settings.openalex.primary_topic_only,
        help="Use primary_topic topic filters in bootstrap task processors. Enabled by default.",
    )
    sample_coverage_parser.add_argument(
        "--openalex-url",
        default=None,
        help="OpenAlex base URL stored in enqueued bootstrap tasks.",
    )
    sample_coverage_parser.add_argument(
        "--openalex-api-key",
        default=None,
        help="OpenAlex API key stored in enqueued bootstrap tasks.",
    )
    sample_coverage_parser.add_argument(
        "--openalex-mailto",
        default=None,
        help="OpenAlex polite-pool email stored in enqueued bootstrap tasks.",
    )
    sample_coverage_parser.add_argument(
        "--report-json",
        default=None,
        help="Optional path to write JSON report.",
    )
    sample_coverage_parser.add_argument(
        "--env-file",
        default=None,
        help="Optional .env path. Defaults to ML_ENV_FILE or project .env.",
    )
    sample_coverage_parser.add_argument(
        "--database-url",
        default=None,
        help="SQLAlchemy database URL. Defaults to DATABASE_URL or POSTGRES_* envs.",
    )
    add_redis_args(sample_coverage_parser)

    sync_parser = subparsers.add_parser(
        "sync-clusters-db",
        help="Synchronize research cluster PostgreSQL rows from Qdrant.",
    )
    sync_parser.add_argument("--scope", choices=["all", "clusters", "periods"], default="all")
    sync_parser.add_argument("--batch-size", type=int, default=256)
    sync_parser.add_argument("--cluster-id", default=None)
    sync_parser.add_argument("--dry-run", action="store_true")
    sync_parser.add_argument("--no-prune-missing", action="store_true")
    sync_parser.add_argument("--database-url", default=None)
    add_qdrant_args(sync_parser)

    init_parser = subparsers.add_parser(
        "init-qdrant",
        help="Initialize Qdrant collections and payload indexes.",
    )
    init_parser.add_argument("vector_size", type=int)
    init_parser.add_argument(
        "--collection",
        choices=[
            "all",
            "papers",
            "research_entities",
            "trend_clusters",
            "trend_cluster_periods",
            "user_profiles",
        ],
        default="all",
    )
    init_parser.add_argument(
        "--distance",
        choices=["Cosine", "Dot", "Euclid", "Manhattan"],
        default=settings.data.qdrant_distance,
    )
    add_qdrant_args(init_parser)

    command_parsers = [
        validate_parser,
        yearly_parser,
        jan_parser,
        coverage_parser,
        sample_coverage_parser,
        sync_parser,
        init_parser,
    ]
    for command_parser in command_parsers:
        add_logging_args(command_parser)
        command_parser.add_argument(
            "--config-file",
            default=None,
            help="Optional TOML override file. Defaults to ML_CONFIG_FILE when set.",
        )
        if not any(action.dest == "env_file" for action in command_parser._actions):
            command_parser.add_argument(
                "--env-file",
                default=None,
                help="Optional .env path. Defaults to ML_ENV_FILE or project .env.",
            )

    args = parser.parse_args(argv)
    args.settings = settings
    return args


def main(argv: list[str] | None = None) -> int:
    """Run the requested data CLI command."""
    args = parse_args(argv)
    args.settings = load_settings(
        config_file=getattr(args, "config_file", None),
        env_file=args.env_file,
    )
    configure_logging(args.settings, verbosity=getattr(args, "verbose", 0))
    log_event(logger, "cli_command_started", category="data", command=args.command)

    try:
        if args.command == "validate-local-data":
            report = run_validate_local_data(args)
        elif args.command == "rebuild-openalex-yearly-topic-stats":
            report = run_rebuild_openalex_yearly_topic_stats(args)
        elif args.command == "check-openalex-jan1-anomalies":
            report = run_check_openalex_jan1_anomalies(args)
        elif args.command == "analyze-coverage":
            report = run_analyze_coverage(args)
        elif args.command == "analyze-sample-month-coverage":
            report = run_analyze_sample_month_coverage(args)
        elif args.command == "sync-clusters-db":
            report = run_sync_clusters_db(args)
        elif args.command == "init-qdrant":
            report = run_init_qdrant(args)
        else:
            raise AssertionError(f"Unhandled command: {args.command}")
        if getattr(args, "report_json", None):
            write_report(Path(args.report_json), report)
    except Exception as exc:
        log_event(
            logger,
            "cli_command_failed",
            level=logging.ERROR,
            exc_info=True,
            category="data",
            command=args.command,
            error_type=exc.__class__.__name__,
        )
        print_json(
            {
                "error": {
                    "code": exc.__class__.__name__,
                    "message": str(exc),
                    "details": {},
                }
            },
            stream=sys.stderr,
        )
        return 1

    log_event(logger, "cli_command_completed", category="data", command=args.command)
    print_json(report)
    return 0


def run_validate_local_data(args: argparse.Namespace) -> dict[str, Any]:
    """Validate local data and optionally apply safe fixes."""
    database_url = args.database_url or _settings(args).database_url
    engine = create_db_engine(database_url, echo=False)
    SessionLocal = create_session_factory(engine, expire_on_commit=False)

    try:
        with SessionLocal() as session:
            validator = LocalDataValidator(session)
            initial_checks = validator.validate()
            fixes = {"applied": [], "skipped": []}
            checks = initial_checks

            if args.fix_safe:
                fixes = validator.fix_safe()
                session.commit()
                checks = validator.validate()

            return build_report(
                checks=checks,
                fix_safe=args.fix_safe,
                fixes=fixes,
                initial_checks=initial_checks if args.fix_safe else None,
            )
    finally:
        engine.dispose()


def run_rebuild_openalex_yearly_topic_stats(args: argparse.Namespace) -> dict[str, Any]:
    """Rebuild yearly OpenAlex topic stats from monthly stats."""
    if args.date_from > args.date_to:
        raise ValueError("--date-from must be before or equal to --date-to.")
    topic_ids = parse_int_csv_arg(args.topic_ids) if args.topic_ids else None
    database_url = args.database_url or _settings(args).database_url
    engine = create_db_engine(database_url, echo=False)
    SessionLocal = create_session_factory(engine, expire_on_commit=False)

    try:
        with SessionLocal() as session:
            repository = OpenAlexYearlyTopicStatsRepository(session)
            items = repository.rebuild_from_monthly(
                date_from=args.date_from,
                date_to=args.date_to,
                topic_ids=topic_ids,
                include_artificial=not args.exclude_artificial,
            )
            if args.dry_run:
                session.rollback()
            else:
                session.commit()
            return {
                "command": "rebuild-openalex-yearly-topic-stats",
                "date_from": args.date_from,
                "date_to": args.date_to,
                "topic_ids": topic_ids,
                "include_artificial": not args.exclude_artificial,
                "dry_run": args.dry_run,
                "upserted": len(items),
                "sample": [
                    {
                        "topic_id": item.topic_id,
                        "stat_year": item.stat_year,
                        "works_count": item.works_count,
                        "artifical_pubdates_estimation": (
                            item.artifical_pubdates_estimation
                        ),
                    }
                    for item in items[:SAMPLE_LIMIT]
                ],
            }
    finally:
        engine.dispose()


def run_check_openalex_jan1_anomalies(args: argparse.Namespace) -> dict[str, Any]:
    """Report January-1 artificial publication estimates by year/topic/area."""
    if args.date_from > args.date_to:
        raise ValueError("--date-from must be before or equal to --date-to.")
    if args.min_estimate < 0:
        raise ValueError("--min-estimate must be non-negative.")
    if args.sample_limit < 0:
        raise ValueError("--sample-limit must be non-negative.")
    topic_ids = parse_int_csv_arg(args.topic_ids) if args.topic_ids else None
    field_ids = parse_int_csv_arg(args.field_ids) if args.field_ids else None
    subfield_ids = parse_int_csv_arg(args.subfield_ids) if args.subfield_ids else None
    database_url = args.database_url or _settings(args).database_url
    engine = create_db_engine(database_url, echo=False)
    SessionLocal = create_session_factory(engine, expire_on_commit=False)

    try:
        with SessionLocal() as session:
            rows = _load_january_anomaly_rows(
                session,
                date_from=args.date_from,
                date_to=args.date_to,
                topic_ids=topic_ids,
                field_ids=field_ids,
                subfield_ids=subfield_ids,
                min_estimate=args.min_estimate,
            )
            return {
                "command": "check-openalex-jan1-anomalies",
                "date_from": args.date_from,
                "date_to": args.date_to,
                "min_estimate": args.min_estimate,
                "summary": _january_anomaly_summary(rows),
                "sample": rows[: args.sample_limit],
            }
    finally:
        engine.dispose()


def run_analyze_coverage(args: argparse.Namespace) -> dict[str, Any]:
    """Analyze database coverage for publication stats, samples, and reports."""
    if args.date_from > args.date_to:
        raise ValueError("--date-from must be before or equal to --date-to.")
    if args.sample_limit < 0:
        raise ValueError("--sample-limit must be non-negative.")
    if args.enqueue_task_batch_size <= 0:
        raise ValueError("--enqueue-task-batch-size must be a positive integer.")
    if args.cluster_dynamics_batch_size <= 0:
        raise ValueError("--cluster-dynamics-batch-size must be a positive integer.")
    if args.collect_batch_size <= 0 or args.collect_batch_size > 500:
        raise ValueError("--collect-batch-size must be between 1 and 500.")
    if args.request_workers <= 0:
        raise ValueError("--request-workers must be a positive integer.")
    if args.rate_limit_rps <= 0:
        raise ValueError("--rate-limit-rps must be positive.")
    if args.max_retries < 0:
        raise ValueError("--max-retries must be non-negative.")
    field_ids = parse_int_csv_arg(args.field_ids) if args.field_ids else None
    topic_ids = parse_int_csv_arg(args.topic_ids) if args.topic_ids else None
    if field_ids and topic_ids:
        raise ValueError("--field-ids and --topic-ids are mutually exclusive.")

    database_url = args.database_url or _settings(args).database_url
    engine = create_db_engine(database_url, echo=False)
    SessionLocal = create_session_factory(engine, expire_on_commit=False)

    try:
        with SessionLocal() as session:
            analyzer = DataCoverageAnalyzer(session, sample_limit=args.sample_limit)
            report = analyzer.analyze(
                date_from=args.date_from,
                date_to=args.date_to,
                field_ids=field_ids,
                topic_ids=topic_ids,
                include_missing_topic_ids=(
                    args.include_missing_topic_ids
                    or args.enqueue_missing_topic_stats
                    or args.enqueue_missing_cluster_dynamics
                ),
            )
            if args.enqueue_missing_topic_stats:
                report["enqueued_worker_tasks"] = enqueue_missing_topic_stats_tasks(
                    report,
                    redis_adapter=RedisAdapter(build_redis_client(args)),
                    queue_name=args.enqueue_queue,
                    task_batch_size=args.enqueue_task_batch_size,
                    languages=parse_csv_arg(args.languages),
                    types=parse_csv_arg(args.types),
                    batch_size=args.collect_batch_size,
                    request_workers=args.request_workers,
                    rate_limit_rps=args.rate_limit_rps,
                    max_retries=args.max_retries,
                    normalize_january_first=bool(args.normalize_january_first),
                    primary_topic_only=bool(args.primary_topic_only),
                    openalex_url=args.openalex_url,
                    openalex_api_key=args.openalex_api_key,
                    openalex_mailto=args.openalex_mailto,
                )
            if args.enqueue_missing_cluster_dynamics:
                report["enqueued_cluster_dynamics_tasks"] = (
                    enqueue_missing_cluster_dynamics_tasks(
                        report,
                        redis_adapter=RedisAdapter(build_redis_client(args)),
                        queue_name=args.cluster_dynamics_queue,
                        batch_size=args.cluster_dynamics_batch_size,
                    )
                )
            return report
    finally:
        engine.dispose()


def run_analyze_sample_month_coverage(args: argparse.Namespace) -> dict[str, Any]:
    """Analyze local monthly paper sample coverage for topics in a field."""
    if args.date_from > args.date_to:
        raise ValueError("--date-from must be before or equal to --date-to.")
    if args.field_id <= 0:
        raise ValueError("--field-id must be a positive integer.")
    if args.sample_limit < 0:
        raise ValueError("--sample-limit must be non-negative.")
    if args.enqueue_task_batch_size <= 0:
        raise ValueError("--enqueue-task-batch-size must be a positive integer.")
    if args.target_count < 0:
        raise ValueError("--target-count must be non-negative.")
    if args.batch_size <= 0 or args.batch_size > 500:
        raise ValueError("--batch-size must be between 1 and 500.")
    if args.request_workers <= 0:
        raise ValueError("--request-workers must be a positive integer.")
    if args.db_workers <= 0:
        raise ValueError("--db-workers must be a positive integer.")
    if args.rate_limit_rps <= 0:
        raise ValueError("--rate-limit-rps must be positive.")
    if args.per_page <= 0 or args.per_page > 100:
        raise ValueError("--per-page must be between 1 and 100.")
    if args.max_retries < 0:
        raise ValueError("--max-retries must be non-negative.")

    topic_ids = parse_int_csv_arg(args.topic_ids) if args.topic_ids else None
    database_url = args.database_url or _settings(args).database_url
    engine = create_db_engine(database_url, echo=False)
    SessionLocal = create_session_factory(engine, expire_on_commit=False)

    try:
        with SessionLocal() as session:
            analyzer = DataCoverageAnalyzer(session, sample_limit=args.sample_limit)
            report = analyzer.analyze_sample_month_coverage(
                date_from=args.date_from,
                date_to=args.date_to,
                field_id=args.field_id,
                topic_ids=topic_ids,
                include_missing_topic_ids=(
                    args.include_missing_topic_ids or args.enqueue_missing_samples
                ),
            )
            if args.enqueue_missing_samples:
                report["enqueued_worker_tasks"] = enqueue_missing_sample_tasks(
                    report,
                    redis_adapter=RedisAdapter(build_redis_client(args)),
                    queue_name=args.enqueue_queue,
                    task_batch_size=args.enqueue_task_batch_size,
                    target_count=args.target_count,
                    languages=parse_csv_arg(args.languages),
                    types=parse_csv_arg(args.types),
                    batch_size=args.batch_size,
                    request_workers=args.request_workers,
                    db_workers=args.db_workers,
                    rate_limit_rps=args.rate_limit_rps,
                    seed=args.seed,
                    per_page=args.per_page,
                    max_retries=args.max_retries,
                    skip_existing=bool(args.skip_existing),
                    enqueue_indexing=bool(
                        args.enqueue_indexing or args.enqueue_missing_samples
                    ),
                    enqueue_cluster_dynamics=True,
                    primary_topic_only=bool(args.primary_topic_only),
                    openalex_url=args.openalex_url,
                    openalex_api_key=args.openalex_api_key,
                    openalex_mailto=args.openalex_mailto,
                )
            return report
    finally:
        engine.dispose()


def run_sync_clusters_db(args: argparse.Namespace) -> dict[str, Any]:
    """Synchronize research cluster read-model tables from Qdrant."""
    if args.batch_size <= 0:
        raise ValueError("--batch-size must be a positive integer.")
    engine = create_db_engine(args.database_url or _settings(args).database_url, echo=False)
    SessionLocal = create_session_factory(engine, expire_on_commit=False)
    try:
        with SessionLocal() as session:
            facade = ClusterDbSyncFacade(
                qdrant_adapter=build_qdrant_adapter(args),
                research_cluster_repository=ResearchClusterRepository(session),
                event_sink=NoopEventSink(),
            )
            common_kwargs = {
                "batch_size": args.batch_size,
                "cluster_id": args.cluster_id,
                "prune_missing": not args.no_prune_missing,
                "dry_run": bool(args.dry_run),
            }
            if args.scope == "clusters":
                result = facade.sync_clusters_from_qdrant(**common_kwargs)
            elif args.scope == "periods":
                result = facade.sync_periods_from_qdrant(**common_kwargs)
            else:
                result = facade.sync_all(**common_kwargs)
            if args.dry_run:
                session.rollback()
            else:
                session.commit()
        return {
            "command": "sync-clusters-db",
            "scope": args.scope,
            "cluster_id": args.cluster_id,
            "dry_run": bool(args.dry_run),
            "prune_missing": not args.no_prune_missing,
            "result": result.model_dump(mode="json"),
        }
    finally:
        engine.dispose()


def run_init_qdrant(args: argparse.Namespace) -> dict[str, Any]:
    """Initialize requested Qdrant collections and payload indexes."""
    if args.vector_size <= 0:
        raise ValueError("vector_size must be a positive integer.")
    initializer = QdrantCollectionInitializer(
        build_qdrant_adapter(args),
        distance=args.distance,
    )
    handlers = {
        "all": initializer.ensure_all,
        "papers": initializer.ensure_papers_collection,
        "research_entities": initializer.ensure_research_entities_collection,
        "trend_clusters": initializer.ensure_trend_clusters_collection,
        "trend_cluster_periods": initializer.ensure_trend_cluster_periods_collection,
        "user_profiles": initializer.ensure_user_profiles_collection,
    }
    handlers[args.collection](args.vector_size)
    return {
        "command": "init-qdrant",
        "status": "ok",
        "collection": args.collection,
        "vector_size": args.vector_size,
        "distance": args.distance,
    }


def enqueue_missing_sample_tasks(
    report: dict[str, Any],
    *,
    redis_adapter: RedisAdapter,
    queue_name: str,
    task_batch_size: int,
    target_count: int,
    languages: list[str],
    types: list[str],
    batch_size: int,
    request_workers: int,
    db_workers: int,
    rate_limit_rps: float,
    seed: int,
    per_page: int,
    max_retries: int,
    skip_existing: bool = False,
    enqueue_indexing: bool = False,
    enqueue_cluster_dynamics: bool = True,
    primary_topic_only: bool = True,
    openalex_url: str | None = None,
    openalex_api_key: str | None = None,
    openalex_mailto: str | None = None,
) -> dict[str, Any]:
    """Enqueue bootstrap_papers tasks for missing local sample months."""
    samples = report["checks"]["samples"]
    field_id = int(report["scope"]["field_id"])
    messages: list[dict[str, Any]] = []
    missing_periods = samples.get("missing_by_period", [])
    if not isinstance(missing_periods, list):
        raise ValueError("Coverage report samples.missing_by_period is invalid.")

    for period in missing_periods:
        topic_ids = period.get("topic_ids") if isinstance(period, dict) else None
        if not isinstance(topic_ids, list):
            raise ValueError(
                "Coverage report does not include full missing topic ids. "
                "Run analyzer with include_missing_topic_ids enabled."
            )
        for chunk in chunked_ints(
            [int(topic_id) for topic_id in topic_ids], task_batch_size
        ):
            messages.append(
                {
                    "task_type": BOOTSTRAP_PAPERS_TASK,
                    "date_from": _iso_date_value(period["period_start"]),
                    "date_to": _iso_date_value(period["period_end"]),
                    "field_id": field_id,
                    "topic_ids": chunk,
                    "target_count": target_count,
                    "target_scope": "month",
                    "target_unit": "topic",
                    "languages": languages,
                    "types": types,
                    "batch_size": batch_size,
                    "request_workers": request_workers,
                    "db_workers": db_workers,
                    "rate_limit_rps": rate_limit_rps,
                    "seed": seed,
                    "per_page": per_page,
                    "max_retries": max_retries,
                    "skip_existing": bool(skip_existing),
                    "enqueue_indexing": bool(enqueue_indexing),
                    "enqueue_cluster_dynamics": bool(enqueue_cluster_dynamics),
                    "primary_topic_only": bool(primary_topic_only),
                    "source_topic_ids": chunk,
                    "workflow_date_from": _iso_date_value(period["period_start"]),
                    "workflow_date_to": _iso_date_value(period["period_end"]),
                    "workflow_granularity": "month",
                    "show_progress": False,
                    **({"openalex_url": openalex_url} if openalex_url else {}),
                    **(
                        {"openalex_api_key": openalex_api_key}
                        if openalex_api_key
                        else {}
                    ),
                    **({"openalex_mailto": openalex_mailto} if openalex_mailto else {}),
                }
            )

    for message in messages:
        redis_adapter.enqueue(queue_name, message)

    return {
        "queue": queue_name,
        "source_check": "samples",
        "missing_topic_periods": int(samples.get("missing_topic_periods") or 0),
        "messages": len(messages),
        "task_batch_size": task_batch_size,
        "target_count": target_count,
        "sample": messages[:3],
    }


def enqueue_missing_cluster_dynamics_tasks(
    report: dict[str, Any],
    *,
    redis_adapter: RedisAdapter,
    queue_name: str,
    batch_size: int,
) -> dict[str, Any]:
    """Enqueue cluster_dynamics_recompute tasks for missing monthly dynamics."""
    cluster_dynamics = report["checks"]["cluster_dynamics"]
    messages: list[dict[str, Any]] = []
    missing_periods = cluster_dynamics.get("missing_by_period", [])
    if not isinstance(missing_periods, list):
        raise ValueError(
            "Coverage report cluster_dynamics.missing_by_period is invalid."
        )

    for period in missing_periods:
        topic_ids = period.get("topic_ids") if isinstance(period, dict) else None
        if not isinstance(topic_ids, list):
            raise ValueError(
                "Coverage report does not include full missing topic ids. "
                "Run analyzer with include_missing_topic_ids enabled."
            )
        cluster_ids = [f"topic:{int(topic_id)}" for topic_id in topic_ids]
        date_from = _iso_date_value(period["period_start"])
        date_to = _iso_date_value(period["period_end"])
        accepted_cluster_ids = acquire_cluster_dynamics_cluster_ids(
            redis_adapter,
            cluster_ids,
            date_from=date_from,
            date_to=date_to,
            granularity="month",
        )
        for chunk in [
            accepted_cluster_ids[index : index + batch_size]
            for index in range(0, len(accepted_cluster_ids), batch_size)
        ]:
            messages.append(
                build_cluster_dynamics_message(
                    chunk,
                    date_from=date_from,
                    date_to=date_to,
                    granularity="month",
                )
            )

    try:
        for message in messages:
            redis_adapter.enqueue(queue_name, message)
    except Exception:
        for message in messages:
            release_cluster_dynamics_dedupe_keys(redis_adapter, message)
        raise

    accepted_count = sum(len(message["cluster_ids"]) for message in messages)
    return {
        "queue": queue_name,
        "source_check": "cluster_dynamics",
        "missing_topic_periods": int(
            cluster_dynamics.get("missing_topic_periods") or 0
        ),
        "messages": len(messages),
        "dedupe_skipped": int(cluster_dynamics.get("missing_topic_periods") or 0)
        - accepted_count,
        "batch_size": batch_size,
        "sample": messages[:3],
    }


def enqueue_missing_topic_stats_tasks(
    report: dict[str, Any],
    *,
    redis_adapter: RedisAdapter,
    queue_name: str,
    task_batch_size: int,
    languages: list[str],
    types: list[str],
    batch_size: int,
    request_workers: int,
    rate_limit_rps: float,
    max_retries: int,
    normalize_january_first: bool = True,
    primary_topic_only: bool = True,
    openalex_url: str | None = None,
    openalex_api_key: str | None = None,
    openalex_mailto: str | None = None,
) -> dict[str, Any]:
    """Enqueue collect_topic_stats tasks for missing publication stat months."""
    publication_stats = report["checks"]["publication_stats"]
    messages: list[dict[str, Any]] = []
    missing_periods = publication_stats.get("missing_by_period", [])
    if not isinstance(missing_periods, list):
        raise ValueError(
            "Coverage report publication_stats.missing_by_period is invalid."
        )

    for period in missing_periods:
        topic_ids = period.get("topic_ids") if isinstance(period, dict) else None
        if not isinstance(topic_ids, list):
            raise ValueError(
                "Coverage report does not include full missing topic ids. "
                "Run analyzer with include_missing_topic_ids enabled."
            )
        for chunk in chunked_ints(
            [int(topic_id) for topic_id in topic_ids], task_batch_size
        ):
            messages.append(
                {
                    "task_type": COLLECT_TOPIC_STATS_TASK,
                    "date_from": _iso_date_value(period["period_start"]),
                    "date_to": _iso_date_value(period["period_end"]),
                    "taxonomy_scope": "topic",
                    "topic_ids": chunk,
                    "languages": languages,
                    "types": types,
                    "batch_size": batch_size,
                    "request_workers": request_workers,
                    "rate_limit_rps": rate_limit_rps,
                    "max_retries": max_retries,
                    "group_by_page_size": 200,
                    "normalize_january_first": bool(normalize_january_first),
                    "primary_topic_only": bool(primary_topic_only),
                    "show_progress": False,
                    **({"openalex_url": openalex_url} if openalex_url else {}),
                    **(
                        {"openalex_api_key": openalex_api_key}
                        if openalex_api_key
                        else {}
                    ),
                    **({"openalex_mailto": openalex_mailto} if openalex_mailto else {}),
                }
            )

    for message in messages:
        redis_adapter.enqueue(queue_name, message)

    return {
        "queue": queue_name,
        "priority": "max",
        "source_check": "publication_stats",
        "missing_topic_periods": int(
            publication_stats.get("missing_topic_periods") or 0
        ),
        "messages": len(messages),
        "task_batch_size": task_batch_size,
        "sample": messages[:3],
    }


def _load_january_anomaly_rows(
    session: Session,
    *,
    date_from: date,
    date_to: date,
    topic_ids: list[int] | None,
    field_ids: list[int] | None,
    subfield_ids: list[int] | None,
    min_estimate: int,
) -> list[dict[str, Any]]:
    stmt = (
        select(
            OpenAlexYearlyTopicStat.stat_year,
            OpenAlexYearlyTopicStat.topic_id,
            Topic.name.label("topic_name"),
            Subfield.id.label("subfield_id"),
            Subfield.name.label("subfield_name"),
            Field.id.label("field_id"),
            Field.name.label("field_name"),
            OpenAlexYearlyTopicStat.works_count,
            OpenAlexYearlyTopicStat.artifical_pubdates_estimation,
        )
        .join(Topic, Topic.id == OpenAlexYearlyTopicStat.topic_id)
        .outerjoin(Subfield, Subfield.id == Topic.subfield_id)
        .outerjoin(Field, Field.id == Subfield.field_id)
        .where(
            OpenAlexYearlyTopicStat.stat_year >= date(date_from.year, 1, 1),
            OpenAlexYearlyTopicStat.stat_year <= date(date_to.year, 1, 1),
            OpenAlexYearlyTopicStat.artifical_pubdates_estimation >= min_estimate,
        )
        .order_by(
            OpenAlexYearlyTopicStat.artifical_pubdates_estimation.desc(),
            OpenAlexYearlyTopicStat.stat_year.asc(),
            Topic.name.asc(),
        )
    )
    if topic_ids:
        stmt = stmt.where(OpenAlexYearlyTopicStat.topic_id.in_(sorted(set(topic_ids))))
    if field_ids:
        stmt = stmt.where(Field.id.in_(sorted(set(field_ids))))
    if subfield_ids:
        stmt = stmt.where(Subfield.id.in_(sorted(set(subfield_ids))))
    return [dict(row) for row in session.execute(stmt).mappings()]


def _january_anomaly_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_year: dict[str, dict[str, Any]] = {}
    by_field: dict[tuple[str, int | None], dict[str, Any]] = {}
    by_subfield: dict[tuple[str, int | None], dict[str, Any]] = {}
    affected_topics: set[int] = set()

    for row in rows:
        year = str(row["stat_year"].year)
        artificial = int(row["artifical_pubdates_estimation"] or 0)
        topic_id = int(row["topic_id"])
        affected_topics.add(topic_id)

        year_item = by_year.setdefault(
            year,
            {"year": year, "topics": set(), "artifical_pubdates_estimation": 0},
        )
        year_item["topics"].add(topic_id)
        year_item["artifical_pubdates_estimation"] += artificial

        field_key = (year, row.get("field_id"))
        field_item = by_field.setdefault(
            field_key,
            {
                "year": year,
                "field_id": row.get("field_id"),
                "field_name": row.get("field_name"),
                "topics": set(),
                "artifical_pubdates_estimation": 0,
            },
        )
        field_item["topics"].add(topic_id)
        field_item["artifical_pubdates_estimation"] += artificial

        subfield_key = (year, row.get("subfield_id"))
        subfield_item = by_subfield.setdefault(
            subfield_key,
            {
                "year": year,
                "subfield_id": row.get("subfield_id"),
                "subfield_name": row.get("subfield_name"),
                "field_id": row.get("field_id"),
                "field_name": row.get("field_name"),
                "topics": set(),
                "artifical_pubdates_estimation": 0,
            },
        )
        subfield_item["topics"].add(topic_id)
        subfield_item["artifical_pubdates_estimation"] += artificial

    return {
        "rows": len(rows),
        "affected_topics": len(affected_topics),
        "artifical_pubdates_estimation": sum(
            int(row["artifical_pubdates_estimation"] or 0) for row in rows
        ),
        "by_year": _finalize_anomaly_groups(by_year.values()),
        "by_field": _finalize_anomaly_groups(by_field.values()),
        "by_subfield": _finalize_anomaly_groups(by_subfield.values()),
    }


def _finalize_anomaly_groups(groups: Any) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for group in groups:
        item = dict(group)
        topics = item.pop("topics", set())
        item["affected_topics"] = len(topics)
        result.append(item)
    return sorted(
        result,
        key=lambda item: (
            str(item.get("year")),
            -(int(item.get("artifical_pubdates_estimation") or 0)),
        ),
    )


def build_report(
    *,
    checks: list[dict[str, Any]],
    fix_safe: bool,
    fixes: dict[str, list[dict[str, Any]]],
    initial_checks: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Build a stable JSON-serializable validation report."""
    total_issues = sum(int(check["count"]) for check in checks)
    checks_failed = sum(1 for check in checks if int(check["count"]) > 0)
    report: dict[str, Any] = {
        "command": "validate-local-data",
        "fix_safe": fix_safe,
        "summary": {
            "total_issues": total_issues,
            "checks_failed": checks_failed,
            "fixes_applied": sum(item["count"] for item in fixes["applied"]),
            "fixes_skipped": sum(item["count"] for item in fixes["skipped"]),
        },
        "checks": checks,
        "fixes": fixes,
    }
    if initial_checks is not None:
        report["initial_summary"] = {
            "total_issues": sum(int(check["count"]) for check in initial_checks),
            "checks_failed": sum(
                1 for check in initial_checks if int(check["count"]) > 0
            ),
        }
        report["initial_checks"] = initial_checks
    return report


def normalize_doi(value: str | None) -> str | None:
    """Normalize a DOI enough for duplicate detection and safe updates."""
    if value is None:
        return None
    normalized = value.strip().lower()
    prefixes = (
        "https://doi.org/",
        "http://doi.org/",
        "https://dx.doi.org/",
        "http://dx.doi.org/",
        "doi:",
    )
    for prefix in prefixes:
        if normalized.startswith(prefix):
            normalized = normalized[len(prefix) :]
            break
    normalized = normalized.strip()
    return normalized or None


def parse_iso_date(value: str) -> date:
    """Parse an ISO date for argparse."""
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Expected YYYY-MM-DD date, got {value!r}."
        ) from exc


def parse_int_csv_arg(value: str) -> list[int]:
    """Parse comma-separated integer ids."""
    result: list[int] = []
    for item in value.split(","):
        item = item.strip()
        if not item:
            continue
        try:
            result.append(int(item))
        except ValueError as exc:
            raise ValueError(
                f"Expected comma-separated integer ids, got {item!r}."
            ) from exc
    return result


def parse_csv_arg(value: str) -> list[str]:
    """Parse comma-separated strings preserving first occurrence order."""
    result: list[str] = []
    seen: set[str] = set()
    for item in value.split(","):
        clean = item.strip()
        if not clean or clean in seen:
            continue
        result.append(clean)
        seen.add(clean)
    return result


def chunked_ints(values: list[int], size: int) -> list[list[int]]:
    """Split integer values into fixed-size chunks."""
    return [values[index : index + size] for index in range(0, len(values), size)]


def _iso_date_value(value: Any) -> str:
    if isinstance(value, date):
        return value.isoformat()
    return date.fromisoformat(str(value)[:10]).isoformat()


def normalize_keyword(value: str) -> str:
    """Normalize keyword value for safe local cleanup."""
    return value.strip().lower()


def normalize_external_id(value: str | None) -> str | None:
    """Normalize source external ids for duplicate detection."""
    if value is None:
        return None
    normalized = value.strip().rstrip("/").lower()
    return normalized or None


def normalize_nullable_trim(value: str | None) -> str | None:
    """Trim nullable text and convert empty strings to None."""
    if value is None:
        return None
    normalized = value.strip()
    return normalized or None


def write_report(path: Path, report: dict[str, Any]) -> None:
    """Write validation report to JSON file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )


def print_json(payload: dict[str, Any], *, stream: Any = sys.stdout) -> None:
    """Print JSON with stable UTF-8 output."""
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str), file=stream)


def add_redis_args(parser: argparse.ArgumentParser) -> None:
    """Add Redis connection arguments."""
    parser.add_argument(
        "--redis-url",
        default=None,
        help="Redis URL. Defaults to REDIS_URL when set.",
    )
    parser.add_argument(
        "--redis-host",
        default=None,
        help="Redis host. Defaults to REDIS_HOST or localhost.",
    )
    parser.add_argument(
        "--redis-port",
        type=int,
        default=None,
        help="Redis port. Defaults to REDIS_PORT or 6379.",
    )
    parser.add_argument(
        "--redis-db",
        type=int,
        default=None,
        help="Redis database number. Defaults to REDIS_DB or 0.",
    )


def add_logging_args(parser: argparse.ArgumentParser) -> None:
    """Add shared process logging verbosity arguments."""
    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="Increase logging verbosity. Use -vv for DEBUG logs.",
    )


def add_qdrant_args(parser: argparse.ArgumentParser) -> None:
    """Add Qdrant connection arguments."""
    parser.add_argument("--qdrant-url", default=None)
    parser.add_argument("--qdrant-host", default=None)
    parser.add_argument("--qdrant-port", type=int, default=None)
    parser.add_argument("--qdrant-api-key", default=None)


def build_redis_client(args: argparse.Namespace) -> Any:
    """Build a redis-py client from settings and CLI overrides."""
    return create_redis_client(
        _settings(args),
        url=args.redis_url,
        host=args.redis_host,
        port=args.redis_port,
        db=args.redis_db,
    )


def build_qdrant_adapter(args: argparse.Namespace) -> QdrantAdapter:
    """Build QdrantAdapter from settings and CLI overrides."""
    return create_qdrant_adapter(
        _settings(args),
        url=args.qdrant_url,
        host=args.qdrant_host,
        port=args.qdrant_port,
        api_key=args.qdrant_api_key,
    )


def _preload_settings(argv: list[str] | None) -> Settings:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--config-file", default=None)
    parser.add_argument("--env-file", default=None)
    args, _unknown = parser.parse_known_args(argv)
    return load_settings(config_file=args.config_file, env_file=args.env_file)


def _settings(args: argparse.Namespace) -> Settings:
    settings = getattr(args, "settings", None)
    if isinstance(settings, Settings):
        return settings
    return load_settings(
        config_file=getattr(args, "config_file", None),
        env_file=getattr(args, "env_file", None),
    )


if __name__ == "__main__":
    raise SystemExit(main())
