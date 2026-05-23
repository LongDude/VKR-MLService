from __future__ import annotations

import argparse
import json
import os
import sys
from collections import defaultdict
from datetime import date
from pathlib import Path
from typing import Any, Callable

from dotenv import load_dotenv
from sqlalchemy import delete, exists, func, or_, select, update
from sqlalchemy.orm import Session


BASE_DIR = Path(__file__).resolve().parent
SRC_DIR = BASE_DIR.parent
PROJECT_DIR = SRC_DIR.parent

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from core.config import Settings
from models import (
    Domain,
    Field,
    Keyword,
    OpenAlexYearlyTopicStat,
    Paper,
    PaperTopic,
    Subfield,
    Topic,
)
from models.session import create_db_engine, create_session_factory
from repositories.openalex_yearly_topic_stats import OpenAlexYearlyTopicStatsRepository


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
            self._append_applied(
                fixes,
                "trim_strings",
                label,
                int(result.rowcount or 0),
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
        result = self.session.execute(delete(Keyword).where(self._is_blank(Keyword.value)))
        self._append_applied(
            fixes,
            "delete_empty_keywords",
            "keywords",
            int(result.rowcount or 0),
        )

    def _normalize_keyword_values(
        self,
        fixes: dict[str, list[dict[str, Any]]],
    ) -> None:
        rows = list(
            self.session.execute(
                select(Keyword.id, Keyword.value).where(self._is_not_blank(Keyword.value))
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
                update(Keyword)
                .where(Keyword.id == keyword_id)
                .values(value=normalized)
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
                update(model).where(id_column == row_id).values({value_column.key: normalized})
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


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse data CLI command and command-specific arguments."""
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
        default=str(PROJECT_DIR / ".env"),
        help="Path to .env file. Defaults to project .env.",
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
        default=str(PROJECT_DIR / ".env"),
        help="Path to .env file. Defaults to project .env.",
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
        default=SAMPLE_LIMIT,
        help="Maximum detailed topic rows in the report.",
    )
    jan_parser.add_argument(
        "--report-json",
        default=None,
        help="Optional path to write JSON report.",
    )
    jan_parser.add_argument(
        "--env-file",
        default=str(PROJECT_DIR / ".env"),
        help="Path to .env file. Defaults to project .env.",
    )
    jan_parser.add_argument(
        "--database-url",
        default=None,
        help="SQLAlchemy database URL. Defaults to DATABASE_URL or POSTGRES_* envs.",
    )

    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Run the requested data CLI command."""
    args = parse_args(argv)
    load_dotenv(args.env_file)

    try:
        if args.command == "validate-local-data":
            report = run_validate_local_data(args)
        elif args.command == "rebuild-openalex-yearly-topic-stats":
            report = run_rebuild_openalex_yearly_topic_stats(args)
        elif args.command == "check-openalex-jan1-anomalies":
            report = run_check_openalex_jan1_anomalies(args)
        else:
            raise AssertionError(f"Unhandled command: {args.command}")
        if args.report_json:
            write_report(Path(args.report_json), report)
    except Exception as exc:
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

    print_json(report)
    return 0


def run_validate_local_data(args: argparse.Namespace) -> dict[str, Any]:
    """Validate local data and optionally apply safe fixes."""
    database_url = args.database_url or Settings.from_env().database_url
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
    database_url = args.database_url or Settings.from_env().database_url
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
    database_url = args.database_url or Settings.from_env().database_url
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
            int(row["artifical_pubdates_estimation"] or 0)
            for row in rows
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
            "checks_failed": sum(1 for check in initial_checks if int(check["count"]) > 0),
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
            raise ValueError(f"Expected comma-separated integer ids, got {item!r}.") from exc
    return result


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


if __name__ == "__main__":
    raise SystemExit(main())
