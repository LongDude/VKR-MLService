from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
from datetime import date
from pathlib import Path
from typing import Any

from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent
SRC_DIR = BASE_DIR.parent
PROJECT_DIR = SRC_DIR.parent

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from adapters.openalex_adapter import OpenAlexAdapter
from adapters.redis_adapter import RedisAdapter
from core.config import Settings
from core.exceptions import AppError
from ingestion.openalex_bootstrap import (
    AsyncRateLimiter,
    OpenAlexBatchImporter,
    OpenAlexBootstrapDownloader,
    OpenAlexBootstrapRequestDTO,
    OpenAlexBootstrapRunner,
    OpenAlexLoadPlanBuilder,
    OpenAlexMonthlyStatsCollector,
    MonthlyCountsLoader,
)
from ingestion.openalex_bootstrap.report import report_to_dict
from ingestion.openalex_topic_stats import OpenAlexTopicStatsCollector, SyncRateLimiter
from models.session import create_db_engine, create_session_factory
from repositories.openalex_topic_stats import OpenAlexTopicStatsRepository
from repositories.openalex_yearly_topic_stats import OpenAlexYearlyTopicStatsRepository
from repositories.taxonomy import TaxonomyRepository


DEFAULT_MONTHLY_COUNTS_CSV = BASE_DIR / "openalex_monthly_counts_2015_2025.csv"
DEFAULT_STATS_REDIS_KEY_PREFIX = "openalex:monthly_counts:v1"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse OpenAlex CLI command and command-specific arguments."""
    parser = argparse.ArgumentParser(description="OpenAlex ingestion CLI utilities.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    bootstrap_parser = subparsers.add_parser(
        "bootstrap-papers",
        help="Bootstrap local paper data from OpenAlex works.",
    )
    bootstrap_parser.add_argument(
        "--target-count",
        type=int,
        default=None,
        help="Target number of papers in local DB after bootstrap.",
    )
    bootstrap_parser.add_argument(
        "--target-period-count",
        type=int,
        default=None,
        help=(
            "Target number of papers inside --date-from/--date-to after bootstrap. "
            "Mutually exclusive with --target-count."
        ),
    )
    bootstrap_parser.add_argument(
        "--date-from",
        type=parse_iso_date,
        required=True,
        help="Publication date lower bound in YYYY-MM-DD format.",
    )
    bootstrap_parser.add_argument(
        "--date-to",
        type=parse_iso_date,
        required=True,
        help="Publication date upper bound in YYYY-MM-DD format.",
    )
    bootstrap_parser.add_argument(
        "--sample",
        action="store_true",
        help="Use OpenAlex sample parameter for plan items.",
    )
    bootstrap_parser.add_argument(
        "--normalize",
        choices=["none", "year", "month"],
        default="month",
        help="Target distribution mode. Defaults to month.",
    )
    bootstrap_parser.add_argument(
        "--stats-source",
        choices=["redis", "csv"],
        default="redis",
        help="Monthly statistics source for normalization. Defaults to redis.",
    )
    bootstrap_parser.add_argument(
        "--stats-redis-key",
        default=None,
        help="Redis key with OpenAlex monthly statistics. Defaults to a key derived from languages/types.",
    )
    bootstrap_parser.add_argument(
        "--missing-stats-policy",
        choices=["error", "fetch"],
        default="error",
        help="What to do when Redis stats are missing for the period. Defaults to error.",
    )
    bootstrap_parser.add_argument(
        "--monthly-counts-csv",
        default=str(DEFAULT_MONTHLY_COUNTS_CSV),
        help="CSV with OpenAlex monthly counts. Used only with --stats-source csv.",
    )
    bootstrap_parser.add_argument(
        "--languages",
        default="en,ru",
        help="Comma-separated OpenAlex languages. Defaults to en,ru.",
    )
    bootstrap_parser.add_argument(
        "--types",
        default="article",
        help="Comma-separated OpenAlex work types. Defaults to article.",
    )
    bootstrap_parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="PostgreSQL import batch size, max 500. Defaults to 500.",
    )
    bootstrap_parser.add_argument(
        "--request-workers",
        type=int,
        default=8,
        help="Concurrent OpenAlex request workers. Defaults to 8.",
    )
    bootstrap_parser.add_argument(
        "--db-workers",
        type=int,
        default=2,
        help="Concurrent PostgreSQL import workers. Defaults to 2.",
    )
    bootstrap_parser.add_argument(
        "--rate-limit-rps",
        type=float,
        default=70.0,
        help="OpenAlex requests per second limit. Defaults to 70.",
    )
    bootstrap_parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Base OpenAlex sample seed. Defaults to 42.",
    )
    bootstrap_parser.add_argument(
        "--max-rounds",
        type=int,
        default=5,
        help="Maximum bootstrap rounds. Defaults to 5.",
    )
    bootstrap_parser.add_argument(
        "--per-page",
        type=int,
        default=100,
        help="OpenAlex per-page value, max 100. Defaults to 100.",
    )
    bootstrap_parser.add_argument(
        "--max-retries",
        type=int,
        default=5,
        help="Retries for 429/5xx OpenAlex responses. Defaults to 5.",
    )
    bootstrap_parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Do not update papers already found in PostgreSQL.",
    )
    bootstrap_parser.add_argument(
        "--enqueue-indexing",
        action="store_true",
        help="Enqueue imported paper ids to queue:paper_indexing.",
    )
    bootstrap_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Build load plan only; do not call OpenAlex, write DB, or enqueue Redis.",
    )
    bootstrap_parser.add_argument(
        "--no-progress",
        action="store_true",
        help="Disable tqdm progress bars.",
    )
    bootstrap_parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="Increase logging verbosity. Use -v for DEBUG, -vv for verbose dependency logs.",
    )
    bootstrap_parser.add_argument(
        "--report-json",
        default=None,
        help="Optional path for JSON report.",
    )
    bootstrap_parser.add_argument(
        "--env-file",
        default=str(PROJECT_DIR / ".env"),
        help="Path to .env file. Defaults to project .env.",
    )
    bootstrap_parser.add_argument(
        "--database-url",
        default=None,
        help="SQLAlchemy database URL. Defaults to DATABASE_URL or POSTGRES_* envs.",
    )
    bootstrap_parser.add_argument(
        "--openalex-url",
        default=None,
        help="OpenAlex base URL. Defaults to OPENALEX_BASE_URL or https://api.openalex.org.",
    )
    add_redis_args(bootstrap_parser)

    stats_parser = subparsers.add_parser(
        "collect-stats",
        aliases=["collect-monthly-stats", "update-stats"],
        help="Collect or update OpenAlex monthly publication statistics in Redis.",
    )
    stats_parser.add_argument(
        "--date-from",
        type=parse_iso_date,
        required=True,
        help="Publication date lower bound in YYYY-MM-DD format.",
    )
    stats_parser.add_argument(
        "--date-to",
        type=parse_iso_date,
        required=True,
        help="Publication date upper bound in YYYY-MM-DD format.",
    )
    stats_parser.add_argument(
        "--languages",
        default="en,ru",
        help="Comma-separated OpenAlex languages. Defaults to en,ru.",
    )
    stats_parser.add_argument(
        "--types",
        default="article",
        help="Comma-separated OpenAlex work types. Defaults to article.",
    )
    stats_parser.add_argument(
        "--stats-redis-key",
        default=None,
        help="Redis key to store monthly statistics. Defaults to a key derived from languages/types.",
    )
    stats_parser.add_argument(
        "--stats-ttl-seconds",
        type=int,
        default=0,
        help="Redis TTL for stats document. 0 means no expiration.",
    )
    stats_parser.add_argument(
        "--missing-only",
        action="store_true",
        help="Collect only months not already present in Redis.",
    )
    stats_parser.add_argument(
        "--request-workers",
        type=int,
        default=8,
        help="Concurrent OpenAlex count request workers. Defaults to 8.",
    )
    stats_parser.add_argument(
        "--rate-limit-rps",
        type=float,
        default=70.0,
        help="OpenAlex requests per second limit. Defaults to 70.",
    )
    stats_parser.add_argument(
        "--max-retries",
        type=int,
        default=5,
        help="Retries for 429/5xx OpenAlex responses. Defaults to 5.",
    )
    stats_parser.add_argument(
        "--no-progress",
        action="store_true",
        help="Disable tqdm progress bars.",
    )
    stats_parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="Increase logging verbosity. Use -v for DEBUG, -vv for verbose dependency logs.",
    )
    stats_parser.add_argument(
        "--env-file",
        default=str(PROJECT_DIR / ".env"),
        help="Path to .env file. Defaults to project .env.",
    )
    stats_parser.add_argument(
        "--openalex-url",
        default=None,
        help="OpenAlex base URL. Defaults to OPENALEX_BASE_URL or https://api.openalex.org.",
    )
    add_redis_args(stats_parser)

    topic_stats_parser = subparsers.add_parser(
        "collect-topic-stats",
        aliases=["update-topic-stats"],
        help="Collect OpenAlex monthly work counts for local topics into PostgreSQL.",
    )
    topic_stats_parser.add_argument(
        "--date-from",
        type=parse_iso_date,
        required=True,
        help="Publication period lower bound in YYYY-MM-DD format.",
    )
    topic_stats_parser.add_argument(
        "--date-to",
        type=parse_iso_date,
        required=True,
        help="Publication period upper bound in YYYY-MM-DD format.",
    )
    topic_stats_parser.add_argument(
        "--topic-ids",
        default=None,
        help="Comma-separated local topic ids. Defaults to all topics; missing openalex_id is skipped.",
    )
    topic_stats_parser.add_argument(
        "--taxonomy-scope",
        choices=["topic", "field", "subfield"],
        default="topic",
        help=(
            "Taxonomy level used for OpenAlex collection. "
            "field/subfield use group_by=topics.id instead of one request per topic."
        ),
    )
    topic_stats_parser.add_argument(
        "--field-ids",
        default=None,
        help="Comma-separated local field ids. Used with --taxonomy-scope field.",
    )
    topic_stats_parser.add_argument(
        "--subfield-ids",
        default=None,
        help="Comma-separated local subfield ids. Used with --taxonomy-scope subfield.",
    )
    topic_stats_parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum topics to process when --topic-ids is not set.",
    )
    topic_stats_parser.add_argument(
        "--offset",
        type=int,
        default=0,
        help="Topic offset when --topic-ids is not set. Defaults to 0.",
    )
    topic_stats_parser.add_argument(
        "--languages",
        default="en,ru",
        help="Comma-separated OpenAlex languages. Defaults to en,ru.",
    )
    topic_stats_parser.add_argument(
        "--types",
        default="article",
        help="Comma-separated OpenAlex work types. Defaults to article.",
    )
    topic_stats_parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="PostgreSQL upsert batch size. Defaults to 500.",
    )
    topic_stats_parser.add_argument(
        "--request-workers",
        type=int,
        default=8,
        help="Concurrent OpenAlex count workers. Defaults to 8.",
    )
    topic_stats_parser.add_argument(
        "--rate-limit-rps",
        type=float,
        default=10.0,
        help="OpenAlex requests per second limit. Defaults to 10 for topic stats.",
    )
    topic_stats_parser.add_argument(
        "--max-retries",
        type=int,
        default=5,
        help="Retries for rate-limit/service/network errors. Defaults to 5.",
    )
    topic_stats_parser.add_argument(
        "--primary-topic-only",
        action="store_true",
        help="Use primary_topic.id instead of topics.id filter.",
    )
    topic_stats_parser.add_argument(
        "--group-by-page-size",
        type=int,
        default=200,
        help="OpenAlex group_by page size for field/subfield scope, max 200. Defaults to 200.",
    )
    topic_stats_parser.add_argument(
        "--normalize-january-first",
        action="store_true",
        help=(
            "Estimate and remove January-1 artificial publication dates. "
            "Also updates openalex_yearly_topic_stats.artifical_pubdates_estimation."
        ),
    )
    topic_stats_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Build the work plan only; do not call OpenAlex or write PostgreSQL.",
    )
    topic_stats_parser.add_argument(
        "--no-progress",
        action="store_true",
        help="Disable tqdm progress bars.",
    )
    topic_stats_parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="Increase logging verbosity. Use -v for DEBUG, -vv for dependency logs.",
    )
    topic_stats_parser.add_argument(
        "--report-json",
        default=None,
        help="Optional path for JSON report.",
    )
    topic_stats_parser.add_argument(
        "--env-file",
        default=str(PROJECT_DIR / ".env"),
        help="Path to .env file. Defaults to project .env.",
    )
    topic_stats_parser.add_argument(
        "--database-url",
        default=None,
        help="SQLAlchemy database URL. Defaults to DATABASE_URL or POSTGRES_* envs.",
    )
    topic_stats_parser.add_argument(
        "--openalex-url",
        default=None,
        help="OpenAlex base URL. Defaults to OPENALEX_BASE_URL or https://api.openalex.org.",
    )
    topic_stats_parser.add_argument(
        "--openalex-api-key",
        default=None,
        help="OpenAlex API key. Defaults to OPENALEX_API_KEY.",
    )
    topic_stats_parser.add_argument(
        "--openalex-mailto",
        default=None,
        help="OpenAlex polite-pool email. Defaults to OPENALEX_MAILTO.",
    )

    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Run the requested OpenAlex CLI command."""
    args = parse_args(argv)
    load_dotenv(args.env_file)
    configure_logging(getattr(args, "verbose", 0))

    try:
        if args.command == "bootstrap-papers":
            report = asyncio.run(run_bootstrap_papers(args))
            payload = report_to_dict(report)
        elif args.command in {"collect-stats", "collect-monthly-stats", "update-stats"}:
            payload = asyncio.run(run_collect_stats(args))
        elif args.command in {"collect-topic-stats", "update-topic-stats"}:
            payload = run_collect_topic_stats(args)
        else:
            raise AssertionError(f"Unhandled command: {args.command}")
        if getattr(args, "report_json", None):
            write_json_payload(args.report_json, payload)
    except AppError as exc:
        print_json(exc.to_dict(), stream=sys.stderr)
        return 1
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

    print_json(payload)
    return 0


async def run_bootstrap_papers(args: argparse.Namespace) -> Any:
    """Build request DTO and run OpenAlex bootstrap."""
    target_count, target_count_scope = resolve_target_count(
        args.target_count,
        args.target_period_count,
    )
    if args.date_from > args.date_to:
        raise ValueError("--date-from must be before or equal to --date-to.")
    languages = parse_csv_arg(args.languages)
    types = parse_csv_arg(args.types)
    stats_redis_key = args.stats_redis_key or default_stats_redis_key(languages, types)

    request = OpenAlexBootstrapRequestDTO(
        target_count=target_count,
        target_count_scope=target_count_scope,
        date_from=args.date_from,
        date_to=args.date_to,
        sample=args.sample,
        normalize=args.normalize,
        monthly_stats_source=args.stats_source,
        monthly_counts_redis_key=stats_redis_key,
        monthly_counts_csv=args.monthly_counts_csv,
        missing_stats_policy=args.missing_stats_policy,
        languages=languages,
        types=types,
        batch_size=args.batch_size,
        request_workers=args.request_workers,
        db_workers=args.db_workers,
        rate_limit_rps=args.rate_limit_rps,
        seed=args.seed,
        max_rounds=args.max_rounds,
        per_page=args.per_page,
        max_retries=args.max_retries,
        skip_existing=args.skip_existing,
        enqueue_indexing=args.enqueue_indexing,
        dry_run=args.dry_run,
        show_progress=not args.no_progress,
    )

    database_url = args.database_url or Settings.from_env().database_url
    engine = create_db_engine(database_url, echo=False)
    SessionLocal = create_session_factory(engine, expire_on_commit=False)

    try:
        redis_adapter = build_optional_redis_adapter(args, request)
        stats_collector = None
        if redis_adapter is not None and request.monthly_stats_source == "redis":
            stats_collector = OpenAlexMonthlyStatsCollector(
                redis_adapter=redis_adapter,
                redis_key=stats_redis_key,
                base_url=args.openalex_url
                or os.getenv("OPENALEX_BASE_URL")
                or "https://api.openalex.org",
                request_workers=request.request_workers,
                rate_limiter=AsyncRateLimiter(request.rate_limit_rps),
                max_retries=request.max_retries,
            )
        runner = OpenAlexBootstrapRunner(
            request=request,
            session_factory=SessionLocal,
            load_plan_builder=OpenAlexLoadPlanBuilder(
                monthly_counts_loader=MonthlyCountsLoader(),
                redis_adapter=redis_adapter,
            ),
            downloader=OpenAlexBootstrapDownloader(
                base_url=args.openalex_url
                or os.getenv("OPENALEX_BASE_URL")
                or "https://api.openalex.org",
                request_workers=request.request_workers,
                rate_limiter=AsyncRateLimiter(request.rate_limit_rps),
                max_retries=request.max_retries,
            ),
            importer=OpenAlexBatchImporter(
                session_factory=SessionLocal,
                batch_size=request.batch_size,
            ),
            redis_adapter=redis_adapter,
            stats_collector=stats_collector,
        )
        return await runner.run()
    finally:
        engine.dispose()


async def run_collect_stats(args: argparse.Namespace) -> dict[str, Any]:
    """Collect OpenAlex monthly count statistics and store them in Redis."""
    if args.date_from > args.date_to:
        raise ValueError("--date-from must be before or equal to --date-to.")
    if args.stats_ttl_seconds < 0:
        raise ValueError("--stats-ttl-seconds must be non-negative.")

    languages = parse_csv_arg(args.languages)
    types = parse_csv_arg(args.types)
    redis_key = args.stats_redis_key or default_stats_redis_key(languages, types)
    collector = OpenAlexMonthlyStatsCollector(
        redis_adapter=RedisAdapter(build_redis_client(args)),
        redis_key=redis_key,
        base_url=args.openalex_url
        or os.getenv("OPENALEX_BASE_URL")
        or "https://api.openalex.org",
        request_workers=args.request_workers,
        rate_limiter=AsyncRateLimiter(args.rate_limit_rps),
        max_retries=args.max_retries,
        ttl_seconds=args.stats_ttl_seconds or None,
    )
    result = await collector.collect_and_store(
        date_from=args.date_from,
        date_to=args.date_to,
        languages=languages,
        types=types,
        missing_only=args.missing_only,
        show_progress=not args.no_progress,
    )
    return {
        "command": "collect-stats",
        "redis_key": result.redis_key,
        "date_from": result.date_from,
        "date_to": result.date_to,
        "collected_months": result.collected_months,
        "openalex_requests": result.openalex_requests,
        "failed": result.failed,
        "errors": result.errors,
        "months": [
            {
                "period": item.period,
                "date_from": item.date_from,
                "date_to": item.date_to,
                "count": item.count,
            }
            for item in result.months
        ],
    }


def run_collect_topic_stats(args: argparse.Namespace) -> dict[str, Any]:
    """Collect monthly OpenAlex publication counts for local topics into PostgreSQL."""
    if args.date_from > args.date_to:
        raise ValueError("--date-from must be before or equal to --date-to.")
    if args.offset < 0:
        raise ValueError("--offset must be non-negative.")
    if args.limit is not None and args.limit < 0:
        raise ValueError("--limit must be non-negative.")

    languages = parse_csv_arg(args.languages)
    types = parse_csv_arg(args.types)
    topic_ids = parse_int_csv_arg(args.topic_ids) if args.topic_ids else None
    field_ids = parse_int_csv_arg(args.field_ids) if args.field_ids else None
    subfield_ids = parse_int_csv_arg(args.subfield_ids) if args.subfield_ids else None
    if args.taxonomy_scope != "topic" and topic_ids:
        raise ValueError("--topic-ids can only be used with --taxonomy-scope topic.")
    if args.taxonomy_scope != "field" and field_ids:
        raise ValueError("--field-ids can only be used with --taxonomy-scope field.")
    if args.taxonomy_scope != "subfield" and subfield_ids:
        raise ValueError("--subfield-ids can only be used with --taxonomy-scope subfield.")
    if args.group_by_page_size <= 0 or args.group_by_page_size > 200:
        raise ValueError("--group-by-page-size must be between 1 and 200.")
    database_url = args.database_url or Settings.from_env().database_url
    openalex_url = (
        args.openalex_url
        or os.getenv("OPENALEX_BASE_URL")
        or "https://api.openalex.org"
    )
    openalex_api_key = args.openalex_api_key or os.getenv("OPENALEX_API_KEY")
    openalex_mailto = args.openalex_mailto or os.getenv("OPENALEX_MAILTO")

    engine = create_db_engine(database_url, echo=False)
    SessionLocal = create_session_factory(engine, expire_on_commit=False)
    session = SessionLocal()
    try:
        collector = OpenAlexTopicStatsCollector(
            taxonomy_repository=TaxonomyRepository(session),
            stats_repository=OpenAlexTopicStatsRepository(session),
            yearly_stats_repository=OpenAlexYearlyTopicStatsRepository(session),
            openalex_adapter_factory=lambda: OpenAlexAdapter(
                openalex_url,
                api_key=openalex_api_key,
                mailto=openalex_mailto,
            ),
            request_workers=args.request_workers,
            rate_limiter=SyncRateLimiter(args.rate_limit_rps),
            max_retries=args.max_retries,
            primary_topic_only=args.primary_topic_only,
        )
        result = collector.collect_and_store(
            date_from=args.date_from,
            date_to=args.date_to,
            topic_ids=topic_ids,
            field_ids=field_ids,
            subfield_ids=subfield_ids,
            taxonomy_scope=args.taxonomy_scope,
            limit=args.limit,
            offset=args.offset,
            languages=languages,
            types=types,
            batch_size=args.batch_size,
            group_by_page_size=args.group_by_page_size,
            normalize_january_first=args.normalize_january_first,
            dry_run=args.dry_run,
            show_progress=not args.no_progress,
        )
        if args.dry_run:
            session.rollback()
        else:
            session.commit()
        return {
            "command": "collect-topic-stats",
            "taxonomy_scope": result.taxonomy_scope,
            "date_from": result.date_from,
            "date_to": result.date_to,
            "total_topics": result.total_topics,
            "topics_with_openalex_id": result.topics_with_openalex_id,
            "skipped_topics_without_openalex_id": result.skipped_topics_without_openalex_id,
            "periods": result.periods,
            "planned_requests": result.planned_requests,
            "openalex_requests": result.openalex_requests,
            "yearly_artificial_estimates": result.yearly_artificial_estimates,
            "collected": result.collected,
            "created": result.created,
            "updated": result.updated,
            "failed": result.failed,
            "dry_run": result.dry_run,
            "errors": result.errors,
        }
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
        engine.dispose()


def resolve_target_count(
    target_count: int | None,
    target_period_count: int | None,
) -> tuple[int, str]:
    """Resolve target count and whether it applies to total DB or requested period."""
    if target_count is not None and target_period_count is not None:
        raise ValueError("--target-count and --target-period-count are mutually exclusive.")
    if target_period_count is not None:
        if target_period_count < 0:
            raise ValueError("--target-period-count must be non-negative.")
        return target_period_count, "period"
    if target_count is not None:
        if target_count < 0:
            raise ValueError("--target-count must be non-negative.")
        return target_count, "total"
    env_value = os.getenv("BOOTSTRAP_PAPERS_COUNT") or os.getenv("BOOSTRAP_PAPERS_COUNT")
    if env_value is None or not env_value.strip():
        raise ValueError(
            "--target-count or --target-period-count is required unless "
            "BOOTSTRAP_PAPERS_COUNT is set."
        )
    resolved_target_count = int(env_value)
    if resolved_target_count < 0:
        raise ValueError("BOOTSTRAP_PAPERS_COUNT must be non-negative.")
    return resolved_target_count, "total"


def build_optional_redis_adapter(
    args: argparse.Namespace,
    request: OpenAlexBootstrapRequestDTO,
) -> RedisAdapter | None:
    """Build Redis adapter when bootstrap needs Redis for stats or task enqueueing."""
    needs_redis_stats = request.normalize != "none" and request.monthly_stats_source == "redis"
    needs_redis_queue = request.enqueue_indexing and not request.dry_run
    if not needs_redis_stats and not needs_redis_queue:
        return None
    return RedisAdapter(build_redis_client(args))


def default_stats_redis_key(languages: list[str], types: list[str]) -> str:
    """Build a stable Redis key for OpenAlex stats with the selected filters."""
    language_part = ",".join(sorted(languages)) or "all"
    type_part = ",".join(sorted(types)) or "all"
    return f"{DEFAULT_STATS_REDIS_KEY_PREFIX}:languages={language_part}:types={type_part}"


def add_redis_args(parser: argparse.ArgumentParser) -> None:
    """Add Redis connection arguments used by --enqueue-indexing."""
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


def build_redis_client(args: argparse.Namespace) -> Any:
    """Build a redis-py client from CLI arguments or environment variables."""
    try:
        from redis import Redis
    except ImportError as exc:
        raise RuntimeError(
            "redis package is not installed. Install dependencies from requirements.txt."
        ) from exc

    redis_url = args.redis_url or os.getenv("REDIS_URL")
    if redis_url:
        return Redis.from_url(redis_url)
    return Redis(
        host=args.redis_host or os.getenv("REDIS_HOST") or "localhost",
        port=args.redis_port or _optional_int_env("REDIS_PORT") or 6379,
        db=args.redis_db if args.redis_db is not None else _optional_int_env("REDIS_DB") or 0,
        password=os.getenv("REDIS_PASSWORD") or None,
    )


def parse_csv_arg(value: str) -> list[str]:
    """Parse comma-separated CLI string."""
    return [item.strip() for item in value.split(",") if item.strip()]


def parse_int_csv_arg(value: str) -> list[int]:
    """Parse comma-separated integers for argparse-backed options."""
    result: list[int] = []
    for item in parse_csv_arg(value):
        try:
            result.append(int(item))
        except ValueError as exc:
            raise ValueError(f"Expected comma-separated integer ids, got {item!r}.") from exc
    return result


def parse_iso_date(value: str) -> date:
    """Parse an ISO date for argparse."""
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Expected YYYY-MM-DD date, got {value!r}."
        ) from exc


def _optional_int_env(name: str) -> int | None:
    value = os.getenv(name)
    if value is None or not value.strip():
        return None
    return int(value)


def configure_logging(verbosity: int = 0) -> None:
    """Configure bootstrap logging."""
    env_level = os.getenv("LOG_LEVEL", "INFO").upper()
    level = logging.DEBUG if verbosity > 0 else getattr(logging, env_level, logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    if verbosity < 2:
        logging.getLogger("httpx").setLevel(logging.WARNING)
        logging.getLogger("httpcore").setLevel(logging.WARNING)
    else:
        logging.getLogger("httpx").setLevel(logging.DEBUG)
        logging.getLogger("httpcore").setLevel(logging.DEBUG)


def print_json(payload: dict[str, Any], *, stream: Any = sys.stdout) -> None:
    """Print JSON with stable UTF-8 output."""
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str), file=stream)


def write_json_payload(path: str | Path, payload: dict[str, Any]) -> None:
    """Write a CLI JSON payload to disk."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )


if __name__ == "__main__":
    raise SystemExit(main())
