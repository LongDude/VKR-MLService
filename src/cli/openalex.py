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
from dto.openalex import (
    OpenAlexBootstrapRequestDTO,
    OpenAlexBootstrapTopicTargetDTO,
    OpenAlexPendingPageDTO,
)
from ingestion.openalex_bootstrap import OpenAlexMonthlyStatsCollector
from ingestion.openalex_bootstrap.report import report_to_dict
from ingestion.openalex_topic_stats import OpenAlexTopicStatsCollector, SyncRateLimiter
from ml.facades.openalex_papers import OpenAlexPapersFacade
from ml.pipelines.openalex_paper_loading_pipeline import OpenAlexPaperLoadingPipeline
from ml.services.openalex_paper_downloader import OpenAlexPaperDownloader
from ml.services.openalex_paper_importer import OpenAlexPaperImporter
from ml.services.openalex_paper_plan import OpenAlexPaperPlanService
from ml.services.openalex_rate_limiter import AsyncRateLimiter
from models.session import create_db_engine, create_session_factory
from repositories.openalex_topic_stats import OpenAlexTopicStatsRepository
from repositories.openalex_yearly_topic_stats import OpenAlexYearlyTopicStatsRepository
from repositories.taxonomy import TaxonomyRepository


DEFAULT_MONTHLY_COUNTS_CSV = BASE_DIR / "openalex_monthly_counts_2015_2025.csv"
DEFAULT_STATS_REDIS_KEY_PREFIX = "openalex:monthly_counts:v1"
DEFAULT_BOOTSTRAP_PENDING_REDIS_KEY = "queue:openalex_bootstrap_papers_pending"


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
        help="Target number of papers for the selected --target-scope.",
    )
    bootstrap_parser.add_argument(
        "--target-scope",
        choices=["total", "period", "year", "month"],
        default="total",
        help=(
            "Quota extent: one aggregate quota for total/period, "
            "per-year quota, or per-month quota. Defaults to total."
        ),
    )
    bootstrap_parser.add_argument(
        "--target-unit",
        choices=["auto", "aggregate", "topic"],
        default="auto",
        help=(
            "Target unit. auto uses per-topic targets for --target-scope month "
            "with --field-ids/--subfield-ids; aggregate keeps one target for "
            "the whole selected taxonomy scope."
        ),
    )
    bootstrap_parser.add_argument(
        "--target-period-count",
        type=int,
        default=None,
        help=argparse.SUPPRESS,
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
        help=argparse.SUPPRESS,
    )
    bootstrap_parser.add_argument(
        "--normalize",
        choices=["none", "year", "month"],
        default="none",
        help=argparse.SUPPRESS,
    )
    bootstrap_parser.add_argument(
        "--stats-source",
        choices=["redis", "csv"],
        default="redis",
        help=argparse.SUPPRESS,
    )
    bootstrap_parser.add_argument(
        "--stats-redis-key",
        default=None,
        help=argparse.SUPPRESS,
    )
    bootstrap_parser.add_argument(
        "--missing-stats-policy",
        choices=["error", "fetch"],
        default="error",
        help=argparse.SUPPRESS,
    )
    bootstrap_parser.add_argument(
        "--monthly-counts-csv",
        default=str(DEFAULT_MONTHLY_COUNTS_CSV),
        help=argparse.SUPPRESS,
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
        "--field-ids",
        default=None,
        help="Comma-separated local field ids used to filter OpenAlex works.",
    )
    bootstrap_parser.add_argument(
        "--subfield-ids",
        default=None,
        help="Comma-separated local subfield ids used to filter OpenAlex works.",
    )
    bootstrap_parser.add_argument(
        "--primary-topic-only",
        action="store_true",
        help="Use primary_topic field/subfield filters instead of topics filters.",
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
        default=1,
        help=argparse.SUPPRESS,
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
    bootstrap_parser.add_argument(
        "--openalex-api-key",
        default=None,
        help="OpenAlex API key. Defaults to OPENALEX_API_KEY. Required unless --dry-run.",
    )
    bootstrap_parser.add_argument(
        "--openalex-mailto",
        default=None,
        help="OpenAlex polite-pool email. Defaults to OPENALEX_MAILTO.",
    )
    bootstrap_parser.add_argument(
        "--rate-limit-defer-after-seconds",
        type=float,
        default=120.0,
        help="Defer unfinished OpenAlex pages to Redis when Retry-After is above this value.",
    )
    bootstrap_parser.add_argument(
        "--pending-redis-key",
        default=DEFAULT_BOOTSTRAP_PENDING_REDIS_KEY,
        help=f"Redis list for deferred bootstrap pages. Defaults to {DEFAULT_BOOTSTRAP_PENDING_REDIS_KEY}.",
    )
    add_redis_args(bootstrap_parser)

    resume_parser = subparsers.add_parser(
        "resume-bootstrap-papers",
        help="Resume deferred OpenAlex bootstrap paper pages from Redis.",
    )
    resume_parser.add_argument(
        "--pending-redis-key",
        default=DEFAULT_BOOTSTRAP_PENDING_REDIS_KEY,
        help=f"Redis list with deferred bootstrap pages. Defaults to {DEFAULT_BOOTSTRAP_PENDING_REDIS_KEY}.",
    )
    resume_parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Maximum deferred pages to process. Defaults to all currently queued pages.",
    )
    resume_parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="PostgreSQL import batch size, max 500. Defaults to 500.",
    )
    resume_parser.add_argument(
        "--request-workers",
        type=int,
        default=8,
        help="Concurrent OpenAlex request workers. Defaults to 8.",
    )
    resume_parser.add_argument(
        "--db-workers",
        type=int,
        default=2,
        help="Concurrent PostgreSQL import workers. Defaults to 2.",
    )
    resume_parser.add_argument(
        "--rate-limit-rps",
        type=float,
        default=70.0,
        help="OpenAlex requests per second limit. Defaults to 70.",
    )
    resume_parser.add_argument(
        "--max-retries",
        type=int,
        default=5,
        help="Retries for 429/5xx OpenAlex responses. Defaults to 5.",
    )
    resume_parser.add_argument(
        "--rate-limit-defer-after-seconds",
        type=float,
        default=120.0,
        help="Defer unfinished OpenAlex pages to Redis when Retry-After is above this value.",
    )
    resume_parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Do not update papers already found in PostgreSQL.",
    )
    resume_parser.add_argument(
        "--enqueue-indexing",
        action="store_true",
        help="Enqueue imported paper ids to queue:paper_indexing.",
    )
    resume_parser.add_argument(
        "--no-progress",
        action="store_true",
        help="Disable tqdm progress bars.",
    )
    resume_parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="Increase logging verbosity. Use -v for DEBUG, -vv for verbose dependency logs.",
    )
    resume_parser.add_argument(
        "--report-json",
        default=None,
        help="Optional path for JSON report.",
    )
    resume_parser.add_argument(
        "--env-file",
        default=str(PROJECT_DIR / ".env"),
        help="Path to .env file. Defaults to project .env.",
    )
    resume_parser.add_argument(
        "--database-url",
        default=None,
        help="SQLAlchemy database URL. Defaults to DATABASE_URL or POSTGRES_* envs.",
    )
    resume_parser.add_argument(
        "--openalex-url",
        default=None,
        help="OpenAlex base URL. Defaults to OPENALEX_BASE_URL or https://api.openalex.org.",
    )
    resume_parser.add_argument(
        "--openalex-api-key",
        default=None,
        help="OpenAlex API key. Defaults to OPENALEX_API_KEY. Required.",
    )
    resume_parser.add_argument(
        "--openalex-mailto",
        default=None,
        help="OpenAlex polite-pool email. Defaults to OPENALEX_MAILTO.",
    )
    add_redis_args(resume_parser)

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
        elif args.command == "resume-bootstrap-papers":
            payload = asyncio.run(run_resume_bootstrap_papers(args))
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
    if payload.get("deferred"):
        return 2
    return 0


async def run_bootstrap_papers(args: argparse.Namespace) -> Any:
    """Build request DTO and run OpenAlex bootstrap."""
    target_count, target_count_scope = resolve_target_count(
        args.target_count,
        args.target_scope,
        args.target_period_count,
    )
    if args.date_from > args.date_to:
        raise ValueError("--date-from must be before or equal to --date-to.")
    if args.max_rounds != 1:
        raise ValueError(
            "--max-rounds is deprecated for bootstrap-papers; only --max-rounds 1 is supported."
        )
    if args.normalize != "none":
        raise ValueError(
            "--normalize is deprecated for bootstrap-papers; use --normalize none."
        )
    languages = parse_csv_arg(args.languages)
    types = parse_csv_arg(args.types)
    field_ids = parse_int_csv_arg(args.field_ids) if args.field_ids else None
    subfield_ids = parse_int_csv_arg(args.subfield_ids) if args.subfield_ids else None
    if field_ids and subfield_ids:
        raise ValueError("--field-ids and --subfield-ids are mutually exclusive.")
    openalex_api_key = resolve_required_openalex_api_key(
        args.openalex_api_key,
        dry_run=args.dry_run,
    )
    openalex_mailto = resolve_optional_openalex_mailto(args.openalex_mailto)
    database_url = args.database_url or Settings.from_env().database_url
    engine = create_db_engine(database_url, echo=False)
    SessionLocal = create_session_factory(engine, expire_on_commit=False)

    try:
        openalex_filter_parts = resolve_bootstrap_taxonomy_filter_parts(
            SessionLocal,
            field_ids=field_ids,
            subfield_ids=subfield_ids,
            primary_topic_only=args.primary_topic_only,
        )
        target_count_unit = resolve_bootstrap_target_unit(
            args.target_unit,
            target_count_scope,
            field_ids=field_ids,
            subfield_ids=subfield_ids,
        )
        topic_targets = (
            resolve_bootstrap_topic_targets(
                SessionLocal,
                field_ids=field_ids,
                subfield_ids=subfield_ids,
                primary_topic_only=args.primary_topic_only,
            )
            if target_count_unit == "topic"
            else []
        )
        request = OpenAlexBootstrapRequestDTO(
            target_count=target_count,
            target_count_scope=target_count_scope,
            target_count_unit=target_count_unit,
            topic_targets=topic_targets,
            date_from=args.date_from,
            date_to=args.date_to,
            sample=True,
            normalize="none",
            monthly_stats_source=args.stats_source,
            monthly_counts_redis_key=args.stats_redis_key,
            monthly_counts_csv=args.monthly_counts_csv,
            missing_stats_policy=args.missing_stats_policy,
            languages=languages,
            types=types,
            openalex_filter_parts=openalex_filter_parts,
            local_field_ids=field_ids or [],
            local_subfield_ids=subfield_ids or [],
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
        redis_adapter = build_optional_redis_adapter(args, request)
        pipeline = OpenAlexPaperLoadingPipeline(
            OpenAlexPapersFacade(
                session_factory=SessionLocal,
                plan_service=OpenAlexPaperPlanService(),
                downloader=OpenAlexPaperDownloader(
                    base_url=args.openalex_url
                    or os.getenv("OPENALEX_BASE_URL")
                    or "https://api.openalex.org",
                    request_workers=request.request_workers,
                    rate_limiter=AsyncRateLimiter(request.rate_limit_rps),
                    max_retries=request.max_retries,
                    api_key=openalex_api_key,
                    mailto=openalex_mailto,
                    rate_limit_defer_after_seconds=args.rate_limit_defer_after_seconds,
                ),
                importer=OpenAlexPaperImporter(
                    session_factory=SessionLocal,
                    batch_size=request.batch_size,
                ),
                redis_adapter=redis_adapter,
                pending_redis_key=args.pending_redis_key,
            )
        )
        return await pipeline.bootstrap_papers(request)
    finally:
        engine.dispose()


async def run_resume_bootstrap_papers(args: argparse.Namespace) -> dict[str, Any]:
    """Resume deferred OpenAlex bootstrap pages stored in Redis."""
    if args.max_pages is not None and args.max_pages <= 0:
        raise ValueError("--max-pages must be a positive integer.")
    if args.batch_size <= 0 or args.batch_size > 500:
        raise ValueError("--batch-size must be between 1 and 500.")
    if args.request_workers <= 0:
        raise ValueError("--request-workers must be a positive integer.")
    if args.db_workers <= 0:
        raise ValueError("--db-workers must be a positive integer.")

    openalex_api_key = resolve_required_openalex_api_key(
        args.openalex_api_key,
        dry_run=False,
    )
    openalex_mailto = resolve_optional_openalex_mailto(args.openalex_mailto)
    redis_adapter = RedisAdapter(build_redis_client(args))
    pages = dequeue_pending_openalex_pages(
        redis_adapter,
        args.pending_redis_key,
        max_pages=args.max_pages,
    )
    if not pages:
        return {
            "command": "resume-bootstrap-papers",
            "pending_redis_key": args.pending_redis_key,
            "loaded_pages": 0,
            "deferred": False,
            "deferred_pages": 0,
            "retry_after_seconds": None,
        }

    database_url = args.database_url or Settings.from_env().database_url
    engine = create_db_engine(database_url, echo=False)
    SessionLocal = create_session_factory(engine, expire_on_commit=False)
    try:
        pipeline = OpenAlexPaperLoadingPipeline(
            OpenAlexPapersFacade(
                session_factory=SessionLocal,
                plan_service=OpenAlexPaperPlanService(),
                downloader=OpenAlexPaperDownloader(
                    base_url=args.openalex_url
                    or os.getenv("OPENALEX_BASE_URL")
                    or "https://api.openalex.org",
                    request_workers=args.request_workers,
                    rate_limiter=AsyncRateLimiter(args.rate_limit_rps),
                    max_retries=args.max_retries,
                    api_key=openalex_api_key,
                    mailto=openalex_mailto,
                    rate_limit_defer_after_seconds=args.rate_limit_defer_after_seconds,
                ),
                importer=OpenAlexPaperImporter(
                    session_factory=SessionLocal,
                    batch_size=args.batch_size,
                ),
                redis_adapter=redis_adapter,
                pending_redis_key=args.pending_redis_key,
            )
        )
        result = await pipeline.resume_pages(
            pages,
            db_workers=args.db_workers,
            skip_existing=args.skip_existing,
            enqueue_indexing=args.enqueue_indexing,
            show_progress=not args.no_progress,
        )
        return {
            "command": "resume-bootstrap-papers",
            "pending_redis_key": args.pending_redis_key,
            **result,
        }
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
    target_scope: str | int | None = "total",
    target_period_count: int | None = None,
) -> tuple[int, str]:
    """Resolve target count and its extent scope."""
    if isinstance(target_scope, int) and target_period_count is None:
        target_period_count = target_scope
        target_scope = "total"
    target_scope = target_scope or "total"
    allowed_scopes = {"total", "period", "year", "month"}
    if target_scope not in allowed_scopes:
        raise ValueError(
            "--target-scope must be one of: total, period, year, month."
        )
    if target_period_count is not None and target_count is not None:
        raise ValueError("--target-count and legacy --target-period-count are mutually exclusive.")
    if target_period_count is not None:
        if target_period_count < 0:
            raise ValueError("--target-period-count must be non-negative.")
        return target_period_count, "period"
    if target_count is not None:
        if target_count < 0:
            raise ValueError("--target-count must be non-negative.")
        return target_count, str(target_scope)
    env_value = os.getenv("BOOTSTRAP_PAPERS_COUNT") or os.getenv("BOOSTRAP_PAPERS_COUNT")
    if env_value is None or not env_value.strip():
        raise ValueError(
            "--target-count is required unless "
            "BOOTSTRAP_PAPERS_COUNT is set."
        )
    resolved_target_count = int(env_value)
    if resolved_target_count < 0:
        raise ValueError("BOOTSTRAP_PAPERS_COUNT must be non-negative.")
    return resolved_target_count, str(target_scope)


def resolve_bootstrap_target_unit(
    target_unit: str | None,
    target_scope: str,
    *,
    field_ids: list[int] | None,
    subfield_ids: list[int] | None,
) -> str:
    """Resolve whether target_count applies to aggregate selection or each topic."""
    target_unit = target_unit or "auto"
    allowed_units = {"auto", "aggregate", "topic"}
    if target_unit not in allowed_units:
        raise ValueError("--target-unit must be one of: auto, aggregate, topic.")
    has_taxonomy_scope = bool(field_ids or subfield_ids)
    if target_unit == "auto":
        return "topic" if target_scope == "month" and has_taxonomy_scope else "aggregate"
    if target_unit == "topic":
        if target_scope == "total":
            raise ValueError("--target-unit topic cannot be used with --target-scope total.")
        if not has_taxonomy_scope:
            raise ValueError("--target-unit topic requires --field-ids or --subfield-ids.")
    return target_unit


def resolve_bootstrap_topic_targets(
    session_factory: Any,
    *,
    field_ids: list[int] | None,
    subfield_ids: list[int] | None,
    primary_topic_only: bool = False,
) -> list[OpenAlexBootstrapTopicTargetDTO]:
    """Resolve selected field/subfield into concrete OpenAlex topic filters."""
    if not field_ids and not subfield_ids:
        raise ValueError("Topic target resolution requires --field-ids or --subfield-ids.")
    prefix = "primary_topic" if primary_topic_only else "topics"
    with session_factory() as session:
        topics = TaxonomyRepository(session).list_topics_for_stats(
            field_ids=field_ids,
            subfield_ids=subfield_ids,
        )
    if not topics:
        raise ValueError("No local topics found for selected field/subfield ids.")

    without_openalex = [int(topic.id) for topic in topics if not topic.openalex_id]
    if without_openalex:
        raise ValueError(
            "Cannot build per-topic OpenAlex filters: "
            f"topics_without_openalex_id={without_openalex}"
        )

    return [
        OpenAlexBootstrapTopicTargetDTO(
            topic_id=int(topic.id),
            filter_part=(
                f"{prefix}.id:"
                f"{normalize_openalex_filter_id(str(topic.openalex_id))}"
            ),
        )
        for topic in topics
    ]


def build_optional_redis_adapter(
    args: argparse.Namespace,
    request: OpenAlexBootstrapRequestDTO,
) -> RedisAdapter | None:
    """Build Redis adapter when bootstrap needs Redis for queues."""
    needs_redis_queue = request.enqueue_indexing and not request.dry_run
    needs_pending_queue = not request.dry_run and bool(getattr(args, "pending_redis_key", None))
    if not needs_redis_queue and not needs_pending_queue:
        return None
    return RedisAdapter(build_redis_client(args))


def dequeue_pending_openalex_pages(
    redis_adapter: RedisAdapter,
    redis_key: str,
    *,
    max_pages: int | None,
) -> list[OpenAlexPendingPageDTO]:
    """Pop pending OpenAlex bootstrap pages from Redis."""
    limit = max_pages if max_pages is not None else redis_adapter.queue_length(redis_key)
    pages: list[OpenAlexPendingPageDTO] = []
    for _ in range(limit):
        payload = redis_adapter.dequeue_nowait(redis_key)
        if payload is None:
            break
        pages.append(OpenAlexPendingPageDTO.model_validate(payload))
    return pages


def enqueue_pending_openalex_pages(
    redis_adapter: RedisAdapter,
    redis_key: str,
    pages: list[OpenAlexPendingPageDTO],
) -> int:
    """Push pending OpenAlex bootstrap pages back to Redis."""
    enqueued = 0
    for page in pages:
        redis_adapter.enqueue(redis_key, page.model_dump(mode="json"))
        enqueued += 1
    return enqueued


def resolve_required_openalex_api_key(
    value: str | None,
    *,
    dry_run: bool,
) -> str | None:
    """Resolve the OpenAlex API key and enforce it for real OpenAlex calls."""
    api_key = (value or os.getenv("OPENALEX_API_KEY") or "").strip()
    if dry_run:
        return api_key or None
    if not api_key:
        raise ValueError(
            "OpenAlex API key is required for real bootstrap requests. "
            "Pass --openalex-api-key or set OPENALEX_API_KEY."
        )
    return api_key


def resolve_optional_openalex_mailto(value: str | None) -> str | None:
    mailto = (value or os.getenv("OPENALEX_MAILTO") or "").strip()
    return mailto or None


def resolve_bootstrap_taxonomy_filter_parts(
    session_factory: Any,
    *,
    field_ids: list[int] | None,
    subfield_ids: list[int] | None,
    primary_topic_only: bool = False,
) -> list[str]:
    """Resolve local field/subfield ids into OpenAlex work filter fragments."""
    if not field_ids and not subfield_ids:
        return []
    prefix = "primary_topic" if primary_topic_only else "topics"
    with session_factory() as session:
        repository = TaxonomyRepository(session)
        if field_ids:
            fields = repository.list_fields_by_ids(field_ids)
            found = {int(field.id) for field in fields}
            missing = [field_id for field_id in field_ids if field_id not in found]
            without_openalex = [int(field.id) for field in fields if not field.openalex_id]
            if missing or without_openalex:
                raise ValueError(
                    "Cannot build OpenAlex field filter: "
                    f"missing_fields={missing}, without_openalex_id={without_openalex}"
                )
            external_ids = [
                normalize_openalex_filter_id(str(field.openalex_id))
                for field in fields
                if field.openalex_id
            ]
            return [f"{prefix}.field.id:{'|'.join(external_ids)}"]
        subfields = repository.list_subfields_by_ids(subfield_ids or [])
        found = {int(subfield.id) for subfield in subfields}
        missing = [
            subfield_id
            for subfield_id in (subfield_ids or [])
            if subfield_id not in found
        ]
        without_openalex = [
            int(subfield.id)
            for subfield in subfields
            if not subfield.openalex_id
        ]
        if missing or without_openalex:
            raise ValueError(
                "Cannot build OpenAlex subfield filter: "
                f"missing_subfields={missing}, without_openalex_id={without_openalex}"
            )
        external_ids = [
            normalize_openalex_filter_id(str(subfield.openalex_id))
            for subfield in subfields
            if subfield.openalex_id
        ]
        return [f"{prefix}.subfield.id:{'|'.join(external_ids)}"]


def normalize_openalex_filter_id(value: str) -> str:
    """Normalize an OpenAlex URL or id for use in API filters."""
    normalized = value.strip().rstrip("/")
    if "/" in normalized:
        normalized = normalized.rsplit("/", 1)[-1]
    return normalized


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
