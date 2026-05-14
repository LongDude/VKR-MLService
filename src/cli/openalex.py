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
    MonthlyCountsLoader,
)
from ingestion.openalex_bootstrap.report import report_to_dict, write_report
from models.session import create_db_engine, create_session_factory


DEFAULT_MONTHLY_COUNTS_CSV = BASE_DIR / "openalex_monthly_counts_2015_2025.csv"


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
        "--monthly-counts-csv",
        default=str(DEFAULT_MONTHLY_COUNTS_CSV),
        help="CSV with OpenAlex monthly counts.",
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

    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Run the requested OpenAlex CLI command."""
    args = parse_args(argv)
    load_dotenv(args.env_file)
    configure_logging()

    try:
        if args.command == "bootstrap-papers":
            report = asyncio.run(run_bootstrap_papers(args))
        else:
            raise AssertionError(f"Unhandled command: {args.command}")
        if args.report_json:
            write_report(args.report_json, report)
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

    print_json(report_to_dict(report))
    return 0


async def run_bootstrap_papers(args: argparse.Namespace) -> Any:
    """Build request DTO and run OpenAlex bootstrap."""
    target_count = resolve_target_count(args.target_count)
    if args.date_from > args.date_to:
        raise ValueError("--date-from must be before or equal to --date-to.")

    request = OpenAlexBootstrapRequestDTO(
        target_count=target_count,
        date_from=args.date_from,
        date_to=args.date_to,
        sample=args.sample,
        normalize=args.normalize,
        monthly_counts_csv=args.monthly_counts_csv,
        languages=parse_csv_arg(args.languages),
        types=parse_csv_arg(args.types),
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
    )

    database_url = args.database_url or Settings.from_env().database_url
    engine = create_db_engine(database_url, echo=False)
    SessionLocal = create_session_factory(engine, expire_on_commit=False)

    try:
        redis_adapter = (
            RedisAdapter(build_redis_client(args))
            if request.enqueue_indexing and not request.dry_run
            else None
        )
        runner = OpenAlexBootstrapRunner(
            request=request,
            session_factory=SessionLocal,
            load_plan_builder=OpenAlexLoadPlanBuilder(
                monthly_counts_loader=MonthlyCountsLoader()
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
        )
        return await runner.run()
    finally:
        engine.dispose()


def resolve_target_count(cli_value: int | None) -> int:
    """Resolve target count from CLI or supported environment variables."""
    if cli_value is not None:
        if cli_value < 0:
            raise ValueError("--target-count must be non-negative.")
        return cli_value
    env_value = os.getenv("BOOTSTRAP_PAPERS_COUNT") or os.getenv("BOOSTRAP_PAPERS_COUNT")
    if env_value is None or not env_value.strip():
        raise ValueError(
            "--target-count is required unless BOOTSTRAP_PAPERS_COUNT is set."
        )
    target_count = int(env_value)
    if target_count < 0:
        raise ValueError("BOOTSTRAP_PAPERS_COUNT must be non-negative.")
    return target_count


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


def configure_logging() -> None:
    """Configure bootstrap logging."""
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def print_json(payload: dict[str, Any], *, stream: Any = sys.stdout) -> None:
    """Print JSON with stable UTF-8 output."""
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str), file=stream)


if __name__ == "__main__":
    raise SystemExit(main())
