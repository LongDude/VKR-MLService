from __future__ import annotations

import argparse
import json
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

from adapters import LMStudioEmbeddingAdapter, QdrantAdapter, RedisAdapter
from core.config import Settings
from core.exceptions import AppError, InvalidRequestError
from ml.constants import DEFAULT_EMBEDDING_MODEL
from ml.facades import PaperIndexingFacade
from models.session import create_db_engine, create_session_factory
from repositories import (
    AuthorRepository,
    InstitutionRepository,
    PaperMetaSourceRepository,
    PaperRepository,
    TaxonomyRepository,
)
from dto.papers import PaperBatchIndexingRequestDTO


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments for paper ML indexing."""
    parser = argparse.ArgumentParser(
        description="Index papers into Qdrant through PaperIndexingFacade.",
    )
    parser.add_argument(
        "paper_ids",
        nargs="*",
        type=int,
        help="One or more local PostgreSQL paper ids.",
    )
    parser.add_argument(
        "--paper-ids",
        dest="paper_ids_csv",
        default=None,
        help="Comma-separated paper ids, for example: 1,2,3.",
    )
    parser.add_argument(
        "--date-from",
        type=parse_iso_date,
        default=None,
        help="Publication date lower bound in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--date-to",
        type=parse_iso_date,
        default=None,
        help="Publication date upper bound in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of papers to index for a date range.",
    )
    parser.add_argument(
        "--offset",
        type=int,
        default=0,
        help="Initial PostgreSQL offset for date-range indexing.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=200,
        help="Number of papers per batch for date-range indexing.",
    )
    parser.add_argument(
        "--force-reindex",
        action="store_true",
        help="Recalculate embedding and overwrite the Qdrant point.",
    )
    parser.add_argument(
        "--env-file",
        default=str(PROJECT_DIR / ".env"),
        help="Path to .env file. Defaults to project .env.",
    )
    parser.add_argument(
        "--database-url",
        default=None,
        help="SQLAlchemy database URL. Defaults to DATABASE_URL or POSTGRES_* envs.",
    )
    parser.add_argument(
        "--lmstudio-url",
        default=None,
        help="LMStudio base URL. Defaults to LMSTUDIO_BASE_URL or http://localhost:1234.",
    )
    parser.add_argument(
        "--embedding-model",
        default=None,
        help=f"Embedding model name. Defaults to {DEFAULT_EMBEDDING_MODEL}.",
    )
    parser.add_argument(
        "--qdrant-url",
        default=None,
        help="Qdrant URL. If omitted, QDRANT_URL or host/port are used.",
    )
    parser.add_argument(
        "--qdrant-host",
        default=None,
        help="Qdrant host. Defaults to QDRANT_HOST when set.",
    )
    parser.add_argument(
        "--qdrant-port",
        type=int,
        default=None,
        help="Qdrant port. Defaults to QDRANT_PORT when set.",
    )
    parser.add_argument(
        "--qdrant-api-key",
        default=None,
        help="Qdrant API key. Defaults to QDRANT_API_KEY when set.",
    )
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
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Run paper indexing by ids or publication-date range."""
    args = parse_args(argv)
    load_dotenv(args.env_file)
    engine = None

    try:
        paper_ids = resolve_paper_ids(args)
        database_url = args.database_url or Settings.from_env().database_url
        engine = create_db_engine(database_url, echo=False)
        SessionLocal = create_session_factory(engine, expire_on_commit=False)

        embedding_adapter = LMStudioEmbeddingAdapter(
            base_url=args.lmstudio_url
            or os.getenv("LMSTUDIO_BASE_URL")
            or "http://localhost:1234",
        )
        qdrant_adapter = build_qdrant_adapter(args)
        redis_adapter = RedisAdapter(build_redis_client(args))

        with SessionLocal() as session:
            facade = PaperIndexingFacade(
                paper_repository=PaperRepository(session),
                taxonomy_repository=TaxonomyRepository(session),
                author_repository=AuthorRepository(session),
                institution_repository=InstitutionRepository(session),
                paper_meta_source_repository=PaperMetaSourceRepository(session),
                embedding_adapter=embedding_adapter,
                qdrant_adapter=qdrant_adapter,
                redis_adapter=redis_adapter,
                embedding_model=args.embedding_model
                or os.getenv("EMBEDDING_MODEL")
                or DEFAULT_EMBEDDING_MODEL,
            )
            response = facade.index_batch(
                PaperBatchIndexingRequestDTO(
                    paper_ids=paper_ids,
                    date_from=args.date_from,
                    date_to=args.date_to,
                    limit=args.limit,
                    offset=args.offset,
                    batch_size=args.batch_size,
                    force_reindex=args.force_reindex,
                )
            )
        print_json(response.model_dump(mode="json"))
        return 0
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
    finally:
        if engine is not None:
            engine.dispose()


def build_qdrant_adapter(args: argparse.Namespace) -> QdrantAdapter:
    """Build QdrantAdapter from CLI arguments or environment variables."""
    qdrant_port = args.qdrant_port or _optional_int_env("QDRANT_PORT")
    return QdrantAdapter(
        url=args.qdrant_url or os.getenv("QDRANT_URL"),
        host=args.qdrant_host or os.getenv("QDRANT_HOST"),
        port=qdrant_port,
        api_key=args.qdrant_api_key or os.getenv("QDRANT_API_KEY"),
    )


def resolve_paper_ids(args: argparse.Namespace) -> list[int]:
    """Merge positional ids and comma-separated ids from CLI arguments."""
    paper_ids = list(args.paper_ids)
    if args.paper_ids_csv:
        try:
            paper_ids.extend(
                int(value.strip())
                for value in args.paper_ids_csv.split(",")
                if value.strip()
            )
        except ValueError as exc:
            raise InvalidRequestError(
                "--paper-ids must be a comma-separated list of integers",
                details={"paper_ids": args.paper_ids_csv},
            ) from exc
    if paper_ids and (args.date_from is not None or args.date_to is not None):
        raise InvalidRequestError("Use either paper ids or a date range, not both.")
    if not paper_ids and args.date_from is None and args.date_to is None:
        raise InvalidRequestError(
            "Provide paper ids or at least one of --date-from/--date-to."
        )
    if (
        args.date_from is not None
        and args.date_to is not None
        and args.date_from > args.date_to
    ):
        raise InvalidRequestError(
            "--date-from must be less than or equal to --date-to",
            details={"date_from": args.date_from, "date_to": args.date_to},
        )
    return list(dict.fromkeys(paper_ids))


def parse_iso_date(value: str) -> date:
    """Parse an ISO date for argparse."""
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Expected YYYY-MM-DD date, got {value!r}."
        ) from exc


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


def _optional_int_env(name: str) -> int | None:
    value = os.getenv(name)
    if value is None or not value.strip():
        return None
    return int(value)


def print_json(payload: dict[str, Any], *, stream: Any = sys.stdout) -> None:
    """Print JSON with stable UTF-8 output."""
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str), file=stream)


if __name__ == "__main__":
    raise SystemExit(main())
