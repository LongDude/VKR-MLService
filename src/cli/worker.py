from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any

from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent
SRC_DIR = BASE_DIR.parent
PROJECT_DIR = SRC_DIR.parent

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from adapters import (
    LMStudioChatAdapter,
    LMStudioEmbeddingAdapter,
    QdrantAdapter,
    RedisAdapter,
)
from core.config import Settings
from core.exceptions import AppError
from ml.constants import DEFAULT_EMBEDDING_MODEL
from ml.facades import (
    ClusterAnalyticsFacade,
    ClusterDynamicsFacade,
    PaperIndexingFacade,
    ResearchEntityIndexingFacade,
    SummaryFacade,
    UserProfileFacade,
)
from ml.pipelines.cluster_dynamics_pipeline import ClusterDynamicsPipeline
from ml.pipelines.paper_indexing_pipeline import PaperIndexingPipeline
from ml.pipelines.research_entities_pipeline import ResearchEntitiesPipeline
from ml.pipelines.trend_recompute_pipeline import TrendRecomputePipeline
from ml.pipelines.user_profile_pipeline import UserProfilePipeline
from ml.workers.redis_worker import (
    CLUSTER_DYNAMICS_RECOMPUTE_QUEUE,
    CLUSTER_RECOMPUTE_QUEUE,
    DEFAULT_QUEUE_ORDER,
    ENTITY_INDEXING_QUEUE,
    PAPER_INDEXING_QUEUE,
    USER_PROFILE_RECOMPUTE_QUEUE,
    RedisMLWorker,
)
from ml.workers.task_handlers import MLTaskHandler
from models.session import create_db_engine, create_session_factory
from repositories import (
    AuthorRepository,
    FavouriteRepository,
    InstitutionRepository,
    PaperGraphRepository,
    PaperMetaSourceRepository,
    PaperRepository,
    TaxonomyRepository,
    TrackedAreaRepository,
)


QUEUE_ALIASES = {
    "paper_indexing": PAPER_INDEXING_QUEUE,
    "entity_indexing": ENTITY_INDEXING_QUEUE,
    "research_entities_indexing": ENTITY_INDEXING_QUEUE,
    "cluster_recompute": CLUSTER_RECOMPUTE_QUEUE,
    "cluster_dynamics_recompute": CLUSTER_DYNAMICS_RECOMPUTE_QUEUE,
    "user_profile_recompute": USER_PROFILE_RECOMPUTE_QUEUE,
}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse worker CLI command and command-specific arguments."""
    parser = argparse.ArgumentParser(description="ML worker CLI utilities.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser(
        "run",
        help="Run Redis ML worker for background tasks.",
    )
    run_parser.add_argument(
        "--queues",
        default=",".join(alias for alias in QUEUE_ALIASES if alias != "research_entities_indexing"),
        help=(
            "Comma-separated queues to process. Accepts names like "
            "paper_indexing,cluster_recompute or full queue:* names."
        ),
    )
    run_parser.add_argument(
        "--max-tasks",
        type=int,
        default=None,
        help="Stop after processing this many tasks. Defaults to run forever.",
    )
    run_parser.add_argument(
        "--idle-sleep",
        type=float,
        default=2.0,
        help="Seconds to sleep when no tasks are available. Defaults to 2.",
    )
    run_parser.add_argument(
        "--paper-indexing-batch-size",
        type=int,
        default=500,
        help=(
            "Maximum queue:paper_indexing messages to combine into one "
            "PaperIndexingPipeline.run_many call. Defaults to 500."
        ),
    )
    run_parser.add_argument(
        "--cluster-recompute-batch-size",
        type=int,
        default=50,
        help=(
            "Maximum queue:cluster_recompute topic messages to combine into one "
            "cluster recompute batch. Defaults to 50."
        ),
    )
    run_parser.add_argument(
        "--env-file",
        default=str(PROJECT_DIR / ".env"),
        help="Path to .env file. Defaults to project .env.",
    )
    run_parser.add_argument(
        "--database-url",
        default=None,
        help="SQLAlchemy database URL. Defaults to DATABASE_URL or POSTGRES_* envs.",
    )
    run_parser.add_argument(
        "--lmstudio-url",
        default=None,
        help="LMStudio base URL. Defaults to LMSTUDIO_BASE_URL or http://localhost:1234.",
    )
    run_parser.add_argument(
        "--embedding-model",
        default=None,
        help=f"Embedding model name. Defaults to {DEFAULT_EMBEDDING_MODEL}.",
    )
    add_redis_args(run_parser)
    add_qdrant_args(run_parser)

    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Run the requested worker command."""
    args = parse_args(argv)
    load_dotenv(args.env_file)
    configure_logging()

    try:
        if args.command == "run":
            payload = run_worker(args)
        else:
            raise AssertionError(f"Unhandled command: {args.command}")
    except AppError as exc:
        print_json(exc.to_dict(), stream=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print_json({"command": "run", "stopped": "keyboard_interrupt"})
        return 130
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


def run_worker(args: argparse.Namespace) -> dict[str, Any]:
    """Create dependencies and run RedisMLWorker."""
    if args.max_tasks is not None and args.max_tasks <= 0:
        raise ValueError("--max-tasks must be a positive integer.")
    if args.idle_sleep < 0:
        raise ValueError("--idle-sleep must be non-negative.")
    if args.paper_indexing_batch_size <= 0:
        raise ValueError("--paper-indexing-batch-size must be a positive integer.")
    if args.cluster_recompute_batch_size <= 0:
        raise ValueError("--cluster-recompute-batch-size must be a positive integer.")

    queue_names = parse_queues(args.queues)
    database_url = args.database_url or Settings.from_env().database_url
    engine = create_db_engine(database_url, echo=False)
    SessionLocal = create_session_factory(engine, expire_on_commit=False)

    processed = 0
    try:
        redis_adapter = RedisAdapter(build_redis_client(args))
        qdrant_adapter = build_qdrant_adapter(args)
        embedding_adapter = LMStudioEmbeddingAdapter(
            base_url=args.lmstudio_url
            or os.getenv("LMSTUDIO_BASE_URL")
            or "http://localhost:1234",
        )
        chat_adapter = LMStudioChatAdapter(
            base_url=args.lmstudio_url
            or os.getenv("LMSTUDIO_BASE_URL")
            or "http://localhost:1234",
        )

        with SessionLocal() as session:
            handler = build_task_handler(
                session=session,
                redis_adapter=redis_adapter,
                qdrant_adapter=qdrant_adapter,
                embedding_adapter=embedding_adapter,
                chat_adapter=chat_adapter,
                embedding_model=args.embedding_model
                or os.getenv("EMBEDDING_MODEL")
                or DEFAULT_EMBEDDING_MODEL,
            )
            worker = RedisMLWorker(
                redis_adapter=redis_adapter,
                task_handler=handler,
                queues=queue_names,
                batch_sizes={
                    PAPER_INDEXING_QUEUE: args.paper_indexing_batch_size,
                    CLUSTER_RECOMPUTE_QUEUE: args.cluster_recompute_batch_size,
                },
                idle_sleep_seconds=args.idle_sleep,
            )

            if args.max_tasks is None:
                worker.run_forever()
            else:
                processed = run_limited_worker(
                    worker,
                    max_tasks=args.max_tasks,
                    idle_sleep_seconds=args.idle_sleep,
                )

        return {
            "command": "run",
            "queues": queue_names,
            "max_tasks": args.max_tasks,
            "batch_sizes": {
                PAPER_INDEXING_QUEUE: args.paper_indexing_batch_size,
                CLUSTER_RECOMPUTE_QUEUE: args.cluster_recompute_batch_size,
            },
            "processed": processed,
        }
    finally:
        engine.dispose()


def build_task_handler(
    *,
    session: Any,
    redis_adapter: RedisAdapter,
    qdrant_adapter: QdrantAdapter,
    embedding_adapter: LMStudioEmbeddingAdapter,
    chat_adapter: LMStudioChatAdapter,
    embedding_model: str,
) -> MLTaskHandler:
    """Build task handler with all ML pipelines configured."""
    taxonomy_repository = TaxonomyRepository(session)
    paper_repository = PaperRepository(session)
    paper_graph_repository = PaperGraphRepository(session)
    summary_facade = SummaryFacade(chat_adapter=chat_adapter)

    paper_indexing_pipeline = PaperIndexingPipeline(
        PaperIndexingFacade(
            paper_repository=paper_repository,
            taxonomy_repository=taxonomy_repository,
            author_repository=AuthorRepository(session),
            institution_repository=InstitutionRepository(session),
            paper_meta_source_repository=PaperMetaSourceRepository(session),
            embedding_adapter=embedding_adapter,
            qdrant_adapter=qdrant_adapter,
            redis_adapter=redis_adapter,
            embedding_model=embedding_model,
        )
    )
    research_entities_pipeline = ResearchEntitiesPipeline(
        ResearchEntityIndexingFacade(
            taxonomy_repository=taxonomy_repository,
            paper_graph_repository=paper_graph_repository,
            embedding_adapter=embedding_adapter,
            qdrant_adapter=qdrant_adapter,
            embedding_model=embedding_model,
        )
    )
    trend_recompute_pipeline = TrendRecomputePipeline(
        ClusterAnalyticsFacade(
            taxonomy_repository=taxonomy_repository,
            paper_repository=paper_repository,
            paper_graph_repository=paper_graph_repository,
            qdrant_adapter=qdrant_adapter,
            redis_adapter=redis_adapter,
            summary_facade=summary_facade,
        )
    )
    cluster_dynamics_pipeline = ClusterDynamicsPipeline(
        ClusterDynamicsFacade(
            taxonomy_repository=taxonomy_repository,
            paper_graph_repository=paper_graph_repository,
            qdrant_adapter=qdrant_adapter,
        )
    )
    user_profile_pipeline = UserProfilePipeline(
        UserProfileFacade(
            favourite_repository=FavouriteRepository(session),
            tracked_area_repository=TrackedAreaRepository(session),
            taxonomy_repository=taxonomy_repository,
            qdrant_adapter=qdrant_adapter,
        )
    )

    return MLTaskHandler(
        session=session,
        paper_indexing_pipeline=paper_indexing_pipeline,
        research_entities_pipeline=research_entities_pipeline,
        trend_recompute_pipeline=trend_recompute_pipeline,
        cluster_dynamics_pipeline=cluster_dynamics_pipeline,
        user_profile_pipeline=user_profile_pipeline,
    )


def run_limited_worker(
    worker: RedisMLWorker,
    *,
    max_tasks: int,
    idle_sleep_seconds: float,
) -> int:
    """Run worker until max_tasks messages have been handled."""
    processed = 0
    while processed < max_tasks:
        handled = worker.run_once(max_messages=max_tasks - processed)
        if handled:
            processed += worker.last_processed_message_count
        else:
            time.sleep(idle_sleep_seconds)
    return processed


def parse_queues(value: str | None) -> tuple[str, ...]:
    """Parse comma-separated queue aliases into Redis queue names."""
    if value is None or not value.strip():
        return tuple(DEFAULT_QUEUE_ORDER)

    queue_names: list[str] = []
    for raw_item in value.split(","):
        item = raw_item.strip()
        if not item:
            continue
        queue_name = QUEUE_ALIASES.get(item, item)
        if not queue_name.startswith("queue:"):
            raise ValueError(
                f"Unknown queue {item!r}. Use one of {sorted(QUEUE_ALIASES)} "
                "or a full queue:* name."
            )
        if queue_name not in queue_names:
            queue_names.append(queue_name)

    if not queue_names:
        raise ValueError("--queues must contain at least one queue name.")
    return tuple(queue_names)


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


def add_qdrant_args(parser: argparse.ArgumentParser) -> None:
    """Add Qdrant connection arguments."""
    parser.add_argument(
        "--qdrant-url",
        default=None,
        help="Qdrant URL. Defaults to QDRANT_URL when set.",
    )
    parser.add_argument(
        "--qdrant-host",
        default=None,
        help="Qdrant host. Defaults to QDRANT_HOST or localhost.",
    )
    parser.add_argument(
        "--qdrant-port",
        type=int,
        default=None,
        help="Qdrant port. Defaults to QDRANT_PORT or 6333.",
    )
    parser.add_argument(
        "--qdrant-api-key",
        default=None,
        help="Qdrant API key. Defaults to QDRANT_API_KEY when set.",
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


def build_qdrant_adapter(args: argparse.Namespace) -> QdrantAdapter:
    """Build QdrantAdapter from CLI arguments or environment variables."""
    qdrant_url = args.qdrant_url or os.getenv("QDRANT_URL")
    api_key = args.qdrant_api_key or os.getenv("QDRANT_API_KEY")
    if qdrant_url:
        return QdrantAdapter(url=qdrant_url, api_key=api_key)

    return QdrantAdapter(
        host=args.qdrant_host or os.getenv("QDRANT_HOST") or "localhost",
        port=args.qdrant_port or _optional_int_env("QDRANT_PORT") or 6333,
        api_key=api_key,
    )


def _optional_int_env(name: str) -> int | None:
    value = os.getenv(name)
    if value is None or not value.strip():
        return None
    return int(value)


def configure_logging() -> None:
    """Configure basic worker logging."""
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def print_json(payload: dict[str, Any], *, stream: Any = sys.stdout) -> None:
    """Print JSON with stable UTF-8 output."""
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str), file=stream)


if __name__ == "__main__":
    raise SystemExit(main())
