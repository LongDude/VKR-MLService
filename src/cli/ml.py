from __future__ import annotations

import argparse
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
    ResearchEntityIndexingFacade,
    SummaryFacade,
    UserProfileFacade,
)
from ml.services.events import (
    CompositeEventSink,
    EventSink,
    LoggingEventSink,
    MLEvent,
    NoopEventSink,
    RedisEventSink,
    TqdmEventSink,
)
from ml.workers.redis_worker import (
    CLUSTER_DYNAMICS_RECOMPUTE_QUEUE,
    CLUSTER_RECOMPUTE_QUEUE,
    ENTITY_INDEXING_QUEUE,
    USER_PROFILE_RECOMPUTE_QUEUE,
)
from models.session import create_db_engine, create_session_factory
from repositories import (
    FavouriteRepository,
    PaperGraphRepository,
    PaperRepository,
    TaxonomyRepository,
    TrackedAreaRepository,
    UserRepository,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse ML CLI command and command-specific arguments."""
    parser = argparse.ArgumentParser(description="ML-service command line utilities.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    research_parser = subparsers.add_parser(
        "index-research-entities",
        help="Index domains, fields, subfields, topics, and keywords into Qdrant.",
    )
    research_parser.add_argument(
        "--entity-type",
        choices=["all", "domain", "field", "subfield", "topic", "keyword"],
        default="all",
        help="Research entity type to index. Defaults to all.",
    )
    research_parser.add_argument(
        "--limit",
        type=int,
        default=10000,
        help="Maximum number of entities to index. Defaults to 10000.",
    )
    research_parser.add_argument(
        "--offset",
        type=int,
        default=0,
        help="Initial repository offset. Defaults to 0.",
    )
    research_parser.add_argument(
        "--force-reindex",
        action="store_true",
        help="Overwrite existing Qdrant points instead of skipping them.",
    )
    research_parser.add_argument(
        "--batch-size",
        type=int,
        default=128,
        help="Embedding and Qdrant upsert batch size. Defaults to 128.",
    )
    add_runtime_args(research_parser)
    add_common_connection_args(research_parser)
    add_redis_connection_args(research_parser)

    trends_parser = subparsers.add_parser(
        "recompute-trends",
        help="Recompute topic-based trend clusters into Qdrant.",
    )
    trends_parser.add_argument(
        "--date-from",
        type=parse_iso_date,
        required=True,
        help="Publication date lower bound in YYYY-MM-DD format.",
    )
    trends_parser.add_argument(
        "--date-to",
        type=parse_iso_date,
        required=True,
        help="Publication date upper bound in YYYY-MM-DD format.",
    )
    trends_parser.add_argument(
        "--limit",
        type=int,
        default=1000,
        help="Maximum number of topics to recompute. Defaults to 1000.",
    )
    trends_parser.add_argument(
        "--force-summary",
        action="store_true",
        help="Regenerate LLM summaries even if a cluster already has one.",
    )
    trends_parser.add_argument(
        "--batch-size",
        type=int,
        default=50,
        help="Topic page size for recomputation. Defaults to 50.",
    )
    add_runtime_args(trends_parser)
    add_common_connection_args(trends_parser)
    add_redis_connection_args(trends_parser)

    dynamics_parser = subparsers.add_parser(
        "recompute-cluster-dynamics",
        help="Recompute cluster period dynamics into Qdrant.",
    )
    dynamics_group = dynamics_parser.add_mutually_exclusive_group(required=True)
    dynamics_group.add_argument(
        "--cluster-id",
        default=None,
        help="One cluster id to recompute, for example topic:120.",
    )
    dynamics_group.add_argument(
        "--all-clusters",
        action="store_true",
        help="Recompute dynamics for top cached trend clusters.",
    )
    dynamics_parser.add_argument(
        "--date-from",
        type=parse_iso_date,
        required=True,
        help="Period lower bound in YYYY-MM-DD format.",
    )
    dynamics_parser.add_argument(
        "--date-to",
        type=parse_iso_date,
        required=True,
        help="Period upper bound in YYYY-MM-DD format.",
    )
    dynamics_parser.add_argument(
        "--granularity",
        choices=["month", "week"],
        default="month",
        help="Period granularity. Defaults to month.",
    )
    dynamics_parser.add_argument(
        "--limit-clusters",
        type=int,
        default=100,
        help="Maximum number of clusters when --all-clusters is used.",
    )
    add_runtime_args(dynamics_parser)
    add_common_connection_args(dynamics_parser)
    add_redis_connection_args(dynamics_parser)

    user_profile_parser = subparsers.add_parser(
        "recompute-user-profile",
        help="Recompute one or all user vector profiles into Qdrant.",
    )
    user_profile_group = user_profile_parser.add_mutually_exclusive_group(required=True)
    user_profile_group.add_argument(
        "--user-id",
        type=int,
        default=None,
        help="User id to recompute.",
    )
    user_profile_group.add_argument(
        "--all-users",
        action="store_true",
        help="Recompute profiles for all users from PostgreSQL.",
    )
    user_profile_parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="User page size when --all-users is used. Defaults to 100.",
    )
    user_profile_parser.add_argument(
        "--force",
        action="store_true",
        help="Force profile recalculation. Recompute mode always upserts current vectors.",
    )
    add_runtime_args(user_profile_parser)
    add_common_connection_args(user_profile_parser)
    add_redis_connection_args(user_profile_parser)

    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Run the requested ML CLI command."""
    args = parse_args(argv)
    load_dotenv(args.env_file)
    configure_logging(getattr(args, "verbose", 0))

    try:
        if args.command == "index-research-entities":
            payload = run_index_research_entities(args)
        elif args.command == "recompute-trends":
            payload = run_recompute_trends(args)
        elif args.command == "recompute-cluster-dynamics":
            payload = run_recompute_cluster_dynamics(args)
        elif args.command == "recompute-user-profile":
            payload = run_recompute_user_profile(args)
        else:
            raise AssertionError(f"Unhandled command: {args.command}")
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


def run_index_research_entities(args: argparse.Namespace) -> dict[str, Any]:
    """Index research entities through ResearchEntityIndexingFacade."""
    if args.enqueue:
        message = {
            "task_type": "entity_indexing",
            "entity_type": args.entity_type,
            "limit": args.limit,
            "offset": args.offset,
            "batch_size": args.batch_size,
            "force_reindex": bool(args.force_reindex),
        }
        return enqueue_messages(args, ENTITY_INDEXING_QUEUE, [message])

    database_url = args.database_url or Settings.from_env().database_url
    engine = create_db_engine(database_url, echo=False)
    SessionLocal = create_session_factory(engine, expire_on_commit=False)

    try:
        event_sink = build_event_sink(args)
        embedding_adapter = LMStudioEmbeddingAdapter(
            base_url=args.lmstudio_url
            or os.getenv("LMSTUDIO_BASE_URL")
            or "http://localhost:1234",
        )
        qdrant_adapter = build_qdrant_adapter(args)

        with SessionLocal() as session:
            facade = ResearchEntityIndexingFacade(
                taxonomy_repository=TaxonomyRepository(session),
                paper_graph_repository=PaperGraphRepository(session),
                embedding_adapter=embedding_adapter,
                qdrant_adapter=qdrant_adapter,
                embedding_model=args.embedding_model
                or os.getenv("EMBEDDING_MODEL")
                or DEFAULT_EMBEDDING_MODEL,
                event_sink=event_sink,
            )
            result = facade.index_all_entities(
                force_reindex=args.force_reindex,
                limit=args.limit,
                offset=args.offset,
                entity_type=args.entity_type,
                batch_size=args.batch_size,
            )
        return {
            "command": "index-research-entities",
            "entity_type": args.entity_type,
            "result": result.model_dump(mode="json"),
        }
    finally:
        engine.dispose()


def run_recompute_trends(args: argparse.Namespace) -> dict[str, Any]:
    """Recompute topic-based trend clusters through ClusterAnalyticsFacade."""
    if args.enqueue:
        message = {
            "task_type": "cluster_recompute",
            "date_from": args.date_from.isoformat(),
            "date_to": args.date_to.isoformat(),
            "limit": args.limit,
            "batch_size": args.batch_size,
            "force_summary": bool(args.force_summary),
        }
        return enqueue_messages(args, CLUSTER_RECOMPUTE_QUEUE, [message])

    database_url = args.database_url or Settings.from_env().database_url
    engine = create_db_engine(database_url, echo=False)
    SessionLocal = create_session_factory(engine, expire_on_commit=False)

    try:
        event_sink = build_event_sink(args)
        qdrant_adapter = build_qdrant_adapter(args)
        redis_adapter = RedisAdapter(build_redis_client(args))
        summary_facade = SummaryFacade(
            chat_adapter=LMStudioChatAdapter(
                base_url=args.lmstudio_url
                or os.getenv("LMSTUDIO_BASE_URL")
                or "http://localhost:1234",
            )
        )

        with SessionLocal() as session:
            facade = ClusterAnalyticsFacade(
                taxonomy_repository=TaxonomyRepository(session),
                paper_repository=PaperRepository(session),
                paper_graph_repository=PaperGraphRepository(session),
                qdrant_adapter=qdrant_adapter,
                redis_adapter=redis_adapter,
                summary_facade=summary_facade,
                event_sink=event_sink,
            )
            result = facade.recompute_all_clusters(
                date_from=args.date_from,
                date_to=args.date_to,
                limit=args.limit,
                force_summary=args.force_summary,
                batch_size=args.batch_size,
            )
        return {
            "command": "recompute-trends",
            "date_from": args.date_from,
            "date_to": args.date_to,
            "result": result.model_dump(mode="json"),
        }
    finally:
        engine.dispose()


def run_recompute_cluster_dynamics(args: argparse.Namespace) -> dict[str, Any]:
    """Recompute time dynamics for one or many trend clusters."""
    if args.enqueue:
        redis_adapter = RedisAdapter(build_redis_client(args))
        cluster_ids = resolve_cluster_ids_for_dynamics_enqueue(args, redis_adapter)
        messages = [
            {
                "task_type": "cluster_dynamics_recompute",
                "cluster_id": cluster_id,
                "date_from": args.date_from.isoformat(),
                "date_to": args.date_to.isoformat(),
                "granularity": args.granularity,
            }
            for cluster_id in cluster_ids
        ]
        return enqueue_messages(
            args,
            CLUSTER_DYNAMICS_RECOMPUTE_QUEUE,
            messages,
            redis_adapter=redis_adapter,
        )

    database_url = args.database_url or Settings.from_env().database_url
    engine = create_db_engine(database_url, echo=False)
    SessionLocal = create_session_factory(engine, expire_on_commit=False)

    try:
        qdrant_adapter = build_qdrant_adapter(args)
        redis_adapter = RedisAdapter(build_redis_client(args))
        event_sink = build_event_sink(args, redis_adapter=redis_adapter)

        with SessionLocal() as session:
            dynamics_facade = ClusterDynamicsFacade(
                taxonomy_repository=TaxonomyRepository(session),
                paper_graph_repository=PaperGraphRepository(session),
                qdrant_adapter=qdrant_adapter,
                event_sink=event_sink,
            )
            cluster_ids = resolve_cluster_ids_for_dynamics(
                args,
                session=session,
                qdrant_adapter=qdrant_adapter,
                redis_adapter=redis_adapter,
                event_sink=event_sink,
            )
            result = recompute_cluster_dynamics_batch(
                dynamics_facade,
                cluster_ids=cluster_ids,
                date_from=args.date_from,
                date_to=args.date_to,
                granularity=args.granularity,
            )
        return {
            "command": "recompute-cluster-dynamics",
            "cluster_ids": cluster_ids,
            "date_from": args.date_from,
            "date_to": args.date_to,
            "granularity": args.granularity,
            "result": result,
        }
    finally:
        engine.dispose()


def run_recompute_user_profile(args: argparse.Namespace) -> dict[str, Any]:
    """Recompute user vector profiles through UserProfileFacade."""
    if args.user_id is not None and args.user_id <= 0:
        raise ValueError("--user-id must be a positive integer.")
    if args.batch_size <= 0:
        raise ValueError("--batch-size must be a positive integer.")

    database_url = args.database_url or Settings.from_env().database_url
    engine = create_db_engine(database_url, echo=False)
    SessionLocal = create_session_factory(engine, expire_on_commit=False)

    try:
        if args.enqueue:
            redis_adapter = RedisAdapter(build_redis_client(args))
            with SessionLocal() as session:
                if args.user_id is not None:
                    user_ids = [args.user_id]
                else:
                    user_ids = list_user_ids(
                        UserRepository(session),
                        batch_size=args.batch_size,
                    )
                return enqueue_messages(
                    args,
                    USER_PROFILE_RECOMPUTE_QUEUE,
                    [
                        {
                            "task_type": "user_profile_recompute",
                            "user_id": user_id,
                            "force": bool(args.force),
                        }
                        for user_id in user_ids
                    ],
                    redis_adapter=redis_adapter,
                )

        event_sink = build_event_sink(args)
        qdrant_adapter = build_qdrant_adapter(args)

        with SessionLocal() as session:
            facade = UserProfileFacade(
                favourite_repository=FavouriteRepository(session),
                tracked_area_repository=TrackedAreaRepository(session),
                taxonomy_repository=TaxonomyRepository(session),
                qdrant_adapter=qdrant_adapter,
                event_sink=event_sink,
            )

            if args.user_id is not None:
                profile = facade.recompute_user_profile(args.user_id)
                result: dict[str, Any] = profile.model_dump(mode="json")
            else:
                result = recompute_user_profiles_batch(
                    facade,
                    user_repository=UserRepository(session),
                    batch_size=args.batch_size,
                    force=args.force,
                    event_sink=event_sink,
                )

        return {
            "command": "recompute-user-profile",
            "user_id": args.user_id,
            "all_users": bool(args.all_users),
            "force": bool(args.force),
            "result": result,
        }
    finally:
        engine.dispose()


def resolve_cluster_ids_for_dynamics(
    args: argparse.Namespace,
    *,
    session: Any,
    qdrant_adapter: QdrantAdapter,
    redis_adapter: RedisAdapter,
    event_sink: EventSink | None = None,
) -> list[str]:
    """Resolve one cluster id or top cached cluster ids for dynamics recompute."""
    if args.cluster_id:
        return [args.cluster_id]

    summary_facade = SummaryFacade(
        chat_adapter=LMStudioChatAdapter(
            base_url=args.lmstudio_url
            or os.getenv("LMSTUDIO_BASE_URL")
            or "http://localhost:1234",
        )
    )
    analytics_facade = ClusterAnalyticsFacade(
        taxonomy_repository=TaxonomyRepository(session),
        paper_repository=PaperRepository(session),
        paper_graph_repository=PaperGraphRepository(session),
        qdrant_adapter=qdrant_adapter,
        redis_adapter=redis_adapter,
        summary_facade=summary_facade,
        event_sink=event_sink,
    )
    return [
        cluster.cluster_key
        for cluster in analytics_facade.get_top_clusters(limit=args.limit_clusters)
    ]


def resolve_cluster_ids_for_dynamics_enqueue(
    args: argparse.Namespace,
    redis_adapter: RedisAdapter,
) -> list[str]:
    """Resolve dynamics cluster ids for enqueue mode without Qdrant/LMStudio work."""
    if args.cluster_id:
        return [args.cluster_id]

    index = redis_adapter.get_json("ml:trend_clusters:index") or {}
    cluster_ids = [str(value) for value in index.get("cluster_ids", [])]
    if args.limit_clusters is not None:
        cluster_ids = cluster_ids[: args.limit_clusters]
    if not cluster_ids:
        raise ValueError(
            "No cached cluster ids found in Redis. Recompute trends first or "
            "pass --cluster-id for enqueue mode."
        )
    return cluster_ids


def recompute_cluster_dynamics_batch(
    facade: ClusterDynamicsFacade,
    *,
    cluster_ids: list[str],
    date_from: date,
    date_to: date,
    granularity: str,
) -> dict[str, Any]:
    """Run cluster dynamics recomputation for a list of clusters."""
    total = 0
    updated = 0
    skipped = 0
    failed = 0
    errors: list[dict[str, Any]] = []

    for cluster_id in cluster_ids:
        total += 1
        try:
            result = facade.recompute_cluster_periods(
                cluster_id=cluster_id,
                date_from=date_from,
                date_to=date_to,
                granularity=granularity,
            )
        except AppError as exc:
            failed += 1
            errors.append(
                {
                    "cluster_id": cluster_id,
                    "code": exc.code,
                    "message": exc.message,
                    "details": exc.details or {},
                }
            )
            continue
        updated += result.updated
        skipped += result.skipped
        failed += result.failed
        for error in result.errors:
            errors.append({"cluster_id": cluster_id, **error})

    return {
        "total": total,
        "updated": updated,
        "skipped": skipped,
        "failed": failed,
        "errors": errors,
    }


def recompute_user_profiles_batch(
    facade: UserProfileFacade,
    *,
    user_repository: UserRepository,
    batch_size: int,
    force: bool,
    event_sink: EventSink | None = None,
) -> dict[str, Any]:
    """Run user profile recomputation over all users in repository pages."""
    total = 0
    updated = 0
    skipped = 0
    failed = 0
    errors: list[dict[str, Any]] = []
    offset = 0
    sink = event_sink or NoopEventSink()

    while True:
        user_ids = user_repository.list_ids(limit=batch_size, offset=offset)
        if not user_ids:
            break
        offset += len(user_ids)

        for user_id in user_ids:
            total += 1
            try:
                if not facade.has_sufficient_profile_data(user_id):
                    skipped += 1
                    sink.emit(
                        MLEvent(
                            event_type="user_profile_batch_progress",
                            task_type="user_profile_recompute",
                            entity_id="all_users",
                            stage="users",
                            current=total,
                            message=(
                                f"users done={total} updated={updated} "
                                f"skipped={skipped} failed={failed}"
                            ),
                        )
                    )
                    continue
                facade.recompute_user_profile(user_id)
            except AppError as exc:
                failed += 1
                errors.append(
                    {
                        "user_id": user_id,
                        "code": exc.code,
                        "message": exc.message,
                        "details": exc.details or {},
                    }
                )
                sink.emit(
                    MLEvent(
                        event_type="user_profile_batch_progress",
                        task_type="user_profile_recompute",
                        entity_id="all_users",
                        stage="users",
                        current=total,
                        message=(
                            f"users done={total} updated={updated} "
                            f"skipped={skipped} failed={failed}"
                        ),
                    )
                )
                continue
            updated += 1
            sink.emit(
                MLEvent(
                    event_type="user_profile_batch_progress",
                    task_type="user_profile_recompute",
                    entity_id="all_users",
                    stage="users",
                    current=total,
                    message=(
                        f"users done={total} updated={updated} "
                        f"skipped={skipped} failed={failed}"
                    ),
                )
            )

    return {
        "total": total,
        "updated": updated,
        "skipped": skipped,
        "failed": failed,
        "force": force,
        "errors": errors,
    }


def list_user_ids(user_repository: UserRepository, *, batch_size: int) -> list[int]:
    """Read all user ids through repository pages."""
    user_ids: list[int] = []
    offset = 0
    while True:
        page = user_repository.list_ids(limit=batch_size, offset=offset)
        if not page:
            break
        user_ids.extend(int(user_id) for user_id in page)
        offset += len(page)
        if len(page) < batch_size:
            break
    return user_ids


def add_common_connection_args(parser: argparse.ArgumentParser) -> None:
    """Add shared database, LMStudio, and Qdrant connection arguments."""
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


def add_runtime_args(parser: argparse.ArgumentParser) -> None:
    """Add shared execution mode, progress, and event arguments."""
    parser.add_argument(
        "--enqueue",
        action="store_true",
        help="Enqueue Redis task(s) for worker instead of running heavy ML work now.",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help=(
            "Increase logging verbosity. -v enables HTTP request logs at INFO; "
            "-vv enables DEBUG logs."
        ),
    )
    parser.add_argument(
        "--no-progress",
        action="store_true",
        help="Disable tqdm progress bars.",
    )
    parser.add_argument(
        "--event-redis",
        action="store_true",
        help="Write latest ML event status to Redis.",
    )
    parser.add_argument(
        "--event-ttl-seconds",
        type=int,
        default=24 * 60 * 60,
        help="TTL for Redis event status keys. Defaults to 86400.",
    )


def add_redis_connection_args(parser: argparse.ArgumentParser) -> None:
    """Add Redis connection arguments for commands that need cache/queues."""
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


def enqueue_messages(
    args: argparse.Namespace,
    queue_name: str,
    messages: list[dict[str, Any]],
    *,
    redis_adapter: RedisAdapter | None = None,
) -> dict[str, Any]:
    """Enqueue prepared ML task messages and return a compact report."""
    adapter = redis_adapter or RedisAdapter(build_redis_client(args))
    for message in messages:
        adapter.enqueue(queue_name, message)
    return {
        "command": args.command,
        "enqueue": True,
        "queue": queue_name,
        "messages": len(messages),
        "sample": messages[:3],
    }


def build_event_sink(
    args: argparse.Namespace,
    redis_adapter: RedisAdapter | None = None,
) -> EventSink:
    """Build a reusable event sink chain for CLI commands."""
    sinks: list[EventSink] = [
        LoggingEventSink(logging.getLogger("ml.cli.events"), verbosity=args.verbose)
    ]
    if not args.no_progress:
        sinks.append(TqdmEventSink())
    if args.event_redis:
        adapter = redis_adapter or RedisAdapter(build_redis_client(args))
        sinks.append(
            RedisEventSink(
                adapter,
                ttl_seconds=max(1, int(args.event_ttl_seconds)),
            )
        )
    if not sinks:
        return NoopEventSink()
    return CompositeEventSink(sinks, logger=logging.getLogger("ml.cli.events"))


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


def configure_logging(verbosity: int = 0) -> None:
    """Configure CLI logging and dependency request logs."""
    env_level = os.getenv("LOG_LEVEL", "INFO").upper()
    if verbosity <= 0:
        level = getattr(logging, env_level, logging.INFO)
    elif verbosity == 1:
        level = logging.INFO
    else:
        level = logging.DEBUG
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    if verbosity <= 0:
        logging.getLogger("httpx").setLevel(logging.WARNING)
        logging.getLogger("httpcore").setLevel(logging.WARNING)
        logging.getLogger("qdrant_client").setLevel(logging.WARNING)
    elif verbosity == 1:
        logging.getLogger("httpx").setLevel(logging.INFO)
        logging.getLogger("httpcore").setLevel(logging.WARNING)
        logging.getLogger("qdrant_client").setLevel(logging.INFO)
    else:
        logging.getLogger("httpx").setLevel(logging.DEBUG)
        logging.getLogger("httpcore").setLevel(logging.DEBUG)
        logging.getLogger("qdrant_client").setLevel(logging.DEBUG)


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


def parse_iso_date(value: str) -> date:
    """Parse an ISO date for argparse."""
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Expected YYYY-MM-DD date, got {value!r}."
        ) from exc


def print_json(payload: dict[str, Any], *, stream: Any = sys.stdout) -> None:
    """Print JSON with stable UTF-8 output."""
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str), file=stream)


if __name__ == "__main__":
    raise SystemExit(main())
