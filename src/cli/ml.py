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
    add_common_connection_args(research_parser)

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
    add_common_connection_args(user_profile_parser)

    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Run the requested ML CLI command."""
    args = parse_args(argv)
    load_dotenv(args.env_file)

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
    database_url = args.database_url or Settings.from_env().database_url
    engine = create_db_engine(database_url, echo=False)
    SessionLocal = create_session_factory(engine, expire_on_commit=False)

    try:
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
    database_url = args.database_url or Settings.from_env().database_url
    engine = create_db_engine(database_url, echo=False)
    SessionLocal = create_session_factory(engine, expire_on_commit=False)

    try:
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
    database_url = args.database_url or Settings.from_env().database_url
    engine = create_db_engine(database_url, echo=False)
    SessionLocal = create_session_factory(engine, expire_on_commit=False)

    try:
        qdrant_adapter = build_qdrant_adapter(args)
        redis_adapter = RedisAdapter(build_redis_client(args))

        with SessionLocal() as session:
            dynamics_facade = ClusterDynamicsFacade(
                taxonomy_repository=TaxonomyRepository(session),
                paper_graph_repository=PaperGraphRepository(session),
                qdrant_adapter=qdrant_adapter,
            )
            cluster_ids = resolve_cluster_ids_for_dynamics(
                args,
                session=session,
                qdrant_adapter=qdrant_adapter,
                redis_adapter=redis_adapter,
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
        qdrant_adapter = build_qdrant_adapter(args)

        with SessionLocal() as session:
            facade = UserProfileFacade(
                favourite_repository=FavouriteRepository(session),
                tracked_area_repository=TrackedAreaRepository(session),
                taxonomy_repository=TaxonomyRepository(session),
                qdrant_adapter=qdrant_adapter,
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
    )
    return [
        cluster.cluster_key
        for cluster in analytics_facade.get_top_clusters(limit=args.limit_clusters)
    ]


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
) -> dict[str, Any]:
    """Run user profile recomputation over all users in repository pages."""
    total = 0
    updated = 0
    skipped = 0
    failed = 0
    errors: list[dict[str, Any]] = []
    offset = 0

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
                continue
            updated += 1

    return {
        "total": total,
        "updated": updated,
        "skipped": skipped,
        "failed": failed,
        "force": force,
        "errors": errors,
    }


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
