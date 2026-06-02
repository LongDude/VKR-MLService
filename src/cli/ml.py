from __future__ import annotations

import argparse
import json
import logging
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from pathlib import Path
from typing import Any

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
from core.config import Settings, load_settings
from core.dependencies import (
    create_chat_adapter,
    create_embedding_adapter,
    create_qdrant_adapter,
    create_redis_client,
)
from core.exceptions import AppError
from core.logging import configure_logging, get_logger, log_event
from dto.keywords import PaperKeywordExtractionBatchRequestDTO
from dto.papers import PaperBatchIndexingRequestDTO
from dto.topic_reports import TopicQuarterReportGenerateRequestDTO
from ml.constants import DEFAULT_EMBEDDING_MODEL
from ml.facades import (
    ClusterAnalyticsFacade,
    ClusterDynamicsFacade,
    KeywordExtractionFacade,
    PaperIndexingFacade,
    ResearchEntityIndexingFacade,
    SummaryFacade,
    TopicQuarterReportFacade,
    UserProfileFacade,
)
from ml.pipelines.keyword_extraction_pipeline import KeywordExtractionPipeline
from ml.pipelines.paper_indexing_pipeline import PaperIndexingPipeline
from ml.pipelines.topic_quarter_report_pipeline import TopicQuarterReportPipeline
from ml.services.events import (
    CompositeEventSink,
    EventSink,
    LoggingEventSink,
    MLEvent,
    NoopEventSink,
    RedisEventSink,
    TqdmEventSink,
)
from ml.services.quarter_periods import QuarterPeriodService
from models.session import create_db_engine, create_session_factory
from repositories import (
    FavouriteRepository,
    AuthorRepository,
    InstitutionRepository,
    OpenAlexTopicStatsRepository,
    PaperGraphRepository,
    PaperRepository,
    ResearchClusterRepository,
    TaxonomyRepository,
    TopicQuarterReportRepository,
    TrackedAreaRepository,
    UserRepository,
)

logger = get_logger(__name__)

def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse ML CLI command and command-specific arguments."""
    settings = _preload_settings(argv)
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
        default=settings.operations.research_entities_limit,
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
        default=settings.operations.research_entities_batch_size,
        help="Embedding and Qdrant upsert batch size. Defaults to 128.",
    )
    add_runtime_args(research_parser, settings=settings)
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
        default=settings.operations.cluster_recompute_batch_size,
        help="Topic page size for recomputation. Defaults to 50.",
    )
    trends_parser.add_argument(
        "--cluster-workers",
        type=int,
        default=1,
        help="Parallel topic cluster recompute workers. Defaults to 1.",
    )
    add_runtime_args(trends_parser, settings=settings)
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
    add_runtime_args(dynamics_parser, settings=settings)
    add_common_connection_args(dynamics_parser)
    add_redis_connection_args(dynamics_parser)

    report_parser = subparsers.add_parser(
        "generate-topic-report",
        help="Generate quarterly LLM reports for one topic.",
    )
    report_parser.add_argument(
        "--topic-id",
        type=int,
        required=True,
        help="Topic id to report.",
    )
    report_parser.add_argument(
        "--date-from",
        type=parse_iso_date,
        required=True,
        help="Report period lower bound in YYYY-MM-DD format.",
    )
    report_parser.add_argument(
        "--date-to",
        type=parse_iso_date,
        required=True,
        help="Report period upper bound in YYYY-MM-DD format.",
    )
    report_parser.add_argument(
        "--force",
        action="store_true",
        help="Regenerate existing topic quarter reports.",
    )
    report_parser.add_argument(
        "--report-language",
        default=settings.operations.topic_report_language,
        help="Narrative language for generated report text. Defaults to ru.",
    )
    add_runtime_args(report_parser, settings=settings)
    add_common_connection_args(report_parser)
    add_redis_connection_args(report_parser)

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
        default=settings.operations.user_profiles_batch_size,
        help="User page size when --all-users is used. Defaults to 100.",
    )
    user_profile_parser.add_argument(
        "--force",
        action="store_true",
        help="Force profile recalculation. Recompute mode always upserts current vectors.",
    )
    add_runtime_args(user_profile_parser, settings=settings)
    add_common_connection_args(user_profile_parser)
    add_redis_connection_args(user_profile_parser)

    paper_parser = subparsers.add_parser(
        "index-papers",
        help="Index local PostgreSQL papers into Qdrant.",
    )
    paper_parser.add_argument("paper_ids", nargs="*", type=int)
    paper_parser.add_argument("--paper-ids", dest="paper_ids_csv", default=None)
    paper_parser.add_argument("--date-from", type=parse_iso_date, default=None)
    paper_parser.add_argument("--date-to", type=parse_iso_date, default=None)
    paper_parser.add_argument("--limit", type=int, default=None)
    paper_parser.add_argument("--offset", type=int, default=0)
    paper_parser.add_argument(
        "--batch-size",
        type=int,
        default=settings.operations.paper_indexing_batch_size,
    )
    paper_parser.add_argument("--force-reindex", action="store_true")
    add_runtime_args(paper_parser, settings=settings)
    add_common_connection_args(paper_parser)
    add_redis_connection_args(paper_parser)

    keyword_parser = subparsers.add_parser(
        "extract-keywords",
        help="Extract keyphrases into papers.extracted_keywords locally.",
    )
    keyword_parser.add_argument("paper_ids", nargs="*", type=int)
    keyword_parser.add_argument("--paper-ids", dest="paper_ids_csv", default=None)
    keyword_parser.add_argument("--date-from", type=parse_iso_date, default=None)
    keyword_parser.add_argument("--date-to", type=parse_iso_date, default=None)
    keyword_parser.add_argument("--topic-id", type=int, default=None)
    keyword_parser.add_argument("--field-id", type=int, default=None)
    keyword_parser.add_argument("--limit", type=int, default=None)
    keyword_parser.add_argument("--offset", type=int, default=0)
    keyword_parser.add_argument(
        "--batch-size",
        type=int,
        default=settings.operations.keyword_extraction_batch_size,
    )
    keyword_parser.add_argument(
        "--top-k",
        type=int,
        default=settings.operations.keyword_extraction_top_k,
    )
    keyword_parser.add_argument("--min-score", type=float, default=None)
    keyword_parser.add_argument(
        "--skip-processed",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    keyword_parser.add_argument("--skip-non-english", action="store_true")
    add_runtime_args(keyword_parser, settings=settings)
    add_common_connection_args(keyword_parser)
    add_redis_connection_args(keyword_parser)

    args = parser.parse_args(argv)
    args.settings = settings
    return args


def main(argv: list[str] | None = None) -> int:
    """Run the requested ML CLI command."""
    args = parse_args(argv)
    args.settings = load_settings(
        config_file=getattr(args, "config_file", None),
        env_file=args.env_file,
    )
    configure_logging(_settings(args), verbosity=getattr(args, "verbose", 0))
    log_event(logger, "cli_command_started", category="ml", command=args.command)

    try:
        if args.command == "index-research-entities":
            payload = run_index_research_entities(args)
        elif args.command == "recompute-trends":
            payload = run_recompute_trends(args)
        elif args.command == "recompute-cluster-dynamics":
            payload = run_recompute_cluster_dynamics(args)
        elif args.command == "generate-topic-report":
            payload = run_generate_topic_report(args)
        elif args.command == "recompute-user-profile":
            payload = run_recompute_user_profile(args)
        elif args.command == "index-papers":
            payload = run_index_papers(args)
        elif args.command == "extract-keywords":
            payload = run_extract_keywords(args)
        else:
            raise AssertionError(f"Unhandled command: {args.command}")
    except AppError as exc:
        log_event(
            logger,
            "cli_command_failed",
            level=logging.WARNING,
            category="ml",
            command=args.command,
            error_code=exc.code,
        )
        print_json(exc.to_dict(), stream=sys.stderr)
        return 1
    except Exception as exc:
        log_event(
            logger,
            "cli_command_failed",
            level=logging.ERROR,
            exc_info=True,
            category="ml",
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

    log_event(logger, "cli_command_completed", category="ml", command=args.command)
    print_json(payload)
    return 0


def run_index_research_entities(args: argparse.Namespace) -> dict[str, Any]:
    """Index research entities through ResearchEntityIndexingFacade."""
    database_url = args.database_url or _settings(args).database_url
    engine = create_db_engine(database_url, echo=False)
    SessionLocal = create_session_factory(engine, expire_on_commit=False)

    try:
        event_sink = build_event_sink(args)
        embedding_adapter = create_embedding_adapter(
            _settings(args),
            base_url=args.lmstudio_url,
        )
        qdrant_adapter = build_qdrant_adapter(args)

        with SessionLocal() as session:
            facade = ResearchEntityIndexingFacade(
                taxonomy_repository=TaxonomyRepository(session),
                paper_graph_repository=PaperGraphRepository(session),
                embedding_adapter=embedding_adapter,
                qdrant_adapter=qdrant_adapter,
                embedding_model=args.embedding_model
                or _settings(args).infrastructure.embedding_model,
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
    if args.cluster_workers <= 0:
        raise ValueError("--cluster-workers must be a positive integer.")

    database_url = args.database_url or _settings(args).database_url
    engine = create_db_engine(database_url, echo=False)
    SessionLocal = create_session_factory(engine, expire_on_commit=False)

    try:
        event_sink = build_event_sink(args)
        qdrant_adapter = build_qdrant_adapter(args)
        redis_adapter = RedisAdapter(build_redis_client(args))
        summary_facade = SummaryFacade(
            chat_adapter=create_chat_adapter(
                _settings(args),
                base_url=args.lmstudio_url,
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
                research_cluster_repository=ResearchClusterRepository(session),
                event_sink=event_sink,
            )
            if args.cluster_workers == 1:
                result = facade.recompute_all_clusters(
                    date_from=args.date_from,
                    date_to=args.date_to,
                    limit=args.limit,
                    force_summary=args.force_summary,
                    batch_size=args.batch_size,
                )
            else:
                topic_ids = list(
                    facade._iter_topic_ids(
                        args.date_from,
                        args.date_to,
                        args.limit,
                        args.batch_size,
                    )
                )
                result = recompute_trends_parallel(
                    topic_ids,
                    args=args,
                    session_factory=SessionLocal,
                    redis_adapter=redis_adapter,
                    event_sink=event_sink,
                )
            session.commit()
        return {
            "command": "recompute-trends",
            "date_from": args.date_from,
            "date_to": args.date_to,
            "cluster_workers": args.cluster_workers,
            "result": result.model_dump(mode="json"),
        }
    finally:
        engine.dispose()


def run_recompute_cluster_dynamics(args: argparse.Namespace) -> dict[str, Any]:
    """Recompute time dynamics for one or many trend clusters."""
    database_url = args.database_url or _settings(args).database_url
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
                research_cluster_repository=ResearchClusterRepository(session),
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
            session.commit()
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


def run_generate_topic_report(args: argparse.Namespace) -> dict[str, Any]:
    """Generate quarterly reports for one topic through TopicQuarterReportPipeline."""
    if args.topic_id <= 0:
        raise ValueError("--topic-id must be a positive integer.")
    quarter_service = QuarterPeriodService()
    periods = quarter_service.quarter_periods(args.date_from, args.date_to)
    requests = [
        TopicQuarterReportGenerateRequestDTO(
            topic_id=args.topic_id,
            period_start=period.date_from,
            period_end=period.date_to,
            force=bool(args.force),
            report_language=args.report_language,
        )
        for period in periods
    ]

    database_url = args.database_url or _settings(args).database_url
    engine = create_db_engine(database_url, echo=False)
    SessionLocal = create_session_factory(engine, expire_on_commit=False)

    try:
        redis_adapter = (
            RedisAdapter(build_redis_client(args)) if args.event_redis else None
        )
        event_sink = build_event_sink(args, redis_adapter=redis_adapter)
        chat_adapter = create_chat_adapter(
            _settings(args),
            base_url=args.lmstudio_url,
        )

        with SessionLocal() as session:
            pipeline = TopicQuarterReportPipeline(
                TopicQuarterReportFacade(
                    taxonomy_repository=TaxonomyRepository(session),
                    paper_repository=PaperRepository(session),
                    research_cluster_repository=ResearchClusterRepository(session),
                    topic_report_repository=TopicQuarterReportRepository(session),
                    openalex_topic_stats_repository=OpenAlexTopicStatsRepository(
                        session
                    ),
                    chat_adapter=chat_adapter,
                    event_sink=event_sink,
                )
            )
            result = pipeline.generate_many(
                requests,
                show_progress=not args.no_progress,
            )
            session.commit()

        return {
            "command": "generate-topic-report",
            "topic_id": args.topic_id,
            "date_from": args.date_from,
            "date_to": args.date_to,
            "periods": [period.key for period in periods],
            "force": bool(args.force),
            "report_language": args.report_language,
            "result": result.model_dump(mode="json"),
        }
    finally:
        engine.dispose()


def run_recompute_user_profile(args: argparse.Namespace) -> dict[str, Any]:
    """Recompute user vector profiles through UserProfileFacade."""
    if args.user_id is not None and args.user_id <= 0:
        raise ValueError("--user-id must be a positive integer.")
    if args.batch_size <= 0:
        raise ValueError("--batch-size must be a positive integer.")

    database_url = args.database_url or _settings(args).database_url
    engine = create_db_engine(database_url, echo=False)
    SessionLocal = create_session_factory(engine, expire_on_commit=False)

    try:
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


def run_index_papers(args: argparse.Namespace) -> dict[str, Any]:
    """Index local papers through PaperIndexingPipeline."""
    paper_ids = _resolve_cli_paper_ids(args)
    if paper_ids and (args.date_from is not None or args.date_to is not None):
        raise ValueError("Use either paper ids or a date range, not both.")
    if not paper_ids and args.date_from is None and args.date_to is None:
        raise ValueError("Provide paper ids or at least one date boundary.")
    if args.date_from is not None and args.date_to is not None and args.date_from > args.date_to:
        raise ValueError("--date-from must be before or equal to --date-to.")

    engine = create_db_engine(args.database_url or _settings(args).database_url, echo=False)
    SessionLocal = create_session_factory(engine, expire_on_commit=False)
    try:
        redis_adapter = RedisAdapter(build_redis_client(args))
        event_sink = build_event_sink(args, redis_adapter=redis_adapter)
        with SessionLocal() as session:
            pipeline = PaperIndexingPipeline(
                PaperIndexingFacade(
                    paper_repository=PaperRepository(session),
                    taxonomy_repository=TaxonomyRepository(session),
                    author_repository=AuthorRepository(session),
                    institution_repository=InstitutionRepository(session),
                    embedding_adapter=create_embedding_adapter(
                        _settings(args),
                        base_url=args.lmstudio_url,
                    ),
                    qdrant_adapter=build_qdrant_adapter(args),
                    redis_adapter=redis_adapter,
                    embedding_model=args.embedding_model
                    or _settings(args).infrastructure.embedding_model,
                    event_sink=event_sink,
                )
            )
            result = pipeline.facade.index_batch(
                PaperBatchIndexingRequestDTO(
                    paper_ids=paper_ids,
                    date_from=args.date_from,
                    date_to=args.date_to,
                    limit=args.limit,
                    offset=args.offset,
                    batch_size=args.batch_size,
                    force_reindex=bool(args.force_reindex),
                )
            )
            session.commit()
        return {"command": "index-papers", "result": result.model_dump(mode="json")}
    finally:
        engine.dispose()


def run_extract_keywords(args: argparse.Namespace) -> dict[str, Any]:
    """Extract paper keywords through KeywordExtractionPipeline."""
    paper_ids = _resolve_cli_paper_ids(args)
    has_filters = any(
        value is not None
        for value in (args.date_from, args.date_to, args.topic_id, args.field_id)
    )
    if paper_ids and has_filters:
        raise ValueError("Use either paper ids or filter mode, not both.")
    if not paper_ids and not has_filters:
        raise ValueError(
            "Provide paper ids or at least one of --date-from/--date-to/--topic-id/--field-id."
        )
    if args.date_from is not None and args.date_to is not None and args.date_from > args.date_to:
        raise ValueError("--date-from must be before or equal to --date-to.")

    engine = create_db_engine(args.database_url or _settings(args).database_url, echo=False)
    SessionLocal = create_session_factory(engine, expire_on_commit=False)
    try:
        event_sink = build_event_sink(args)
        with SessionLocal() as session:
            pipeline = KeywordExtractionPipeline(
                KeywordExtractionFacade(
                    paper_repository=PaperRepository(session),
                    embedding_adapter=create_embedding_adapter(
                        _settings(args),
                        base_url=args.lmstudio_url,
                    ),
                    embedding_model=args.embedding_model
                    or _settings(args).infrastructure.embedding_model,
                    event_sink=event_sink,
                )
            )
            result = pipeline.run_papers(
                PaperKeywordExtractionBatchRequestDTO(
                    paper_ids=paper_ids,
                    date_from=args.date_from,
                    date_to=args.date_to,
                    topic_id=args.topic_id,
                    field_id=args.field_id,
                    limit=args.limit,
                    offset=args.offset,
                    batch_size=args.batch_size,
                    top_k=args.top_k,
                    min_score=args.min_score,
                    skip_processed=bool(args.skip_processed),
                    skip_non_english=bool(args.skip_non_english),
                )
            )
            session.commit()
        return {"command": "extract-keywords", "result": result.model_dump(mode="json")}
    finally:
        engine.dispose()


def _resolve_cli_paper_ids(args: argparse.Namespace) -> list[int]:
    paper_ids = list(args.paper_ids)
    if args.paper_ids_csv:
        paper_ids.extend(
            int(value.strip())
            for value in args.paper_ids_csv.split(",")
            if value.strip()
        )
    return list(dict.fromkeys(paper_ids))


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
        chat_adapter=create_chat_adapter(
            _settings(args),
            base_url=args.lmstudio_url,
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


def recompute_trends_parallel(
    topic_ids: list[int],
    *,
    args: argparse.Namespace,
    session_factory: Any,
    redis_adapter: RedisAdapter,
    event_sink: EventSink,
) -> Any:
    """Recompute topic clusters concurrently with one DB session per task."""
    from dto.common import BatchOperationResultDTO

    result = BatchOperationResultDTO(total=len(topic_ids))
    if not topic_ids:
        return result

    def run_one(topic_id: int) -> tuple[int, bool, dict[str, Any] | None]:
        with session_factory() as session:
            facade = ClusterAnalyticsFacade(
                taxonomy_repository=TaxonomyRepository(session),
                paper_repository=PaperRepository(session),
                paper_graph_repository=PaperGraphRepository(session),
                qdrant_adapter=build_qdrant_adapter(args),
                redis_adapter=redis_adapter,
                summary_facade=SummaryFacade(
                    chat_adapter=create_chat_adapter(
                        _settings(args),
                        base_url=args.lmstudio_url,
                    )
                ),
                research_cluster_repository=ResearchClusterRepository(session),
                event_sink=event_sink,
            )
            try:
                facade.recompute_cluster(
                    f"topic:{topic_id}",
                    force_summary=args.force_summary,
                )
            except AppError as exc:
                session.rollback()
                details = exc.details or {}
                if details.get("reason") in {
                    "insufficient_indexed_papers",
                    "no_indexed_vectors",
                }:
                    return topic_id, False, None
                return (
                    topic_id,
                    False,
                    {
                        "cluster_id": f"topic:{topic_id}",
                        "topic_id": topic_id,
                        "code": exc.code,
                        "message": exc.message,
                        "details": details,
                    },
                )
            session.commit()
            return topic_id, True, None

    with ThreadPoolExecutor(max_workers=max(1, args.cluster_workers)) as executor:
        futures = [executor.submit(run_one, topic_id) for topic_id in topic_ids]
        for future in as_completed(futures):
            topic_id, updated, error = future.result()
            if updated:
                result.updated += 1
            elif error is None:
                result.skipped += 1
            else:
                result.failed += 1
                result.errors.append(error)

    return result


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
        "--config-file",
        default=None,
        help="Optional TOML override file. Defaults to ML_CONFIG_FILE when set.",
    )
    parser.add_argument(
        "--env-file",
        default=None,
        help="Optional .env path. Defaults to ML_ENV_FILE or project .env.",
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


def add_runtime_args(
    parser: argparse.ArgumentParser,
    *,
    settings: Settings,
) -> None:
    """Add shared progress and event arguments for local execution."""
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
        default=settings.operations.event_ttl_seconds,
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
    """Build QdrantAdapter from settings and CLI overrides."""
    return create_qdrant_adapter(
        _settings(args),
        url=args.qdrant_url,
        host=args.qdrant_host,
        port=args.qdrant_port,
        api_key=args.qdrant_api_key,
    )


def build_redis_client(args: argparse.Namespace) -> Any:
    """Build a redis-py client from settings and CLI overrides."""
    return create_redis_client(
        _settings(args),
        url=args.redis_url,
        host=args.redis_host,
        port=args.redis_port,
        db=args.redis_db,
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
