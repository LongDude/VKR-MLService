from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
import time
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
    OpenAlexAdapter,
    QdrantAdapter,
    RedisAdapter,
)
from core.config import Settings
from core.exceptions import AppError
from dto.openalex import OpenAlexBootstrapRequestDTO, OpenAlexBootstrapTopicTargetDTO
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
from ml.facades.openalex_papers import OpenAlexPapersFacade
from ml.pipelines.cluster_dynamics_pipeline import ClusterDynamicsPipeline
from ml.pipelines.keyword_extraction_pipeline import KeywordExtractionPipeline
from ml.pipelines.openalex_paper_loading_pipeline import OpenAlexPaperLoadingPipeline
from ml.pipelines.paper_indexing_pipeline import PaperIndexingPipeline
from ml.pipelines.research_entities_pipeline import ResearchEntitiesPipeline
from ml.pipelines.topic_quarter_report_pipeline import TopicQuarterReportPipeline
from ml.pipelines.trend_recompute_pipeline import TrendRecomputePipeline
from ml.pipelines.user_profile_pipeline import UserProfilePipeline
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
    DEFAULT_QUEUE_ORDER,
    ENTITY_INDEXING_QUEUE,
    KEYWORD_EXTRACTION_QUEUE,
    OPENALEX_BOOTSTRAP_PAPERS_QUEUE,
    OPENALEX_TOPIC_STATS_QUEUE,
    PAPER_INDEXING_QUEUE,
    TOPIC_QUARTER_REPORT_QUEUE,
    USER_PROFILE_RECOMPUTE_QUEUE,
    RedisMLWorker,
)
from ml.workers.task_handlers import MLTaskHandler
from ingestion.openalex_topic_stats import OpenAlexTopicStatsCollector, SyncRateLimiter
from ml.services.openalex_paper_downloader import OpenAlexPaperDownloader
from ml.services.openalex_paper_importer import OpenAlexPaperImporter
from ml.services.openalex_paper_plan import OpenAlexPaperPlanService
from ml.services.openalex_rate_limiter import AsyncRateLimiter
from models.session import create_db_engine, create_session_factory
from repositories import (
    AuthorRepository,
    FavouriteRepository,
    InstitutionRepository,
    OpenAlexTopicStatsRepository,
    OpenAlexYearlyTopicStatsRepository,
    PaperGraphRepository,
    PaperRepository,
    ResearchClusterRepository,
    TaxonomyRepository,
    TopicQuarterReportRepository,
    TrackedAreaRepository,
)


QUEUE_ALIASES = {
    "openalex_topic_stats": OPENALEX_TOPIC_STATS_QUEUE,
    "collect_topic_stats": OPENALEX_TOPIC_STATS_QUEUE,
    "collect-topic-stats": OPENALEX_TOPIC_STATS_QUEUE,
    "openalex_bootstrap_papers": OPENALEX_BOOTSTRAP_PAPERS_QUEUE,
    "bootstrap_papers": OPENALEX_BOOTSTRAP_PAPERS_QUEUE,
    "bootstrap-papers": OPENALEX_BOOTSTRAP_PAPERS_QUEUE,
    "keyword_extraction": KEYWORD_EXTRACTION_QUEUE,
    "paper_indexing": PAPER_INDEXING_QUEUE,
    "entity_indexing": ENTITY_INDEXING_QUEUE,
    "research_entities_indexing": ENTITY_INDEXING_QUEUE,
    "cluster_recompute": CLUSTER_RECOMPUTE_QUEUE,
    "cluster_dynamics_recompute": CLUSTER_DYNAMICS_RECOMPUTE_QUEUE,
    "topic_quarter_reports": TOPIC_QUARTER_REPORT_QUEUE,
    "topic_quarter_report": TOPIC_QUARTER_REPORT_QUEUE,
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
        "-v",
        "--verbose",
        action="count",
        default=0,
        help=(
            "Increase logging verbosity. Default logs task starts/completions; "
            "-v also shows HTTP request logs; -vv enables verbose dependency logs."
        ),
    )
    run_parser.add_argument(
        "--no-progress",
        action="store_true",
        help="Disable tqdm progress bars for worker tasks.",
    )
    run_parser.add_argument(
        "--event-redis",
        action="store_true",
        help="Write latest task status events to Redis.",
    )
    run_parser.add_argument(
        "--event-ttl-seconds",
        type=int,
        default=24 * 60 * 60,
        help="TTL for Redis event status keys. Defaults to 86400.",
    )
    run_parser.add_argument(
        "--keyword-extraction-batch-size",
        type=int,
        default=128,
        help=(
            "Maximum queue:keyword_extraction messages to combine into one "
            "KeywordExtractionPipeline.run_papers call. Defaults to 128."
        ),
    )
    run_parser.add_argument(
        "--max-keyword-task-size",
        type=int,
        default=None,
        help=(
            "Maximum paper_ids per keyword_extraction handler call. Defaults to "
            "--keyword-extraction-batch-size."
        ),
    )
    run_parser.add_argument(
        "--paper-indexing-batch-size",
        type=int,
        default=128,
        help=(
            "Maximum queue:paper_indexing messages to combine into one "
            "PaperIndexingPipeline.run_many call. Defaults to 128."
        ),
    )
    run_parser.add_argument(
        "--max-paper-task-size",
        type=int,
        default=None,
        help=(
            "Maximum paper_ids per paper_indexing handler call. Defaults to "
            "--paper-indexing-batch-size."
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
        "--cluster-recompute-workers",
        type=int,
        default=1,
        help="Parallel workers inside one cluster recompute batch. Defaults to 1.",
    )
    run_parser.add_argument(
        "--max-cluster-task-size",
        type=int,
        default=None,
        help=(
            "Maximum topic_ids per cluster recompute handler call. Defaults to "
            "--cluster-recompute-batch-size."
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
    configure_logging(getattr(args, "verbose", 0))

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
    if args.keyword_extraction_batch_size <= 0:
        raise ValueError("--keyword-extraction-batch-size must be a positive integer.")
    if args.max_keyword_task_size is not None and args.max_keyword_task_size <= 0:
        raise ValueError("--max-keyword-task-size must be a positive integer.")
    if args.paper_indexing_batch_size <= 0:
        raise ValueError("--paper-indexing-batch-size must be a positive integer.")
    if args.max_paper_task_size is not None and args.max_paper_task_size <= 0:
        raise ValueError("--max-paper-task-size must be a positive integer.")
    if args.cluster_recompute_batch_size <= 0:
        raise ValueError("--cluster-recompute-batch-size must be a positive integer.")
    if args.cluster_recompute_workers <= 0:
        raise ValueError("--cluster-recompute-workers must be a positive integer.")
    if args.max_cluster_task_size is not None and args.max_cluster_task_size <= 0:
        raise ValueError("--max-cluster-task-size must be a positive integer.")
    if args.event_ttl_seconds <= 0:
        raise ValueError("--event-ttl-seconds must be a positive integer.")
    max_keyword_task_size = args.max_keyword_task_size or args.keyword_extraction_batch_size
    max_paper_task_size = args.max_paper_task_size or args.paper_indexing_batch_size
    max_cluster_task_size = (
        args.max_cluster_task_size or args.cluster_recompute_batch_size
    )

    queue_names = parse_queues(args.queues)
    database_url = args.database_url or Settings.from_env().database_url
    engine = create_db_engine(database_url, echo=False)
    SessionLocal = create_session_factory(engine, expire_on_commit=False)

    processed = 0
    try:
        redis_adapter = RedisAdapter(build_redis_client(args))
        event_sink = build_event_sink(args, redis_adapter=redis_adapter)
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
                session_factory=SessionLocal,
                session=session,
                redis_adapter=redis_adapter,
                qdrant_adapter=qdrant_adapter,
                embedding_adapter=embedding_adapter,
                chat_adapter=chat_adapter,
                embedding_model=args.embedding_model
                or os.getenv("EMBEDDING_MODEL")
                or DEFAULT_EMBEDDING_MODEL,
                event_sink=event_sink,
                cluster_recompute_workers=args.cluster_recompute_workers,
                cluster_recompute_pipeline_factory=(
                    build_cluster_recompute_pipeline_factory(
                        args=args,
                        session_factory=SessionLocal,
                        redis_adapter=redis_adapter,
                        event_sink=event_sink,
                    )
                    if args.cluster_recompute_workers > 1
                    else None
                ),
            )
            worker = RedisMLWorker(
                redis_adapter=redis_adapter,
                task_handler=handler,
                queues=queue_names,
                batch_sizes={
                    OPENALEX_BOOTSTRAP_PAPERS_QUEUE: 1,
                    KEYWORD_EXTRACTION_QUEUE: args.keyword_extraction_batch_size,
                    PAPER_INDEXING_QUEUE: args.paper_indexing_batch_size,
                    CLUSTER_RECOMPUTE_QUEUE: args.cluster_recompute_batch_size,
                },
                max_task_sizes={
                    OPENALEX_BOOTSTRAP_PAPERS_QUEUE: 1,
                    KEYWORD_EXTRACTION_QUEUE: max_keyword_task_size,
                    PAPER_INDEXING_QUEUE: max_paper_task_size,
                    CLUSTER_RECOMPUTE_QUEUE: max_cluster_task_size,
                },
                idle_sleep_seconds=args.idle_sleep,
                show_progress=not args.no_progress,
                event_sink=event_sink,
            )
            logging.getLogger(__name__).info(
                "Starting Redis ML worker queues=%s max_tasks=%s progress=%s verbosity=%s",
                ",".join(queue_names),
                args.max_tasks,
                not args.no_progress,
                args.verbose,
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
                OPENALEX_TOPIC_STATS_QUEUE: 1,
                OPENALEX_BOOTSTRAP_PAPERS_QUEUE: 1,
                KEYWORD_EXTRACTION_QUEUE: args.keyword_extraction_batch_size,
                PAPER_INDEXING_QUEUE: args.paper_indexing_batch_size,
                CLUSTER_RECOMPUTE_QUEUE: args.cluster_recompute_batch_size,
            },
            "max_task_sizes": {
                OPENALEX_TOPIC_STATS_QUEUE: 1,
                OPENALEX_BOOTSTRAP_PAPERS_QUEUE: 1,
                KEYWORD_EXTRACTION_QUEUE: max_keyword_task_size,
                PAPER_INDEXING_QUEUE: max_paper_task_size,
                CLUSTER_RECOMPUTE_QUEUE: max_cluster_task_size,
            },
            "progress": not args.no_progress,
            "event_redis": bool(args.event_redis),
            "verbosity": args.verbose,
            "processed": processed,
            "cluster_recompute_workers": args.cluster_recompute_workers,
        }
    finally:
        engine.dispose()


def build_task_handler(
    *,
    session_factory: Any,
    session: Any,
    redis_adapter: RedisAdapter,
    qdrant_adapter: QdrantAdapter,
    embedding_adapter: LMStudioEmbeddingAdapter,
    chat_adapter: LMStudioChatAdapter,
    embedding_model: str,
    event_sink: EventSink,
    cluster_recompute_workers: int = 1,
    cluster_recompute_pipeline_factory: Any | None = None,
) -> MLTaskHandler:
    """Build task handler with all ML pipelines configured."""
    taxonomy_repository = TaxonomyRepository(session)
    paper_repository = PaperRepository(session)
    paper_graph_repository = PaperGraphRepository(session)
    research_cluster_repository = ResearchClusterRepository(session)
    summary_facade = SummaryFacade(chat_adapter=chat_adapter)

    keyword_extraction_pipeline = KeywordExtractionPipeline(
        KeywordExtractionFacade(
            paper_repository=paper_repository,
            embedding_adapter=embedding_adapter,
            embedding_model=embedding_model,
            event_sink=event_sink,
        )
    )

    paper_indexing_pipeline = PaperIndexingPipeline(
        PaperIndexingFacade(
            paper_repository=paper_repository,
            taxonomy_repository=taxonomy_repository,
            author_repository=AuthorRepository(session),
            institution_repository=InstitutionRepository(session),
            embedding_adapter=embedding_adapter,
            qdrant_adapter=qdrant_adapter,
            redis_adapter=redis_adapter,
            embedding_model=embedding_model,
            event_sink=event_sink,
        )
    )
    research_entities_pipeline = ResearchEntitiesPipeline(
        ResearchEntityIndexingFacade(
            taxonomy_repository=taxonomy_repository,
            paper_graph_repository=paper_graph_repository,
            embedding_adapter=embedding_adapter,
            qdrant_adapter=qdrant_adapter,
            embedding_model=embedding_model,
            event_sink=event_sink,
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
            research_cluster_repository=research_cluster_repository,
            event_sink=event_sink,
        )
    )
    cluster_dynamics_pipeline = ClusterDynamicsPipeline(
        ClusterDynamicsFacade(
            taxonomy_repository=taxonomy_repository,
            paper_graph_repository=paper_graph_repository,
            qdrant_adapter=qdrant_adapter,
            research_cluster_repository=research_cluster_repository,
            event_sink=event_sink,
        )
    )
    topic_quarter_report_pipeline = TopicQuarterReportPipeline(
        TopicQuarterReportFacade(
            taxonomy_repository=taxonomy_repository,
            paper_repository=paper_repository,
            research_cluster_repository=research_cluster_repository,
            topic_report_repository=TopicQuarterReportRepository(session),
            openalex_topic_stats_repository=OpenAlexTopicStatsRepository(session),
            chat_adapter=chat_adapter,
            event_sink=event_sink,
        )
    )
    user_profile_pipeline = UserProfilePipeline(
        UserProfileFacade(
            favourite_repository=FavouriteRepository(session),
            tracked_area_repository=TrackedAreaRepository(session),
            taxonomy_repository=taxonomy_repository,
            qdrant_adapter=qdrant_adapter,
            event_sink=event_sink,
        )
    )

    return MLTaskHandler(
        session=session,
        keyword_extraction_pipeline=keyword_extraction_pipeline,
        paper_indexing_pipeline=paper_indexing_pipeline,
        research_entities_pipeline=research_entities_pipeline,
        trend_recompute_pipeline=trend_recompute_pipeline,
        cluster_dynamics_pipeline=cluster_dynamics_pipeline,
        topic_quarter_report_pipeline=topic_quarter_report_pipeline,
        user_profile_pipeline=user_profile_pipeline,
        openalex_topic_stats_collector_factory=build_openalex_topic_stats_collector_factory(
            session,
        ),
        openalex_paper_bootstrap_runner=build_openalex_paper_bootstrap_runner(
            session_factory=session_factory,
            redis_adapter=redis_adapter,
        ),
        event_sink=event_sink,
        cluster_recompute_workers=cluster_recompute_workers,
        cluster_recompute_pipeline_factory=cluster_recompute_pipeline_factory,
    )


def build_openalex_topic_stats_collector_factory(session: Any) -> Any:
    """Build OpenAlex topic stats collectors from worker task messages."""

    def _message_bool(message: dict[str, Any], field: str, default: bool) -> bool:
        value = message.get(field, default)
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            lowered = value.strip().lower()
            if lowered in {"1", "true", "yes", "y", "on"}:
                return True
            if lowered in {"0", "false", "no", "n", "off"}:
                return False
        return bool(value)

    def factory(message: dict[str, Any]) -> OpenAlexTopicStatsCollector:
        openalex_url = (
            str(message.get("openalex_url")).strip()
            if message.get("openalex_url")
            else os.getenv("OPENALEX_BASE_URL") or "https://api.openalex.org"
        )
        api_key = (
            str(message.get("openalex_api_key")).strip()
            if message.get("openalex_api_key")
            else os.getenv("OPENALEX_API_KEY")
        )
        mailto = (
            str(message.get("openalex_mailto")).strip()
            if message.get("openalex_mailto")
            else os.getenv("OPENALEX_MAILTO")
        )
        return OpenAlexTopicStatsCollector(
            taxonomy_repository=TaxonomyRepository(session),
            stats_repository=OpenAlexTopicStatsRepository(session),
            yearly_stats_repository=OpenAlexYearlyTopicStatsRepository(session),
            openalex_adapter_factory=lambda: OpenAlexAdapter(
                openalex_url,
                api_key=api_key or None,
                mailto=mailto or None,
            ),
            request_workers=max(1, int(message.get("request_workers") or 8)),
            rate_limiter=SyncRateLimiter(float(message.get("rate_limit_rps") or 10.0)),
            max_retries=max(0, int(message.get("max_retries") or 5)),
            primary_topic_only=_message_bool(message, "primary_topic_only", True),
        )

    return factory


def build_openalex_paper_bootstrap_runner(
    *,
    session_factory: Any,
    redis_adapter: RedisAdapter,
) -> Any:
    """Build a callable that runs OpenAlex paper bootstrap from worker messages."""

    def runner(message: dict[str, Any]) -> Any:
        request = build_openalex_bootstrap_request(message, session_factory)
        pipeline = OpenAlexPaperLoadingPipeline(
            OpenAlexPapersFacade(
                session_factory=session_factory,
                plan_service=OpenAlexPaperPlanService(),
                downloader=OpenAlexPaperDownloader(
                    base_url=str(
                        message.get("openalex_url")
                        or os.getenv("OPENALEX_BASE_URL")
                        or "https://api.openalex.org"
                    ),
                    request_workers=request.request_workers,
                    rate_limiter=AsyncRateLimiter(request.rate_limit_rps),
                    max_retries=request.max_retries,
                    api_key=(
                        str(message.get("openalex_api_key")).strip()
                        if message.get("openalex_api_key")
                        else os.getenv("OPENALEX_API_KEY") or None
                    ),
                    mailto=(
                        str(message.get("openalex_mailto")).strip()
                        if message.get("openalex_mailto")
                        else os.getenv("OPENALEX_MAILTO") or None
                    ),
                    rate_limit_defer_after_seconds=float(
                        message.get("rate_limit_defer_after_seconds") or 120.0
                    ),
                ),
                importer=OpenAlexPaperImporter(
                    session_factory=session_factory,
                    batch_size=request.batch_size,
                ),
                redis_adapter=redis_adapter,
                pending_redis_key=str(
                    message.get("pending_redis_key")
                    or "queue:openalex_bootstrap_papers_pending"
                ),
            )
        )
        return asyncio.run(pipeline.bootstrap_papers(request))

    return runner


def build_openalex_bootstrap_request(
    message: dict[str, Any],
    session_factory: Any,
) -> OpenAlexBootstrapRequestDTO:
    topic_ids = _int_list_message(message, "topic_ids")
    field_ids = _optional_int_list_message(message, "field_ids")
    if not field_ids and message.get("field_id") is not None:
        field_ids = [int(message["field_id"])]
    subfield_ids = _optional_int_list_message(message, "subfield_ids")

    topic_targets = (
        build_openalex_topic_targets(
            session_factory,
            topic_ids=topic_ids,
            primary_topic_only=_bool_message(message, "primary_topic_only", True),
        )
        if topic_ids
        else []
    )
    target_unit = str(message.get("target_unit") or ("topic" if topic_targets else "aggregate"))
    if target_unit not in {"aggregate", "topic"}:
        raise ValueError("bootstrap_papers target_unit must be aggregate or topic.")

    return OpenAlexBootstrapRequestDTO(
        target_count=int(message.get("target_count") or 0),
        target_count_scope=str(message.get("target_scope") or "month"),
        target_count_unit=target_unit,
        topic_targets=topic_targets,
        date_from=_date_message(message, "date_from"),
        date_to=_date_message(message, "date_to"),
        sample=True,
        normalize="none",
        monthly_stats_source=str(message.get("stats_source") or "redis"),
        monthly_counts_redis_key=message.get("stats_redis_key"),
        monthly_counts_csv=message.get("monthly_counts_csv"),
        missing_stats_policy=str(message.get("missing_stats_policy") or "error"),
        languages=_str_list_message(message, "languages", ["en", "ru"]),
        types=_str_list_message(message, "types", ["article"]),
        openalex_filter_parts=_str_list_message(message, "openalex_filter_parts", []),
        local_field_ids=field_ids or [],
        local_subfield_ids=subfield_ids or [],
        batch_size=int(message.get("batch_size") or 500),
        request_workers=int(message.get("request_workers") or 8),
        db_workers=int(message.get("db_workers") or 2),
        rate_limit_rps=float(message.get("rate_limit_rps") or 70.0),
        seed=int(message.get("seed") or 42),
        max_rounds=1,
        per_page=int(message.get("per_page") or 100),
        max_retries=int(message.get("max_retries") or 5),
        skip_existing=_bool_message(message, "skip_existing", False),
        enqueue_indexing=_bool_message(message, "enqueue_indexing", False),
        dry_run=_bool_message(message, "dry_run", False),
        show_progress=_bool_message(message, "show_progress", False),
    )


def build_openalex_topic_targets(
    session_factory: Any,
    *,
    topic_ids: list[int],
    primary_topic_only: bool,
) -> list[OpenAlexBootstrapTopicTargetDTO]:
    prefix = "primary_topic" if primary_topic_only else "topics"
    with session_factory() as session:
        topics = TaxonomyRepository(session).list_topics_by_ids(topic_ids)
    missing = sorted(set(topic_ids) - {int(topic.id) for topic in topics})
    if missing:
        raise ValueError(f"Cannot build OpenAlex topic filters: missing_topic_ids={missing}")
    without_openalex = [int(topic.id) for topic in topics if not topic.openalex_id]
    if without_openalex:
        raise ValueError(
            "Cannot build OpenAlex topic filters: "
            f"topics_without_openalex_id={without_openalex}"
        )
    return [
        OpenAlexBootstrapTopicTargetDTO(
            topic_id=int(topic.id),
            filter_part=(
                f"{prefix}.id:{normalize_openalex_filter_id(str(topic.openalex_id))}"
            ),
        )
        for topic in topics
    ]


def normalize_openalex_filter_id(value: str) -> str:
    return value.strip().rstrip("/").rsplit("/", 1)[-1]


def _date_message(message: dict[str, Any], field: str) -> date:
    value = message.get(field)
    if isinstance(value, date):
        return value
    if value is None:
        raise ValueError(f"bootstrap_papers {field} is required.")
    return date.fromisoformat(str(value)[:10])


def _int_list_message(message: dict[str, Any], field: str) -> list[int]:
    values = _optional_int_list_message(message, field)
    if not values:
        return []
    return values


def _optional_int_list_message(message: dict[str, Any], field: str) -> list[int] | None:
    value = message.get(field)
    if value is None:
        return None
    if isinstance(value, str):
        return [int(item.strip()) for item in value.split(",") if item.strip()]
    if isinstance(value, (list, tuple, set)):
        return [int(item) for item in value]
    return [int(value)]


def _str_list_message(
    message: dict[str, Any],
    field: str,
    default: list[str],
) -> list[str]:
    value = message.get(field)
    if value is None:
        return list(default)
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    if isinstance(value, (list, tuple, set)):
        return [str(item).strip() for item in value if str(item).strip()]
    return [str(value).strip()]


def _bool_message(message: dict[str, Any], field: str, default: bool) -> bool:
    value = message.get(field, default)
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"1", "true", "yes", "y", "on"}:
            return True
        if lowered in {"0", "false", "no", "n", "off"}:
            return False
    return bool(value)


def build_cluster_recompute_pipeline_factory(
    *,
    args: argparse.Namespace,
    session_factory: Any,
    redis_adapter: RedisAdapter,
    event_sink: EventSink,
) -> Any:
    """Build isolated cluster recompute pipelines for parallel worker batches."""

    def factory() -> tuple[TrendRecomputePipeline, Any]:
        session = session_factory()
        chat_adapter = LMStudioChatAdapter(
            base_url=args.lmstudio_url
            or os.getenv("LMSTUDIO_BASE_URL")
            or "http://localhost:1234",
        )
        pipeline = TrendRecomputePipeline(
            ClusterAnalyticsFacade(
                taxonomy_repository=TaxonomyRepository(session),
                paper_repository=PaperRepository(session),
                paper_graph_repository=PaperGraphRepository(session),
                qdrant_adapter=build_qdrant_adapter(args),
                redis_adapter=redis_adapter,
                summary_facade=SummaryFacade(chat_adapter=chat_adapter),
                research_cluster_repository=ResearchClusterRepository(session),
                event_sink=event_sink,
            )
        )
        return pipeline, session

    return factory


def run_limited_worker(
    worker: RedisMLWorker,
    *,
    max_tasks: int,
    idle_sleep_seconds: float,
) -> int:
    """Run worker until max_tasks messages have been handled."""
    processed = 0
    worker.event_sink.emit(
        MLEvent(
            event_type="worker_queue_started",
            task_type="worker",
            entity_id="queue",
            stage="queue",
            current=0,
            total=max_tasks,
            message="Worker queue processing started",
        )
    )
    while processed < max_tasks:
        handled = worker.run_once(max_messages=max_tasks - processed)
        if handled:
            processed += worker.last_processed_message_count
            worker.event_sink.emit(
                MLEvent(
                    event_type="worker_queue_progress",
                    task_type="worker",
                    entity_id="queue",
                    stage="queue",
                    current=processed,
                    total=max_tasks,
                    message=f"Processed {processed} worker messages",
                )
            )
        else:
            time.sleep(idle_sleep_seconds)
    worker.event_sink.emit(
        MLEvent(
            event_type="worker_queue_completed",
            task_type="worker",
            entity_id="queue",
            stage="queue",
            current=processed,
            total=max_tasks,
            message="Worker queue processing completed",
        )
    )
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


def build_event_sink(
    args: argparse.Namespace,
    *,
    redis_adapter: RedisAdapter,
) -> EventSink:
    """Build the worker event sink chain from CLI flags."""
    sinks: list[EventSink] = [
        LoggingEventSink(logging.getLogger("ml.worker.events"), verbosity=args.verbose)
    ]
    if not args.no_progress:
        sinks.append(TqdmEventSink())
    if args.event_redis:
        sinks.append(
            RedisEventSink(
                redis_adapter,
                ttl_seconds=max(1, int(args.event_ttl_seconds)),
            )
        )
    if not sinks:
        return NoopEventSink()
    return CompositeEventSink(sinks, logger=logging.getLogger("ml.worker.events"))


def _optional_int_env(name: str) -> int | None:
    value = os.getenv(name)
    if value is None or not value.strip():
        return None
    return int(value)


def configure_logging(verbosity: int = 0) -> None:
    """Configure basic worker logging."""
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


def print_json(payload: dict[str, Any], *, stream: Any = sys.stdout) -> None:
    """Print JSON with stable UTF-8 output."""
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str), file=stream)


if __name__ == "__main__":
    raise SystemExit(main())
