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

from adapters import LMStudioEmbeddingAdapter, RedisAdapter
from core.config import Settings
from core.exceptions import AppError, InvalidRequestError
from dto.keywords import PaperKeywordExtractionBatchRequestDTO
from ml.constants import DEFAULT_EMBEDDING_MODEL
from ml.facades import KeywordExtractionFacade
from ml.pipelines.keyword_extraction_pipeline import KeywordExtractionPipeline
from ml.workers.redis_worker import KEYWORD_EXTRACTION_QUEUE
from models.session import create_db_engine, create_session_factory
from repositories import PaperRepository

DEFAULT_ENQUEUE_PAGE_SIZE = 1000


class KeywordExtractionTaskEnqueuer:
    def __init__(
        self,
        *,
        paper_repository: PaperRepository,
        redis_adapter: RedisAdapter,
        queue_name: str = KEYWORD_EXTRACTION_QUEUE,
    ) -> None:
        self.paper_repository = paper_repository
        self.redis_adapter = redis_adapter
        self.queue_name = queue_name

    def enqueue(
        self,
        request: PaperKeywordExtractionBatchRequestDTO,
    ) -> dict[str, Any]:
        paper_ids = self._resolve_candidate_ids(request)
        messages = 0
        for chunk in chunked(paper_ids, request.batch_size):
            self.redis_adapter.enqueue(
                self.queue_name,
                {
                    "task_type": "keyword_extraction",
                    "paper_ids": chunk,
                    "top_k": request.top_k,
                    "min_score": request.min_score,
                    "skip_processed": request.skip_processed,
                    "skip_non_english": request.skip_non_english,
                },
            )
            messages += 1

        return {
            "candidate_count": len(paper_ids),
            "enqueued_papers": len(paper_ids),
            "enqueued_messages": messages,
            "queue": self.queue_name,
            "sample_enqueued_ids": paper_ids[:20],
            "top_k": request.top_k,
            "min_score": request.min_score,
            "skip_processed": request.skip_processed,
            "skip_non_english": request.skip_non_english,
        }

    def _resolve_candidate_ids(
        self,
        request: PaperKeywordExtractionBatchRequestDTO,
    ) -> list[int]:
        if request.paper_ids:
            return self.paper_repository.list_ids_for_keyword_extraction(
                paper_ids=request.paper_ids,
                skip_processed=request.skip_processed,
            )

        ids: list[int] = []
        offset = request.offset
        remaining = request.limit
        while True:
            page_size = DEFAULT_ENQUEUE_PAGE_SIZE
            if remaining is not None:
                page_size = min(page_size, remaining)
            if page_size <= 0:
                break
            page = self.paper_repository.list_ids_for_keyword_extraction(
                date_from=request.date_from,
                date_to=request.date_to,
                topic_id=request.topic_id,
                field_id=request.field_id,
                skip_processed=request.skip_processed,
                limit=page_size,
                offset=offset,
            )
            if not page:
                break
            ids.extend(page)
            offset += len(page)
            if remaining is not None:
                remaining -= len(page)
                if remaining <= 0:
                    break
            if len(page) < page_size:
                break
        return ids


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract paper keyphrases into papers.extracted_keywords.",
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
    parser.add_argument("--date-from", type=parse_iso_date, default=None)
    parser.add_argument("--date-to", type=parse_iso_date, default=None)
    parser.add_argument("--topic-id", type=int, default=None)
    parser.add_argument("--field-id", type=int, default=None)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--batch-size", type=int, default=200)
    parser.add_argument("--top-k", type=int, default=10)
    parser.add_argument("--min-score", type=float, default=None)
    parser.set_defaults(skip_processed=True)
    parser.add_argument(
        "--skip-processed",
        dest="skip_processed",
        action="store_true",
        help="Skip papers where extracted_keywords is already set. Enabled by default.",
    )
    parser.add_argument(
        "--no-skip-processed",
        dest="skip_processed",
        action="store_false",
        help="Process papers even when extracted_keywords is already set.",
    )
    parser.add_argument(
        "--skip-non-english",
        action="store_true",
        help="Skip non-English papers instead of recording per-paper failures.",
    )
    parser.add_argument(
        "--enqueue",
        action="store_true",
        help="Create Redis keyword_extraction tasks instead of running extraction locally.",
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
    add_redis_args(parser)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    load_dotenv(args.env_file)
    engine = None
    try:
        request = build_request(args)
        database_url = args.database_url or Settings.from_env().database_url
        engine = create_db_engine(database_url, echo=False)
        SessionLocal = create_session_factory(engine, expire_on_commit=False)

        if args.enqueue:
            payload = run_enqueue(args, request, SessionLocal)
        else:
            payload = run_extract(args, request, SessionLocal)
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

    print_json(payload)
    return 0


def build_request(args: argparse.Namespace) -> PaperKeywordExtractionBatchRequestDTO:
    paper_ids = resolve_paper_ids(args)
    has_filters = any(
        value is not None
        for value in (args.date_from, args.date_to, args.topic_id, args.field_id)
    )
    if paper_ids and has_filters:
        raise InvalidRequestError("Use either paper ids or filter mode, not both.")
    if not paper_ids and not has_filters:
        raise InvalidRequestError(
            "Provide paper ids or at least one of --date-from/--date-to/--topic-id/--field-id."
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
    if args.limit is not None and args.limit <= 0:
        raise InvalidRequestError("--limit must be a positive integer")
    if args.offset < 0:
        raise InvalidRequestError("--offset must be non-negative")
    if args.batch_size <= 0:
        raise InvalidRequestError("--batch-size must be a positive integer")
    if args.top_k < 0:
        raise InvalidRequestError("--top-k must be non-negative")

    return PaperKeywordExtractionBatchRequestDTO(
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
        skip_processed=args.skip_processed,
        skip_non_english=args.skip_non_english,
    )


def run_extract(
    args: argparse.Namespace,
    request: PaperKeywordExtractionBatchRequestDTO,
    session_factory: Any,
) -> dict[str, Any]:
    with session_factory() as session:
        pipeline = KeywordExtractionPipeline(
            KeywordExtractionFacade(
                paper_repository=PaperRepository(session),
                embedding_adapter=LMStudioEmbeddingAdapter(
                    base_url=args.lmstudio_url
                    or os.getenv("LMSTUDIO_BASE_URL")
                    or "http://localhost:1234",
                ),
                embedding_model=args.embedding_model
                or os.getenv("EMBEDDING_MODEL")
                or DEFAULT_EMBEDDING_MODEL,
            )
        )
        try:
            result = pipeline.run_papers(request)
            session.commit()
        except Exception:
            session.rollback()
            raise
    return {
        "command": "extract-keywords",
        "enqueue": False,
        "result": result.model_dump(mode="json"),
    }


def run_enqueue(
    args: argparse.Namespace,
    request: PaperKeywordExtractionBatchRequestDTO,
    session_factory: Any,
) -> dict[str, Any]:
    redis_adapter = RedisAdapter(build_redis_client(args))
    with session_factory() as session:
        result = KeywordExtractionTaskEnqueuer(
            paper_repository=PaperRepository(session),
            redis_adapter=redis_adapter,
        ).enqueue(request)
    return {
        "command": "extract-keywords",
        "enqueue": True,
        "result": result,
    }


def resolve_paper_ids(args: argparse.Namespace) -> list[int]:
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
    return list(dict.fromkeys(paper_ids))


def add_redis_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--redis-url", default=None)
    parser.add_argument("--redis-host", default=None)
    parser.add_argument("--redis-port", type=int, default=None)
    parser.add_argument("--redis-db", type=int, default=None)


def build_redis_client(args: argparse.Namespace) -> Any:
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
        db=args.redis_db
        if args.redis_db is not None
        else _optional_int_env("REDIS_DB") or 0,
        password=os.getenv("REDIS_PASSWORD") or None,
    )


def parse_iso_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Expected YYYY-MM-DD date, got {value!r}."
        ) from exc


def chunked(values: list[int], size: int) -> list[list[int]]:
    return [values[index : index + size] for index in range(0, len(values), size)]


def _optional_int_env(name: str) -> int | None:
    value = os.getenv(name)
    if value is None or not value.strip():
        return None
    return int(value)


def print_json(payload: dict[str, Any], *, stream: Any = sys.stdout) -> None:
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str), file=stream)


if __name__ == "__main__":
    raise SystemExit(main())
