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

from adapters import QdrantAdapter, RedisAdapter
from core.config import Settings
from core.exceptions import AppError
from ml.constants import PAPERS_COLLECTION
from ml.workers.redis_worker import FAILED_TASKS_QUEUE, PAPER_INDEXING_QUEUE
from models.session import create_db_engine, create_session_factory
from repositories import PaperRepository


DEFAULT_PAGE_SIZE = 1000
DEFAULT_QDRANT_RETRIEVE_SIZE = 256


class PaperIndexingTaskEnqueuer:
    """Create Redis paper indexing tasks without running embedding generation."""

    def __init__(
        self,
        *,
        paper_repository: PaperRepository | None = None,
        redis_adapter: RedisAdapter,
        qdrant_adapter: QdrantAdapter | None = None,
        queue_name: str = PAPER_INDEXING_QUEUE,
        papers_collection: str = PAPERS_COLLECTION,
    ) -> None:
        self.paper_repository = paper_repository
        self.redis_adapter = redis_adapter
        self.qdrant_adapter = qdrant_adapter
        self.queue_name = queue_name
        self.papers_collection = papers_collection

    def enqueue(
        self,
        *,
        date_from: date | None = None,
        date_to: date | None = None,
        paper_ids: list[int] | None = None,
        missing_only: bool = False,
        force_reindex: bool = False,
    ) -> dict[str, Any]:
        """Resolve paper ids and enqueue one Redis message per paper."""
        if date_from is None and date_to is None and not paper_ids:
            raise ValueError("Provide --paper-ids or at least one date boundary.")
        if date_from is not None and date_to is not None and date_from > date_to:
            raise ValueError("--date-from must be before or equal to --date-to.")
        if missing_only and self.qdrant_adapter is None:
            raise ValueError("--missing-only requires Qdrant connection settings.")

        candidate_ids = self._resolve_candidate_ids(
            date_from=date_from,
            date_to=date_to,
            paper_ids=paper_ids or [],
        )
        skipped_existing: list[int] = []
        enqueue_ids = candidate_ids

        if missing_only:
            enqueue_ids, skipped_existing = self._filter_missing(candidate_ids)

        for paper_id in enqueue_ids:
            self.redis_adapter.enqueue(
                self.queue_name,
                {
                    "task_type": "paper_indexing",
                    "paper_id": paper_id,
                    "force_reindex": force_reindex,
                },
            )

        return {
            "candidate_count": len(candidate_ids),
            "enqueued": len(enqueue_ids),
            "skipped_existing": len(skipped_existing),
            "queue": self.queue_name,
            "missing_only": missing_only,
            "force_reindex": force_reindex,
            "sample_enqueued_ids": enqueue_ids[:20],
            "sample_skipped_existing_ids": skipped_existing[:20],
        }

    def _resolve_candidate_ids(
        self,
        *,
        date_from: date | None,
        date_to: date | None,
        paper_ids: list[int],
    ) -> list[int]:
        ordered_ids: list[int] = []
        seen: set[int] = set()

        for paper_id in paper_ids:
            if paper_id not in seen:
                ordered_ids.append(paper_id)
                seen.add(paper_id)

        if date_from is not None or date_to is not None:
            if self.paper_repository is None:
                raise ValueError("Date-based enqueueing requires PostgreSQL access.")
            offset = 0
            while True:
                page_ids = self.paper_repository.list_ids_by_period(
                    date_from=date_from,
                    date_to=date_to,
                    limit=DEFAULT_PAGE_SIZE,
                    offset=offset,
                )
                if not page_ids:
                    break
                offset += len(page_ids)
                for paper_id in page_ids:
                    paper_id = int(paper_id)
                    if paper_id not in seen:
                        ordered_ids.append(paper_id)
                        seen.add(paper_id)

        return ordered_ids

    def _filter_missing(self, paper_ids: list[int]) -> tuple[list[int], list[int]]:
        if not paper_ids:
            return [], []
        missing_ids: list[int] = []
        existing_ids: list[int] = []

        for chunk in chunked(paper_ids, DEFAULT_QDRANT_RETRIEVE_SIZE):
            points = self.qdrant_adapter.retrieve(  # type: ignore[union-attr]
                self.papers_collection,
                chunk,
                with_vectors=False,
            )
            existing = {int(point.id) for point in points}
            for paper_id in chunk:
                if paper_id in existing:
                    existing_ids.append(paper_id)
                else:
                    missing_ids.append(paper_id)

        return missing_ids, existing_ids


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse task CLI command and command-specific arguments."""
    parser = argparse.ArgumentParser(description="Redis task enqueueing CLI utilities.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    enqueue_parser = subparsers.add_parser(
        "enqueue-indexing",
        help="Enqueue paper indexing tasks into Redis without running ML locally.",
    )
    enqueue_parser.add_argument(
        "--date-from",
        type=parse_iso_date,
        default=None,
        help="Publication date lower bound in YYYY-MM-DD format.",
    )
    enqueue_parser.add_argument(
        "--date-to",
        type=parse_iso_date,
        default=None,
        help="Publication date upper bound in YYYY-MM-DD format.",
    )
    enqueue_parser.add_argument(
        "--paper-ids",
        default=None,
        help="Path to a text file with paper ids separated by whitespace, comma, or newline.",
    )
    enqueue_parser.add_argument(
        "--missing-only",
        action="store_true",
        help="Enqueue only papers missing from Qdrant papers_content_v1.",
    )
    enqueue_parser.add_argument(
        "--force-reindex",
        action="store_true",
        help="Set force_reindex=true in enqueued messages.",
    )
    add_database_args(enqueue_parser)
    add_redis_args(enqueue_parser)
    add_qdrant_args(enqueue_parser)

    restore_parser = subparsers.add_parser(
        "restore-failed",
        help="Move failed worker messages back from queue:failed_tasks to their source queues.",
    )
    restore_parser.add_argument(
        "--failed-queue",
        default=FAILED_TASKS_QUEUE,
        help=f"Queue with failed task wrappers. Defaults to {FAILED_TASKS_QUEUE}.",
    )
    restore_parser.add_argument(
        "--target-queue",
        default=None,
        help="Override target queue for all restored messages. Defaults to wrapper source_queue.",
    )
    restore_parser.add_argument(
        "--task-type",
        default=None,
        help="Restore only failed messages whose inner message.task_type matches this value.",
    )
    restore_parser.add_argument(
        "--limit",
        type=int,
        default=1000,
        help="Maximum failed wrappers to inspect/process. Defaults to 1000.",
    )
    restore_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview what would be restored without modifying Redis.",
    )
    add_database_args(restore_parser)
    add_redis_args(restore_parser)

    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Run the requested task CLI command."""
    args = parse_args(argv)
    load_dotenv(args.env_file)

    try:
        if args.command == "enqueue-indexing":
            payload = run_enqueue_indexing(args)
        elif args.command == "restore-failed":
            payload = run_restore_failed(args)
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


def run_enqueue_indexing(args: argparse.Namespace) -> dict[str, Any]:
    """Enqueue paper indexing tasks into Redis."""
    redis_adapter = RedisAdapter(build_redis_client(args))
    qdrant_adapter = build_qdrant_adapter(args) if args.missing_only else None
    file_paper_ids = read_paper_ids_file(args.paper_ids) if args.paper_ids else []

    if args.date_from is not None or args.date_to is not None:
        database_url = args.database_url or Settings.from_env().database_url
        engine = create_db_engine(database_url, echo=False)
        SessionLocal = create_session_factory(engine, expire_on_commit=False)
        try:
            with SessionLocal() as session:
                enqueuer = PaperIndexingTaskEnqueuer(
                    paper_repository=PaperRepository(session),
                    redis_adapter=redis_adapter,
                    qdrant_adapter=qdrant_adapter,
                )
                result = enqueuer.enqueue(
                    date_from=args.date_from,
                    date_to=args.date_to,
                    paper_ids=file_paper_ids,
                    missing_only=args.missing_only,
                    force_reindex=args.force_reindex,
                )
        finally:
            engine.dispose()
    else:
        enqueuer = PaperIndexingTaskEnqueuer(
            redis_adapter=redis_adapter,
            qdrant_adapter=qdrant_adapter,
        )
        result = enqueuer.enqueue(
            paper_ids=file_paper_ids,
            missing_only=args.missing_only,
            force_reindex=args.force_reindex,
        )

    return {
        "command": "enqueue-indexing",
        "date_from": args.date_from,
        "date_to": args.date_to,
        "paper_ids_file": args.paper_ids,
        "result": result,
    }


class FailedTaskRestorer:
    """Restore failed worker task wrappers to executable Redis queues."""

    def __init__(
        self,
        redis_adapter: RedisAdapter,
        *,
        failed_queue: str = FAILED_TASKS_QUEUE,
    ) -> None:
        self.redis_adapter = redis_adapter
        self.failed_queue = failed_queue

    def restore(
        self,
        *,
        limit: int,
        target_queue: str | None = None,
        task_type: str | None = None,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Restore failed tasks and return operation counters."""
        if limit <= 0:
            raise ValueError("--limit must be a positive integer.")

        if dry_run:
            wrappers = self.redis_adapter.peek_queue(self.failed_queue, limit)
            return self._preview(
                wrappers,
                target_queue=target_queue,
                task_type=task_type,
            )

        restored = 0
        skipped = 0
        invalid = 0
        restored_by_queue: dict[str, int] = {}
        skipped_samples: list[dict[str, Any]] = []
        invalid_samples: list[dict[str, Any]] = []

        for _ in range(limit):
            wrapper = self.redis_adapter.dequeue_nowait(self.failed_queue)
            if wrapper is None:
                break

            extracted = self._extract_restore_target(
                wrapper,
                target_queue=target_queue,
            )
            if extracted is None:
                invalid += 1
                invalid_samples.append(self._sample(wrapper))
                continue

            queue_name, message = extracted
            if task_type and message.get("task_type") != task_type:
                skipped += 1
                skipped_samples.append(self._sample(wrapper))
                self.redis_adapter.enqueue(self.failed_queue, wrapper)
                continue

            try:
                self.redis_adapter.enqueue(queue_name, message)
            except Exception:
                self.redis_adapter.enqueue(self.failed_queue, wrapper)
                raise

            restored += 1
            restored_by_queue[queue_name] = restored_by_queue.get(queue_name, 0) + 1

        return {
            "failed_queue": self.failed_queue,
            "dry_run": False,
            "limit": limit,
            "restored": restored,
            "skipped": skipped,
            "invalid": invalid,
            "restored_by_queue": restored_by_queue,
            "skipped_samples": skipped_samples[:20],
            "invalid_samples": invalid_samples[:20],
        }

    def _preview(
        self,
        wrappers: list[dict[str, Any]],
        *,
        target_queue: str | None,
        task_type: str | None,
    ) -> dict[str, Any]:
        restorable = 0
        skipped = 0
        invalid = 0
        by_queue: dict[str, int] = {}
        samples: list[dict[str, Any]] = []

        for wrapper in wrappers:
            extracted = self._extract_restore_target(
                wrapper,
                target_queue=target_queue,
            )
            if extracted is None:
                invalid += 1
                continue
            queue_name, message = extracted
            if task_type and message.get("task_type") != task_type:
                skipped += 1
                continue
            restorable += 1
            by_queue[queue_name] = by_queue.get(queue_name, 0) + 1
            samples.append(
                {
                    "target_queue": queue_name,
                    "task_type": message.get("task_type"),
                    "message": message,
                }
            )

        return {
            "failed_queue": self.failed_queue,
            "dry_run": True,
            "inspected": len(wrappers),
            "restorable": restorable,
            "skipped": skipped,
            "invalid": invalid,
            "restorable_by_queue": by_queue,
            "samples": samples[:20],
        }

    def _extract_restore_target(
        self,
        wrapper: dict[str, Any],
        *,
        target_queue: str | None,
    ) -> tuple[str, dict[str, Any]] | None:
        message = wrapper.get("message")
        if not isinstance(message, dict):
            return None

        queue_name = target_queue or wrapper.get("source_queue")
        if not isinstance(queue_name, str) or not queue_name.startswith("queue:"):
            return None
        return queue_name, message

    def _sample(self, wrapper: dict[str, Any]) -> dict[str, Any]:
        return {
            "source_queue": wrapper.get("source_queue"),
            "task_type": (
                wrapper.get("message", {}).get("task_type")
                if isinstance(wrapper.get("message"), dict)
                else None
            ),
            "error": wrapper.get("error"),
        }


def run_restore_failed(args: argparse.Namespace) -> dict[str, Any]:
    """Restore failed ML task wrappers back to Redis work queues."""
    redis_adapter = RedisAdapter(build_redis_client(args))
    result = FailedTaskRestorer(
        redis_adapter,
        failed_queue=args.failed_queue,
    ).restore(
        limit=args.limit,
        target_queue=args.target_queue,
        task_type=args.task_type,
        dry_run=args.dry_run,
    )

    return {
        "command": "restore-failed",
        "result": result,
    }


def add_database_args(parser: argparse.ArgumentParser) -> None:
    """Add shared environment and database connection arguments."""
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
    """Add Qdrant arguments used only by --missing-only."""
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


def read_paper_ids_file(path: str) -> list[int]:
    """Read paper ids from a text file and preserve first occurrence order."""
    raw_text = Path(path).read_text(encoding="utf-8")
    values = raw_text.replace(",", " ").split()
    ids: list[int] = []
    seen: set[int] = set()
    for value in values:
        paper_id = int(value)
        if paper_id <= 0:
            raise ValueError(f"Paper ids must be positive integers, got {paper_id}.")
        if paper_id not in seen:
            ids.append(paper_id)
            seen.add(paper_id)
    return ids


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


def parse_iso_date(value: str) -> date:
    """Parse an ISO date for argparse."""
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Expected YYYY-MM-DD date, got {value!r}."
        ) from exc


def chunked(values: list[int], size: int) -> list[list[int]]:
    """Split integer values into fixed-size chunks."""
    return [values[index : index + size] for index in range(0, len(values), size)]


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
