from __future__ import annotations

from typing import Any

from adapters.redis_adapter import RedisAdapter
from dto.keywords import PaperKeywordExtractionBatchRequestDTO
from ml.task_contracts import KEYWORD_EXTRACTION_QUEUE, KEYWORD_EXTRACTION_TASK
from repositories.papers import PaperRepository

DEFAULT_ENQUEUE_PAGE_SIZE = 1000


class KeywordExtractionTaskEnqueuer:
    """Resolve candidate papers and create canonical keyword extraction tasks."""

    def __init__(
        self,
        *,
        paper_repository: PaperRepository,
        redis_adapter: RedisAdapter | None,
        queue_name: str = KEYWORD_EXTRACTION_QUEUE,
    ) -> None:
        self.paper_repository = paper_repository
        self.redis_adapter = redis_adapter
        self.queue_name = queue_name

    def enqueue(
        self,
        request: PaperKeywordExtractionBatchRequestDTO,
        *,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        paper_ids = self._resolve_candidate_ids(request)
        messages = [
            {
                "task_type": KEYWORD_EXTRACTION_TASK,
                "paper_ids": chunk,
                "top_k": request.top_k,
                "min_score": request.min_score,
                "skip_processed": request.skip_processed,
                "skip_non_english": request.skip_non_english,
            }
            for chunk in _chunked(paper_ids, request.batch_size)
        ]
        if not dry_run:
            if self.redis_adapter is None:
                raise ValueError("Redis adapter is required unless --dry-run is used.")
            for message in messages:
                self.redis_adapter.enqueue(self.queue_name, message)
        return {
            "queue": self.queue_name,
            "dry_run": dry_run,
            "candidate_count": len(paper_ids),
            "enqueued_papers": len(paper_ids),
            "messages": len(messages),
            "sample": messages[:3],
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


def _chunked(values: list[int], size: int) -> list[list[int]]:
    return [values[index : index + size] for index in range(0, len(values), size)]


__all__ = ["KeywordExtractionTaskEnqueuer"]
