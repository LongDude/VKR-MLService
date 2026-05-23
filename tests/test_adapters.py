from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import httpx
import pandas as pd
import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from adapters.lmstudio_chat_adapter import LMStudioChatAdapter
from adapters.lmstudio_embedding_adapter import LMStudioEmbeddingAdapter
from adapters.openalex_adapter import OpenAlexAdapter
from adapters.qdrant_adapter import QdrantAdapter
from adapters.redis_adapter import RedisAdapter
from cli.tasks import FailedTaskRestorer
from core.exceptions import (
    EmbeddingGenerationError,
    ExternalServiceRateLimitError,
    InvalidRequestError,
    LLMGenerationError,
    RedisOperationError,
)
from dto.common import BatchOperationResultDTO, OperationResultDTO
from dto.external import OpenAlexSearchFiltersDTO
from dto.keywords import KeywordExtractionMetadataDTO, KeywordExtractionResponseDTO
from dto.papers import PaperBatchIndexingRequestDTO, PaperIndexingRequestDTO
from dto.qdrant import QdrantPayloadIndexDTO, QdrantPointDTO
from dto.trends import ClusterSummaryDTO
from ingestion.openalex_bootstrap.monthly_counts import MonthlyCount, MonthlyCountsLoader
from cli.openalex import (
    default_stats_redis_key,
    resolve_bootstrap_target_unit,
    resolve_target_count,
)
from ml.constants import PAPERS_COLLECTION, TREND_CLUSTERS_COLLECTION
from ml.facades.cluster_analytics import ClusterAnalyticsFacade
from ml.facades.cluster_db_sync import ClusterDbSyncFacade
from ml.facades.keyword_extraction import KeywordExtractionFacade
from ml.facades.paper_indexing import PaperIndexingFacade
from ml.services.events import NoopEventSink
from ml.services.qdrant_collections import QdrantCollectionInitializer
from ml.services.qdrant_payloads import QdrantPayloadBuilder
from ml.services.events import CompositeEventSink, MLEvent, RedisEventSink
from ml.workers.redis_worker import (
    CLUSTER_RECOMPUTE_QUEUE,
    FAILED_TASKS_QUEUE,
    KEYWORD_EXTRACTION_QUEUE,
    PAPER_INDEXING_QUEUE,
    RedisMLWorker,
)
from ml.workers.task_handlers import MLTaskHandler


class FakeRedisClient:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}
        self.queues: dict[str, list[str]] = {}

    def get(self, key: str) -> str | None:
        return self.values.get(key)

    def set(
        self,
        key: str,
        value: str,
        ex: int | None = None,
        nx: bool = False,
    ) -> bool:
        if nx and key in self.values:
            return False
        self.values[key] = value
        return True

    def delete(self, key: str) -> None:
        self.values.pop(key, None)

    def rpush(self, queue_name: str, payload: str) -> None:
        self.queues.setdefault(queue_name, []).append(payload)

    def blpop(self, queue_name: str, timeout: int = 5) -> tuple[str, str] | None:
        queue = self.queues.setdefault(queue_name, [])
        if not queue:
            return None
        return queue_name, queue.pop(0)

    def lpop(self, queue_name: str) -> str | None:
        queue = self.queues.setdefault(queue_name, [])
        if not queue:
            return None
        return queue.pop(0)

    def lrange(self, queue_name: str, start: int, end: int) -> list[str]:
        queue = self.queues.setdefault(queue_name, [])
        return queue[start : end + 1]

    def llen(self, queue_name: str) -> int:
        return len(self.queues.setdefault(queue_name, []))


class InMemoryEventSink:
    def __init__(self) -> None:
        self.events: list[MLEvent] = []

    def emit(self, event: MLEvent) -> None:
        self.events.append(event)


class FailingEventSink:
    def emit(self, event: MLEvent) -> None:
        raise RuntimeError("sink failed")


def test_redis_adapter_json_queue_and_lock() -> None:
    client = FakeRedisClient()
    adapter = RedisAdapter(client)

    adapter.set_json("paper:1", {"title": "A"}, ttl_seconds=60)
    adapter.enqueue("jobs", {"paper_id": 1})

    assert adapter.get_json("paper:1") == {"title": "A"}
    assert adapter.peek_queue("jobs", limit=1) == [{"paper_id": 1}]
    assert adapter.queue_length("jobs") == 1
    assert adapter.dequeue("jobs") == {"paper_id": 1}
    assert adapter.dequeue("jobs") is None
    assert adapter.acquire_lock("lock:1", ttl_seconds=10) is True
    assert adapter.acquire_lock("lock:1", ttl_seconds=10) is False
    adapter.release_lock("lock:1")
    assert adapter.acquire_lock("lock:1", ttl_seconds=10) is True


def test_composite_event_sink_isolates_sink_failures() -> None:
    sink = InMemoryEventSink()
    composite = CompositeEventSink([FailingEventSink(), sink])
    event = MLEvent(
        event_type="paper_indexing_started",
        task_type="paper_indexing",
        entity_id=1,
        stage="started",
    )

    composite.emit(event)

    assert sink.events == [event]


def test_redis_event_sink_writes_latest_status() -> None:
    client = FakeRedisClient()
    adapter = RedisAdapter(client)
    sink = RedisEventSink(adapter, ttl_seconds=60)

    sink.emit(
        MLEvent(
            event_type="cluster_completed",
            task_type="cluster_recompute",
            entity_id="topic:1",
            stage="completed",
            current=1,
            total=1,
            message="done",
        )
    )

    status = adapter.get_json("ml:task_status:cluster_recompute:topic:1")
    assert status is not None
    assert status["event_type"] == "cluster_completed"
    assert status["current"] == 1


def test_redis_adapter_wraps_decode_errors() -> None:
    client = FakeRedisClient()
    client.values["bad"] = "not-json"

    with pytest.raises(RedisOperationError):
        RedisAdapter(client).get_json("bad")


def test_monthly_counts_loader_reads_and_detects_missing_redis_periods() -> None:
    client = FakeRedisClient()
    adapter = RedisAdapter(client)
    loader = MonthlyCountsLoader()
    redis_key = default_stats_redis_key(["ru", "en"], ["article"])
    loader.merge_into_redis(
        adapter,
        redis_key,
        [
            MonthlyCount(
                period="2024-01",
                date_from=date(2024, 1, 1),
                date_to=date(2024, 1, 31),
                count=10,
            )
        ],
    )

    loaded = loader.load_from_redis(
        adapter,
        redis_key,
        date_from=date(2024, 1, 15),
        date_to=date(2024, 2, 15),
    )
    missing = loader.missing_redis_months(
        adapter,
        redis_key,
        date_from=date(2024, 1, 15),
        date_to=date(2024, 2, 15),
    )

    assert redis_key.endswith("languages=en,ru:types=article")
    assert loaded[0].period == "2024-01"
    assert loaded[0].date_from == date(2024, 1, 15)
    assert loaded[0].date_to == date(2024, 1, 31)
    assert [item.period for item in missing] == ["2024-02"]


def test_resolve_target_count_supports_period_scope() -> None:
    assert resolve_target_count(100, "period") == (100, "period")
    assert resolve_target_count(50, "year") == (50, "year")
    assert resolve_target_count(25, "month") == (25, "month")
    assert resolve_target_count(200, "total") == (200, "total")
    assert resolve_target_count(None, 100) == (100, "period")
    with pytest.raises(ValueError):
        resolve_target_count(200, "total", 100)


def test_resolve_bootstrap_target_unit_auto_uses_topic_for_monthly_taxonomy() -> None:
    assert (
        resolve_bootstrap_target_unit(
            "auto",
            "month",
            field_ids=[1],
            subfield_ids=None,
        )
        == "topic"
    )
    assert (
        resolve_bootstrap_target_unit(
            "auto",
            "period",
            field_ids=[1],
            subfield_ids=None,
        )
        == "aggregate"
    )
    assert (
        resolve_bootstrap_target_unit(
            "aggregate",
            "month",
            field_ids=[1],
            subfield_ids=None,
        )
        == "aggregate"
    )
    with pytest.raises(ValueError):
        resolve_bootstrap_target_unit(
            "topic",
            "month",
            field_ids=None,
            subfield_ids=None,
        )


def test_failed_task_restorer_splits_large_paper_indexing_batches() -> None:
    client = FakeRedisClient()
    adapter = RedisAdapter(client)
    paper_ids = list(range(1000))
    adapter.enqueue(
        "queue:failed_tasks",
        {
            "source_queue": PAPER_INDEXING_QUEUE,
            "message": {
                "task_type": "paper_indexing",
                "paper_ids": paper_ids,
                "force_reindex": True,
            },
            "error": {"code": "timeout"},
        },
    )

    restorer = FailedTaskRestorer(adapter)
    preview = restorer.restore(
        limit=10,
        dry_run=True,
        split_paper_indexing_batch_size=128,
    )
    result = restorer.restore(
        limit=10,
        split_paper_indexing_batch_size=128,
    )
    restored_messages = [
        adapter.dequeue_nowait(PAPER_INDEXING_QUEUE)
        for _ in range(adapter.queue_length(PAPER_INDEXING_QUEUE))
    ]

    assert preview["restorable"] == 1
    assert preview["output_messages"] == 8
    assert preview["sample_chunk_sizes"][:2] == [128, 128]
    assert result["processed_failed_wrappers"] == 1
    assert result["restored"] == 8
    assert adapter.queue_length("queue:failed_tasks") == 0
    assert [len(message["paper_ids"]) for message in restored_messages] == [
        128,
        128,
        128,
        128,
        128,
        128,
        128,
        104,
    ]
    assert all(message["force_reindex"] is True for message in restored_messages)


def test_failed_task_restorer_can_disable_paper_indexing_split() -> None:
    client = FakeRedisClient()
    adapter = RedisAdapter(client)
    paper_ids = list(range(5))
    adapter.enqueue(
        "queue:failed_tasks",
        {
            "source_queue": PAPER_INDEXING_QUEUE,
            "message": {
                "task_type": "paper_indexing",
                "paper_ids": paper_ids,
                "force_reindex": False,
            },
            "error": {"code": "timeout"},
        },
    )

    result = FailedTaskRestorer(adapter).restore(
        limit=10,
        split_paper_indexing=False,
        split_paper_indexing_batch_size=2,
    )
    restored = adapter.dequeue_nowait(PAPER_INDEXING_QUEUE)

    assert result["restored"] == 1
    assert restored["paper_ids"] == paper_ids


def test_failed_task_restorer_splits_large_cluster_recompute_batches() -> None:
    client = FakeRedisClient()
    adapter = RedisAdapter(client)
    topic_ids = list(range(121))
    adapter.enqueue(
        "queue:failed_tasks",
        {
            "source_queue": CLUSTER_RECOMPUTE_QUEUE,
            "message": {
                "task_type": "recompute_topic_clusters",
                "topic_ids": topic_ids,
                "force_summary": True,
            },
            "error": {"code": "timeout"},
        },
    )

    restorer = FailedTaskRestorer(adapter)
    preview = restorer.restore(
        limit=10,
        dry_run=True,
        split_cluster_recompute_batch_size=50,
    )
    result = restorer.restore(
        limit=10,
        split_cluster_recompute_batch_size=50,
    )
    restored_messages = [
        adapter.dequeue_nowait(CLUSTER_RECOMPUTE_QUEUE)
        for _ in range(adapter.queue_length(CLUSTER_RECOMPUTE_QUEUE))
    ]

    assert preview["restorable"] == 1
    assert preview["output_messages"] == 3
    assert preview["sample_chunk_sizes"] == [50, 50, 21]
    assert result["processed_failed_wrappers"] == 1
    assert result["restored"] == 3
    assert [len(message["topic_ids"]) for message in restored_messages] == [50, 50, 21]
    assert all(message["force_summary"] is True for message in restored_messages)


def test_failed_task_restorer_can_disable_cluster_recompute_split() -> None:
    client = FakeRedisClient()
    adapter = RedisAdapter(client)
    topic_ids = list(range(5))
    adapter.enqueue(
        "queue:failed_tasks",
        {
            "source_queue": CLUSTER_RECOMPUTE_QUEUE,
            "message": {
                "task_type": "recompute_topic_clusters",
                "topic_ids": topic_ids,
                "force_summary": False,
            },
            "error": {"code": "timeout"},
        },
    )

    result = FailedTaskRestorer(adapter).restore(
        limit=10,
        split_cluster_recompute=False,
        split_cluster_recompute_batch_size=2,
    )
    restored = adapter.dequeue_nowait(CLUSTER_RECOMPUTE_QUEUE)

    assert result["restored"] == 1
    assert restored["topic_ids"] == topic_ids


class CapturingTaskHandler:
    def __init__(self) -> None:
        self.messages: list[dict[str, Any]] = []

    def handle(self, message: dict[str, Any]) -> None:
        self.messages.append(message)


class CapturingEventSink:
    def __init__(self) -> None:
        self.events: list[MLEvent] = []

    def emit(self, event: MLEvent) -> None:
        self.events.append(event)


def test_redis_worker_splits_oversized_paper_indexing_messages_before_handling() -> None:
    client = FakeRedisClient()
    adapter = RedisAdapter(client)
    handler = CapturingTaskHandler()
    adapter.enqueue(
        PAPER_INDEXING_QUEUE,
        {
            "task_type": "paper_indexing",
            "paper_ids": list(range(10)),
            "force_reindex": False,
        },
    )
    worker = RedisMLWorker(
        redis_adapter=adapter,
        task_handler=handler,  # type: ignore[arg-type]
        queues=(PAPER_INDEXING_QUEUE,),
        batch_sizes={PAPER_INDEXING_QUEUE: 1},
        max_task_sizes={PAPER_INDEXING_QUEUE: 4},
    )

    assert worker.run_once() is True
    assert [len(message["paper_ids"]) for message in handler.messages] == [4, 4, 2]


def test_redis_worker_splits_oversized_keyword_extraction_messages_before_handling() -> None:
    client = FakeRedisClient()
    adapter = RedisAdapter(client)
    handler = CapturingTaskHandler()
    adapter.enqueue(
        KEYWORD_EXTRACTION_QUEUE,
        {
            "task_type": "keyword_extraction",
            "paper_ids": list(range(10)),
            "top_k": 5,
            "min_score": 0.1,
            "skip_processed": True,
            "skip_non_english": True,
        },
    )
    worker = RedisMLWorker(
        redis_adapter=adapter,
        task_handler=handler,  # type: ignore[arg-type]
        queues=(KEYWORD_EXTRACTION_QUEUE,),
        batch_sizes={KEYWORD_EXTRACTION_QUEUE: 1},
        max_task_sizes={KEYWORD_EXTRACTION_QUEUE: 4},
    )

    assert worker.run_once() is True
    assert [len(message["paper_ids"]) for message in handler.messages] == [4, 4, 2]
    assert all(message["top_k"] == 5 for message in handler.messages)
    assert all(message["skip_non_english"] is True for message in handler.messages)


def test_redis_worker_splits_oversized_cluster_recompute_messages_before_handling() -> None:
    client = FakeRedisClient()
    adapter = RedisAdapter(client)
    handler = CapturingTaskHandler()
    adapter.enqueue(
        CLUSTER_RECOMPUTE_QUEUE,
        {
            "task_type": "recompute_topic_clusters",
            "topic_ids": list(range(10)),
            "force_summary": True,
        },
    )
    worker = RedisMLWorker(
        redis_adapter=adapter,
        task_handler=handler,  # type: ignore[arg-type]
        queues=(CLUSTER_RECOMPUTE_QUEUE,),
        batch_sizes={CLUSTER_RECOMPUTE_QUEUE: 1},
        max_task_sizes={CLUSTER_RECOMPUTE_QUEUE: 4},
    )

    assert worker.run_once() is True
    assert [len(message["topic_ids"]) for message in handler.messages] == [4, 4, 2]
    assert all(message["force_summary"] is True for message in handler.messages)


def test_redis_worker_emits_completed_result_summary() -> None:
    class ResultHandler:
        def handle(self, message: dict[str, Any]) -> OperationResultDTO:
            return OperationResultDTO(
                success=True,
                message="done",
                details={
                    "task_type": message["task_type"],
                    "status": "skipped",
                    "reason": "report_exists",
                },
            )

    client = FakeRedisClient()
    adapter = RedisAdapter(client)
    sink = CapturingEventSink()
    adapter.enqueue(
        PAPER_INDEXING_QUEUE,
        {
            "task_type": "paper_indexing",
            "paper_id": 10,
            "force_reindex": False,
        },
    )
    worker = RedisMLWorker(
        redis_adapter=adapter,
        task_handler=ResultHandler(),  # type: ignore[arg-type]
        queues=(PAPER_INDEXING_QUEUE,),
        event_sink=sink,
    )

    assert worker.run_once() is True

    completed = [
        event for event in sink.events if event.event_type == "worker_task_completed"
    ]
    assert len(completed) == 1
    assert completed[0].payload["result"]["details"]["status"] == "skipped"
    assert completed[0].payload["result"]["details"]["reason"] == "report_exists"


def test_redis_worker_saves_interrupted_task_to_failed_queue() -> None:
    class InterruptingHandler:
        def __init__(self) -> None:
            self.session = SimpleNamespace(rollbacks=0)

            def rollback() -> None:
                self.session.rollbacks += 1

            self.session.rollback = rollback

        def handle(self, _message: dict[str, Any]) -> None:
            raise KeyboardInterrupt()

    client = FakeRedisClient()
    adapter = RedisAdapter(client)
    adapter.enqueue(
        PAPER_INDEXING_QUEUE,
        {
            "task_type": "paper_indexing",
            "paper_id": 10,
            "force_reindex": False,
        },
    )
    handler = InterruptingHandler()
    worker = RedisMLWorker(
        redis_adapter=adapter,
        task_handler=handler,  # type: ignore[arg-type]
        queues=(PAPER_INDEXING_QUEUE,),
    )

    with pytest.raises(KeyboardInterrupt):
        worker.run_once()

    failed = adapter.dequeue_nowait(FAILED_TASKS_QUEUE)
    assert failed is not None
    assert failed["source_queue"] == PAPER_INDEXING_QUEUE
    assert failed["message"]["paper_ids"] == [10]
    assert failed["error"]["code"] == "Interrupt"
    assert handler.session.rollbacks == 1


class PartiallyFailingTrendPipeline:
    def recompute_cluster(self, cluster_id: str, force_summary: bool = False) -> dict[str, Any]:
        if cluster_id == "topic:2":
            raise InvalidRequestError("broken cluster", details={"cluster_id": cluster_id})
        return {"cluster_id": cluster_id, "force_summary": force_summary}


def test_task_handler_continues_cluster_batch_after_topic_error() -> None:
    handler = MLTaskHandler(
        trend_recompute_pipeline=PartiallyFailingTrendPipeline(),  # type: ignore[arg-type]
    )

    result = handler.handle(
        {
            "task_type": "recompute_topic_clusters",
            "topic_ids": [1, 2, 3],
            "force_summary": True,
        }
    )

    assert result.success is False
    assert result.details["updated"] == 2
    assert result.details["failed"] == 1
    assert result.details["errors"][0]["task_id"] == "topic:2"


class CapturingKeywordExtractionPipeline:
    def __init__(self) -> None:
        self.requests: list[Any] = []

    def run_papers(self, request: Any) -> BatchOperationResultDTO:
        self.requests.append(request)
        return BatchOperationResultDTO(total=len(request.paper_ids), updated=len(request.paper_ids))


def test_task_handler_dispatches_keyword_extraction_batch() -> None:
    pipeline = CapturingKeywordExtractionPipeline()
    handler = MLTaskHandler(
        keyword_extraction_pipeline=pipeline,  # type: ignore[arg-type]
    )

    result = handler.handle(
        {
            "task_type": "keyword_extraction",
            "paper_ids": [1, 2],
            "top_k": 7,
            "min_score": 0.2,
            "skip_processed": False,
            "skip_non_english": True,
        }
    )

    request = pipeline.requests[0]
    assert result.success is True
    assert request.paper_ids == [1, 2]
    assert request.top_k == 7
    assert request.min_score == 0.2
    assert request.skip_processed is False
    assert request.skip_non_english is True


class FakeQdrantClient:
    def __init__(self) -> None:
        self.collections: set[str] = set()
        self.points: dict[int | str, dict[str, Any]] = {}
        self.indexes: list[tuple[str, str, str]] = []

    def collection_exists(self, collection_name: str) -> bool:
        return collection_name in self.collections

    def create_collection(self, collection_name: str, vectors_config: Any) -> None:
        self.collections.add(collection_name)

    def create_payload_index(
        self,
        collection_name: str,
        field_name: str,
        field_schema: str,
    ) -> None:
        self.indexes.append((collection_name, field_name, field_schema))

    def upsert(self, collection_name: str, points: list[dict[str, Any]]) -> None:
        for point in points:
            point_id = self._get_value(point, "id")
            self.points[point_id] = {
                "id": point_id,
                "vector": self._get_value(point, "vector"),
                "payload": self._get_value(point, "payload"),
            }

    def search(
        self,
        collection_name: str,
        query_vector: list[float],
        limit: int,
        query_filter: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        return [
            {
                "id": point_id,
                "score": 0.9,
                "payload": point["payload"],
                "vector": point["vector"],
            }
            for point_id, point in list(self.points.items())[:limit]
        ]

    def retrieve(
        self,
        collection_name: str,
        ids: list[int | str],
        with_vectors: bool = False,
    ) -> list[dict[str, Any]]:
        result = []
        for point_id in ids:
            point = self.points.get(point_id)
            if point is None:
                continue
            result.append(
                {
                    "id": point_id,
                    "payload": point["payload"],
                    "vector": point["vector"] if with_vectors else None,
                }
            )
        return result

    def scroll(
        self,
        collection_name: str,
        scroll_filter: dict[str, Any] | None = None,
        limit: int = 10,
        offset: int | None = None,
        with_payload: bool = True,
        with_vectors: bool = False,
    ) -> tuple[list[dict[str, Any]], int | None]:
        items = list(self.points.items())
        start = int(offset or 0)
        page_items = items[start : start + limit]
        next_offset = start + limit if start + limit < len(items) else None
        return (
            [
                {
                    "id": point_id,
                    "payload": point["payload"] if with_payload else {},
                    "vector": point["vector"] if with_vectors else None,
                }
                for point_id, point in page_items
            ],
            next_offset,
        )

    def _get_value(self, value: Any, key: str) -> Any:
        if isinstance(value, dict):
            return value[key]
        return getattr(value, key)


def test_qdrant_adapter_uses_client_without_hardcoded_collection() -> None:
    client = FakeQdrantClient()
    adapter = QdrantAdapter(client)

    adapter.ensure_collection("papers", vector_size=3)
    adapter.ensure_payload_indexes(
        "papers",
        [QdrantPayloadIndexDTO(field_name="publication_year", field_schema="integer")],
    )
    adapter.upsert_points(
        "papers",
        [QdrantPointDTO(id=1, vector=[0.1, 0.2, 0.3], payload={"paper_id": 1})],
    )

    assert "papers" in client.collections
    assert len(client.indexes) == 1
    assert client.indexes[0][0] == "papers"
    assert client.indexes[0][1] == "publication_year"
    assert str(client.indexes[0][2]).lower().endswith("integer")
    assert adapter.exists("papers", 1) is True
    assert adapter.retrieve("papers", [1], with_vectors=True)[0].vector == [0.1, 0.2, 0.3]
    assert adapter.search("papers", [0.1, 0.2, 0.3], top_k=1)[0].payload == {"paper_id": 1}


def test_qdrant_paper_payload_uses_paper_openalex_fields() -> None:
    payload = QdrantPayloadBuilder().build_paper_payload(
        SimpleNamespace(
            id=1,
            title="Demo",
            doi="10.1/demo",
            openalex_id="https://openalex.org/W1",
            references_count=12,
            type="article",
            created_by_user_id=7,
        ),
        text_hash="hash",
        external_ids={"openalex": "https://openalex.org/W1"},
    )

    assert payload["openalex_id"] == "https://openalex.org/W1"
    assert payload["references_count"] == 12
    assert payload["text_hash"] == "hash"
    assert "type" not in payload
    assert "created_by_user_id" not in payload
    assert "external_ids" not in payload


def test_qdrant_paper_indexes_match_new_paper_payload_fields() -> None:
    indexes = dict(QdrantCollectionInitializer.PAPERS_INDEXES)

    assert indexes["openalex_id"] == "keyword"
    assert indexes["references_count"] == "integer"
    assert "type" not in indexes


def test_qdrant_adapter_maps_application_string_ids_to_uuid_points() -> None:
    client = FakeQdrantClient()
    adapter = QdrantAdapter(client)

    adapter.upsert_point(
        "trends",
        "topic:10001",
        [0.1, 0.2, 0.3],
        {"cluster_id": "topic:10001"},
    )

    assert "topic:10001" not in client.points
    retrieved = adapter.retrieve("trends", ["topic:10001"], with_vectors=True)
    hits = adapter.search("trends", [0.1, 0.2, 0.3], top_k=1)

    assert retrieved[0].id == "topic:10001"
    assert retrieved[0].payload == {"cluster_id": "topic:10001"}
    assert hits[0].id == "topic:10001"


def test_qdrant_adapter_scroll_points_with_fake_client() -> None:
    client = FakeQdrantClient()
    adapter = QdrantAdapter(client)

    adapter.upsert_point("trends", "topic:1", [0.1, 0.2, 0.3], {"cluster_key": "topic:1"})
    adapter.upsert_point("trends", "topic:2", [0.2, 0.3, 0.4], {"cluster_key": "topic:2"})

    points = adapter.scroll_points("trends", batch_size=1, with_vectors=False)

    assert [point.id for point in points] == ["topic:1", "topic:2"]
    assert points[0].payload == {"cluster_key": "topic:1"}


def test_qdrant_adapter_works_with_official_in_memory_client() -> None:
    qdrant_client = pytest.importorskip("qdrant_client")
    client = qdrant_client.QdrantClient(":memory:")
    adapter = QdrantAdapter(client)

    adapter.ensure_collection("papers", vector_size=3)
    adapter.ensure_payload_indexes(
        "papers",
        [QdrantPayloadIndexDTO(field_name="paper_id", field_schema="integer")],
    )
    adapter.upsert_point("papers", 1, [0.1, 0.2, 0.3], {"paper_id": 1})

    retrieved = adapter.retrieve("papers", [1], with_vectors=True)
    hits = adapter.search("papers", [0.1, 0.2, 0.3], top_k=1)

    assert adapter.exists("papers", 1) is True
    assert retrieved[0].id == 1
    assert retrieved[0].payload == {"paper_id": 1}
    assert hits[0].id == 1
    assert hits[0].payload == {"paper_id": 1}


def test_qdrant_adapter_scroll_points_restores_original_ids() -> None:
    qdrant_client = pytest.importorskip("qdrant_client")
    client = qdrant_client.QdrantClient(":memory:")
    adapter = QdrantAdapter(client)

    adapter.ensure_collection("clusters", vector_size=3)
    adapter.upsert_point(
        "clusters",
        "topic:1",
        [0.1, 0.2, 0.3],
        {"cluster_key": "topic:1"},
    )
    adapter.upsert_point(
        "clusters",
        "topic:2",
        [0.2, 0.3, 0.4],
        {"cluster_key": "topic:2"},
    )

    points = adapter.scroll_points("clusters", batch_size=1, with_vectors=False)

    assert {point.id for point in points} == {"topic:1", "topic:2"}
    assert {point.payload["cluster_key"] for point in points} == {"topic:1", "topic:2"}


class FakeClusterTaxonomyRepository:
    def __init__(self, paper_ids: list[int]) -> None:
        self.paper_ids = paper_ids

    def get_topic_by_id(self, topic_id: int) -> Any:
        return SimpleNamespace(id=topic_id, name=f"Topic {topic_id}")

    def list_paper_ids_by_topic(self, topic_id: int) -> list[int]:
        return self.paper_ids


class FakeClusterPaperRepository:
    def get_by_ids(self, paper_ids: list[int]) -> list[Any]:
        return [
            SimpleNamespace(
                id=paper_id,
                title=f"Paper {paper_id}",
                abstract=f"Abstract {paper_id}",
                publication_date=date(2025, 1, 1),
                cited_by_count=1,
            )
            for paper_id in paper_ids
        ]


class FakeClusterQdrantAdapter:
    def __init__(self) -> None:
        self.retrieve_calls: list[tuple[str, list[int | str], bool]] = []
        self.upserts: list[tuple[str, int | str, list[float], dict[str, Any]]] = []

    def retrieve(
        self,
        collection_name: str,
        point_ids: list[int | str],
        with_vectors: bool = False,
    ) -> list[QdrantPointDTO]:
        self.retrieve_calls.append((collection_name, point_ids, with_vectors))
        if collection_name == TREND_CLUSTERS_COLLECTION:
            return []
        return [
            QdrantPointDTO(
                id=point_id,
                vector=[float(point_id), 1.0, 0.5],
                payload={"paper_id": point_id, "keywords": ["chunked"]},
            )
            for point_id in point_ids
            if isinstance(point_id, int)
        ]

    def upsert_point(
        self,
        collection_name: str,
        point_id: int | str,
        vector: list[float],
        payload: dict[str, Any],
    ) -> None:
        self.upserts.append((collection_name, point_id, vector, payload))


class FakeClusterRedisAdapter:
    def __init__(self) -> None:
        self.values: dict[str, Any] = {}

    def get_json(self, key: str) -> Any:
        return self.values.get(key)

    def set_json(
        self,
        key: str,
        value: dict[str, Any],
        ttl_seconds: int | None = None,
    ) -> None:
        self.values[key] = value


class FakeSummaryFacade:
    def summarize_cluster(
        self,
        cluster_name: str,
        paper_titles: list[str],
        abstracts: list[str],
        top_keywords: list[str],
    ) -> ClusterSummaryDTO:
        return ClusterSummaryDTO(title="Semantic Cluster Name", summary="summary")


class FakeResearchClusterRepository:
    def __init__(self) -> None:
        self.cluster_payloads: list[dict[str, Any]] = []

    def upsert_cluster_from_payload(self, payload: dict[str, Any]) -> tuple[Any, bool]:
        self.cluster_payloads.append(payload)
        return SimpleNamespace(id=1, cluster_key=payload["cluster_key"]), True


def test_cluster_analytics_retrieves_qdrant_vectors_in_chunks() -> None:
    paper_ids = list(range(1, 452))
    qdrant_adapter = FakeClusterQdrantAdapter()
    cluster_repository = FakeResearchClusterRepository()
    facade = ClusterAnalyticsFacade(
        taxonomy_repository=FakeClusterTaxonomyRepository(paper_ids),  # type: ignore[arg-type]
        paper_repository=FakeClusterPaperRepository(),  # type: ignore[arg-type]
        paper_graph_repository=SimpleNamespace(),
        qdrant_adapter=qdrant_adapter,  # type: ignore[arg-type]
        redis_adapter=FakeClusterRedisAdapter(),  # type: ignore[arg-type]
        summary_facade=FakeSummaryFacade(),  # type: ignore[arg-type]
        research_cluster_repository=cluster_repository,  # type: ignore[arg-type]
    )

    cluster = facade.recompute_cluster("topic:1")
    paper_retrieve_sizes = [
        len(point_ids)
        for collection_name, point_ids, with_vectors in qdrant_adapter.retrieve_calls
        if collection_name == PAPERS_COLLECTION and with_vectors
    ]

    assert cluster.cluster_key == "topic:1"
    assert cluster.name == "Semantic Cluster Name"
    assert len(paper_retrieve_sizes) > 1
    assert max(paper_retrieve_sizes) <= 256
    assert qdrant_adapter.upserts[0][3]["indexed_paper_count"] == len(paper_ids)
    assert qdrant_adapter.upserts[0][3]["source_topic_name"] == "Topic 1"
    assert cluster_repository.cluster_payloads[0]["cluster_key"] == "topic:1"


class FakeSyncQdrantAdapter:
    def __init__(self) -> None:
        self.cluster_points = [
            QdrantPointDTO(
                id="topic:1",
                vector=[],
                payload={
                    "cluster_key": "topic:1",
                    "cluster_type": "topic",
                    "name": "Topic 1",
                    "trend_score": 0.5,
                },
            )
        ]
        self.period_points = [
            QdrantPointDTO(
                id="topic:1:month:2026-01-01",
                vector=[],
                payload={
                    "cluster_id": "topic:1",
                    "cluster_key": "topic:1",
                    "cluster_name": "Topic 1",
                    "period_start": "2026-01-01",
                    "period_end": "2026-01-31",
                    "paper_count": 10,
                },
            )
        ]

    def scroll_points(
        self,
        collection_name: str,
        *,
        batch_size: int = 256,
        with_vectors: bool = False,
        filters: dict[str, Any] | None = None,
    ) -> list[QdrantPointDTO]:
        if collection_name == TREND_CLUSTERS_COLLECTION:
            return self.cluster_points
        return self.period_points

    def retrieve(
        self,
        collection_name: str,
        point_ids: list[int | str],
        with_vectors: bool = False,
    ) -> list[QdrantPointDTO]:
        return [point for point in self.cluster_points if point.id in point_ids]


class FakeSyncClusterRepository:
    def __init__(self) -> None:
        self.clusters: set[str] = set()
        self.periods: set[tuple[str, str, str]] = set()

    def upsert_cluster_from_payload(self, payload: dict[str, Any]) -> tuple[Any, bool]:
        cluster_key = str(payload["cluster_key"])
        created = cluster_key not in self.clusters
        self.clusters.add(cluster_key)
        return SimpleNamespace(id=1, cluster_key=cluster_key), created

    def upsert_period_from_payload(self, payload: dict[str, Any]) -> tuple[Any, bool]:
        key = (
            str(payload["cluster_key"]),
            str(payload["period_start"]),
            str(payload["period_end"]),
        )
        created = key not in self.periods
        self.periods.add(key)
        return SimpleNamespace(id=1), created

    def delete_clusters_not_in_keys(self, cluster_keys: set[str]) -> int:
        return 0

    def delete_period_stats_not_in_keys(
        self,
        period_keys: set[tuple[str, date, date]],
        *,
        cluster_key: str | None = None,
    ) -> int:
        return 0


def test_cluster_db_sync_facade_syncs_qdrant_payloads() -> None:
    result = ClusterDbSyncFacade(
        qdrant_adapter=FakeSyncQdrantAdapter(),  # type: ignore[arg-type]
        research_cluster_repository=FakeSyncClusterRepository(),  # type: ignore[arg-type]
    ).sync_all(batch_size=1, prune_missing=True)

    assert result.success is True
    assert result.details["created"] == 2
    assert result.details["failed"] == 0


class FakeKeywordEmbeddingAdapter:
    def __init__(self, vectors: dict[str, list[float]]) -> None:
        self.vectors = vectors
        self.calls: list[tuple[list[str], str]] = []

    def embed_text(self, text: str, model: str = "qwen3-embedding") -> Any:
        self.calls.append(([text], model))
        return SimpleNamespace(vector=self.vectors.get(text, []), model=model)

    def embed_batch(self, texts: list[str], model: str = "qwen3-embedding") -> list[Any]:
        self.calls.append((list(texts), model))
        return [
            SimpleNamespace(vector=self.vectors.get(text, []), model=model)
            for text in texts
        ]


def test_keyword_extraction_semantic_feature_uses_lmstudio_cosine() -> None:
    facade = object.__new__(KeywordExtractionFacade)
    facade.embedding_adapter = FakeKeywordEmbeddingAdapter(
        {
            "alpha": [1.0, 0.0],
            "beta": [0.0, 1.0],
            "alpha.": [1.0, 0.0],
            "beta.": [0.0, 1.0],
        }
    )
    facade.embedding_model = "qwen3-embedding"
    documents = pd.DataFrame({"id": ["doc"], "text": ["alpha. beta."]})
    features = pd.DataFrame(
        {
            "doc_id": ["doc", "doc"],
            "candidate": ["alpha", "beta"],
        }
    )

    result = facade._add_semantic_feature(documents, features)

    assert result["embedrank_best_sentence_cosine"].tolist() == [1.0, 1.0]
    assert facade.embedding_adapter.calls == [
        (["alpha", "beta"], "qwen3-embedding"),
        (["alpha.", "beta."], "qwen3-embedding"),
    ]


def test_keyword_candidate_features_use_pke_candidates_and_filter_stopword_edges() -> None:
    facade = object.__new__(KeywordExtractionFacade)
    facade._english_stopwords = {"and", "the", "of"}
    documents = pd.DataFrame(
        {
            "id": ["doc"],
            "text": ["Graph neural networks cover categories and applications."],
        }
    )
    pke_scores = {
        "doc": {
            "pke_yake_w8": [
                ("categories , and", 0.1),
                ("graph neural networks", 0.2),
            ],
            "pke_singlerank_w4": [
                ("the method", 0.3),
                ("applications", 0.4),
            ],
        }
    }

    features = facade._build_candidate_features(documents, pke_scores)

    assert set(features["candidate"].tolist()) == {
        "graph neural networks",
        "applications",
    }


def test_keyword_extraction_output_filters_score_before_top_k() -> None:
    facade = object.__new__(KeywordExtractionFacade)

    result = facade._apply_output_filters(
        [("a", 0.9), ("b", 0.2), ("c", 0.1)],
        top_k=2,
        min_score=0.15,
    )

    assert result == [("a", 0.9), ("b", 0.2)]


def test_keyword_extraction_rejects_non_english_metadata() -> None:
    facade = object.__new__(KeywordExtractionFacade)

    with pytest.raises(NotImplementedError):
        facade._resolve_english_metadata(
            KeywordExtractionMetadataDTO(title="Заголовок", language="ru")
        )


class FakeKeywordPaperRepository:
    def __init__(self) -> None:
        self.saved: dict[int, list[str]] = {}

    def save_extracted_keywords(self, keywords_by_paper_id: dict[int, list[str]]) -> None:
        self.saved.update(keywords_by_paper_id)


def test_keyword_extraction_batch_skips_non_english_when_requested() -> None:
    facade = object.__new__(KeywordExtractionFacade)
    facade.paper_repository = FakeKeywordPaperRepository()
    facade.event_sink = NoopEventSink()
    facade.paper_extraction_chunk_size = 25

    def fake_extract_batch(request: Any) -> list[KeywordExtractionResponseDTO]:
        return [
            KeywordExtractionResponseDTO(
                paper_id=metadata.paper_id,
                keywords=["graph"],
            )
            for metadata in request.items
        ]

    facade.extract_batch = fake_extract_batch  # type: ignore[method-assign]
    result = facade._extract_loaded_papers(
        [
            SimpleNamespace(id=1, title="English", abstract="Abstract", language="en"),
            SimpleNamespace(id=2, title="Russian", abstract="Аннотация", language="ru"),
        ],
        top_k=10,
        min_score=None,
        skip_non_english=True,
    )

    assert result.updated == 1
    assert result.skipped == 1
    assert facade.paper_repository.saved == {1: ["graph"]}


def test_keyword_extraction_loaded_papers_updates_progress_by_internal_chunks() -> None:
    facade = object.__new__(KeywordExtractionFacade)
    facade.paper_repository = FakeKeywordPaperRepository()
    facade.event_sink = InMemoryEventSink()
    facade.paper_extraction_chunk_size = 2
    chunk_sizes: list[int] = []

    def fake_extract_batch(request: Any) -> list[KeywordExtractionResponseDTO]:
        chunk_sizes.append(len(request.items))
        return [
            KeywordExtractionResponseDTO(
                paper_id=metadata.paper_id,
                keywords=[f"kw-{metadata.paper_id}"],
            )
            for metadata in request.items
        ]

    facade.extract_batch = fake_extract_batch  # type: ignore[method-assign]
    result = facade._extract_loaded_papers(
        [
            SimpleNamespace(id=1, title="One", abstract="Abstract", language="en"),
            SimpleNamespace(id=2, title="Two", abstract="Abstract", language="en"),
            SimpleNamespace(id=3, title="Three", abstract="Abstract", language="en"),
        ],
        top_k=10,
        min_score=None,
        skip_non_english=False,
    )

    progress_events = [
        event
        for event in facade.event_sink.events
        if event.event_type == "keyword_extraction_progress"
    ]
    assert result.updated == 3
    assert chunk_sizes == [2, 1]
    assert [event.entity_id for event in progress_events] == ["batch", "batch"]
    assert [event.current for event in progress_events] == [2, 3]
    assert facade.paper_repository.saved == {
        1: ["kw-1"],
        2: ["kw-2"],
        3: ["kw-3"],
    }


def test_paper_indexing_merges_extracted_keywords_without_keyword_ids() -> None:
    facade = object.__new__(PaperIndexingFacade)

    result = facade._merge_keyword_values(
        ["Graph Learning", "AI"],
        ["ai", {"keyword": "Embeddings"}, {"value": "Ranking"}],
    )

    assert result == ["Graph Learning", "AI", "Embeddings", "Ranking"]


def test_lmstudio_embedding_adapter_validates_response() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/v1/embeddings"
        return httpx.Response(
            200,
            json={
                "data": [{"embedding": [0.1, 0.2, 0.3]}],
                "model": "qwen3-embedding",
                "usage": {"total_tokens": 12},
            },
        )

    client = httpx.Client(transport=httpx.MockTransport(handler))
    result = LMStudioEmbeddingAdapter("http://lmstudio", client=client).embed_text("text")

    assert result.vector == [0.1, 0.2, 0.3]
    assert result.dimension == 3
    assert result.token_count == 12


def test_lmstudio_embedding_adapter_rejects_invalid_vectors() -> None:
    client = httpx.Client(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, json={"data": [{"embedding": []}]})
        )
    )

    with pytest.raises(EmbeddingGenerationError):
        LMStudioEmbeddingAdapter("http://lmstudio", client=client).embed_text("text")


def test_lmstudio_chat_adapter_supports_json_response_format() -> None:
    captured_payload: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured_payload.update(json.loads(request.content.decode("utf-8")))
        return httpx.Response(
            200,
            json={"choices": [{"message": {"content": '{"summary": "ok"}'}}]},
        )

    client = httpx.Client(
        transport=httpx.MockTransport(handler)
    )

    result = LMStudioChatAdapter("http://lmstudio", client=client).summarize_cluster(
        "Graph learning",
        ["Abstract"],
    )

    assert result == {"summary": "ok"}
    assert captured_payload["response_format"]["type"] == "json_schema"
    assert captured_payload["response_format"]["json_schema"]["name"] == "json_response"


def test_lmstudio_chat_adapter_rejects_invalid_json_when_requested() -> None:
    client = httpx.Client(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200,
                json={"choices": [{"message": {"content": "not-json"}}]},
            )
        )
    )

    with pytest.raises(LLMGenerationError):
        LMStudioChatAdapter("http://lmstudio", client=client).chat_completion(
            [{"role": "user", "content": "x"}],
            response_format={"type": "json_object"},
        )


def test_openalex_adapter_normalizes_work_response() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "id": "https://openalex.org/W1",
                "doi": "https://doi.org/10.1/demo",
                "title": "Demo paper",
                "abstract_inverted_index": {"Hello": [0], "world": [1]},
                "publication_year": 2024,
                "authorships": [
                    {
                        "author": {"id": "A1", "display_name": "Ada"},
                        "institutions": [{"id": "I1", "display_name": "Lab"}],
                        "is_corresponding": True,
                    }
                ],
                "topics": [
                    {
                        "id": "T1",
                        "display_name": "AI",
                        "score": 0.8,
                        "domain": {"display_name": "Computer Science"},
                    }
                ],
                "keywords": [{"keyword": "embeddings", "score": 0.7}],
                "primary_location": {
                    "landing_page_url": "https://example.test/paper",
                    "pdf_url": "https://example.test/paper.pdf",
                },
                "open_access": {"is_oa": True},
                "cited_by_count": 5,
                "referenced_works_count": 9,
            },
        )

    adapter = OpenAlexAdapter(
        "https://openalex.test",
        client=httpx.Client(transport=httpx.MockTransport(handler)),
    )

    paper = adapter.get_work_by_doi("10.1/demo")

    assert paper is not None
    assert paper.title == "Demo paper"
    assert paper.abstract == "Hello world"
    assert paper.authors[0].display_name == "Ada"
    assert paper.institutions[0].display_name == "Lab"
    assert paper.topics[0].domain_name == "Computer Science"
    assert paper.keywords[0].value == "embeddings"
    assert paper.landings[0].landing_url == "https://example.test/paper"
    assert paper.references_count == 9
    assert paper.raw is not None


def test_openalex_adapter_count_works_uses_meta_count_and_topic_filter() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        params = request.url.params
        assert request.url.path == "/works"
        assert params["page"] == "1"
        assert params["per-page"] == "1"
        assert params["select"] == "id"
        filter_value = params["filter"]
        assert "type:article" in filter_value
        assert "language:en" in filter_value
        assert "from_publication_date:2026-01-01" in filter_value
        assert "to_publication_date:2026-01-31" in filter_value
        assert "topics.id:T123" in filter_value
        return httpx.Response(200, json={"meta": {"count": 42}, "results": []})

    adapter = OpenAlexAdapter(
        "https://openalex.test",
        client=httpx.Client(transport=httpx.MockTransport(handler)),
    )

    count = adapter.count_works(
        OpenAlexSearchFiltersDTO(
            date_from=date(2026, 1, 1),
            date_to=date(2026, 1, 31),
            type="article",
            language="en",
        ),
        topic_external_id="https://openalex.org/T123",
    )

    assert count == 42


def test_openalex_adapter_adds_openalex_auth_params() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        params = request.url.params
        assert params["api_key"] == "secret"
        assert params["mailto"] == "dev@example.test"
        return httpx.Response(200, json={"meta": {"count": 1}, "results": []})

    adapter = OpenAlexAdapter(
        "https://openalex.test",
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        api_key="secret",
        mailto="dev@example.test",
    )

    assert adapter.count_works(OpenAlexSearchFiltersDTO(type="article")) == 1


def test_openalex_adapter_count_maps_429_to_rate_limit() -> None:
    adapter = OpenAlexAdapter(
        "https://openalex.test",
        client=httpx.Client(
            transport=httpx.MockTransport(lambda request: httpx.Response(429))
        ),
    )

    with pytest.raises(ExternalServiceRateLimitError):
        adapter.count_works(OpenAlexSearchFiltersDTO())
