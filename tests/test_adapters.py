from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import httpx
import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from adapters.lmstudio_chat_adapter import LMStudioChatAdapter
from adapters.lmstudio_embedding_adapter import LMStudioEmbeddingAdapter
from adapters.openalex_adapter import OpenAlexAdapter
from adapters.qdrant_adapter import QdrantAdapter
from adapters.redis_adapter import RedisAdapter
from core.exceptions import (
    EmbeddingGenerationError,
    ExternalServiceRateLimitError,
    InvalidRequestError,
    LLMGenerationError,
    RedisOperationError,
)
from dto.common import BatchOperationResultDTO
from dto.external import OpenAlexSearchFiltersDTO
from dto.papers import PaperBatchIndexingRequestDTO, PaperIndexingRequestDTO
from dto.qdrant import QdrantPayloadIndexDTO, QdrantPointDTO
from dto.search import SemanticSearchRequestDTO


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


def test_redis_adapter_wraps_decode_errors() -> None:
    client = FakeRedisClient()
    client.values["bad"] = "not-json"

    with pytest.raises(RedisOperationError):
        RedisAdapter(client).get_json("bad")


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
    client = httpx.Client(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200,
                json={"choices": [{"message": {"content": '{"summary": "ok"}'}}]},
            )
        )
    )

    result = LMStudioChatAdapter("http://lmstudio", client=client).summarize_cluster(
        "Graph learning",
        ["Abstract"],
    )

    assert result == {"summary": "ok"}


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
    assert paper.raw is not None


def test_openalex_adapter_maps_429_to_rate_limit() -> None:
    adapter = OpenAlexAdapter(
        "https://openalex.test",
        client=httpx.Client(
            transport=httpx.MockTransport(lambda request: httpx.Response(429))
        ),
    )

    with pytest.raises(ExternalServiceRateLimitError):
        adapter.search_works("x", OpenAlexSearchFiltersDTO())
