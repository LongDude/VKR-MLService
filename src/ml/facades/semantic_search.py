from __future__ import annotations

import json
from datetime import date
from time import perf_counter
from typing import Any

from adapters.lmstudio_embedding_adapter import LMStudioEmbeddingAdapter
from adapters.openalex_adapter import OpenAlexAdapter
from adapters.qdrant_adapter import QdrantAdapter
from adapters.redis_adapter import RedisAdapter
from core.exceptions import EmbeddingGenerationError, InvalidRequestError
from dto.external import OpenAlexSearchFiltersDTO
from dto.search import (
    SearchFiltersDTO,
    SemanticSearchMLHitDTO,
    SemanticSearchMLResponseDTO,
    SemanticSearchRequestDTO,
)
from utils.hashing import calculate_text_hash

from ml.constants import DEFAULT_EMBEDDING_MODEL, PAPERS_COLLECTION


SEARCH_CACHE_TTL_SECONDS = 300


class SemanticSearchFacade:
    def __init__(
        self,
        *,
        embedding_adapter: LMStudioEmbeddingAdapter,
        qdrant_adapter: QdrantAdapter,
        openalex_adapter: OpenAlexAdapter | None = None,
        redis_adapter: RedisAdapter | None = None,
        collection_name: str = PAPERS_COLLECTION,
        embedding_model: str = DEFAULT_EMBEDDING_MODEL,
        search_cache_ttl_seconds: int = SEARCH_CACHE_TTL_SECONDS,
    ) -> None:
        self.embedding_adapter = embedding_adapter
        self.qdrant_adapter = qdrant_adapter
        self.openalex_adapter = openalex_adapter
        self.redis_adapter = redis_adapter
        self.collection_name = collection_name
        self.embedding_model = embedding_model
        self.search_cache_ttl_seconds = search_cache_ttl_seconds

    def search(
        self,
        request: SemanticSearchRequestDTO,
    ) -> SemanticSearchMLResponseDTO:
        query = self._validate_query(request.query)
        cache_key = self._cache_key(request, query)
        if request.cache_policy != "force_external_refresh":
            cached_response = self._get_cached_response(cache_key)
            if cached_response is not None:
                return cached_response

        started_at = perf_counter()
        embedding = self.embedding_adapter.embed_text(
            query,
            model=self.embedding_model,
        )
        if embedding is None or not embedding.vector:
            raise EmbeddingGenerationError(
                "Query embedding vector was not generated",
                details={"query": query},
            )

        qdrant_filter = self._build_qdrant_filter(request.filters)
        qdrant_hits = self.qdrant_adapter.search(
            self.collection_name,
            embedding.vector,
            request.top_k,
            filters=qdrant_filter,
        )
        hits = [
            SemanticSearchMLHitDTO(
                paper_id=self._paper_id_from_hit(hit.id, hit.payload),
                score=hit.score,
                payload=hit.payload,
            )
            for hit in qdrant_hits
        ]

        external_candidates = []
        if self._should_fetch_external(request, len(hits)):
            external_candidates = self._search_external(request, query)

        response = SemanticSearchMLResponseDTO(
            query=query,
            hits=hits,
            external_candidates=external_candidates,
            embedding_model=embedding.model or self.embedding_model,
            elapsed_ms=int((perf_counter() - started_at) * 1000),
        )
        self._set_cached_response(cache_key, response)
        return response

    def _build_qdrant_filter(
        self,
        filters: SearchFiltersDTO,
    ) -> dict[str, Any] | None:
        must: list[dict[str, Any]] = []

        if filters.publication_year_from is not None:
            must.append(
                {
                    "key": "publication_year",
                    "range": {"gte": filters.publication_year_from},
                }
            )
        if filters.publication_year_to is not None:
            must.append(
                {
                    "key": "publication_year",
                    "range": {"lte": filters.publication_year_to},
                }
            )
        if filters.date_from is not None:
            must.append(
                {
                    "key": "publication_date",
                    "range": {"gte": filters.date_from.isoformat()},
                }
            )
        if filters.date_to is not None:
            must.append(
                {
                    "key": "publication_date",
                    "range": {"lte": filters.date_to.isoformat()},
                }
            )

        self._append_any_filter(must, "domain_ids", filters.domain_ids)
        self._append_any_filter(must, "field_ids", filters.field_ids)
        self._append_any_filter(must, "subfield_ids", filters.subfield_ids)
        self._append_any_filter(must, "topic_ids", filters.topic_ids)
        self._append_any_filter(must, "keyword_ids", filters.keyword_ids)

        self._append_value_filter(must, "language", filters.language)
        self._append_value_filter(must, "is_open_access", filters.is_open_access)
        self._append_value_filter(must, "type", filters.type)

        return {"must": must} if must else None

    def _append_any_filter(
        self,
        must: list[dict[str, Any]],
        field_name: str,
        values: list[int],
    ) -> None:
        if values:
            must.append({"key": field_name, "match": {"any": values}})

    def _append_value_filter(
        self,
        must: list[dict[str, Any]],
        field_name: str,
        value: Any,
    ) -> None:
        if value is not None:
            must.append({"key": field_name, "match": {"value": value}})

    def _search_external(
        self,
        request: SemanticSearchRequestDTO,
        query: str,
    ):
        if self.openalex_adapter is None:
            return []
        filters = self._to_openalex_filters(request.filters)
        result = self.openalex_adapter.search_works(
            query,
            filters,
            page=1,
            per_page=request.top_k,
        )
        return result.items

    def _to_openalex_filters(
        self,
        filters: SearchFiltersDTO,
    ) -> OpenAlexSearchFiltersDTO:
        return OpenAlexSearchFiltersDTO(
            publication_year_from=filters.publication_year_from,
            publication_year_to=filters.publication_year_to,
            date_from=filters.date_from,
            date_to=filters.date_to,
            type=filters.type,
            language=filters.language,
            is_open_access=filters.is_open_access,
        )

    def _should_fetch_external(
        self,
        request: SemanticSearchRequestDTO,
        hit_count: int,
    ) -> bool:
        if request.cache_policy == "local_only":
            return False
        if request.cache_policy == "force_external_refresh":
            return True
        return hit_count < request.top_k

    def _paper_id_from_hit(
        self,
        point_id: int | str,
        payload: dict[str, Any],
    ) -> int | str:
        return payload.get("paper_id") or point_id

    def _validate_query(self, query: str) -> str:
        normalized = " ".join(query.split())
        if not normalized:
            raise InvalidRequestError("Search query must not be empty")
        return normalized

    def _cache_key(
        self,
        request: SemanticSearchRequestDTO,
        query: str,
    ) -> str:
        payload = request.model_dump(mode="json")
        payload["query"] = query
        serialized = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        return f"ml:semantic_search:{calculate_text_hash(serialized)}"

    def _get_cached_response(
        self,
        cache_key: str,
    ) -> SemanticSearchMLResponseDTO | None:
        if self.redis_adapter is None:
            return None
        cached = self.redis_adapter.get_json(cache_key)
        if cached is None:
            return None
        return SemanticSearchMLResponseDTO.model_validate(cached)

    def _set_cached_response(
        self,
        cache_key: str,
        response: SemanticSearchMLResponseDTO,
    ) -> None:
        if self.redis_adapter is None:
            return
        self.redis_adapter.set_json(
            cache_key,
            response.model_dump(mode="json"),
            ttl_seconds=self.search_cache_ttl_seconds,
        )


__all__ = ["SemanticSearchFacade"]
