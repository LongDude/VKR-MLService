from __future__ import annotations

from adapters.qdrant_adapter import QdrantAdapter
from core.exceptions import InvalidRequestError
from dto.qdrant import QdrantPayloadIndexDTO
from ml.constants import (
    PAPERS_COLLECTION,
    RESEARCH_ENTITIES_COLLECTION,
    TREND_CLUSTER_PERIODS_COLLECTION,
    TREND_CLUSTERS_COLLECTION,
    USER_PROFILES_COLLECTION,
)


class QdrantCollectionInitializer:
    PAPERS_INDEXES = [
        ("paper_id", "integer"),
        ("doi", "keyword"),
        ("publication_year", "integer"),
        ("publication_date", "datetime"),
        ("language", "keyword"),
        ("type", "keyword"),
        ("is_open_access", "bool"),
        ("domain_ids", "integer"),
        ("field_ids", "integer"),
        ("subfield_ids", "integer"),
        ("topic_ids", "integer"),
        ("keyword_ids", "integer"),
        ("cited_by_count", "integer"),
    ]
    RESEARCH_ENTITIES_INDEXES = [
        ("entity_type", "keyword"),
        ("entity_id", "integer"),
        ("domain_id", "integer"),
        ("field_id", "integer"),
        ("subfield_id", "integer"),
        ("paper_count", "integer"),
        ("recent_paper_count", "integer"),
    ]
    TREND_CLUSTERS_INDEXES = [
        ("cluster_id", "keyword"),
        ("cluster_type", "keyword"),
        ("source_topic_id", "integer"),
        ("domain_id", "integer"),
        ("field_id", "integer"),
        ("subfield_id", "integer"),
        ("status", "keyword"),
        ("trend_score", "float"),
        ("growth_rate_30d", "float"),
        ("paper_count_30d", "integer"),
    ]
    TREND_CLUSTER_PERIODS_INDEXES = [
        ("cluster_id", "keyword"),
        ("period", "keyword"),
        ("period_start", "datetime"),
        ("period_end", "datetime"),
        ("paper_count", "integer"),
        ("growth_rate", "float"),
        ("semantic_drift", "float"),
    ]
    USER_PROFILES_INDEXES = [
        ("user_id", "integer"),
        ("updated_at", "datetime"),
    ]

    def __init__(
        self,
        qdrant_adapter: QdrantAdapter,
        *,
        distance: str = "Cosine",
    ) -> None:
        self.qdrant_adapter = qdrant_adapter
        self.distance = distance

    def ensure_all(self, vector_size: int) -> None:
        self.ensure_papers_collection(vector_size)
        self.ensure_research_entities_collection(vector_size)
        self.ensure_trend_clusters_collection(vector_size)
        self.ensure_trend_cluster_periods_collection(vector_size)
        self.ensure_user_profiles_collection(vector_size)

    def ensure_papers_collection(self, vector_size: int) -> None:
        self._ensure_collection(
            PAPERS_COLLECTION,
            vector_size,
            self.PAPERS_INDEXES,
        )

    def ensure_research_entities_collection(self, vector_size: int) -> None:
        self._ensure_collection(
            RESEARCH_ENTITIES_COLLECTION,
            vector_size,
            self.RESEARCH_ENTITIES_INDEXES,
        )

    def ensure_trend_clusters_collection(self, vector_size: int) -> None:
        self._ensure_collection(
            TREND_CLUSTERS_COLLECTION,
            vector_size,
            self.TREND_CLUSTERS_INDEXES,
        )

    def ensure_trend_cluster_periods_collection(self, vector_size: int) -> None:
        self._ensure_collection(
            TREND_CLUSTER_PERIODS_COLLECTION,
            vector_size,
            self.TREND_CLUSTER_PERIODS_INDEXES,
        )

    def ensure_user_profiles_collection(self, vector_size: int) -> None:
        self._ensure_collection(
            USER_PROFILES_COLLECTION,
            vector_size,
            self.USER_PROFILES_INDEXES,
        )

    def _ensure_collection(
        self,
        collection_name: str,
        vector_size: int,
        indexes: list[tuple[str, str]],
    ) -> None:
        if vector_size <= 0:
            raise InvalidRequestError(
                "vector_size must be positive",
                details={"vector_size": vector_size},
            )
        self.qdrant_adapter.ensure_collection(
            collection_name,
            vector_size=vector_size,
            distance=self.distance,
        )
        self.qdrant_adapter.ensure_payload_indexes(
            collection_name,
            [self._payload_index(field_name, field_schema) for field_name, field_schema in indexes],
        )

    def _payload_index(
        self,
        field_name: str,
        field_schema: str,
    ) -> QdrantPayloadIndexDTO:
        return QdrantPayloadIndexDTO(
            field_name=field_name,
            field_schema=field_schema,
        )


__all__ = ["QdrantCollectionInitializer"]
