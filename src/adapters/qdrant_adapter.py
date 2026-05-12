from __future__ import annotations

from typing import Any

from core.exceptions import QdrantIndexError
from dto.qdrant import QdrantPayloadIndexDTO, QdrantPointDTO, QdrantSearchHitDTO

try:
    from qdrant_client import QdrantClient, models as qdrant_models
except ImportError:
    QdrantClient = None
    qdrant_models = None


class QdrantAdapter:
    def __init__(
        self,
        client: Any | None = None,
        *,
        url: str | None = None,
        host: str | None = None,
        port: int | None = None,
        api_key: str | None = None,
        timeout_seconds: float | None = None,
        prefer_grpc: bool = False,
    ) -> None:
        if client is None:
            if QdrantClient is None:
                raise QdrantIndexError(
                    "qdrant_client is not installed. Install qdrant-client to use QdrantAdapter.",
                    details={"qdrant_client_installed": False},
                )
            client_kwargs: dict[str, Any] = {"prefer_grpc": prefer_grpc}
            if url is not None:
                client_kwargs["url"] = url
            if host is not None:
                client_kwargs["host"] = host
            if port is not None:
                client_kwargs["port"] = port
            if api_key is not None:
                client_kwargs["api_key"] = api_key
            if timeout_seconds is not None:
                client_kwargs["timeout"] = timeout_seconds
            client = QdrantClient(**client_kwargs)
        self._client = client

    def ensure_collection(
        self,
        collection_name: str,
        vector_size: int,
        distance: str = "Cosine",
    ) -> None:
        try:
            if self._collection_exists(collection_name):
                return
            self._client.create_collection(
                collection_name=collection_name,
                vectors_config=self._vector_params(vector_size, distance),
            )
        except Exception as exc:
            raise self._error(
                f"Failed to ensure Qdrant collection {collection_name!r}",
                exc,
                collection_name=collection_name,
            ) from exc

    def ensure_payload_indexes(
        self,
        collection_name: str,
        indexes: list[QdrantPayloadIndexDTO],
    ) -> None:
        try:
            for index in indexes:
                self._client.create_payload_index(
                    collection_name=collection_name,
                    field_name=index.field_name,
                    field_schema=self._payload_schema(index.field_schema),
                )
        except Exception as exc:
            raise self._error(
                f"Failed to ensure Qdrant payload indexes for {collection_name!r}",
                exc,
                collection_name=collection_name,
            ) from exc

    def upsert_point(
        self,
        collection_name: str,
        point_id: int | str,
        vector: list[float],
        payload: dict[str, Any],
    ) -> None:
        self.upsert_points(
            collection_name,
            [QdrantPointDTO(id=point_id, vector=vector, payload=payload)],
        )

    def upsert_points(
        self,
        collection_name: str,
        points: list[QdrantPointDTO],
    ) -> None:
        try:
            self._client.upsert(
                collection_name=collection_name,
                points=[self._point_struct(point) for point in points],
            )
        except Exception as exc:
            raise self._error(
                f"Failed to upsert Qdrant points into {collection_name!r}",
                exc,
                collection_name=collection_name,
            ) from exc

    def search(
        self,
        collection_name: str,
        vector: list[float],
        top_k: int,
        filters: dict[str, Any] | None = None,
    ) -> list[QdrantSearchHitDTO]:
        try:
            if hasattr(self._client, "query_points"):
                result = self._client.query_points(
                    collection_name=collection_name,
                    query=vector,
                    limit=top_k,
                    query_filter=filters,
                    with_payload=True,
                    with_vectors=False,
                )
                result = getattr(result, "points", result)
            elif hasattr(self._client, "search"):
                result = self._client.search(
                    collection_name=collection_name,
                    query_vector=vector,
                    limit=top_k,
                    query_filter=filters,
                )
            else:
                raise RuntimeError("Qdrant client has neither query_points nor search method")
            return [self._to_search_hit(point) for point in result]
        except Exception as exc:
            raise self._error(
                f"Failed to search Qdrant collection {collection_name!r}",
                exc,
                collection_name=collection_name,
            ) from exc

    def retrieve(
        self,
        collection_name: str,
        point_ids: list[int | str],
        with_vectors: bool = False,
    ) -> list[QdrantPointDTO]:
        try:
            result = self._client.retrieve(
                collection_name=collection_name,
                ids=point_ids,
                with_vectors=with_vectors,
            )
            return [self._to_point(point) for point in result]
        except Exception as exc:
            raise self._error(
                f"Failed to retrieve Qdrant points from {collection_name!r}",
                exc,
                collection_name=collection_name,
            ) from exc

    def exists(
        self,
        collection_name: str,
        point_id: int | str,
    ) -> bool:
        return bool(self.retrieve(collection_name, [point_id], with_vectors=False))

    def _collection_exists(self, collection_name: str) -> bool:
        if hasattr(self._client, "collection_exists"):
            return bool(self._client.collection_exists(collection_name))
        try:
            self._client.get_collection(collection_name)
            return True
        except Exception:
            return False

    def _vector_params(self, vector_size: int, distance: str) -> Any:
        if qdrant_models is None:
            return {"size": vector_size, "distance": distance}
        distance_value = getattr(qdrant_models.Distance, distance.upper(), None)
        if distance_value is None:
            raise ValueError(f"Unsupported Qdrant distance: {distance!r}")
        return qdrant_models.VectorParams(size=vector_size, distance=distance_value)

    def _point_struct(self, point: QdrantPointDTO) -> Any:
        if qdrant_models is None:
            return point.model_dump()
        return qdrant_models.PointStruct(
            id=point.id,
            vector=point.vector,
            payload=point.payload,
        )

    def _payload_schema(self, field_schema: str) -> Any:
        if qdrant_models is None:
            return field_schema
        normalized = field_schema.lower()
        schema_map = {
            "keyword": qdrant_models.PayloadSchemaType.KEYWORD,
            "integer": qdrant_models.PayloadSchemaType.INTEGER,
            "int": qdrant_models.PayloadSchemaType.INTEGER,
            "float": qdrant_models.PayloadSchemaType.FLOAT,
            "bool": qdrant_models.PayloadSchemaType.BOOL,
            "boolean": qdrant_models.PayloadSchemaType.BOOL,
            "datetime": qdrant_models.PayloadSchemaType.DATETIME,
            "text": qdrant_models.PayloadSchemaType.TEXT,
        }
        try:
            return schema_map[normalized]
        except KeyError as exc:
            raise ValueError(f"Unsupported Qdrant payload schema: {field_schema!r}") from exc

    def _to_search_hit(self, point: Any) -> QdrantSearchHitDTO:
        return QdrantSearchHitDTO(
            id=self._get_attr(point, "id"),
            score=float(self._get_attr(point, "score", 0.0)),
            payload=self._get_attr(point, "payload", {}) or {},
            vector=self._get_attr(point, "vector", None),
        )

    def _to_point(self, point: Any) -> QdrantPointDTO:
        return QdrantPointDTO(
            id=self._get_attr(point, "id"),
            vector=self._get_attr(point, "vector", None) or [],
            payload=self._get_attr(point, "payload", {}) or {},
        )

    def _get_attr(self, value: Any, key: str, default: Any = None) -> Any:
        if isinstance(value, dict):
            return value.get(key, default)
        return getattr(value, key, default)

    def _error(self, message: str, exc: Exception, **details: Any) -> QdrantIndexError:
        if qdrant_models is None:
            details["qdrant_client_installed"] = False
        details["reason"] = str(exc)
        return QdrantIndexError(message, details=details)


__all__ = ["QdrantAdapter"]
