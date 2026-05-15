from __future__ import annotations

from copy import deepcopy
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid5

from core.exceptions import QdrantIndexError
from dto.qdrant import QdrantPayloadIndexDTO, QdrantPointDTO, QdrantSearchHitDTO

try:
    from qdrant_client import QdrantClient, models as qdrant_models
except ImportError:
    QdrantClient = None
    qdrant_models = None


ORIGINAL_POINT_ID_PAYLOAD_KEY = "_ml_original_point_id"
POINT_ID_NAMESPACE_PREFIX = "vkr-mlservice:qdrant-point:"


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
                ids=[self._qdrant_point_id(point_id) for point_id in point_ids],
                with_vectors=with_vectors,
            )
            return [self._to_point(point) for point in result]
        except Exception as exc:
            raise self._error(
                f"Failed to retrieve Qdrant points from {collection_name!r}",
                exc,
                collection_name=collection_name,
            ) from exc

    def scroll_points(
        self,
        collection_name: str,
        *,
        batch_size: int = 256,
        with_vectors: bool = False,
        filters: dict[str, Any] | None = None,
    ) -> list[QdrantPointDTO]:
        """Read all points from a collection in pages without mutating Qdrant."""
        if batch_size <= 0:
            raise QdrantIndexError(
                "Qdrant scroll batch size must be positive",
                details={"collection_name": collection_name, "batch_size": batch_size},
            )
        try:
            points: list[QdrantPointDTO] = []
            offset: Any = None
            while True:
                result = self._client.scroll(
                    collection_name=collection_name,
                    scroll_filter=filters,
                    limit=batch_size,
                    offset=offset,
                    with_payload=True,
                    with_vectors=with_vectors,
                )
                page, next_offset = self._scroll_result(result)
                points.extend(self._to_point(point) for point in page)
                if next_offset is None:
                    break
                offset = next_offset
            return points
        except Exception as exc:
            raise self._error(
                f"Failed to scroll Qdrant points from {collection_name!r}",
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
        point_id = self._qdrant_point_id(point.id)
        payload = self._payload_with_original_id(point.payload, point.id)
        if qdrant_models is None:
            return {
                "id": point_id,
                "vector": point.vector,
                "payload": payload,
            }
        return qdrant_models.PointStruct(
            id=point_id,
            vector=point.vector,
            payload=payload,
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
        raw_payload = self._get_attr(point, "payload", {}) or {}
        return QdrantSearchHitDTO(
            id=self._public_point_id(self._get_attr(point, "id"), raw_payload),
            score=float(self._get_attr(point, "score", 0.0)),
            payload=self._public_payload(raw_payload),
            vector=self._get_attr(point, "vector", None),
        )

    def _to_point(self, point: Any) -> QdrantPointDTO:
        raw_payload = self._get_attr(point, "payload", {}) or {}
        return QdrantPointDTO(
            id=self._public_point_id(self._get_attr(point, "id"), raw_payload),
            vector=self._get_attr(point, "vector", None) or [],
            payload=self._public_payload(raw_payload),
        )

    def _scroll_result(self, result: Any) -> tuple[list[Any], Any]:
        if isinstance(result, tuple) and len(result) == 2:
            return list(result[0]), result[1]
        points = getattr(result, "points", None)
        next_offset = getattr(result, "next_page_offset", None)
        if points is not None:
            return list(points), next_offset
        if isinstance(result, dict):
            return list(result.get("points", [])), result.get("next_page_offset")
        return list(result), None

    def _qdrant_point_id(self, point_id: int | str) -> int | str:
        """Convert application point ids to Qdrant-compatible ids.

        Qdrant REST accepts only unsigned integers and UUID strings as point ids.
        ML-layer ids such as ``topic:120`` are therefore mapped to deterministic
        UUIDv5 values. The original id is still stored in payload and restored in
        adapter DTOs.
        """
        if isinstance(point_id, int):
            if point_id >= 0:
                return point_id
            return str(uuid5(NAMESPACE_URL, f"{POINT_ID_NAMESPACE_PREFIX}{point_id}"))

        value = str(point_id)
        if value.isdecimal():
            return int(value)
        try:
            return str(UUID(value))
        except ValueError:
            return str(uuid5(NAMESPACE_URL, f"{POINT_ID_NAMESPACE_PREFIX}{value}"))

    def _payload_with_original_id(
        self,
        payload: dict[str, Any],
        point_id: int | str,
    ) -> dict[str, Any]:
        qdrant_point_id = self._qdrant_point_id(point_id)
        if qdrant_point_id == point_id or str(qdrant_point_id) == str(point_id):
            return payload

        result = deepcopy(payload)
        result.setdefault(ORIGINAL_POINT_ID_PAYLOAD_KEY, str(point_id))
        return result

    def _public_point_id(
        self,
        raw_point_id: Any,
        payload: dict[str, Any],
    ) -> int | str:
        original = payload.get(ORIGINAL_POINT_ID_PAYLOAD_KEY)
        if original is not None:
            return str(original)
        if isinstance(raw_point_id, int):
            return raw_point_id
        return str(raw_point_id)

    def _public_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        if ORIGINAL_POINT_ID_PAYLOAD_KEY not in payload:
            return payload
        result = dict(payload)
        result.pop(ORIGINAL_POINT_ID_PAYLOAD_KEY, None)
        return result

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
