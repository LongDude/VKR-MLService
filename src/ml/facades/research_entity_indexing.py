from __future__ import annotations

from collections.abc import Callable
from datetime import date, datetime, timedelta, timezone
from numbers import Real
from typing import Any

from adapters.lmstudio_embedding_adapter import LMStudioEmbeddingAdapter
from adapters.qdrant_adapter import QdrantAdapter
from core.exceptions import (
    AppError,
    EmbeddingGenerationError,
    EntityNotFoundError,
    InvalidRequestError,
)
from dto.common import BatchOperationResultDTO, OperationResultDTO
from dto.qdrant import QdrantPointDTO
from repositories.graph import PaperGraphRepository
from repositories.taxonomy import TaxonomyRepository

from ml.constants import DEFAULT_EMBEDDING_MODEL, RESEARCH_ENTITIES_COLLECTION
from ml.services.events import EventSink, MLEvent, NoopEventSink
from ml.services.qdrant_payloads import QdrantPayloadBuilder
from ml.services.text_preparation import TextPreparationService


DEFAULT_RECENT_WINDOW_DAYS = 365
DEFAULT_EMBEDDING_VERSION = "v1"


class ResearchEntityIndexingFacade:
    """Facade for indexing taxonomy and keyword reference entities into Qdrant."""

    def __init__(
        self,
        *,
        taxonomy_repository: TaxonomyRepository,
        paper_graph_repository: PaperGraphRepository,
        embedding_adapter: LMStudioEmbeddingAdapter,
        qdrant_adapter: QdrantAdapter,
        text_preparation_service: TextPreparationService | None = None,
        payload_builder: QdrantPayloadBuilder | None = None,
        collection_name: str = RESEARCH_ENTITIES_COLLECTION,
        embedding_model: str = DEFAULT_EMBEDDING_MODEL,
        embedding_version: str = DEFAULT_EMBEDDING_VERSION,
        recent_window_days: int = DEFAULT_RECENT_WINDOW_DAYS,
        event_sink: EventSink | None = None,
    ) -> None:
        self.taxonomy_repository = taxonomy_repository
        self.paper_graph_repository = paper_graph_repository
        self.embedding_adapter = embedding_adapter
        self.qdrant_adapter = qdrant_adapter
        self.text_preparation_service = (
            text_preparation_service or TextPreparationService()
        )
        self.payload_builder = payload_builder or QdrantPayloadBuilder()
        self.collection_name = collection_name
        self.embedding_model = embedding_model
        self.embedding_version = embedding_version
        self.recent_window_days = recent_window_days
        self.event_sink = event_sink or NoopEventSink()

    def index_all_entities(
        self,
        force_reindex: bool = False,
        limit: int | None = None,
        offset: int = 0,
        entity_type: str = "all",
        batch_size: int = 128,
    ) -> BatchOperationResultDTO:
        """Index reference entities into ``research_entities_v1``.

        The method supports all taxonomy levels or one selected entity type.
        Entities are read from PostgreSQL in pages, embedded through
        ``LMStudioEmbeddingAdapter.embed_batch``, and written to Qdrant through
        ``upsert_points``. Existing points are skipped unless ``force_reindex``
        is true.
        """
        if limit is not None and limit < 0:
            raise InvalidRequestError(
                "limit must be non-negative",
                details={"limit": limit},
            )
        if offset < 0:
            raise InvalidRequestError(
                "offset must be non-negative",
                details={"offset": offset},
            )
        if batch_size <= 0:
            raise InvalidRequestError(
                "batch_size must be positive",
                details={"batch_size": batch_size},
            )
        if entity_type not in self._supported_entity_types():
            raise InvalidRequestError(
                "Unsupported research entity type",
                details={
                    "entity_type": entity_type,
                    "supported": sorted(self._supported_entity_types()),
                },
            )

        result = BatchOperationResultDTO()
        self._emit(
            "entity_indexing_started",
            entity_id=entity_type,
            stage="started",
            current=0,
            total=limit,
            message=f"Starting research entity indexing: {entity_type}",
            payload={"offset": offset, "batch_size": batch_size},
        )
        remaining = limit
        offset_remaining = offset

        for current_type, list_method in self._selected_entity_sources(entity_type):
            source_offset = offset_remaining
            if entity_type == "all" and offset_remaining:
                entity_count = self._entity_count(current_type)
                if entity_count is not None and offset_remaining >= entity_count:
                    offset_remaining -= entity_count
                    continue
                offset_remaining = 0
            else:
                offset_remaining = 0

            while True:
                if remaining is not None and remaining <= 0:
                    self._emit(
                        "entity_indexing_completed",
                        entity_id=entity_type,
                        stage="completed",
                        current=result.total,
                        total=result.total,
                        message=(
                            "Research entity indexing completed: "
                            f"updated={result.updated} skipped={result.skipped} "
                            f"failed={result.failed}"
                        ),
                        payload=result.model_dump(mode="json"),
                    )
                    return result
                page_size = batch_size if remaining is None else min(batch_size, remaining)
                entities = list_method(page_size, source_offset)
                if not entities:
                    break

                batch_result = self._index_entity_batch(
                    current_type,
                    entities,
                    force_reindex=force_reindex,
                )
                self._merge_batch_result(result, batch_result)
                self._emit(
                    "entity_indexing_progress",
                    entity_id=current_type,
                    stage="batches",
                    current=result.total,
                    total=limit,
                    message=(
                        f"{current_type}: updated={result.updated} "
                        f"skipped={result.skipped} failed={result.failed}"
                    ),
                    payload=batch_result.model_dump(mode="json"),
                )

                loaded_count = len(entities)
                source_offset += loaded_count
                if remaining is not None:
                    remaining -= loaded_count
                if loaded_count < page_size:
                    break

        self._emit(
            "entity_indexing_completed",
            entity_id=entity_type,
            stage="completed",
            current=result.total,
            total=result.total,
            message=(
                f"Research entity indexing completed: updated={result.updated} "
                f"skipped={result.skipped} failed={result.failed}"
            ),
            payload=result.model_dump(mode="json"),
        )
        return result

    def index_topic(
        self,
        topic_id: int,
        force_reindex: bool = False,
    ) -> OperationResultDTO:
        """Index one topic entity by id."""
        topic = self.taxonomy_repository.get_topic_by_id(topic_id)
        if topic is None:
            raise EntityNotFoundError(
                "Topic not found",
                details={"topic_id": topic_id},
            )
        return self._index_entity("topic", topic, force_reindex=force_reindex)

    def index_keyword(
        self,
        keyword_id: int,
        force_reindex: bool = False,
    ) -> OperationResultDTO:
        """Index one keyword entity by id."""
        keyword = self.taxonomy_repository.get_keyword_by_id(keyword_id)
        if keyword is None:
            raise EntityNotFoundError(
                "Keyword not found",
                details={"keyword_id": keyword_id},
            )
        return self._index_entity("keyword", keyword, force_reindex=force_reindex)

    def _selected_entity_sources(
        self,
        entity_type: str,
    ) -> list[tuple[str, Callable[[int, int], list[Any]]]]:
        sources = self._entity_sources()
        if entity_type == "all":
            return sources
        return [(name, method) for name, method in sources if name == entity_type]

    def _entity_sources(self) -> list[tuple[str, Callable[[int, int], list[Any]]]]:
        return [
            ("domain", self.taxonomy_repository.list_domains),
            ("field", self.taxonomy_repository.list_fields),
            ("subfield", self.taxonomy_repository.list_subfields),
            ("topic", self.taxonomy_repository.list_topics),
            ("keyword", self.taxonomy_repository.list_keywords),
        ]

    def _supported_entity_types(self) -> set[str]:
        return {"all", "domain", "field", "subfield", "topic", "keyword"}

    def _entity_count(self, entity_type: str) -> int | None:
        method_name = {
            "domain": "count_domains",
            "field": "count_fields",
            "subfield": "count_subfields",
            "topic": "count_topics",
            "keyword": "count_keywords",
        }[entity_type]
        method = getattr(self.taxonomy_repository, method_name, None)
        if not callable(method):
            return None
        return int(method())

    def _index_entity_batch(
        self,
        entity_type: str,
        entities: list[Any],
        *,
        force_reindex: bool,
    ) -> BatchOperationResultDTO:
        result = BatchOperationResultDTO(total=len(entities))
        if not entities:
            return result

        records: list[dict[str, Any]] = []
        for entity in entities:
            try:
                entity_id = self._entity_id(entity)
                name = self._entity_name(entity)
                hierarchy = self._build_hierarchy(entity_type, entity)
                embedding_context = self._embedding_context(entity_type, hierarchy)
                embedding_text = (
                    self.text_preparation_service.build_research_entity_embedding_text(
                        entity_type=entity_type,
                        name=name,
                        domain_name=embedding_context.get("domain_name"),
                        field_name=embedding_context.get("field_name"),
                        subfield_name=embedding_context.get("subfield_name"),
                    )
                )
                records.append(
                    {
                        "entity": entity,
                        "entity_id": entity_id,
                        "name": name,
                        "point_id": self._point_id(entity_type, entity_id),
                        "hierarchy": hierarchy,
                        "embedding_text": embedding_text,
                    }
                )
            except AppError as exc:
                result.failed += 1
                result.errors.append(
                    self._error_payload(exc, entity_type, getattr(entity, "id", None))
                )

        if not records:
            return result

        existing_point_ids = self._existing_point_ids(
            [record["point_id"] for record in records],
            force_reindex=force_reindex,
        )
        pending_records = [
            record for record in records if record["point_id"] not in existing_point_ids
        ]
        result.skipped += len(records) - len(pending_records)
        if not pending_records:
            return result

        self._emit(
            "entity_embedding_started",
            entity_id=entity_type,
            stage="embedding",
            current=0,
            total=len(pending_records),
            message=f"Generating {len(pending_records)} {entity_type} embeddings",
        )
        embeddings = self.embedding_adapter.embed_batch(
            [record["embedding_text"] for record in pending_records],
            model=self.embedding_model,
        )
        self._emit(
            "entity_embedding_completed",
            entity_id=entity_type,
            stage="embedding",
            current=len(pending_records),
            total=len(pending_records),
            message=f"{entity_type} embeddings generated",
        )
        if len(embeddings) != len(pending_records):
            raise EmbeddingGenerationError(
                "Embedding response length does not match batch size",
                details={
                    "expected": len(pending_records),
                    "actual": len(embeddings),
                },
            )

        points: list[QdrantPointDTO] = []
        for record, embedding in zip(pending_records, embeddings, strict=True):
            if embedding is None or not embedding.vector:
                result.failed += 1
                result.errors.append(
                    {
                        "entity_type": entity_type,
                        "entity_id": record["entity_id"],
                        "code": EmbeddingGenerationError.code,
                        "message": "Embedding vector was not generated",
                        "details": {
                            "entity_type": entity_type,
                            "entity_id": record["entity_id"],
                        },
                    }
                )
                continue

            payload = self.payload_builder.build_research_entity_payload(
                record["entity"],
                entity_type=entity_type,
                entity_id=record["entity_id"],
                name=record["name"],
                **record["hierarchy"],
                **self._build_count_payload(entity_type, record["entity_id"]),
                embedding_model=embedding.model or self.embedding_model,
                embedding_version=self.embedding_version,
                indexed_at=datetime.now(timezone.utc),
            )
            points.append(
                QdrantPointDTO(
                    id=record["point_id"],
                    vector=embedding.vector,
                    payload=payload,
                )
            )

        if points:
            self.qdrant_adapter.upsert_points(self.collection_name, points)
            result.updated += len(points)
            self._emit(
                "entity_qdrant_upsert_completed",
                entity_id=entity_type,
                stage="qdrant_upsert",
                current=len(points),
                total=len(pending_records),
                message=f"Upserted {len(points)} {entity_type} points",
                payload={"collection": self.collection_name},
            )

        return result

    def _existing_point_ids(
        self,
        point_ids: list[str],
        *,
        force_reindex: bool,
    ) -> set[str]:
        if force_reindex or not point_ids:
            return set()
        points = self.qdrant_adapter.retrieve(
            self.collection_name,
            point_ids,
            with_vectors=False,
        )
        return {str(point.id) for point in points}

    def _index_entity(
        self,
        entity_type: str,
        entity: Any,
        *,
        force_reindex: bool,
    ) -> OperationResultDTO:
        entity_id = self._entity_id(entity)
        name = self._entity_name(entity)
        point_id = self._point_id(entity_type, entity_id)

        if not force_reindex and self.qdrant_adapter.exists(
            self.collection_name,
            point_id,
        ):
            return OperationResultDTO(
                success=True,
                message="Research entity is already indexed; skipped",
                details={
                    "entity_type": entity_type,
                    "entity_id": entity_id,
                    "point_id": point_id,
                    "skipped": True,
                },
            )

        hierarchy = self._build_hierarchy(entity_type, entity)
        embedding_context = self._embedding_context(entity_type, hierarchy)
        embedding_text = (
            self.text_preparation_service.build_research_entity_embedding_text(
                entity_type=entity_type,
                name=name,
                domain_name=embedding_context.get("domain_name"),
                field_name=embedding_context.get("field_name"),
                subfield_name=embedding_context.get("subfield_name"),
            )
        )
        embedding = self.embedding_adapter.embed_text(
            embedding_text,
            model=self.embedding_model,
        )
        if embedding is None or not embedding.vector:
            raise EmbeddingGenerationError(
                "Embedding vector was not generated",
                details={"entity_type": entity_type, "entity_id": entity_id},
            )

        payload = self.payload_builder.build_research_entity_payload(
            entity,
            entity_type=entity_type,
            entity_id=entity_id,
            name=name,
            **hierarchy,
            **self._build_count_payload(entity_type, entity_id),
            embedding_model=embedding.model or self.embedding_model,
            embedding_version=self.embedding_version,
            indexed_at=datetime.now(timezone.utc),
        )
        self.qdrant_adapter.upsert_point(
            self.collection_name,
            point_id,
            embedding.vector,
            payload,
        )
        self._emit(
            "entity_indexing_completed",
            entity_id=point_id,
            stage="completed",
            message="Research entity indexed successfully",
            payload={"entity_type": entity_type, "entity_id": entity_id},
        )

        return OperationResultDTO(
            success=True,
            message="Research entity indexed successfully",
            details={
                "entity_type": entity_type,
                "entity_id": entity_id,
                "point_id": point_id,
                "skipped": False,
            },
        )

    def _build_hierarchy(self, entity_type: str, entity: Any) -> dict[str, Any]:
        entity_id = self._entity_id(entity)
        name = self._entity_name(entity)
        if entity_type == "domain":
            return {"domain_id": entity_id, "domain_name": name}
        if entity_type == "field":
            domain = self._domain_for_field(entity)
            return {
                "domain_id": getattr(entity, "domain_id", None),
                "domain_name": self._optional_name(domain),
                "field_id": entity_id,
                "field_name": name,
            }
        if entity_type == "subfield":
            field = self._field_for_subfield(entity)
            domain = self._domain_for_field(field) if field is not None else None
            return {
                "domain_id": getattr(field, "domain_id", None) if field else None,
                "domain_name": self._optional_name(domain),
                "field_id": getattr(entity, "field_id", None),
                "field_name": self._optional_name(field),
                "subfield_id": entity_id,
                "subfield_name": name,
            }
        if entity_type == "topic":
            subfield = self._subfield_for_topic(entity)
            field = self._field_for_subfield(subfield) if subfield is not None else None
            domain = self._domain_for_field(field) if field is not None else None
            return {
                "domain_id": getattr(field, "domain_id", None) if field else None,
                "domain_name": self._optional_name(domain),
                "field_id": getattr(subfield, "field_id", None) if subfield else None,
                "field_name": self._optional_name(field),
                "subfield_id": getattr(entity, "subfield_id", None),
                "subfield_name": self._optional_name(subfield),
            }
        return {}

    def _build_count_payload(self, entity_type: str, entity_id: int) -> dict[str, int]:
        payload: dict[str, int] = {}
        paper_count = self._paper_count(entity_type, entity_id)
        recent_paper_count = self._recent_paper_count(entity_type, entity_id)
        if paper_count is not None:
            payload["paper_count"] = paper_count
        if recent_paper_count is not None:
            payload["recent_paper_count"] = recent_paper_count
        return payload

    def _paper_count(self, entity_type: str, entity_id: int) -> int | None:
        if entity_type == "topic":
            list_method = getattr(
                self.taxonomy_repository,
                "list_paper_ids_by_topic",
                None,
            )
            if callable(list_method):
                return len(list_method(entity_id))
        list_method = getattr(
            self.taxonomy_repository,
            f"list_paper_ids_by_{entity_type}",
            None,
        )
        if callable(list_method):
            return len(list_method(entity_id))
        return self._call_count_method(
            [
                f"count_papers_by_{entity_type}",
                f"count_papers_by_{entity_type}_id",
                f"count_papers_for_{entity_type}",
            ],
            entity_id,
        )

    def _recent_paper_count(self, entity_type: str, entity_id: int) -> int | None:
        method_count = self._call_count_method(
            [
                f"count_recent_papers_by_{entity_type}",
                f"count_recent_papers_by_{entity_type}_id",
                f"count_recent_papers_for_{entity_type}",
            ],
            entity_id,
        )
        if method_count is not None:
            return method_count
        if entity_type != "topic":
            return None

        method = getattr(
            self.paper_graph_repository,
            "count_papers_by_topic_and_period",
            None,
        )
        if not callable(method):
            return None
        today = date.today()
        date_from = today - timedelta(days=self.recent_window_days)
        points = method(entity_id, date_from, today, "month")
        return sum(int(getattr(point, "count", 0)) for point in points)

    def _call_count_method(self, method_names: list[str], entity_id: int) -> int | None:
        for method_name in method_names:
            method = getattr(self.paper_graph_repository, method_name, None)
            if not callable(method):
                continue
            value = method(entity_id)
            if value is None:
                return None
            if isinstance(value, Real) and not isinstance(value, bool):
                return int(value)
            raise InvalidRequestError(
                "Repository count method returned a non-numeric value",
                details={"method": method_name, "value": value},
            )
        return None

    def _merge_batch_result(
        self,
        target: BatchOperationResultDTO,
        source: BatchOperationResultDTO,
    ) -> None:
        target.total += source.total
        target.created += source.created
        target.updated += source.updated
        target.skipped += source.skipped
        target.failed += source.failed
        target.errors.extend(source.errors)

    def _error_payload(
        self,
        exc: AppError,
        entity_type: str,
        entity_id: Any,
    ) -> dict[str, Any]:
        return {
            "entity_type": entity_type,
            "entity_id": entity_id,
            "code": exc.code,
            "message": exc.message,
            "details": exc.details or {},
        }

    def _domain_for_field(self, field: Any | None) -> Any | None:
        if field is None:
            return None
        domain = getattr(field, "domain", None)
        if domain is not None:
            return domain
        domain_id = getattr(field, "domain_id", None)
        if domain_id is None:
            return None
        return self.taxonomy_repository.get_domain_by_id(domain_id)

    def _field_for_subfield(self, subfield: Any | None) -> Any | None:
        if subfield is None:
            return None
        field = getattr(subfield, "field", None)
        if field is not None:
            return field
        field_id = getattr(subfield, "field_id", None)
        if field_id is None:
            return None
        return self.taxonomy_repository.get_field_by_id(field_id)

    def _subfield_for_topic(self, topic: Any | None) -> Any | None:
        if topic is None:
            return None
        subfield = getattr(topic, "subfield", None)
        if subfield is not None:
            return subfield
        subfield_id = getattr(topic, "subfield_id", None)
        if subfield_id is None:
            return None
        return self.taxonomy_repository.get_subfield_by_id(subfield_id)

    def _embedding_context(
        self,
        entity_type: str,
        hierarchy: dict[str, Any],
    ) -> dict[str, Any]:
        if entity_type == "domain":
            return {}
        if entity_type == "field":
            return {"domain_name": hierarchy.get("domain_name")}
        if entity_type == "subfield":
            return {
                "domain_name": hierarchy.get("domain_name"),
                "field_name": hierarchy.get("field_name"),
            }
        if entity_type == "topic":
            return {
                "domain_name": hierarchy.get("domain_name"),
                "field_name": hierarchy.get("field_name"),
                "subfield_name": hierarchy.get("subfield_name"),
            }
        return {}

    def _point_id(self, entity_type: str, entity_id: int) -> str:
        return f"{entity_type}:{entity_id}"

    def _entity_id(self, entity: Any) -> int:
        entity_id = getattr(entity, "id", None)
        if entity_id is None:
            raise InvalidRequestError("Research entity id is required")
        return int(entity_id)

    def _entity_name(self, entity: Any) -> str:
        name = getattr(entity, "name", None) or getattr(entity, "value", None)
        if name is None or not str(name).strip():
            raise InvalidRequestError(
                "Research entity name is required",
                details={"entity_id": getattr(entity, "id", None)},
            )
        return str(name).strip()

    def _optional_name(self, entity: Any | None) -> str | None:
        if entity is None:
            return None
        name = getattr(entity, "name", None) or getattr(entity, "value", None)
        return str(name).strip() if name else None

    def _emit(
        self,
        event_type: str,
        *,
        entity_id: str | int | None = None,
        stage: str | None = None,
        current: int | None = None,
        total: int | None = None,
        message: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> None:
        self.event_sink.emit(
            MLEvent(
                event_type=event_type,
                task_type="entity_indexing",
                entity_id=entity_id,
                stage=stage,
                current=current,
                total=total,
                message=message,
                payload=payload or {},
            )
        )


__all__ = ["ResearchEntityIndexingFacade"]
