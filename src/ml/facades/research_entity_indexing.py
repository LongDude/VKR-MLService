from __future__ import annotations

from collections.abc import Callable, Iterator
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
from repositories.graph import PaperGraphRepository
from repositories.taxonomy import TaxonomyRepository

from ml.constants import DEFAULT_EMBEDDING_MODEL, RESEARCH_ENTITIES_COLLECTION
from ml.services.qdrant_payloads import QdrantPayloadBuilder
from ml.services.text_preparation import TextPreparationService


ENTITY_PAGE_SIZE = 500
DEFAULT_RECENT_WINDOW_DAYS = 365
DEFAULT_EMBEDDING_VERSION = "v1"


class ResearchEntityIndexingFacade:
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

    def index_all_entities(
        self,
        force_reindex: bool = False,
        limit: int | None = None,
    ) -> BatchOperationResultDTO:
        if limit is not None and limit < 0:
            raise InvalidRequestError(
                "limit must be non-negative",
                details={"limit": limit},
            )

        result = BatchOperationResultDTO()
        remaining = limit
        for entity_type, iterator_factory in self._entity_iterators():
            for entity in iterator_factory():
                if remaining is not None and remaining <= 0:
                    return result

                result.total += 1
                entity_id = self._entity_id(entity)
                try:
                    operation = self._index_entity(
                        entity_type,
                        entity,
                        force_reindex=force_reindex,
                    )
                except AppError as exc:
                    result.failed += 1
                    result.errors.append(
                        {
                            "entity_type": entity_type,
                            "entity_id": entity_id,
                            "code": exc.code,
                            "message": exc.message,
                            "details": exc.details or {},
                        }
                    )
                else:
                    if operation.details.get("skipped"):
                        result.skipped += 1
                    else:
                        result.updated += 1

                if remaining is not None:
                    remaining -= 1

        return result

    def index_topic(
        self,
        topic_id: int,
        force_reindex: bool = False,
    ) -> OperationResultDTO:
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
        keyword = self.taxonomy_repository.get_keyword_by_id(keyword_id)
        if keyword is None:
            raise EntityNotFoundError(
                "Keyword not found",
                details={"keyword_id": keyword_id},
            )
        return self._index_entity("keyword", keyword, force_reindex=force_reindex)

    def _entity_iterators(
        self,
    ) -> list[tuple[str, Callable[[], Iterator[Any]]]]:
        return [
            ("domain", lambda: self._iter_entities(self.taxonomy_repository.list_domains)),
            ("field", lambda: self._iter_entities(self.taxonomy_repository.list_fields)),
            (
                "subfield",
                lambda: self._iter_entities(self.taxonomy_repository.list_subfields),
            ),
            ("topic", lambda: self._iter_entities(self.taxonomy_repository.list_topics)),
            (
                "keyword",
                lambda: self._iter_entities(self.taxonomy_repository.list_keywords),
            ),
        ]

    def _iter_entities(
        self,
        list_method: Callable[[int, int], list[Any]],
    ) -> Iterator[Any]:
        offset = 0
        while True:
            entities = list_method(ENTITY_PAGE_SIZE, offset)
            if not entities:
                break
            yield from entities
            if len(entities) < ENTITY_PAGE_SIZE:
                break
            offset += ENTITY_PAGE_SIZE

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


__all__ = ["ResearchEntityIndexingFacade"]
