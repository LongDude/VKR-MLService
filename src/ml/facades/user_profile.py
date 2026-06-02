from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from adapters.qdrant_adapter import QdrantAdapter
from core.exceptions import InsufficientUserProfileDataError
from core.logging import get_logger, logged_call
from dto.qdrant import QdrantPointDTO
from dto.recommendations import UserProfileDTO
from ml.constants import (
    PAPERS_COLLECTION,
    RESEARCH_ENTITIES_COLLECTION,
    USER_PROFILES_COLLECTION,
)
from ml.services.events import EventSink, MLEvent, NoopEventSink
from ml.services.qdrant_payloads import QdrantPayloadBuilder
from ml.services.vector_math import VectorMathService
from repositories.favourites import FavouriteRepository
from repositories.taxonomy import TaxonomyRepository
from repositories.tracked_areas import TrackedAreaRepository

FAVOURITE_PAPER_WEIGHT = 0.45
TRACKED_TOPIC_WEIGHT = 0.25
TRACKED_FIELD_WEIGHT = 0.10
TRACKED_SUBFIELD_WEIGHT = 0.15
TRACKED_KEYWORD_WEIGHT = 0.10
TRACKED_DOMAIN_WEIGHT = 0.05
PROFILE_VERSION = "v1"
logger = get_logger(__name__)


class UserProfileFacade:
    """Build and read user vector profiles from favourites and tracked entities."""

    def __init__(
        self,
        *,
        favourite_repository: FavouriteRepository,
        tracked_area_repository: TrackedAreaRepository,
        taxonomy_repository: TaxonomyRepository,
        qdrant_adapter: QdrantAdapter,
        vector_math_service: VectorMathService | None = None,
        payload_builder: QdrantPayloadBuilder | None = None,
        event_sink: EventSink | None = None,
        papers_collection: str = PAPERS_COLLECTION,
        research_entities_collection: str = RESEARCH_ENTITIES_COLLECTION,
        user_profiles_collection: str = USER_PROFILES_COLLECTION,
    ) -> None:
        self.favourite_repository = favourite_repository
        self.tracked_area_repository = tracked_area_repository
        self.taxonomy_repository = taxonomy_repository
        self.qdrant_adapter = qdrant_adapter
        self.vector_math_service = vector_math_service or VectorMathService()
        self.payload_builder = payload_builder or QdrantPayloadBuilder()
        self.event_sink = event_sink or NoopEventSink()
        self.papers_collection = papers_collection
        self.research_entities_collection = research_entities_collection
        self.user_profiles_collection = user_profiles_collection

    @logged_call(logger, "user_profile_recompute")
    def recompute_user_profile(
        self,
        user_id: int,
    ) -> UserProfileDTO:
        """Recompute a user profile vector and upsert it into Qdrant."""
        self._emit(
            "user_profile_started",
            entity_id=user_id,
            stage="started",
            message="Starting user profile recompute",
        )
        sources = self._load_source_ids(user_id)
        if not self._has_any_source(sources):
            self._emit(
                "user_profile_failed",
                entity_id=user_id,
                stage="failed",
                message="User profile has no source data",
                payload={"source_counts": self._source_counts(sources)},
            )
            raise InsufficientUserProfileDataError(
                "User profile has no source data",
                details={
                    "user_id": user_id,
                    "source_counts": self._source_counts(sources),
                },
            )

        self._emit(
            "user_profile_sources_loaded",
            entity_id=user_id,
            stage="sources",
            message="Loaded user profile sources",
            payload={"source_counts": self._source_counts(sources)},
        )
        category_vectors = self._load_category_vectors(sources)
        weighted_vectors = self._weighted_vectors(category_vectors)
        if not weighted_vectors:
            self._emit(
                "user_profile_failed",
                entity_id=user_id,
                stage="failed",
                message="User profile has no available vectors",
                payload={
                    "source_counts": self._source_counts(sources),
                    "available_vector_counts": {
                        key: len(value) for key, value in category_vectors.items()
                    },
                },
            )
            raise InsufficientUserProfileDataError(
                "User profile has no available vectors",
                details={
                    "user_id": user_id,
                    "source_counts": self._source_counts(sources),
                    "available_vector_counts": {
                        key: len(value) for key, value in category_vectors.items()
                    },
                },
            )

        profile_vector = self.vector_math_service.weighted_mean_vector(weighted_vectors)
        updated_at = datetime.now(timezone.utc)
        source_counts = {
            **self._source_counts(sources),
            "available_vectors": {
                key: len(value) for key, value in category_vectors.items()
            },
        }
        profile = UserProfileDTO(
            user_id=user_id,
            source_counts=source_counts,
            vector_dimension=len(profile_vector),
            updated_at=updated_at,
        )
        payload = self.payload_builder.build_user_profile_payload(
            profile,
            profile_id=self._profile_point_id(user_id),
            profile_version=PROFILE_VERSION,
            vector_dimension=len(profile_vector),
            source="ml_service",
            tracked_domain_ids=sources["tracked_domain_ids"],
            tracked_field_ids=sources["tracked_field_ids"],
            tracked_subfield_ids=sources["tracked_subfield_ids"],
            tracked_topic_ids=sources["tracked_topic_ids"],
            tracked_keyword_ids=sources["tracked_keyword_ids"],
            favourite_paper_ids=sources["favourite_paper_ids"],
            updated_at=updated_at,
        )
        self.qdrant_adapter.upsert_point(
            self.user_profiles_collection,
            self._profile_point_id(user_id),
            profile_vector,
            payload,
        )
        self._emit(
            "user_profile_completed",
            entity_id=user_id,
            stage="completed",
            message="User profile recompute completed",
            payload={
                "vector_dimension": len(profile_vector),
                "source_counts": source_counts,
            },
        )
        return profile

    def get_user_profile_vector(
        self,
        user_id: int,
        recompute_if_missing: bool = True,
    ) -> list[float]:
        """Return a stored user profile vector, recomputing it when requested."""
        points = self.qdrant_adapter.retrieve(
            self.user_profiles_collection,
            [self._profile_point_id(user_id)],
            with_vectors=True,
        )
        if points and points[0].vector:
            return points[0].vector
        if not recompute_if_missing:
            raise InsufficientUserProfileDataError(
                "User profile vector is missing",
                details={"user_id": user_id},
            )
        self.recompute_user_profile(user_id)
        points = self.qdrant_adapter.retrieve(
            self.user_profiles_collection,
            [self._profile_point_id(user_id)],
            with_vectors=True,
        )
        if points and points[0].vector:
            return points[0].vector
        raise InsufficientUserProfileDataError(
            "User profile vector could not be retrieved after recompute",
            details={"user_id": user_id},
        )

    def has_sufficient_profile_data(
        self,
        user_id: int,
    ) -> bool:
        """Return whether a user has at least one configured profile source."""
        return self._has_any_source(self._load_source_ids(user_id))

    def _load_source_ids(self, user_id: int) -> dict[str, list[int]]:
        return {
            "favourite_paper_ids": self.favourite_repository.list_paper_ids(user_id),
            "tracked_domain_ids": self.tracked_area_repository.list_domain_ids(user_id),
            "tracked_field_ids": self.tracked_area_repository.list_field_ids(user_id),
            "tracked_subfield_ids": self.tracked_area_repository.list_subfield_ids(
                user_id
            ),
            "tracked_topic_ids": self.tracked_area_repository.list_topic_ids(user_id),
            "tracked_keyword_ids": self.tracked_area_repository.list_keyword_ids(
                user_id
            ),
        }

    def _load_category_vectors(
        self,
        sources: dict[str, list[int]],
    ) -> dict[str, list[list[float]]]:
        return {
            "favourite_papers": self._paper_vectors(
                sources["favourite_paper_ids"],
            ),
            "tracked_domains": self._entity_vectors(
                "domain",
                sources["tracked_domain_ids"],
            ),
            "tracked_fields": self._entity_vectors(
                "field",
                sources["tracked_field_ids"],
            ),
            "tracked_subfields": self._entity_vectors(
                "subfield",
                sources["tracked_subfield_ids"],
            ),
            "tracked_topics": self._entity_vectors(
                "topic",
                sources["tracked_topic_ids"],
            ),
            "tracked_keywords": self._entity_vectors(
                "keyword",
                sources["tracked_keyword_ids"],
            ),
        }

    def _entity_vectors(
        self,
        entity_type: str,
        entity_ids: list[int],
    ) -> list[list[float]]:
        if not entity_ids:
            return []
        point_ids = [f"{entity_type}:{entity_id}" for entity_id in entity_ids]
        return self._vectors_from_points(
            self.qdrant_adapter.retrieve(
                self.research_entities_collection,
                point_ids,
                with_vectors=True,
            )
        )

    def _paper_vectors(self, paper_ids: list[int]) -> list[list[float]]:
        if not paper_ids:
            return []
        return self._vectors_from_points(
            self.qdrant_adapter.retrieve(
                self.papers_collection,
                paper_ids,
                with_vectors=True,
            )
        )

    def _weighted_vectors(
        self,
        category_vectors: dict[str, list[list[float]]],
    ) -> list[tuple[list[float], float]]:
        category_weights = {
            "favourite_papers": FAVOURITE_PAPER_WEIGHT,
            "tracked_topics": TRACKED_TOPIC_WEIGHT,
            "tracked_fields": TRACKED_FIELD_WEIGHT,
            "tracked_subfields": TRACKED_SUBFIELD_WEIGHT,
            "tracked_keywords": TRACKED_KEYWORD_WEIGHT,
            "tracked_domains": TRACKED_DOMAIN_WEIGHT,
        }
        weighted_vectors: list[tuple[list[float], float]] = []
        for category, weight in category_weights.items():
            vectors = category_vectors.get(category, [])
            if not vectors:
                continue
            per_vector_weight = weight / len(vectors)
            weighted_vectors.extend((vector, per_vector_weight) for vector in vectors)
        return weighted_vectors

    def _vectors_from_points(self, points: list[QdrantPointDTO]) -> list[list[float]]:
        return [point.vector for point in points if point.vector]

    def _source_counts(self, sources: dict[str, list[int]]) -> dict[str, Any]:
        return {key: len(value) for key, value in sources.items()}

    def _has_any_source(self, sources: dict[str, list[int]]) -> bool:
        return any(sources.values())

    def _profile_point_id(self, user_id: int) -> str:
        return f"user:{user_id}"

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
                task_type="user_profile_recompute",
                entity_id=entity_id,
                stage=stage,
                current=current,
                total=total,
                message=message,
                payload=payload or {},
            )
        )


__all__ = ["UserProfileFacade"]
