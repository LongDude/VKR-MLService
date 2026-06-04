from __future__ import annotations

import logging

import pytest

from core.exceptions import InsufficientUserProfileDataError
from dto.qdrant import QdrantPointDTO
from ml.constants import RESEARCH_ENTITIES_COLLECTION, USER_PROFILES_COLLECTION
from ml.facades.user_profile import UserProfileFacade


class EmptyFavouriteRepository:
    def list_paper_ids(self, _user_id: int) -> list[int]:
        return []


class EmptyTrackedAreaRepository:
    def list_domain_ids(self, _user_id: int) -> list[int]:
        return []

    def list_field_ids(self, _user_id: int) -> list[int]:
        return []

    def list_subfield_ids(self, _user_id: int) -> list[int]:
        return []

    def list_topic_ids(self, _user_id: int) -> list[int]:
        return []

    def list_keyword_ids(self, _user_id: int) -> list[int]:
        return []


class TopicOnlyTrackedAreaRepository(EmptyTrackedAreaRepository):
    def list_topic_ids(self, _user_id: int) -> list[int]:
        return [42]


class QdrantAdapter:
    def __init__(self) -> None:
        self.upserted: tuple[str, int | str, list[float], dict] | None = None

    def retrieve(
        self,
        collection_name: str,
        point_ids: list[int | str],
        *,
        with_vectors: bool = False,
    ) -> list[QdrantPointDTO]:
        if (
            collection_name == RESEARCH_ENTITIES_COLLECTION
            and point_ids == ["topic:42"]
            and with_vectors
        ):
            return [QdrantPointDTO(id="topic:42", vector=[2.0, 4.0], payload={})]
        return []

    def upsert_point(
        self,
        collection_name: str,
        point_id: int | str,
        vector: list[float],
        payload: dict,
    ) -> None:
        self.upserted = (collection_name, point_id, vector, payload)


def test_recompute_user_profile_uses_tracked_vectors_without_favourites() -> None:
    qdrant_adapter = QdrantAdapter()
    facade = UserProfileFacade(
        favourite_repository=EmptyFavouriteRepository(),  # type: ignore[arg-type]
        tracked_area_repository=TopicOnlyTrackedAreaRepository(),  # type: ignore[arg-type]
        taxonomy_repository=object(),  # type: ignore[arg-type]
        qdrant_adapter=qdrant_adapter,  # type: ignore[arg-type]
    )

    profile = facade.recompute_user_profile(13)

    assert profile.user_id == 13
    assert profile.vector_dimension == 2
    assert profile.source_counts["favourite_paper_ids"] == 0
    assert profile.source_counts["tracked_topic_ids"] == 1
    assert profile.source_counts["available_vectors"]["tracked_topics"] == 1
    assert qdrant_adapter.upserted is not None
    collection_name, point_id, vector, payload = qdrant_adapter.upserted
    assert collection_name == USER_PROFILES_COLLECTION
    assert point_id == "user:13"
    assert vector == [2.0, 4.0]
    assert "favourite_paper_ids" not in payload
    assert payload["tracked_topic_ids"] == [42]


def test_recompute_user_profile_empty_sources_logs_skip_without_traceback(caplog) -> None:
    facade = UserProfileFacade(
        favourite_repository=EmptyFavouriteRepository(),  # type: ignore[arg-type]
        tracked_area_repository=EmptyTrackedAreaRepository(),  # type: ignore[arg-type]
        taxonomy_repository=object(),  # type: ignore[arg-type]
        qdrant_adapter=QdrantAdapter(),  # type: ignore[arg-type]
    )

    with caplog.at_level(logging.WARNING):
        with pytest.raises(
            InsufficientUserProfileDataError,
            match="User profile has no available vectors",
        ):
            facade.recompute_user_profile(13)

    assert "user_profile_recompute_skipped" in caplog.text
    assert "Traceback" not in caplog.text
