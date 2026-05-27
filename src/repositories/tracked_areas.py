from __future__ import annotations

from typing import Any

from sqlalchemy import delete, select

from dto.tracked import TrackedEntityDTO, UserTrackedEntitiesDTO
from models import (
    Domain,
    Field,
    Keyword,
    Subfield,
    Topic,
    UserTrackedDomain,
    UserTrackedField,
    UserTrackedKeyword,
    UserTrackedSubfield,
    UserTrackedTopic,
)

from .base import BaseRepository


class TrackedAreaRepository(BaseRepository):
    def add_domain(self, user_id: int, domain_id: int) -> None:
        """Track a domain for a user idempotently."""
        self._add(UserTrackedDomain, user_id, domain_id, "domain_id")

    def remove_domain(self, user_id: int, domain_id: int) -> None:
        """Stop tracking a domain for a user idempotently."""
        self._remove(UserTrackedDomain, user_id, domain_id, UserTrackedDomain.domain_id)

    def add_field(self, user_id: int, field_id: int) -> None:
        """Track a field for a user idempotently."""
        self._add(UserTrackedField, user_id, field_id, "field_id")

    def remove_field(self, user_id: int, field_id: int) -> None:
        """Stop tracking a field for a user idempotently."""
        self._remove(UserTrackedField, user_id, field_id, UserTrackedField.field_id)

    def add_subfield(self, user_id: int, subfield_id: int) -> None:
        """Track a subfield for a user idempotently."""
        self._add(UserTrackedSubfield, user_id, subfield_id, "subfield_id")

    def remove_subfield(self, user_id: int, subfield_id: int) -> None:
        """Stop tracking a subfield for a user idempotently."""
        self._remove(
            UserTrackedSubfield,
            user_id,
            subfield_id,
            UserTrackedSubfield.subfield_id,
        )

    def add_topic(self, user_id: int, topic_id: int) -> None:
        """Track a topic for a user idempotently."""
        self._add(UserTrackedTopic, user_id, topic_id, "topic_id")

    def remove_topic(self, user_id: int, topic_id: int) -> None:
        """Stop tracking a topic for a user idempotently."""
        self._remove(UserTrackedTopic, user_id, topic_id, UserTrackedTopic.topic_id)

    def add_keyword(self, user_id: int, keyword_id: int) -> None:
        """Track a keyword for a user idempotently."""
        self._add(UserTrackedKeyword, user_id, keyword_id, "keyword_id")

    def remove_keyword(self, user_id: int, keyword_id: int) -> None:
        """Stop tracking a keyword for a user idempotently."""
        self._remove(
            UserTrackedKeyword,
            user_id,
            keyword_id,
            UserTrackedKeyword.keyword_id,
        )

    def list_domain_ids(self, user_id: int) -> list[int]:
        """List tracked domain ids for a user."""
        return self._list_ids(UserTrackedDomain, user_id, UserTrackedDomain.domain_id)

    def list_field_ids(self, user_id: int) -> list[int]:
        """List tracked field ids for a user."""
        return self._list_ids(UserTrackedField, user_id, UserTrackedField.field_id)

    def list_subfield_ids(self, user_id: int) -> list[int]:
        """List tracked subfield ids for a user."""
        return self._list_ids(
            UserTrackedSubfield,
            user_id,
            UserTrackedSubfield.subfield_id,
        )

    def list_topic_ids(self, user_id: int) -> list[int]:
        """List tracked topic ids for a user."""
        return self._list_ids(UserTrackedTopic, user_id, UserTrackedTopic.topic_id)

    def list_keyword_ids(self, user_id: int) -> list[int]:
        """List tracked keyword ids for a user."""
        return self._list_ids(UserTrackedKeyword, user_id, UserTrackedKeyword.keyword_id)

    def get_user_profile_source(self, user_id: int) -> UserTrackedEntitiesDTO:
        """Return tracked entities grouped for downstream profile construction."""
        return UserTrackedEntitiesDTO(
            user_id=user_id,
            domains=self._list_tracked_entities(
                user_id,
                UserTrackedDomain,
                Domain,
                UserTrackedDomain.domain_id,
                Domain.id,
                Domain.name,
                "domain",
            ),
            fields=self._list_tracked_entities(
                user_id,
                UserTrackedField,
                Field,
                UserTrackedField.field_id,
                Field.id,
                Field.name,
                "field",
            ),
            subfields=self._list_tracked_entities(
                user_id,
                UserTrackedSubfield,
                Subfield,
                UserTrackedSubfield.subfield_id,
                Subfield.id,
                Subfield.name,
                "subfield",
            ),
            topics=self._list_tracked_entities(
                user_id,
                UserTrackedTopic,
                Topic,
                UserTrackedTopic.topic_id,
                Topic.id,
                Topic.name,
                "topic",
            ),
            keywords=self._list_tracked_entities(
                user_id,
                UserTrackedKeyword,
                Keyword,
                UserTrackedKeyword.keyword_id,
                Keyword.id,
                Keyword.value,
                "keyword",
            ),
        )

    def _add(
        self,
        model: type[Any],
        user_id: int,
        entity_id: int,
        entity_id_field: str,
    ) -> None:
        if self.session.get(model, (user_id, entity_id)) is None:
            self.session.add(model(user_id=user_id, **{entity_id_field: entity_id}))

    def _remove(
        self,
        model: type[Any],
        user_id: int,
        entity_id: int,
        entity_id_column: Any,
    ) -> None:
        stmt = delete(model).where(
            model.user_id == user_id,
            entity_id_column == entity_id,
        )
        self.session.execute(stmt)

    def _list_ids(
        self,
        model: type[Any],
        user_id: int,
        entity_id_column: Any,
    ) -> list[int]:
        stmt = (
            select(entity_id_column)
            .where(model.user_id == user_id)
            .order_by(model.created_at.desc())
        )
        return list(self.session.scalars(stmt).all())

    def _list_tracked_entities(
        self,
        user_id: int,
        link_model: type[Any],
        entity_model: type[Any],
        link_entity_id_column: Any,
        entity_id_column: Any,
        entity_name_column: Any,
        entity_type: str,
    ) -> list[TrackedEntityDTO]:
        stmt = (
            select(entity_id_column, entity_name_column, link_model.created_at)
            .join(entity_model, entity_id_column == link_entity_id_column)
            .where(link_model.user_id == user_id)
            .order_by(link_model.created_at.desc())
        )
        return [
            TrackedEntityDTO(
                entity_type=entity_type,
                id=entity_id,
                name=name,
                created_at=created_at,
            )
            for entity_id, name, created_at in self.session.execute(stmt)
        ]


__all__ = ["TrackedAreaRepository"]
