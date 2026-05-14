from __future__ import annotations

from collections import defaultdict
from decimal import Decimal
from typing import Any

from sqlalchemy import func, select

from core.exceptions import InvalidRequestError
from dto.external import ExternalKeywordDTO, ExternalTopicDTO
from models import (
    Domain,
    Field,
    Keyword,
    PaperKeyword,
    PaperTopic,
    Subfield,
    Topic,
)

from .base import BaseRepository


class TaxonomyRepository(BaseRepository):
    def get_domain_by_id(self, domain_id: int) -> Domain | None:
        """Return a domain by primary key."""
        return self.session.get(Domain, domain_id)

    def get_field_by_id(self, field_id: int) -> Field | None:
        """Return a field by primary key."""
        return self.session.get(Field, field_id)

    def get_subfield_by_id(self, subfield_id: int) -> Subfield | None:
        """Return a subfield by primary key."""
        return self.session.get(Subfield, subfield_id)

    def get_topic_by_id(self, topic_id: int) -> Topic | None:
        """Return a topic by primary key."""
        return self.session.get(Topic, topic_id)

    def get_keyword_by_id(self, keyword_id: int) -> Keyword | None:
        """Return a keyword by primary key."""
        return self.session.get(Keyword, keyword_id)

    def upsert(
        self,
        data: ExternalKeywordDTO | ExternalTopicDTO,
    ) -> Keyword | Topic:
        """Insert or update a taxonomy entity from an external API DTO.

        ``ExternalTopicDTO`` is expanded into its domain/field/subfield hierarchy
        before the topic is saved. ``ExternalKeywordDTO`` is normalized to the
        local lowercase keyword value. The transaction is not committed.
        """
        if isinstance(data, ExternalKeywordDTO):
            return self._upsert_keyword(data)
        if isinstance(data, ExternalTopicDTO):
            return self._upsert_topic(data)
        raise InvalidRequestError(
            "Unsupported external taxonomy DTO",
            details={"dto_type": data.__class__.__name__},
        )

    def upsert_bulk(
        self,
        items: list[ExternalKeywordDTO | ExternalTopicDTO],
    ) -> list[Keyword | Topic]:
        """Insert or update many external taxonomy DTOs and return instances.

        Duplicates are collapsed independently for topics and keywords. The
        session is flushed before returning so ids can be used for paper links.
        """
        deduplicated = list(self._deduplicate_external_taxonomy(items).values())
        entities = [self.upsert(item) for item in deduplicated]
        self.session.flush()
        return entities

    def upsertBulk(
        self,
        items: list[ExternalKeywordDTO | ExternalTopicDTO],
    ) -> list[Keyword | Topic]:
        """Compatibility wrapper for callers that use camelCase naming."""
        return self.upsert_bulk(items)

    def get_or_create_domain(self, openalex_id: str | None, name: str) -> Domain:
        """Return an existing domain or create one."""
        clean_name = name.strip()
        domain = self._get_by_openalex_or_name(Domain, openalex_id, clean_name)
        if domain is not None:
            return domain
        domain = Domain(openalex_id=openalex_id, name=clean_name)
        self.session.add(domain)
        return domain

    def get_or_create_field(
        self,
        domain_id: int | None,
        openalex_id: str | None,
        name: str,
    ) -> Field:
        """Return an existing field or create one."""
        clean_name = name.strip()
        field = self._get_by_openalex_or_name(Field, openalex_id, clean_name)
        if field is not None:
            return field
        field = Field(domain_id=domain_id, openalex_id=openalex_id, name=clean_name)
        self.session.add(field)
        return field

    def get_or_create_subfield(
        self,
        field_id: int | None,
        openalex_id: str | None,
        name: str,
    ) -> Subfield:
        """Return an existing subfield or create one."""
        clean_name = name.strip()
        subfield = self._get_by_openalex_or_name(Subfield, openalex_id, clean_name)
        if subfield is not None:
            return subfield
        subfield = Subfield(field_id=field_id, openalex_id=openalex_id, name=clean_name)
        self.session.add(subfield)
        return subfield

    def get_or_create_topic(
        self,
        subfield_id: int | None,
        openalex_id: str | None,
        name: str,
    ) -> Topic:
        """Return an existing topic or create one."""
        clean_name = name.strip()
        topic = self._get_by_openalex_or_name(Topic, openalex_id, clean_name)
        if topic is not None:
            return topic
        topic = Topic(subfield_id=subfield_id, openalex_id=openalex_id, name=clean_name)
        self.session.add(topic)
        return topic

    def get_or_create_keyword(self, value: str) -> Keyword:
        """Return an existing lowercase keyword or create one."""
        clean_value = value.strip().lower()
        keyword = self.session.scalar(select(Keyword).where(Keyword.value == clean_value))
        if keyword is not None:
            return keyword
        keyword = Keyword(value=clean_value)
        self.session.add(keyword)
        return keyword

    def attach_topic_to_paper(
        self,
        paper_id: int,
        topic_id: int,
        score: float | None,
    ) -> None:
        """Attach a topic to a paper idempotently."""
        link = self._pending_instance(
            PaperTopic,
            paper_id=paper_id,
            topic_id=topic_id,
        ) or self.session.get(PaperTopic, (paper_id, topic_id))
        normalized_score = self._score_to_decimal(score)
        if link is None:
            self.session.add(
                PaperTopic(paper_id=paper_id, topic_id=topic_id, score=normalized_score)
            )
            return
        link.score = normalized_score

    def attach_keyword_to_paper(
        self,
        paper_id: int,
        keyword_id: int,
        score: float | None,
    ) -> None:
        """Attach a keyword to a paper idempotently."""
        link = self._pending_instance(
            PaperKeyword,
            paper_id=paper_id,
            keyword_id=keyword_id,
        ) or self.session.get(PaperKeyword, (paper_id, keyword_id))
        normalized_score = self._score_to_decimal(score)
        if link is None:
            self.session.add(
                PaperKeyword(
                    paper_id=paper_id,
                    keyword_id=keyword_id,
                    score=normalized_score,
                )
            )
            return
        link.score = normalized_score

    def list_topics_by_paper(self, paper_id: int) -> list[Topic]:
        """List topics attached to a paper."""
        stmt = (
            select(Topic)
            .join(PaperTopic, PaperTopic.topic_id == Topic.id)
            .where(PaperTopic.paper_id == paper_id)
            .order_by(PaperTopic.score.desc().nullslast(), Topic.name.asc())
        )
        return list(self.session.scalars(stmt).all())

    def list_topics_by_papers(self, paper_ids: list[int]) -> dict[int, list[Topic]]:
        """List topics for many papers keyed by paper id."""
        if not paper_ids:
            return {}
        stmt = (
            select(PaperTopic.paper_id, Topic)
            .join(Topic, PaperTopic.topic_id == Topic.id)
            .where(PaperTopic.paper_id.in_(paper_ids))
            .order_by(
                PaperTopic.paper_id.asc(),
                PaperTopic.score.desc().nullslast(),
                Topic.name.asc(),
            )
        )
        topics_by_paper: dict[int, list[Topic]] = defaultdict(list)
        for paper_id, topic in self.session.execute(stmt):
            topics_by_paper[int(paper_id)].append(topic)
        return dict(topics_by_paper)

    def list_keywords_by_paper(self, paper_id: int) -> list[Keyword]:
        """List keywords attached to a paper."""
        stmt = (
            select(Keyword)
            .join(PaperKeyword, PaperKeyword.keyword_id == Keyword.id)
            .where(PaperKeyword.paper_id == paper_id)
            .order_by(PaperKeyword.score.desc().nullslast(), Keyword.value.asc())
        )
        return list(self.session.scalars(stmt).all())

    def list_keywords_by_papers(self, paper_ids: list[int]) -> dict[int, list[Keyword]]:
        """List keywords for many papers keyed by paper id."""
        if not paper_ids:
            return {}
        stmt = (
            select(PaperKeyword.paper_id, Keyword)
            .join(Keyword, PaperKeyword.keyword_id == Keyword.id)
            .where(PaperKeyword.paper_id.in_(paper_ids))
            .order_by(
                PaperKeyword.paper_id.asc(),
                PaperKeyword.score.desc().nullslast(),
                Keyword.value.asc(),
            )
        )
        keywords_by_paper: dict[int, list[Keyword]] = defaultdict(list)
        for paper_id, keyword in self.session.execute(stmt):
            keywords_by_paper[int(paper_id)].append(keyword)
        return dict(keywords_by_paper)

    def list_paper_ids_by_topic(self, topic_id: int) -> list[int]:
        """List paper ids attached to a topic."""
        stmt = select(PaperTopic.paper_id).where(PaperTopic.topic_id == topic_id)
        return list(self.session.scalars(stmt).all())

    def list_domains(self, limit: int, offset: int) -> list[Domain]:
        """List domains ordered by name."""
        return self._list_entities(Domain, Domain.name, limit, offset)

    def list_fields(self, limit: int, offset: int) -> list[Field]:
        """List fields ordered by name."""
        return self._list_entities(Field, Field.name, limit, offset)

    def list_subfields(self, limit: int, offset: int) -> list[Subfield]:
        """List subfields ordered by name."""
        return self._list_entities(Subfield, Subfield.name, limit, offset)

    def list_topics(self, limit: int, offset: int) -> list[Topic]:
        """List topics ordered by name."""
        return self._list_entities(Topic, Topic.name, limit, offset)

    def list_keywords(self, limit: int, offset: int) -> list[Keyword]:
        """List keywords ordered by value."""
        return self._list_entities(Keyword, Keyword.value, limit, offset)

    def count_domains(self) -> int:
        """Count domain entities."""
        return self._count_entities(Domain)

    def count_fields(self) -> int:
        """Count field entities."""
        return self._count_entities(Field)

    def count_subfields(self) -> int:
        """Count subfield entities."""
        return self._count_entities(Subfield)

    def count_topics(self) -> int:
        """Count topic entities."""
        return self._count_entities(Topic)

    def count_keywords(self) -> int:
        """Count keyword entities."""
        return self._count_entities(Keyword)

    def _get_by_openalex_or_name(
        self,
        model: type[Domain] | type[Field] | type[Subfield] | type[Topic],
        openalex_id: str | None,
        name: str,
    ):
        if openalex_id:
            entity = self.session.scalar(
                select(model).where(model.openalex_id == openalex_id)
            )
            if entity is not None:
                return entity
        return self.session.scalar(select(model).where(model.name == name))

    def _list_entities(self, model, order_column, limit: int, offset: int):
        stmt = select(model).order_by(order_column.asc()).limit(limit).offset(offset)
        return list(self.session.scalars(stmt).all())

    def _count_entities(self, model) -> int:
        return int(self.session.scalar(select(func.count()).select_from(model)) or 0)

    def _score_to_decimal(self, score: float | None) -> Decimal | None:
        if score is None:
            return None
        return Decimal(str(score))

    def _upsert_topic(self, data: ExternalTopicDTO) -> Topic:
        name = data.name.strip()
        if not name:
            raise InvalidRequestError("External topic name is required")

        domain = (
            self.get_or_create_domain(None, data.domain_name)
            if data.domain_name
            else None
        )
        if domain is not None:
            self.session.flush()
        field = (
            self.get_or_create_field(
                domain.id if domain is not None else None,
                None,
                data.field_name,
            )
            if data.field_name
            else None
        )
        if field is not None and domain is not None and field.domain_id != domain.id:
            field.domain_id = domain.id
        if field is not None:
            self.session.flush()

        subfield = (
            self.get_or_create_subfield(
                field.id if field is not None else None,
                None,
                data.subfield_name,
            )
            if data.subfield_name
            else None
        )
        if subfield is not None and field is not None and subfield.field_id != field.id:
            subfield.field_id = field.id
        if subfield is not None:
            self.session.flush()

        topic = self._get_by_openalex_or_name(Topic, data.external_id, name)
        if topic is None:
            topic = Topic(
                subfield_id=subfield.id if subfield is not None else None,
                openalex_id=data.external_id,
                name=name,
            )
            self.session.add(topic)
            return topic

        topic.name = name
        if data.external_id is not None:
            topic.openalex_id = data.external_id
        if subfield is not None:
            topic.subfield_id = subfield.id
        return topic

    def _upsert_keyword(self, data: ExternalKeywordDTO) -> Keyword:
        value = data.value.strip().lower()
        if not value:
            raise InvalidRequestError("External keyword value is required")
        keyword = self.session.scalar(select(Keyword).where(Keyword.value == value))
        if keyword is None:
            keyword = Keyword(value=value)
            self.session.add(keyword)
        return keyword

    def _deduplicate_external_taxonomy(
        self,
        items: list[ExternalKeywordDTO | ExternalTopicDTO],
    ) -> dict[str, ExternalKeywordDTO | ExternalTopicDTO]:
        deduplicated: dict[str, ExternalKeywordDTO | ExternalTopicDTO] = {}
        for item in items:
            if isinstance(item, ExternalKeywordDTO):
                key = f"keyword:{item.value.strip().lower()}"
            else:
                key = "topic:" + (
                    item.external_id
                    or "|".join(
                        self._normalize_optional_text(value)
                        for value in (
                            item.domain_name,
                            item.field_name,
                            item.subfield_name,
                            item.name,
                        )
                    )
                )
            deduplicated[key] = item
        return deduplicated

    def _normalize_optional_text(self, value: Any) -> str:
        if value is None:
            return ""
        return " ".join(str(value).strip().lower().split())


__all__ = ["TaxonomyRepository"]
