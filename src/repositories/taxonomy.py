from __future__ import annotations

from decimal import Decimal

from sqlalchemy import select

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
        link = self.session.get(PaperTopic, (paper_id, topic_id))
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
        link = self.session.get(PaperKeyword, (paper_id, keyword_id))
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

    def list_keywords_by_paper(self, paper_id: int) -> list[Keyword]:
        """List keywords attached to a paper."""
        stmt = (
            select(Keyword)
            .join(PaperKeyword, PaperKeyword.keyword_id == Keyword.id)
            .where(PaperKeyword.paper_id == paper_id)
            .order_by(PaperKeyword.score.desc().nullslast(), Keyword.value.asc())
        )
        return list(self.session.scalars(stmt).all())

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

    def _score_to_decimal(self, score: float | None) -> Decimal | None:
        if score is None:
            return None
        return Decimal(str(score))


__all__ = ["TaxonomyRepository"]
