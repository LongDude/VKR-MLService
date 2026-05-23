from __future__ import annotations

from collections import defaultdict
from decimal import Decimal
from typing import Any, Literal

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from core.exceptions import InvalidRequestError
from dto.external import ExternalKeywordDTO, ExternalTopicDTO
from models import (
    Domain,
    Field,
    Keyword,
    Paper,
    PaperKeyword,
    PaperTopic,
    Subfield,
    Topic,
)

from .base import BaseRepository


TopicMatchMode = Literal["soft", "strict"]


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
        if self._is_postgresql():
            return self._upsert_bulk_postgresql(deduplicated)
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

    def attach_topics_to_papers_bulk(
        self,
        items: list[tuple[int, int, float | None]],
    ) -> None:
        """Attach topics to papers in a conflict-safe batch."""
        if not items:
            return
        deduplicated: dict[tuple[int, int], tuple[int, int, float | None]] = {}
        for paper_id, topic_id, score in items:
            deduplicated[(paper_id, topic_id)] = (paper_id, topic_id, score)
        if not self._is_postgresql():
            for paper_id, topic_id, score in deduplicated.values():
                self.attach_topic_to_paper(paper_id, topic_id, score)
            self.session.flush()
            return
        values = [
            {
                "paper_id": paper_id,
                "topic_id": topic_id,
                "score": self._score_to_decimal(score),
            }
            for paper_id, topic_id, score in deduplicated.values()
        ]
        stmt = pg_insert(PaperTopic).values(values)
        self.session.execute(
            stmt.on_conflict_do_update(
                index_elements=[PaperTopic.paper_id, PaperTopic.topic_id],
                set_={"score": stmt.excluded.score},
            )
        )

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

    def attach_keywords_to_papers_bulk(
        self,
        items: list[tuple[int, int, float | None]],
    ) -> None:
        """Attach keywords to papers in a conflict-safe batch."""
        if not items:
            return
        deduplicated: dict[tuple[int, int], tuple[int, int, float | None]] = {}
        for paper_id, keyword_id, score in items:
            deduplicated[(paper_id, keyword_id)] = (paper_id, keyword_id, score)
        if not self._is_postgresql():
            for paper_id, keyword_id, score in deduplicated.values():
                self.attach_keyword_to_paper(paper_id, keyword_id, score)
            self.session.flush()
            return
        values = [
            {
                "paper_id": paper_id,
                "keyword_id": keyword_id,
                "score": self._score_to_decimal(score),
            }
            for paper_id, keyword_id, score in deduplicated.values()
        ]
        stmt = pg_insert(PaperKeyword).values(values)
        self.session.execute(
            stmt.on_conflict_do_update(
                index_elements=[PaperKeyword.paper_id, PaperKeyword.keyword_id],
                set_={"score": stmt.excluded.score},
            )
        )

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

    def list_paper_ids_by_topic(
        self,
        topic_id: int,
        *,
        topic_match: TopicMatchMode = "soft",
    ) -> list[int]:
        """List paper ids matching a topic in soft or strict mode."""
        self._validate_topic_match(topic_match)
        if topic_match == "strict":
            stmt = select(Paper.id).where(Paper.primary_topic_id == topic_id)
            return list(self.session.scalars(stmt).all())
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

    def list_topics_by_ids(self, topic_ids: list[int]) -> list[Topic]:
        """List topics by ids preserving caller order where possible."""
        if not topic_ids:
            return []
        unique_ids = list(dict.fromkeys(int(topic_id) for topic_id in topic_ids))
        topics = {
            topic.id: topic
            for topic in self.session.scalars(
                select(Topic).where(Topic.id.in_(unique_ids))
            )
        }
        return [topics[topic_id] for topic_id in unique_ids if topic_id in topics]

    def list_fields_by_ids(self, field_ids: list[int]) -> list[Field]:
        """List fields by ids preserving caller order where possible."""
        if not field_ids:
            return []
        unique_ids = list(dict.fromkeys(int(field_id) for field_id in field_ids))
        fields = {
            field.id: field
            for field in self.session.scalars(select(Field).where(Field.id.in_(unique_ids)))
        }
        return [fields[field_id] for field_id in unique_ids if field_id in fields]

    def list_subfields_by_ids(self, subfield_ids: list[int]) -> list[Subfield]:
        """List subfields by ids preserving caller order where possible."""
        if not subfield_ids:
            return []
        unique_ids = list(dict.fromkeys(int(subfield_id) for subfield_id in subfield_ids))
        subfields = {
            subfield.id: subfield
            for subfield in self.session.scalars(
                select(Subfield).where(Subfield.id.in_(unique_ids))
            )
        }
        return [
            subfields[subfield_id]
            for subfield_id in unique_ids
            if subfield_id in subfields
        ]

    def list_topics_for_stats(
        self,
        limit: int | None = None,
        offset: int = 0,
        field_ids: list[int] | None = None,
        subfield_ids: list[int] | None = None,
    ) -> list[Topic]:
        """List topics ordered by id for external statistics collection."""
        stmt = select(Topic).order_by(Topic.id.asc()).offset(offset)
        if field_ids:
            stmt = stmt.join(Subfield, Topic.subfield_id == Subfield.id).where(
                Subfield.field_id.in_(sorted(set(field_ids)))
            )
        if subfield_ids:
            stmt = stmt.where(Topic.subfield_id.in_(sorted(set(subfield_ids))))
        if limit is not None:
            stmt = stmt.limit(limit)
        return list(self.session.scalars(stmt).all())

    def list_topics_with_openalex_id(
        self,
        limit: int | None = None,
        offset: int = 0,
    ) -> list[Topic]:
        """List topics that can be matched to OpenAlex by external id."""
        stmt = (
            select(Topic)
            .where(Topic.openalex_id.is_not(None))
            .order_by(Topic.id.asc())
            .offset(offset)
        )
        if limit is not None:
            stmt = stmt.limit(limit)
        return list(self.session.scalars(stmt).all())

    def count_topics_with_openalex_id(self) -> int:
        """Count topics that can be matched to OpenAlex by external id."""
        return int(
            self.session.scalar(
                select(func.count())
                .select_from(Topic)
                .where(Topic.openalex_id.is_not(None))
            )
            or 0
        )

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

    def _upsert_bulk_postgresql(
        self,
        items: list[ExternalKeywordDTO | ExternalTopicDTO],
    ) -> list[Keyword | Topic]:
        ordered_keys = [self._external_taxonomy_key(item) for item in items]
        keywords = [item for item in items if isinstance(item, ExternalKeywordDTO)]
        topics = [item for item in items if isinstance(item, ExternalTopicDTO)]
        entities_by_key: dict[str, Keyword | Topic] = {}
        entities_by_key.update(self._upsert_keywords_postgresql(keywords))
        entities_by_key.update(self._upsert_topics_postgresql(topics))
        self.session.flush()
        return [entities_by_key[key] for key in ordered_keys]

    def _upsert_keywords_postgresql(
        self,
        items: list[ExternalKeywordDTO],
    ) -> dict[str, Keyword]:
        values = sorted({item.value.strip().lower() for item in items if item.value.strip()})
        if not values:
            return {}
        stmt = pg_insert(Keyword).values([{"value": value} for value in values])
        self.session.execute(
            stmt.on_conflict_do_nothing(index_elements=[Keyword.value])
        )
        self.session.flush()
        keywords = self.session.scalars(
            select(Keyword).where(Keyword.value.in_(values))
        )
        return {f"keyword:{keyword.value}": keyword for keyword in keywords}

    def _upsert_topics_postgresql(
        self,
        items: list[ExternalTopicDTO],
    ) -> dict[str, Topic]:
        if not items:
            return {}
        domains_by_name = self._ensure_domains_postgresql(
            sorted({item.domain_name.strip() for item in items if item.domain_name})
        )
        fields_by_name = self._ensure_fields_by_name(
            {
                item.field_name.strip(): (
                    domains_by_name[item.domain_name.strip()].id
                    if item.domain_name and item.domain_name.strip() in domains_by_name
                    else None
                )
                for item in items
                if item.field_name
            }
        )
        subfields_by_name = self._ensure_subfields_by_name(
            {
                item.subfield_name.strip(): (
                    fields_by_name[item.field_name.strip()].id
                    if item.field_name and item.field_name.strip() in fields_by_name
                    else None
                )
                for item in items
                if item.subfield_name
            }
        )

        with_external_id: list[dict[str, Any]] = []
        without_external_id: list[ExternalTopicDTO] = []
        for item in items:
            name = item.name.strip()
            if not name:
                raise InvalidRequestError("External topic name is required")
            subfield = (
                subfields_by_name.get(item.subfield_name.strip())
                if item.subfield_name
                else None
            )
            if item.external_id:
                with_external_id.append(
                    {
                        "name": name,
                        "openalex_id": item.external_id,
                        "subfield_id": subfield.id if subfield is not None else None,
                    }
                )
            else:
                without_external_id.append(item)

        if with_external_id:
            stmt = pg_insert(Topic).values(with_external_id)
            self.session.execute(
                stmt.on_conflict_do_update(
                    index_elements=[Topic.openalex_id],
                    set_={
                        "name": stmt.excluded.name,
                        "subfield_id": func.coalesce(
                            stmt.excluded.subfield_id,
                            Topic.subfield_id,
                        ),
                    },
                )
            )

        existing_by_name = self._topics_by_names(
            [item.name.strip() for item in without_external_id]
        )
        for item in without_external_id:
            name = item.name.strip()
            subfield = (
                subfields_by_name.get(item.subfield_name.strip())
                if item.subfield_name
                else None
            )
            topic = existing_by_name.get(name)
            if topic is None:
                topic = Topic(
                    name=name,
                    subfield_id=subfield.id if subfield is not None else None,
                )
                self.session.add(topic)
                existing_by_name[name] = topic
                continue
            if subfield is not None:
                topic.subfield_id = subfield.id

        self.session.flush()
        result: dict[str, Topic] = {}
        external_ids = [item.external_id for item in items if item.external_id]
        if external_ids:
            for topic in self.session.scalars(
                select(Topic).where(Topic.openalex_id.in_(external_ids))
            ):
                result[f"topic:{topic.openalex_id}"] = topic
        for item in items:
            if item.external_id:
                continue
            key = self._external_taxonomy_key(item)
            topic = existing_by_name.get(item.name.strip())
            if topic is None:
                topic = self.session.scalar(select(Topic).where(Topic.name == item.name.strip()))
            if topic is not None:
                result[key] = topic
        return result

    def _ensure_domains_postgresql(self, names: list[str]) -> dict[str, Domain]:
        if not names:
            return {}
        stmt = pg_insert(Domain).values([{"name": name} for name in names])
        self.session.execute(
            stmt.on_conflict_do_nothing(index_elements=[Domain.name])
        )
        self.session.flush()
        return {
            domain.name: domain
            for domain in self.session.scalars(select(Domain).where(Domain.name.in_(names)))
        }

    def _ensure_fields_by_name(self, names_to_domain_id: dict[str, int | None]) -> dict[str, Field]:
        if not names_to_domain_id:
            return {}
        existing = {
            field.name: field
            for field in self.session.scalars(
                select(Field).where(Field.name.in_(names_to_domain_id.keys()))
            )
        }
        for name, domain_id in names_to_domain_id.items():
            field = existing.get(name)
            if field is None:
                field = Field(name=name, domain_id=domain_id)
                self.session.add(field)
                existing[name] = field
            elif domain_id is not None:
                field.domain_id = domain_id
        self.session.flush()
        return existing

    def _ensure_subfields_by_name(
        self,
        names_to_field_id: dict[str, int | None],
    ) -> dict[str, Subfield]:
        if not names_to_field_id:
            return {}
        existing = {
            subfield.name: subfield
            for subfield in self.session.scalars(
                select(Subfield).where(Subfield.name.in_(names_to_field_id.keys()))
            )
        }
        for name, field_id in names_to_field_id.items():
            subfield = existing.get(name)
            if subfield is None:
                subfield = Subfield(name=name, field_id=field_id)
                self.session.add(subfield)
                existing[name] = subfield
            elif field_id is not None:
                subfield.field_id = field_id
        self.session.flush()
        return existing

    def _topics_by_names(self, names: list[str]) -> dict[str, Topic]:
        clean_names = {name for name in names if name}
        if not clean_names:
            return {}
        return {
            topic.name: topic
            for topic in self.session.scalars(select(Topic).where(Topic.name.in_(clean_names)))
        }

    def _external_taxonomy_key(
        self,
        item: ExternalKeywordDTO | ExternalTopicDTO,
    ) -> str:
        if isinstance(item, ExternalKeywordDTO):
            return f"keyword:{item.value.strip().lower()}"
        return "topic:" + (
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

    def _validate_topic_match(self, value: TopicMatchMode) -> None:
        if value not in {"soft", "strict"}:
            raise InvalidRequestError(
                "Topic match mode must be 'soft' or 'strict'",
                details={"topic_match": value},
            )


__all__ = ["TaxonomyRepository", "TopicMatchMode"]
