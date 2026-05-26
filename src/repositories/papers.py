from __future__ import annotations

from collections.abc import Iterable
from datetime import date, datetime, timezone
from typing import Any, Literal

from sqlalchemy import func, or_, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from core.exceptions import EntityNotFoundError, InvalidRequestError
from dto.external import ExternalPaperDTO
from dto.papers import PaperCreateDTO, PaperUpdateDTO
from models import Field, Paper, PaperTopic, Subfield, Topic

from .base import BaseRepository


TopicMatchMode = Literal["soft", "strict"]


class PaperRepository(BaseRepository):
    def get_by_id(self, paper_id: int) -> Paper | None:
        """Return a paper by primary key."""
        return self.session.get(Paper, paper_id)

    def get_by_ids(self, paper_ids: list[int]) -> list[Paper]:
        """Return papers whose ids are in the supplied list."""
        if not paper_ids:
            return []
        stmt = select(Paper).where(Paper.id.in_(paper_ids))
        return list(self.session.scalars(stmt).all())

    def get_by_doi(self, doi: str) -> Paper | None:
        """Return a paper by DOI."""
        stmt = select(Paper).where(Paper.doi == doi)
        return self.session.scalar(stmt)

    def get_by_openalex_id(self, openalex_id: str) -> Paper | None:
        """Return a paper by its OpenAlex id stored on papers.openalex_id."""
        stmt = select(Paper).where(Paper.openalex_id == openalex_id).limit(1)
        return self.session.scalar(stmt)

    def get_by_external_id(
        self,
        source_name: str,
        external_id: str,
    ) -> Paper | None:
        """Compatibility wrapper for OpenAlex external id lookup."""
        self._require_openalex_source(source_name)
        return self.get_by_openalex_id(external_id)

    def count_all(self) -> int:
        """Return total paper count."""
        return int(self.session.scalar(select(func.count()).select_from(Paper)) or 0)

    def count_by_period(
        self,
        date_from: date | None,
        date_to: date | None,
    ) -> int:
        """Return paper count whose publication_date falls within the period."""
        stmt = select(func.count()).select_from(Paper)
        if date_from is not None:
            stmt = stmt.where(Paper.publication_date >= date_from)
        if date_to is not None:
            stmt = stmt.where(Paper.publication_date <= date_to)
        return int(self.session.scalar(stmt) or 0)

    def count_by_period_and_taxonomy(
        self,
        date_from: date | None,
        date_to: date | None,
        *,
        field_ids: list[int] | None = None,
        subfield_ids: list[int] | None = None,
        topic_match: TopicMatchMode = "soft",
    ) -> int:
        """Return distinct paper count in a period filtered by topic field/subfield."""
        self._validate_topic_match(topic_match)
        if not field_ids and not subfield_ids:
            return self.count_by_period(date_from, date_to)
        stmt = select(func.count(func.distinct(Paper.id))).select_from(Paper)
        if topic_match == "strict":
            stmt = stmt.join(Topic, Topic.id == Paper.primary_topic_id)
        else:
            stmt = stmt.join(PaperTopic, PaperTopic.paper_id == Paper.id).join(
                Topic,
                Topic.id == PaperTopic.topic_id,
            )
        if date_from is not None:
            stmt = stmt.where(Paper.publication_date >= date_from)
        if date_to is not None:
            stmt = stmt.where(Paper.publication_date <= date_to)
        if subfield_ids:
            stmt = stmt.where(Topic.subfield_id.in_(sorted(set(subfield_ids))))
        if field_ids:
            stmt = stmt.join(Subfield, Subfield.id == Topic.subfield_id).where(
                Subfield.field_id.in_(sorted(set(field_ids)))
            )
        return int(self.session.scalar(stmt) or 0)

    def count_by_period_and_topic(
        self,
        date_from: date | None,
        date_to: date | None,
        topic_id: int,
        *,
        topic_match: TopicMatchMode = "soft",
    ) -> int:
        """Return distinct paper count in a period filtered by topic."""
        self._validate_topic_match(topic_match)
        stmt = select(func.count(func.distinct(Paper.id))).select_from(Paper)
        if topic_match == "strict":
            stmt = stmt.where(Paper.primary_topic_id == topic_id)
        else:
            stmt = stmt.join(PaperTopic, PaperTopic.paper_id == Paper.id).where(
                PaperTopic.topic_id == topic_id,
            )
        if date_from is not None:
            stmt = stmt.where(Paper.publication_date >= date_from)
        if date_to is not None:
            stmt = stmt.where(Paper.publication_date <= date_to)
        return int(self.session.scalar(stmt) or 0)

    def existing_external_paper_keys(
        self,
        items: list[ExternalPaperDTO],
        source_name: str = "openalex",
    ) -> set[str]:
        """Return stable external-paper keys that already exist in PostgreSQL."""
        self._require_openalex_source(source_name)
        openalex_ids = {self._require_external_id(item) for item in items}
        existing: set[str] = set()

        if openalex_ids:
            stmt = select(Paper.openalex_id).where(Paper.openalex_id.in_(openalex_ids))
            existing.update(
                f"external:{openalex_id}"
                for openalex_id in self.session.scalars(stmt).all()
            )

        return existing

    def resolve_external_paper_ids(
        self,
        items: list[ExternalPaperDTO],
        source_name: str = "openalex",
    ) -> list[int]:
        """Resolve database paper ids for imported external papers."""
        self._require_openalex_source(source_name)
        openalex_ids = [self._require_external_id(item) for item in items]
        papers_by_openalex = self._papers_by_openalex_ids(openalex_ids)
        paper_ids: list[int] = []
        seen: set[int] = set()
        for openalex_id in openalex_ids:
            paper = papers_by_openalex.get(openalex_id)
            if paper is None or paper.id in seen:
                continue
            paper_ids.append(int(paper.id))
            seen.add(int(paper.id))
        return paper_ids

    def create(self, data: PaperCreateDTO) -> Paper:
        """Create a paper without committing the transaction."""
        paper = Paper(**data.model_dump(exclude_unset=True))
        self.session.add(paper)
        return paper

    def update(self, paper_id: int, data: PaperUpdateDTO) -> Paper:
        """Update a paper or raise when it does not exist."""
        paper = self.get_by_id(paper_id)
        if paper is None:
            raise EntityNotFoundError(
                "Paper not found",
                details={"paper_id": paper_id},
            )
        for field, value in data.model_dump(exclude_unset=True).items():
            setattr(paper, field, value)
        return paper

    def upsert_from_external(
        self,
        data: ExternalPaperDTO,
        source_name: str = "openalex",
    ) -> Paper:
        """Create or update a paper from normalized OpenAlex data."""
        self._require_openalex_source(source_name)
        external_id = self._require_external_id(data)
        paper = self.get_by_openalex_id(external_id)

        values = self._external_values(data)
        if paper is None:
            paper = Paper(**values)
            self.session.add(paper)
            return paper

        self._apply_external_values(paper, data)
        return paper

    def upsert(
        self,
        data: ExternalPaperDTO,
        source_name: str = "openalex",
    ) -> Paper:
        """Insert or update a paper from an external API paper DTO."""
        return self.upsert_from_external(
            data,
            source_name=source_name,
        )

    def upsert_bulk(
        self,
        items: list[ExternalPaperDTO],
        source_name: str = "openalex",
    ) -> list[Paper]:
        """Insert or update many external papers and return ORM instances."""
        self._require_openalex_source(source_name)
        deduplicated = list(self._deduplicate_external_papers(items).values())
        if self._is_postgresql():
            return self._upsert_bulk_postgresql(deduplicated)
        papers = [
            self.upsert(
                item,
                source_name=source_name,
            )
            for item in deduplicated
        ]
        self.session.flush()
        return papers

    def upsertBulk(
        self,
        items: list[ExternalPaperDTO],
        source_name: str = "openalex",
    ) -> list[Paper]:
        """Compatibility wrapper for callers that use camelCase naming."""
        return self.upsert_bulk(
            items,
            source_name=source_name,
        )

    def list_by_period(
        self,
        date_from: date | None,
        date_to: date | None,
        limit: int,
        offset: int,
        *,
        topic_id: int | None = None,
        topic_match: TopicMatchMode = "soft",
    ) -> list[Paper]:
        """List papers whose publication_date falls within the optional period."""
        self._validate_topic_match(topic_match)
        stmt = select(Paper).order_by(Paper.publication_date.desc().nullslast(), Paper.id.desc())
        if date_from is not None:
            stmt = stmt.where(Paper.publication_date >= date_from)
        if date_to is not None:
            stmt = stmt.where(Paper.publication_date <= date_to)
        if topic_id is not None:
            if topic_match == "strict":
                stmt = stmt.where(Paper.primary_topic_id == topic_id)
            else:
                stmt = stmt.join(PaperTopic, PaperTopic.paper_id == Paper.id).where(
                    PaperTopic.topic_id == topic_id,
                )
        stmt = stmt.limit(limit).offset(offset)
        return list(self.session.scalars(stmt).all())

    def list_ids_by_period(
        self,
        date_from: date | None,
        date_to: date | None,
        limit: int,
        offset: int,
        *,
        topic_id: int | None = None,
        topic_match: TopicMatchMode = "soft",
    ) -> list[int]:
        """List paper ids whose publication_date falls within the optional period."""
        self._validate_topic_match(topic_match)
        stmt = select(Paper.id).order_by(
            Paper.publication_date.desc().nullslast(),
            Paper.id.desc(),
        )
        if date_from is not None:
            stmt = stmt.where(Paper.publication_date >= date_from)
        if date_to is not None:
            stmt = stmt.where(Paper.publication_date <= date_to)
        if topic_id is not None:
            if topic_match == "strict":
                stmt = stmt.where(Paper.primary_topic_id == topic_id)
            else:
                stmt = stmt.join(PaperTopic, PaperTopic.paper_id == Paper.id).where(
                    PaperTopic.topic_id == topic_id,
                )
        stmt = stmt.limit(limit).offset(offset)
        return list(self.session.scalars(stmt).all())

    def list_top_cited_by_topic_and_period(
        self,
        topic_id: int,
        date_from: date,
        date_to: date,
        *,
        limit: int = 5,
        topic_match: TopicMatchMode = "soft",
    ) -> list[Paper]:
        """List most cited papers for a topic within a publication period."""
        self._validate_topic_match(topic_match)
        if limit <= 0:
            return []
        stmt = select(Paper)
        if topic_match == "strict":
            stmt = stmt.where(Paper.primary_topic_id == int(topic_id))
        else:
            stmt = stmt.join(PaperTopic, PaperTopic.paper_id == Paper.id).where(
                PaperTopic.topic_id == int(topic_id)
            )
        stmt = (
            stmt.where(
                Paper.publication_date >= date_from,
                Paper.publication_date <= date_to,
            )
            .order_by(
                Paper.cited_by_count.desc().nullslast(),
                Paper.publication_date.desc().nullslast(),
                Paper.id.desc(),
            )
            .limit(limit)
        )
        return list(self.session.scalars(stmt).all())

    def list_for_keyword_extraction(
        self,
        *,
        date_from: date | None = None,
        date_to: date | None = None,
        topic_id: int | None = None,
        field_id: int | None = None,
        skip_processed: bool = True,
        limit: int = 200,
        offset: int = 0,
    ) -> list[Paper]:
        """List papers eligible for keyword extraction using metadata filters."""
        stmt = select(Paper).order_by(
            Paper.publication_date.desc().nullslast(),
            Paper.id.desc(),
        )
        stmt = self._apply_keyword_extraction_filters(
            stmt,
            date_from=date_from,
            date_to=date_to,
            topic_id=topic_id,
            field_id=field_id,
            skip_processed=skip_processed,
        )
        stmt = stmt.limit(limit).offset(offset)
        return list(self.session.scalars(stmt).all())

    def list_ids_for_keyword_extraction(
        self,
        *,
        paper_ids: list[int] | None = None,
        date_from: date | None = None,
        date_to: date | None = None,
        topic_id: int | None = None,
        field_id: int | None = None,
        skip_processed: bool = True,
        limit: int | None = None,
        offset: int = 0,
    ) -> list[int]:
        """List paper ids eligible for keyword extraction."""
        unique_ids = self._unique_ids(paper_ids or [])
        stmt = select(Paper.id)
        if unique_ids:
            stmt = stmt.where(Paper.id.in_(unique_ids))
        else:
            stmt = stmt.order_by(
                Paper.publication_date.desc().nullslast(),
                Paper.id.desc(),
            )
        stmt = self._apply_keyword_extraction_filters(
            stmt,
            date_from=date_from,
            date_to=date_to,
            topic_id=topic_id,
            field_id=field_id,
            skip_processed=skip_processed,
        )
        if limit is not None:
            stmt = stmt.limit(limit)
        stmt = stmt.offset(offset)
        ids = [int(paper_id) for paper_id in self.session.scalars(stmt).all()]
        if not unique_ids:
            return ids
        id_set = set(ids)
        return [paper_id for paper_id in unique_ids if paper_id in id_set]

    def list_recent(self, date_from: date, limit: int) -> list[Paper]:
        """List recent papers from the supplied publication date."""
        stmt = (
            select(Paper)
            .where(Paper.publication_date >= date_from)
            .order_by(Paper.publication_date.desc().nullslast(), Paper.id.desc())
            .limit(limit)
        )
        return list(self.session.scalars(stmt).all())

    def list_recent_indexed(
        self,
        date_from: date,
        limit: int,
        *,
        domain_ids: list[int] | None = None,
        field_ids: list[int] | None = None,
        subfield_ids: list[int] | None = None,
        topic_ids: list[int] | None = None,
    ) -> list[Paper]:
        """List indexed papers, optionally filtered by taxonomy hierarchy."""
        stmt = (
            select(Paper)
            .where(Paper.is_indexed.is_(True), Paper.publication_date >= date_from)
            .order_by(Paper.publication_date.desc().nullslast(), Paper.id.desc())
            .limit(limit)
        )
        taxonomy_filter = self._paper_ids_for_taxonomy_filter(
            domain_ids=domain_ids or [],
            field_ids=field_ids or [],
            subfield_ids=subfield_ids or [],
            topic_ids=topic_ids or [],
        )
        if taxonomy_filter is not None:
            stmt = stmt.where(Paper.id.in_(taxonomy_filter))

        return list(self.session.scalars(stmt).all())

    def get_indexed_text_hashes(self, paper_ids: list[int]) -> dict[int, str]:
        """Return text hashes for papers currently marked as indexed."""
        if not paper_ids:
            return {}
        stmt = select(Paper.id, Paper.text_hash).where(
            Paper.id.in_(set(paper_ids)),
            Paper.is_indexed.is_(True),
            Paper.text_hash.is_not(None),
        )
        return {
            int(paper_id): str(text_hash)
            for paper_id, text_hash in self.session.execute(stmt)
            if text_hash
        }

    def mark_loaded(self, paper_ids: Iterable[int]) -> None:
        """Mark successfully loaded papers as waiting for embedding/indexing."""
        unique_ids = self._unique_ids(paper_ids)
        if not unique_ids:
            return
        self._update_papers(
            unique_ids,
            is_indexed=False,
            text_hash=None,
            updated_at=self._now(),
        )

    def mark_indexing_started(self, paper_ids: Iterable[int]) -> None:
        """Mark papers as actively being embedded or indexed."""
        unique_ids = self._unique_ids(paper_ids)
        if not unique_ids:
            return
        self._update_papers(
            unique_ids,
            is_indexed=False,
            updated_at=self._now(),
        )

    def mark_indexed(self, text_hashes_by_paper_id: dict[int, str]) -> None:
        """Mark papers as successfully indexed in Qdrant."""
        if not text_hashes_by_paper_id:
            return
        now = self._now()
        for paper_id, text_hash in text_hashes_by_paper_id.items():
            self.session.execute(
                update(Paper)
                .where(Paper.id == int(paper_id))
                .values(
                    text_hash=text_hash,
                    is_indexed=True,
                    indexed_at=now,
                    updated_at=now,
                )
            )

    def mark_failed(
        self,
        paper_ids: Iterable[int],
        error_message: str,
    ) -> None:
        """Mark papers whose embedding or Qdrant indexing failed."""
        _ = error_message
        unique_ids = self._unique_ids(paper_ids)
        if not unique_ids:
            return
        self._update_papers(
            unique_ids,
            is_indexed=False,
            updated_at=self._now(),
        )

    def save_extracted_keywords(
        self,
        keywords_by_paper_id: dict[int, list[str]],
    ) -> None:
        """Persist extracted keyword strings into papers.extracted_keywords."""
        if not keywords_by_paper_id:
            return
        now = self._now()
        for paper_id, keywords in keywords_by_paper_id.items():
            clean_keywords = [
                str(keyword).strip()
                for keyword in keywords
                if str(keyword).strip()
            ]
            self.session.execute(
                update(Paper)
                .where(Paper.id == int(paper_id))
                .values(
                    extracted_keywords=clean_keywords,
                    updated_at=now,
                )
            )

    def _normalize_title(self, title: str) -> str:
        return " ".join(title.strip().lower().split())

    def _deduplicate_external_papers(
        self,
        items: list[ExternalPaperDTO],
    ) -> dict[str, ExternalPaperDTO]:
        deduplicated: dict[str, ExternalPaperDTO] = {}
        for item in items:
            key = f"external:{self._require_external_id(item)}"
            deduplicated[key] = item
        return deduplicated

    def _upsert_bulk_postgresql(
        self,
        items: list[ExternalPaperDTO],
    ) -> list[Paper]:
        if not items:
            return []

        openalex_ids = [self._require_external_id(item) for item in items]
        stmt = pg_insert(Paper).values([self._external_values(item) for item in items])
        self.session.execute(
            stmt.on_conflict_do_update(
                index_elements=[Paper.openalex_id],
                set_={
                    "title": stmt.excluded.title,
                    "doi": func.coalesce(stmt.excluded.doi, Paper.doi),
                    "publication_year": func.coalesce(
                        stmt.excluded.publication_year,
                        Paper.publication_year,
                    ),
                    "publication_date": func.coalesce(
                        stmt.excluded.publication_date,
                        Paper.publication_date,
                    ),
                    "language": func.coalesce(
                        stmt.excluded.language,
                        Paper.language,
                    ),
                    "abstract": func.coalesce(
                        stmt.excluded.abstract,
                        Paper.abstract,
                    ),
                    "is_open_access": func.coalesce(
                        stmt.excluded.is_open_access,
                        Paper.is_open_access,
                    ),
                    "cited_by_count": func.coalesce(
                        stmt.excluded.cited_by_count,
                        Paper.cited_by_count,
                    ),
                    "references_count": func.coalesce(
                        stmt.excluded.references_count,
                        Paper.references_count,
                    ),
                    "primary_topic_id": func.coalesce(
                        stmt.excluded.primary_topic_id,
                        Paper.primary_topic_id,
                    ),
                    "extracted_keywords": func.coalesce(
                        stmt.excluded.extracted_keywords,
                        Paper.extracted_keywords,
                    ),
                    "updated_at": func.now(),
                },
            )
        )

        self.session.flush()
        papers_by_openalex = self._papers_by_openalex_ids(openalex_ids)
        papers: list[Paper] = []
        for item, openalex_id in zip(items, openalex_ids, strict=True):
            paper = papers_by_openalex.get(openalex_id)
            if paper is None:
                raise InvalidRequestError(
                    "Paper could not be resolved after conflict-safe upsert",
                    details={
                        "openalex_id": item.external_id,
                        "doi": item.doi,
                        "title": item.title,
                        "publication_year": item.publication_year,
                    },
                )
            papers.append(paper)
        return papers

    def _papers_by_openalex_ids(
        self,
        openalex_ids: list[str],
    ) -> dict[str, Paper]:
        if not openalex_ids:
            return {}
        stmt = select(Paper).where(Paper.openalex_id.in_(set(openalex_ids)))
        return {
            str(paper.openalex_id): paper
            for paper in self.session.scalars(stmt)
            if paper.openalex_id
        }

    def _external_values(self, item: ExternalPaperDTO) -> dict[str, Any]:
        return {
            "title": item.title,
            "doi": item.doi,
            "publication_year": item.publication_year,
            "publication_date": item.publication_date,
            "language": item.language,
            "abstract": item.abstract,
            "is_open_access": item.is_open_access,
            "cited_by_count": item.cited_by_count,
            "openalex_id": item.external_id,
            "references_count": item.references_count,
            "primary_topic_id": item.primary_topic_id,
            "extracted_keywords": self._extracted_keywords_payload(item),
        }

    def _apply_external_values(
        self,
        paper: Paper,
        item: ExternalPaperDTO,
    ) -> None:
        for field, value in self._external_values(item).items():
            if value is not None:
                if field == "doi" and not self._can_assign_doi(paper, value):
                    continue
                setattr(paper, field, value)
        paper.updated_at = self._now()

    def _can_assign_doi(self, paper: Paper, doi: str) -> bool:
        if paper.doi == doi:
            return True
        existing = self.get_by_doi(doi)
        return existing is None or existing.id == paper.id

    def _require_external_id(self, item: ExternalPaperDTO) -> str:
        if not item.external_id or not item.external_id.strip():
            raise InvalidRequestError("OpenAlex paper external_id is required")
        return item.external_id.strip()

    def _update_papers(self, paper_ids: list[int], **values: Any) -> None:
        self.session.execute(update(Paper).where(Paper.id.in_(paper_ids)).values(**values))

    def _apply_keyword_extraction_filters(
        self,
        stmt: Any,
        *,
        date_from: date | None,
        date_to: date | None,
        topic_id: int | None,
        field_id: int | None,
        skip_processed: bool,
    ) -> Any:
        if date_from is not None:
            stmt = stmt.where(Paper.publication_date >= date_from)
        if date_to is not None:
            stmt = stmt.where(Paper.publication_date <= date_to)
        if skip_processed:
            stmt = stmt.where(Paper.extracted_keywords.is_(None))
        if topic_id is not None or field_id is not None:
            paper_ids = select(PaperTopic.paper_id).join(
                Topic,
                Topic.id == PaperTopic.topic_id,
            )
            if topic_id is not None:
                paper_ids = paper_ids.where(PaperTopic.topic_id == int(topic_id))
            if field_id is not None:
                paper_ids = paper_ids.join(
                    Subfield,
                    Subfield.id == Topic.subfield_id,
                ).where(Subfield.field_id == int(field_id))
            stmt = stmt.where(Paper.id.in_(paper_ids))
        return stmt

    def _paper_ids_for_taxonomy_filter(
        self,
        *,
        domain_ids: list[int],
        field_ids: list[int],
        subfield_ids: list[int],
        topic_ids: list[int],
    ) -> Any | None:
        domain_ids = self._unique_ids(domain_ids)
        field_ids = self._unique_ids(field_ids)
        subfield_ids = self._unique_ids(subfield_ids)
        topic_ids = self._unique_ids(topic_ids)
        if not domain_ids and not field_ids and not subfield_ids and not topic_ids:
            return None

        stmt = (
            select(PaperTopic.paper_id)
            .join(Topic, Topic.id == PaperTopic.topic_id)
            .outerjoin(Subfield, Subfield.id == Topic.subfield_id)
            .outerjoin(Field, Field.id == Subfield.field_id)
        )
        conditions = []
        if topic_ids:
            conditions.append(PaperTopic.topic_id.in_(topic_ids))
        if subfield_ids:
            conditions.append(Topic.subfield_id.in_(subfield_ids))
        if field_ids:
            conditions.append(Subfield.field_id.in_(field_ids))
        if domain_ids:
            conditions.append(Field.domain_id.in_(domain_ids))
        return stmt.where(or_(*conditions)).distinct()

    def _extracted_keywords_payload(
        self,
        item: ExternalPaperDTO,
    ) -> list[str] | list[dict[str, Any]] | None:
        if item.extracted_keywords is not None:
            return item.extracted_keywords
        if not item.keywords:
            return None
        payload: list[dict[str, Any]] = []
        for keyword in item.keywords:
            value = keyword.value.strip() if keyword.value else ""
            if not value:
                continue
            item_payload: dict[str, Any] = {"keyword": value}
            if keyword.score is not None:
                item_payload["score"] = float(keyword.score)
            payload.append(item_payload)
        return payload or None

    def _unique_ids(self, paper_ids: Iterable[int]) -> list[int]:
        return list(dict.fromkeys(int(paper_id) for paper_id in paper_ids))

    def _now(self) -> datetime:
        return datetime.now(timezone.utc)

    def _require_openalex_source(self, source_name: str) -> None:
        if source_name.strip().lower() != "openalex":
            raise InvalidRequestError(
                "Only OpenAlex paper external ids are stored on papers.openalex_id",
                details={"source_name": source_name},
            )

    def _validate_topic_match(self, value: TopicMatchMode) -> None:
        if value not in {"soft", "strict"}:
            raise InvalidRequestError(
                "Topic match mode must be 'soft' or 'strict'",
                details={"topic_match": value},
            )


__all__ = ["PaperRepository", "TopicMatchMode"]
