from __future__ import annotations

from datetime import date
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from core.exceptions import EntityNotFoundError, InvalidRequestError
from dto.external import ExternalPaperDTO
from dto.papers import PaperCreateDTO, PaperUpdateDTO
from models import MetaSource, Paper, PaperMetaSource

from .base import BaseRepository


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

    def get_by_external_id(
        self,
        source_name: str,
        external_id: str,
    ) -> Paper | None:
        """Return a paper linked to an external metadata source id."""
        stmt = (
            select(Paper)
            .join(PaperMetaSource, PaperMetaSource.paper_id == Paper.id)
            .join(MetaSource, MetaSource.id == PaperMetaSource.meta_source_id)
            .where(
                MetaSource.name == source_name,
                PaperMetaSource.external_id == external_id,
            )
        )
        return self.session.scalar(stmt)

    def get_by_title_normalized(
        self,
        title: str,
        publication_year: int | None = None,
    ) -> Paper | None:
        """Return a paper by normalized title and optional publication year."""
        normalized = self._normalize_title(title)
        stmt = select(Paper).where(func.lower(func.trim(Paper.title)) == normalized)
        if publication_year is not None:
            stmt = stmt.where(Paper.publication_year == publication_year)
        return self.session.scalar(stmt.limit(1))

    def count_all(self) -> int:
        """Return total paper count."""
        return int(self.session.scalar(select(func.count()).select_from(Paper)) or 0)

    def existing_external_paper_keys(
        self,
        items: list[ExternalPaperDTO],
        source_name: str = "openalex",
    ) -> set[str]:
        """Return stable external-paper keys that already exist in PostgreSQL."""
        external_ids = {item.external_id for item in items if item.external_id}
        dois = {item.doi for item in items if item.doi}
        title_years = {
            (self._normalize_title(item.title), item.publication_year)
            for item in items
            if item.title and item.title.strip()
        }
        normalized_titles = {title for title, _ in title_years}
        titles_any_year = {title for title, year in title_years if year is None}
        existing: set[str] = set()

        if external_ids:
            stmt = (
                select(PaperMetaSource.external_id)
                .join(MetaSource, MetaSource.id == PaperMetaSource.meta_source_id)
                .where(
                    MetaSource.name == source_name,
                    PaperMetaSource.external_id.in_(external_ids),
                )
            )
            existing.update(
                f"external:{external_id}"
                for external_id in self.session.scalars(stmt).all()
            )

        if dois:
            stmt = select(Paper.doi).where(Paper.doi.in_(dois))
            existing.update(f"doi:{doi}" for doi in self.session.scalars(stmt).all())

        if normalized_titles:
            stmt = select(Paper.title, Paper.publication_year).where(
                func.lower(func.trim(Paper.title)).in_(normalized_titles)
            )
            for title, publication_year in self.session.execute(stmt):
                key = (self._normalize_title(title), publication_year)
                if key in title_years:
                    existing.add(f"title:{key[0]}:{key[1] or ''}")
                if key[0] in titles_any_year:
                    existing.add(f"title:{key[0]}:")

        return existing

    def resolve_external_paper_ids(
        self,
        items: list[ExternalPaperDTO],
        source_name: str = "openalex",
    ) -> list[int]:
        """Resolve database paper ids for imported external papers."""
        paper_ids: list[int] = []
        seen: set[int] = set()
        for item in items:
            paper = (
                self.get_by_external_id(source_name, item.external_id)
                if item.external_id
                else None
            )
            if paper is None and item.doi:
                paper = self.get_by_doi(item.doi)
            if paper is None and item.title:
                paper = self.get_by_title_normalized(item.title, item.publication_year)
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
        created_by_user_id: int | None = None,
        source_name: str = "openalex",
    ) -> Paper:
        """Create or update a paper from normalized external data."""
        paper = (
            self.get_by_external_id(source_name, data.external_id)
            if data.external_id
            else None
        )
        if paper is None:
            paper = self.get_by_doi(data.doi) if data.doi else None
        if paper is None:
            paper = self.get_by_title_normalized(data.title, data.publication_year)

        values = {
            "title": data.title,
            "doi": data.doi,
            "publication_year": data.publication_year,
            "publication_date": data.publication_date,
            "type": data.type,
            "language": data.language,
            "abstract": data.abstract,
            "is_open_access": data.is_open_access,
            "cited_by_count": data.cited_by_count,
        }

        if paper is None:
            paper = Paper(
                **values,
                created_by_user_id=created_by_user_id,
            )
            self.session.add(paper)
            return paper

        for field, value in values.items():
            if value is not None:
                setattr(paper, field, value)
        if paper.created_by_user_id is None and created_by_user_id is not None:
            paper.created_by_user_id = created_by_user_id
        return paper

    def upsert(
        self,
        data: ExternalPaperDTO,
        created_by_user_id: int | None = None,
        source_name: str = "openalex",
    ) -> Paper:
        """Insert or update a paper from an external API paper DTO.

        Matching uses external source id first, DOI second, then normalized title
        with publication year. Existing rows receive non-null fields from the
        supplied DTO. The transaction is intentionally not committed here.
        """
        return self.upsert_from_external(
            data,
            created_by_user_id=created_by_user_id,
            source_name=source_name,
        )

    def upsert_bulk(
        self,
        items: list[ExternalPaperDTO],
        created_by_user_id: int | None = None,
        source_name: str = "openalex",
    ) -> list[Paper]:
        """Insert or update many external papers and return ORM instances.

        Duplicates inside the batch are collapsed by DOI, external id, or
        normalized title/year. The session is flushed so ids are available for
        relationship repositories.
        """
        deduplicated = list(self._deduplicate_external_papers(items).values())
        if self._is_postgresql():
            return self._upsert_bulk_postgresql(
                deduplicated,
                created_by_user_id=created_by_user_id,
                source_name=source_name,
            )
        papers = [
            self.upsert(
                item,
                created_by_user_id=created_by_user_id,
                source_name=source_name,
            )
            for item in deduplicated
        ]
        self.session.flush()
        return papers

    def upsertBulk(
        self,
        items: list[ExternalPaperDTO],
        created_by_user_id: int | None = None,
        source_name: str = "openalex",
    ) -> list[Paper]:
        """Compatibility wrapper for callers that use camelCase naming."""
        return self.upsert_bulk(
            items,
            created_by_user_id=created_by_user_id,
            source_name=source_name,
        )

    def list_by_period(
        self,
        date_from: date | None,
        date_to: date | None,
        limit: int,
        offset: int,
    ) -> list[Paper]:
        """List papers whose publication_date falls within the optional period."""
        stmt = select(Paper).order_by(Paper.publication_date.desc().nullslast(), Paper.id.desc())
        if date_from is not None:
            stmt = stmt.where(Paper.publication_date >= date_from)
        if date_to is not None:
            stmt = stmt.where(Paper.publication_date <= date_to)
        stmt = stmt.limit(limit).offset(offset)
        return list(self.session.scalars(stmt).all())

    def list_ids_by_period(
        self,
        date_from: date | None,
        date_to: date | None,
        limit: int,
        offset: int,
    ) -> list[int]:
        """List paper ids whose publication_date falls within the optional period."""
        stmt = select(Paper.id).order_by(
            Paper.publication_date.desc().nullslast(),
            Paper.id.desc(),
        )
        if date_from is not None:
            stmt = stmt.where(Paper.publication_date >= date_from)
        if date_to is not None:
            stmt = stmt.where(Paper.publication_date <= date_to)
        stmt = stmt.limit(limit).offset(offset)
        return list(self.session.scalars(stmt).all())

    def list_recent(self, date_from: date, limit: int) -> list[Paper]:
        """List recent papers from the supplied publication date."""
        stmt = (
            select(Paper)
            .where(Paper.publication_date >= date_from)
            .order_by(Paper.publication_date.desc().nullslast(), Paper.id.desc())
            .limit(limit)
        )
        return list(self.session.scalars(stmt).all())

    def _normalize_title(self, title: str) -> str:
        return " ".join(title.strip().lower().split())

    def _deduplicate_external_papers(
        self,
        items: list[ExternalPaperDTO],
    ) -> dict[str, ExternalPaperDTO]:
        deduplicated: dict[str, ExternalPaperDTO] = {}
        for item in items:
            key = (
                item.doi
                or item.external_id
                or f"{self._normalize_title(item.title)}:{item.publication_year or ''}"
            )
            deduplicated[key] = item
        return deduplicated

    def _upsert_bulk_postgresql(
        self,
        items: list[ExternalPaperDTO],
        *,
        created_by_user_id: int | None,
        source_name: str,
    ) -> list[Paper]:
        existing_by_external = self._papers_by_external_ids(
            [item.external_id for item in items if item.external_id],
            source_name=source_name,
        )
        with_doi_values: list[dict[str, Any]] = []
        fallback_items: list[ExternalPaperDTO] = []

        for item in items:
            paper = (
                existing_by_external.get(item.external_id)
                if item.external_id
                else None
            )
            if paper is not None:
                self._apply_external_values(paper, item, created_by_user_id)
                continue
            if item.doi:
                with_doi_values.append(
                    {
                        **self._external_values(item),
                        "created_by_user_id": created_by_user_id,
                    }
                )
            else:
                fallback_items.append(item)

        if with_doi_values:
            stmt = pg_insert(Paper).values(with_doi_values)
            self.session.execute(
                stmt.on_conflict_do_update(
                    index_elements=[Paper.doi],
                    set_={
                        "title": stmt.excluded.title,
                        "publication_year": func.coalesce(
                            stmt.excluded.publication_year,
                            Paper.publication_year,
                        ),
                        "publication_date": func.coalesce(
                            stmt.excluded.publication_date,
                            Paper.publication_date,
                        ),
                        "type": func.coalesce(stmt.excluded.type, Paper.type),
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
                        "created_by_user_id": func.coalesce(
                            Paper.created_by_user_id,
                            stmt.excluded.created_by_user_id,
                        ),
                    },
                )
            )

        for item in fallback_items:
            self.upsert_from_external(
                item,
                created_by_user_id=created_by_user_id,
                source_name=source_name,
            )

        self.session.flush()
        papers: list[Paper] = []
        for item in items:
            paper = self._resolve_external_paper(item, source_name)
            if paper is None:
                raise InvalidRequestError(
                    "Paper could not be resolved after conflict-safe upsert",
                    details={
                        "external_id": item.external_id,
                        "doi": item.doi,
                        "title": item.title,
                        "publication_year": item.publication_year,
                    },
                )
            papers.append(paper)
        return papers

    def _papers_by_external_ids(
        self,
        external_ids: list[str],
        *,
        source_name: str,
    ) -> dict[str, Paper]:
        if not external_ids:
            return {}
        stmt = (
            select(PaperMetaSource.external_id, Paper)
            .join(Paper, Paper.id == PaperMetaSource.paper_id)
            .join(MetaSource, MetaSource.id == PaperMetaSource.meta_source_id)
            .where(
                MetaSource.name == source_name,
                PaperMetaSource.external_id.in_(set(external_ids)),
            )
        )
        return {external_id: paper for external_id, paper in self.session.execute(stmt)}

    def _resolve_external_paper(
        self,
        item: ExternalPaperDTO,
        source_name: str,
    ) -> Paper | None:
        paper = (
            self.get_by_external_id(source_name, item.external_id)
            if item.external_id
            else None
        )
        if paper is None and item.doi:
            paper = self.get_by_doi(item.doi)
        if paper is None and item.title:
            paper = self.get_by_title_normalized(item.title, item.publication_year)
        return paper

    def _external_values(self, item: ExternalPaperDTO) -> dict[str, Any]:
        return {
            "title": item.title,
            "doi": item.doi,
            "publication_year": item.publication_year,
            "publication_date": item.publication_date,
            "type": item.type,
            "language": item.language,
            "abstract": item.abstract,
            "is_open_access": item.is_open_access,
            "cited_by_count": item.cited_by_count,
        }

    def _apply_external_values(
        self,
        paper: Paper,
        item: ExternalPaperDTO,
        created_by_user_id: int | None,
    ) -> None:
        for field, value in self._external_values(item).items():
            if value is not None:
                if field == "doi" and not self._can_assign_doi(paper, value):
                    continue
                setattr(paper, field, value)
        if paper.created_by_user_id is None and created_by_user_id is not None:
            paper.created_by_user_id = created_by_user_id

    def _can_assign_doi(self, paper: Paper, doi: str) -> bool:
        if paper.doi == doi:
            return True
        existing = self.get_by_doi(doi)
        return existing is None or existing.id == paper.id


__all__ = ["PaperRepository"]
