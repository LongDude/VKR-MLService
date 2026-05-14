from __future__ import annotations

from datetime import date

from sqlalchemy import func, select

from core.exceptions import EntityNotFoundError
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


__all__ = ["PaperRepository"]
