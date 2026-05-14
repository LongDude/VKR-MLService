from __future__ import annotations

from collections import defaultdict

from sqlalchemy import select

from core.exceptions import InvalidRequestError
from dto.external import ExternalInstitutionDTO
from dto.institutions import InstitutionCreateDTO
from models import AuthorInstitution, Institution, PaperAuthor

from .base import BaseRepository


class InstitutionRepository(BaseRepository):
    def get_by_id(self, institution_id: int) -> Institution | None:
        """Return an institution by primary key."""
        return self.session.get(Institution, institution_id)

    def get_by_ror(self, ror: str) -> Institution | None:
        """Return an institution by ROR."""
        stmt = select(Institution).where(Institution.ror == ror)
        return self.session.scalar(stmt)

    def upsert(self, data: ExternalInstitutionDTO) -> Institution:
        """Insert or update an institution from an external API DTO.

        Institutions are matched by ROR when it is present, otherwise by
        normalized display name. Existing rows are updated with non-null fields
        from the new external version. The method does not commit.
        """
        display_name = data.display_name.strip()
        if not display_name:
            raise InvalidRequestError("External institution display_name is required")

        institution = self.get_by_ror(data.ror) if data.ror else None
        if institution is None:
            institution = self.session.scalar(
                select(Institution).where(Institution.display_name == display_name)
            )

        if institution is None:
            institution = Institution(
                display_name=display_name,
                ror=data.ror,
                country_code=data.country_code,
                type=data.type,
            )
            self.session.add(institution)
            return institution

        institution.display_name = display_name
        for field in ("ror", "country_code", "type"):
            value = getattr(data, field)
            if value is not None:
                setattr(institution, field, value)
        return institution

    def upsert_bulk(
        self,
        items: list[ExternalInstitutionDTO],
    ) -> list[Institution]:
        """Insert or update many external institutions and return instances.

        Duplicates inside the batch are collapsed by ROR, external id, or
        normalized display name. The session is flushed before returning.
        """
        deduplicated = list(self._deduplicate_external_institutions(items).values())
        institutions = [self.upsert(item) for item in deduplicated]
        self.session.flush()
        return institutions

    def upsertBulk(
        self,
        items: list[ExternalInstitutionDTO],
    ) -> list[Institution]:
        """Compatibility wrapper for callers that use camelCase naming."""
        return self.upsert_bulk(items)

    def get_or_create(self, data: InstitutionCreateDTO) -> Institution:
        """Return an existing institution or create one."""
        if data.ror:
            institution = self.get_by_ror(data.ror)
            if institution is not None:
                return institution
        else:
            institution = self.session.scalar(
                select(Institution).where(
                    Institution.display_name == data.display_name.strip()
                )
            )
            if institution is not None:
                return institution

        institution = Institution(
            display_name=data.display_name.strip(),
            ror=data.ror,
            country_code=data.country_code,
            type=data.type,
        )
        self.session.add(institution)
        return institution

    def attach_to_author(self, author_id: int, institution_id: int) -> None:
        """Attach an institution to an author idempotently."""
        link = self._pending_instance(
            AuthorInstitution,
            author_id=author_id,
            institution_id=institution_id,
        ) or self.session.get(AuthorInstitution, (author_id, institution_id))
        if link is None:
            self.session.add(
                AuthorInstitution(
                    author_id=author_id,
                    institution_id=institution_id,
                )
            )

    def list_by_author(self, author_id: int) -> list[Institution]:
        """List institutions attached to an author."""
        stmt = (
            select(Institution)
            .join(AuthorInstitution, AuthorInstitution.institution_id == Institution.id)
            .where(AuthorInstitution.author_id == author_id)
            .order_by(Institution.display_name.asc())
        )
        return list(self.session.scalars(stmt).all())

    def list_by_papers(self, paper_ids: list[int]) -> dict[int, list[Institution]]:
        """List institutions linked through paper authors keyed by paper id."""
        if not paper_ids:
            return {}
        stmt = (
            select(PaperAuthor.paper_id, Institution)
            .join(
                AuthorInstitution,
                AuthorInstitution.author_id == PaperAuthor.author_id,
            )
            .join(Institution, Institution.id == AuthorInstitution.institution_id)
            .where(PaperAuthor.paper_id.in_(paper_ids))
            .order_by(PaperAuthor.paper_id.asc(), Institution.display_name.asc())
        )
        institutions_by_paper: dict[int, list[Institution]] = defaultdict(list)
        seen: set[tuple[int, int]] = set()
        for paper_id, institution in self.session.execute(stmt):
            key = (int(paper_id), int(institution.id))
            if key in seen:
                continue
            seen.add(key)
            institutions_by_paper[int(paper_id)].append(institution)
        return dict(institutions_by_paper)

    def _deduplicate_external_institutions(
        self,
        items: list[ExternalInstitutionDTO],
    ) -> dict[str, ExternalInstitutionDTO]:
        deduplicated: dict[str, ExternalInstitutionDTO] = {}
        for item in items:
            key = item.ror or item.external_id or item.display_name.strip().lower()
            deduplicated[key] = item
        return deduplicated


__all__ = ["InstitutionRepository"]
