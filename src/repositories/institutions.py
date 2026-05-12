from __future__ import annotations

from sqlalchemy import select

from dto.institutions import InstitutionCreateDTO
from models import AuthorInstitution, Institution

from .base import BaseRepository


class InstitutionRepository(BaseRepository):
    def get_by_id(self, institution_id: int) -> Institution | None:
        """Return an institution by primary key."""
        return self.session.get(Institution, institution_id)

    def get_by_ror(self, ror: str) -> Institution | None:
        """Return an institution by ROR."""
        stmt = select(Institution).where(Institution.ror == ror)
        return self.session.scalar(stmt)

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
        link = self.session.get(AuthorInstitution, (author_id, institution_id))
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


__all__ = ["InstitutionRepository"]
