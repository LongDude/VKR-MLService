from __future__ import annotations

from collections import defaultdict

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert

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
        if self._is_postgresql():
            return self._upsert_bulk_postgresql(deduplicated)
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

    def attach_to_author_bulk(
        self,
        items: list[tuple[int, int]],
    ) -> None:
        """Attach institutions to authors in a conflict-safe batch."""
        if not items:
            return
        deduplicated = sorted(set(items))
        if not self._is_postgresql():
            for author_id, institution_id in deduplicated:
                self.attach_to_author(author_id, institution_id)
            self.session.flush()
            return
        stmt = pg_insert(AuthorInstitution).values(
            [
                {"author_id": author_id, "institution_id": institution_id}
                for author_id, institution_id in deduplicated
            ]
        )
        self.session.execute(
            stmt.on_conflict_do_nothing(
                index_elements=[
                    AuthorInstitution.author_id,
                    AuthorInstitution.institution_id,
                ]
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

    def _upsert_bulk_postgresql(
        self,
        items: list[ExternalInstitutionDTO],
    ) -> list[Institution]:
        ordered_keys: list[str] = []
        with_ror: list[dict[str, str | None]] = []
        without_ror: list[ExternalInstitutionDTO] = []
        for item in items:
            display_name = item.display_name.strip()
            if not display_name:
                raise InvalidRequestError(
                    "External institution display_name is required"
                )
            if item.ror:
                ordered_keys.append(f"ror:{item.ror}")
                with_ror.append(
                    {
                        "display_name": display_name,
                        "ror": item.ror,
                        "country_code": item.country_code,
                        "type": item.type,
                    }
                )
            else:
                ordered_keys.append(f"name:{display_name}")
                without_ror.append(item)

        if with_ror:
            stmt = pg_insert(Institution).values(with_ror)
            self.session.execute(
                stmt.on_conflict_do_update(
                    index_elements=[Institution.ror],
                    set_={
                        "display_name": stmt.excluded.display_name,
                        "country_code": func.coalesce(
                            stmt.excluded.country_code,
                            Institution.country_code,
                        ),
                        "type": func.coalesce(
                            stmt.excluded.type,
                            Institution.type,
                        ),
                    },
                )
            )

        names = [item.display_name.strip() for item in without_ror]
        existing_by_name = self._institutions_by_display_names(names)
        for item in without_ror:
            display_name = item.display_name.strip()
            institution = existing_by_name.get(display_name)
            if institution is None:
                institution = Institution(
                    display_name=display_name,
                    ror=None,
                    country_code=item.country_code,
                    type=item.type,
                )
                self.session.add(institution)
                existing_by_name[display_name] = institution
                continue
            institution.display_name = display_name
            if item.country_code is not None:
                institution.country_code = item.country_code
            if item.type is not None:
                institution.type = item.type

        self.session.flush()
        institutions_by_key: dict[str, Institution] = {}
        rors = [value["ror"] for value in with_ror if value["ror"]]
        if rors:
            for institution in self.session.scalars(
                select(Institution).where(Institution.ror.in_(rors))
            ):
                institutions_by_key[f"ror:{institution.ror}"] = institution
        for name, institution in self._institutions_by_display_names(names).items():
            institutions_by_key[f"name:{name}"] = institution
        return [institutions_by_key[key] for key in ordered_keys]

    def _institutions_by_display_names(
        self, names: list[str]
    ) -> dict[str, Institution]:
        if not names:
            return {}
        stmt = select(Institution).where(Institution.display_name.in_(set(names)))
        return {
            institution.display_name: institution
            for institution in self.session.scalars(stmt)
        }


__all__ = ["InstitutionRepository"]
