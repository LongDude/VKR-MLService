from __future__ import annotations

from collections.abc import Iterable

from sqlalchemy import update, select

from core.exceptions import EntityNotFoundError
from dto.external import ExternalLandingDTO
from models import Landing

from .base import BaseRepository


class LandingRepository(BaseRepository):
    def list_by_paper(self, paper_id: int) -> list[Landing]:
        """List landing records for a paper."""
        stmt = (
            select(Landing)
            .where(Landing.paper_id == paper_id)
            .order_by(Landing.is_best.desc().nullslast(), Landing.id.asc())
        )
        return list(self.session.scalars(stmt).all())

    def get_best_by_paper(self, paper_id: int) -> Landing | None:
        """Return the best landing record for a paper."""
        stmt = (
            select(Landing)
            .where(Landing.paper_id == paper_id, Landing.is_best.is_(True))
            .order_by(Landing.id.asc())
            .limit(1)
        )
        return self.session.scalar(stmt)

    def upsert(self, paper_id: int, data: ExternalLandingDTO) -> Landing:
        """Insert or update a landing record by paper and landing URL."""
        landing = self._pending_instance(
            Landing,
            paper_id=paper_id,
            landing_url=data.landing_url,
        ) or self.session.scalar(
            select(Landing).where(
                Landing.paper_id == paper_id,
                Landing.landing_url == data.landing_url,
            )
        )
        if landing is None:
            landing = Landing(
                paper_id=paper_id,
                landing_url=data.landing_url,
                pdf_url=data.pdf_url,
                license=data.license,
                version=data.version,
                is_best=data.is_best,
            )
            self.session.add(landing)
        else:
            for field in ("pdf_url", "license", "version", "is_best"):
                value = getattr(data, field)
                if value is not None:
                    setattr(landing, field, value)

        if data.is_best is True:
            self._set_best_for_instance(paper_id, landing)
        return landing

    def upsert_bulk(
        self,
        items: Iterable[tuple[int, ExternalLandingDTO]],
    ) -> list[Landing]:
        """Insert or update many landing records for already upserted papers.

        The iterable contains ``(paper_id, ExternalLandingDTO)`` pairs because a
        landing URL is unique only inside a paper. Duplicate paper/url pairs in
        the same batch are collapsed. The session is flushed before returning.
        """
        deduplicated: dict[tuple[int, str], ExternalLandingDTO] = {}
        for paper_id, data in items:
            deduplicated[(paper_id, data.landing_url)] = data

        landings = [
            self.upsert(paper_id, data)
            for (paper_id, _), data in deduplicated.items()
        ]
        self.session.flush()
        return landings

    def upsertBulk(
        self,
        items: Iterable[tuple[int, ExternalLandingDTO]],
    ) -> list[Landing]:
        """Compatibility wrapper for callers that use camelCase naming."""
        return self.upsert_bulk(items)

    def set_best(self, paper_id: int, landing_id: int) -> None:
        """Mark one landing as best and clear the flag from others."""
        landing = self.session.get(Landing, landing_id)
        if landing is None or landing.paper_id != paper_id:
            raise EntityNotFoundError(
                "Landing not found",
                details={"paper_id": paper_id, "landing_id": landing_id},
            )
        self._set_best_for_instance(paper_id, landing)

    def _set_best_for_instance(self, paper_id: int, landing: Landing) -> None:
        self.session.execute(
            update(Landing)
            .where(Landing.paper_id == paper_id, Landing.id != landing.id)
            .values(is_best=False)
        )
        landing.is_best = True


__all__ = ["LandingRepository"]
