from __future__ import annotations

from sqlalchemy import select

from core.exceptions import DuplicateEntityError
from models import MetaSource, Paper, PaperMetaSource

from .base import BaseRepository


class PaperMetaSourceRepository(BaseRepository):
    def get_source_by_name(self, name: str) -> MetaSource | None:
        """Return a metadata source by name."""
        stmt = select(MetaSource).where(MetaSource.name == name)
        return self.session.scalar(stmt)

    def get_or_create_source(self, name: str, prefix: str) -> MetaSource:
        """Return an existing metadata source or create it."""
        source = self.get_source_by_name(name)
        if source is not None:
            return source
        source = MetaSource(name=name, prefix=prefix)
        self.session.add(source)
        self.session.flush()
        return source

    def find_paper_by_external_id(
        self,
        source_name: str,
        external_id: str,
    ) -> Paper | None:
        """Return the paper attached to the source external id."""
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

    def attach_external_id(
        self,
        paper_id: int,
        source_name: str,
        prefix: str,
        external_id: str,
    ) -> None:
        """Attach a source external id to a paper idempotently."""
        source = self.get_or_create_source(source_name, prefix)
        self.session.flush()

        existing_external = self.session.scalar(
            select(PaperMetaSource).where(
                PaperMetaSource.meta_source_id == source.id,
                PaperMetaSource.external_id == external_id,
            )
        )
        if existing_external is not None and existing_external.paper_id != paper_id:
            raise DuplicateEntityError(
                "External id is already attached to another paper",
                details={
                    "paper_id": paper_id,
                    "existing_paper_id": existing_external.paper_id,
                    "source_name": source_name,
                    "external_id": external_id,
                },
            )
        if existing_external is not None:
            return

        existing_link = self.session.get(PaperMetaSource, (paper_id, source.id))
        if existing_link is not None:
            existing_link.external_id = external_id
            return

        self.session.add(
            PaperMetaSource(
                paper_id=paper_id,
                meta_source_id=source.id,
                external_id=external_id,
            )
        )

    def list_external_ids(self, paper_id: int) -> dict[str, str]:
        """Return external ids for a paper keyed by source name."""
        stmt = (
            select(MetaSource.name, PaperMetaSource.external_id)
            .join(PaperMetaSource, PaperMetaSource.meta_source_id == MetaSource.id)
            .where(PaperMetaSource.paper_id == paper_id)
        )
        return {name: external_id for name, external_id in self.session.execute(stmt)}


__all__ = ["PaperMetaSourceRepository"]
