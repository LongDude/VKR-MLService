from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

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
        if self._is_postgresql():
            stmt = pg_insert(MetaSource).values(name=name, prefix=prefix)
            self.session.execute(
                stmt.on_conflict_do_update(
                    index_elements=[MetaSource.name],
                    set_={"prefix": stmt.excluded.prefix},
                )
            )
            self.session.flush()
            source = self.get_source_by_name(name)
            if source is not None:
                return source
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

        pending_external = self._pending_instance(
            PaperMetaSource,
            meta_source_id=source.id,
            external_id=external_id,
        )
        if pending_external is not None and pending_external.paper_id != paper_id:
            raise DuplicateEntityError(
                "External id is already attached to another paper",
                details={
                    "paper_id": paper_id,
                    "existing_paper_id": pending_external.paper_id,
                    "source_name": source_name,
                    "external_id": external_id,
                },
            )
        if pending_external is not None:
            return

        existing_link = self._pending_instance(
            PaperMetaSource,
            paper_id=paper_id,
            meta_source_id=source.id,
        ) or self.session.get(PaperMetaSource, (paper_id, source.id))
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

    def attach_external_ids_bulk(
        self,
        items: list[tuple[int, str]],
        source_name: str,
        prefix: str,
    ) -> dict[str, object]:
        """Attach many source external ids without failing on duplicate conflicts."""
        if not items:
            return {"attached": 0, "conflicts": []}
        source = self.get_or_create_source(source_name, prefix)
        self.session.flush()
        deduplicated: dict[tuple[int, str], tuple[int, str]] = {
            (paper_id, external_id): (paper_id, external_id)
            for paper_id, external_id in items
            if external_id
        }
        if not deduplicated:
            return {"attached": 0, "conflicts": []}

        external_ids = [external_id for _, external_id in deduplicated.values()]
        existing_stmt = select(PaperMetaSource.external_id, PaperMetaSource.paper_id).where(
            PaperMetaSource.meta_source_id == source.id,
            PaperMetaSource.external_id.in_(set(external_ids)),
        )
        existing_by_external_id = {
            external_id: int(paper_id)
            for external_id, paper_id in self.session.execute(existing_stmt)
        }

        values: list[dict[str, object]] = []
        conflicts: list[dict[str, object]] = []
        for paper_id, external_id in deduplicated.values():
            existing_paper_id = existing_by_external_id.get(external_id)
            if existing_paper_id is not None and existing_paper_id != paper_id:
                conflicts.append(
                    {
                        "paper_id": paper_id,
                        "existing_paper_id": existing_paper_id,
                        "source_name": source_name,
                        "external_id": external_id,
                    }
                )
                continue
            values.append(
                {
                    "paper_id": paper_id,
                    "meta_source_id": source.id,
                    "external_id": external_id,
                }
            )

        if not values:
            return {"attached": 0, "conflicts": conflicts}

        if self._is_postgresql():
            stmt = pg_insert(PaperMetaSource).values(values)
            result = self.session.execute(stmt.on_conflict_do_nothing())
            return {
                "attached": int(result.rowcount or 0),
                "conflicts": conflicts,
            }

        attached = 0
        for value in values:
            self.attach_external_id(
                int(value["paper_id"]),
                source_name,
                prefix,
                str(value["external_id"]),
            )
            attached += 1
        return {"attached": attached, "conflicts": conflicts}

    def list_external_ids(self, paper_id: int) -> dict[str, str]:
        """Return external ids for a paper keyed by source name."""
        stmt = (
            select(MetaSource.name, PaperMetaSource.external_id)
            .join(PaperMetaSource, PaperMetaSource.meta_source_id == MetaSource.id)
            .where(PaperMetaSource.paper_id == paper_id)
        )
        return {name: external_id for name, external_id in self.session.execute(stmt)}

    def list_external_ids_by_papers(
        self,
        paper_ids: list[int],
    ) -> dict[int, dict[str, str]]:
        """Return external ids for many papers keyed by paper id and source name."""
        if not paper_ids:
            return {}
        stmt = (
            select(PaperMetaSource.paper_id, MetaSource.name, PaperMetaSource.external_id)
            .join(MetaSource, PaperMetaSource.meta_source_id == MetaSource.id)
            .where(PaperMetaSource.paper_id.in_(paper_ids))
        )
        result: dict[int, dict[str, str]] = {}
        for paper_id, source_name, external_id in self.session.execute(stmt):
            result.setdefault(int(paper_id), {})[source_name] = external_id
        return result


__all__ = ["PaperMetaSourceRepository"]
