from __future__ import annotations

from collections import defaultdict

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from core.exceptions import InvalidRequestError
from dto.authors import AuthorCreateDTO
from dto.external import ExternalAuthorDTO
from models import Author, PaperAuthor

from .base import BaseRepository


class AuthorRepository(BaseRepository):
    def get_by_id(self, author_id: int) -> Author | None:
        """Return an author by primary key."""
        return self.session.get(Author, author_id)

    def get_by_orcid(self, orcid: str) -> Author | None:
        """Return an author by ORCID."""
        stmt = select(Author).where(Author.orcid == orcid)
        return self.session.scalar(stmt)

    def upsert(self, data: ExternalAuthorDTO) -> Author:
        """Insert or update an author from an external API author DTO.

        Authors are matched by ORCID when it is present, otherwise by normalized
        display name. Existing rows are updated with non-empty values from the
        external DTO. The transaction is not committed by this method.
        """
        display_name = data.display_name.strip()
        if not display_name:
            raise InvalidRequestError("External author display_name is required")

        author = self.get_by_orcid(data.orcid) if data.orcid else None
        if author is None:
            author = self.session.scalar(
                select(Author).where(Author.display_name == display_name)
            )

        if author is None:
            author = Author(display_name=display_name, orcid=data.orcid)
            self.session.add(author)
            return author

        author.display_name = display_name
        if data.orcid is not None:
            author.orcid = data.orcid
        return author

    def upsert_bulk(self, items: list[ExternalAuthorDTO]) -> list[Author]:
        """Insert or update many external authors and return ORM instances.

        Duplicates inside the supplied batch are collapsed by ORCID when
        available, otherwise by normalized display name. The session is flushed
        so returned instances have primary keys for relationship creation.
        """
        deduplicated = list(self._deduplicate_external_authors(items).values())
        if self._is_postgresql():
            return self._upsert_bulk_postgresql(deduplicated)
        authors = [self.upsert(item) for item in deduplicated]
        self.session.flush()
        return authors

    def upsertBulk(self, items: list[ExternalAuthorDTO]) -> list[Author]:
        """Compatibility wrapper for callers that use camelCase naming."""
        return self.upsert_bulk(items)

    def get_or_create(self, data: AuthorCreateDTO) -> Author:
        """Return an existing author or create one."""
        if data.orcid:
            author = self.get_by_orcid(data.orcid)
            if author is not None:
                return author
        else:
            author = self.session.scalar(
                select(Author).where(Author.display_name == data.display_name.strip())
            )
            if author is not None:
                return author

        author = Author(display_name=data.display_name.strip(), orcid=data.orcid)
        self.session.add(author)
        return author

    def attach_to_paper(
        self,
        paper_id: int,
        author_id: int,
        author_order: int | None,
        is_corresponding: bool = False,
    ) -> None:
        """Attach an author to a paper idempotently."""
        link = self._pending_instance(
            PaperAuthor,
            paper_id=paper_id,
            author_id=author_id,
        ) or self.session.get(PaperAuthor, (paper_id, author_id))
        if link is None:
            self.session.add(
                PaperAuthor(
                    paper_id=paper_id,
                    author_id=author_id,
                    author_order=author_order,
                    is_corresponding=is_corresponding,
                )
            )
            return
        link.author_order = author_order
        link.is_corresponding = is_corresponding

    def attach_to_paper_bulk(
        self,
        items: list[tuple[int, int, int | None, bool]],
    ) -> None:
        """Attach authors to papers in a conflict-safe batch."""
        if not items:
            return
        deduplicated: dict[tuple[int, int], tuple[int, int, int | None, bool]] = {}
        for paper_id, author_id, author_order, is_corresponding in items:
            deduplicated[(paper_id, author_id)] = (
                paper_id,
                author_id,
                author_order,
                is_corresponding,
            )
        if not self._is_postgresql():
            for (
                paper_id,
                author_id,
                author_order,
                is_corresponding,
            ) in deduplicated.values():
                self.attach_to_paper(
                    paper_id,
                    author_id,
                    author_order,
                    is_corresponding=is_corresponding,
                )
            self.session.flush()
            return

        values = [
            {
                "paper_id": paper_id,
                "author_id": author_id,
                "author_order": author_order,
                "is_corresponding": is_corresponding,
            }
            for paper_id, author_id, author_order, is_corresponding in deduplicated.values()
        ]
        stmt = pg_insert(PaperAuthor).values(values)
        self.session.execute(
            stmt.on_conflict_do_update(
                index_elements=[PaperAuthor.paper_id, PaperAuthor.author_id],
                set_={
                    "author_order": stmt.excluded.author_order,
                    "is_corresponding": stmt.excluded.is_corresponding,
                },
            )
        )

    def list_by_paper(self, paper_id: int) -> list[Author]:
        """List authors attached to a paper."""
        stmt = (
            select(Author)
            .join(PaperAuthor, PaperAuthor.author_id == Author.id)
            .where(PaperAuthor.paper_id == paper_id)
            .order_by(PaperAuthor.author_order.asc().nullslast(), Author.id.asc())
        )
        return list(self.session.scalars(stmt).all())

    def list_by_papers(self, paper_ids: list[int]) -> dict[int, list[Author]]:
        """List authors for many papers keyed by paper id."""
        if not paper_ids:
            return {}
        stmt = (
            select(PaperAuthor.paper_id, Author)
            .join(Author, PaperAuthor.author_id == Author.id)
            .where(PaperAuthor.paper_id.in_(paper_ids))
            .order_by(
                PaperAuthor.paper_id.asc(),
                PaperAuthor.author_order.asc().nullslast(),
                Author.id.asc(),
            )
        )
        authors_by_paper: dict[int, list[Author]] = defaultdict(list)
        for paper_id, author in self.session.execute(stmt):
            authors_by_paper[int(paper_id)].append(author)
        return dict(authors_by_paper)

    def _deduplicate_external_authors(
        self,
        items: list[ExternalAuthorDTO],
    ) -> dict[str, ExternalAuthorDTO]:
        deduplicated: dict[str, ExternalAuthorDTO] = {}
        for item in items:
            key = item.orcid or item.external_id or item.display_name.strip().lower()
            deduplicated[key] = item
        return deduplicated

    def _upsert_bulk_postgresql(
        self,
        items: list[ExternalAuthorDTO],
    ) -> list[Author]:
        ordered_keys: list[str] = []
        with_orcid: list[dict[str, str]] = []
        without_orcid: list[ExternalAuthorDTO] = []
        for item in items:
            display_name = item.display_name.strip()
            if not display_name:
                raise InvalidRequestError("External author display_name is required")
            if item.orcid:
                ordered_keys.append(f"orcid:{item.orcid}")
                with_orcid.append({"display_name": display_name, "orcid": item.orcid})
            else:
                ordered_keys.append(f"name:{display_name}")
                without_orcid.append(item)

        if with_orcid:
            stmt = pg_insert(Author).values(with_orcid)
            self.session.execute(
                stmt.on_conflict_do_update(
                    index_elements=[Author.orcid],
                    set_={"display_name": stmt.excluded.display_name},
                )
            )

        names = [item.display_name.strip() for item in without_orcid]
        existing_by_name = self._authors_by_display_names(names)
        for item in without_orcid:
            display_name = item.display_name.strip()
            if display_name in existing_by_name:
                existing_by_name[display_name].display_name = display_name
                continue
            author = Author(display_name=display_name, orcid=None)
            self.session.add(author)
            existing_by_name[display_name] = author

        self.session.flush()
        authors_by_key: dict[str, Author] = {}
        orcids = [value["orcid"] for value in with_orcid]
        if orcids:
            for author in self.session.scalars(
                select(Author).where(Author.orcid.in_(orcids))
            ):
                authors_by_key[f"orcid:{author.orcid}"] = author
        for name, author in self._authors_by_display_names(names).items():
            authors_by_key[f"name:{name}"] = author
        return [authors_by_key[key] for key in ordered_keys]

    def _authors_by_display_names(self, names: list[str]) -> dict[str, Author]:
        if not names:
            return {}
        stmt = select(Author).where(Author.display_name.in_(set(names)))
        return {author.display_name: author for author in self.session.scalars(stmt)}


__all__ = ["AuthorRepository"]
