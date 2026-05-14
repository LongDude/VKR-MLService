from __future__ import annotations

from collections import defaultdict

from sqlalchemy import select

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


__all__ = ["AuthorRepository"]
