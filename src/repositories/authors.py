from __future__ import annotations

from sqlalchemy import select

from dto.authors import AuthorCreateDTO
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
        link = self.session.get(PaperAuthor, (paper_id, author_id))
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


__all__ = ["AuthorRepository"]
