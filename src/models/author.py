from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import BigInteger, DateTime, Text, text
from sqlalchemy.ext.associationproxy import AssociationProxy, association_proxy
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .associations import AuthorInstitution, PaperAuthor
from .base import Base

if TYPE_CHECKING:
    from .institution import Institution
    from .paper import Paper


class Author(Base):
    __tablename__ = "authors"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    display_name: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    orcid: Mapped[str | None] = mapped_column(Text, unique=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )

    paper_links: Mapped[list["PaperAuthor"]] = relationship(
        back_populates="author", cascade="all, delete-orphan"
    )
    institution_links: Mapped[list["AuthorInstitution"]] = relationship(
        back_populates="author", cascade="all, delete-orphan"
    )
    papers: AssociationProxy[list["Paper"]] = association_proxy(
        "paper_links",
        "paper",
        creator=lambda paper: PaperAuthor(paper=paper),
    )
    institutions: AssociationProxy[list["Institution"]] = association_proxy(
        "institution_links",
        "institution",
        creator=lambda institution: AuthorInstitution(institution=institution),
    )

    def __repr__(self) -> str:
        return f"Author(id={self.id!r}, display_name={self.display_name!r})"
