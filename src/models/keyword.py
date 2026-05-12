from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import BigInteger, Text
from sqlalchemy.ext.associationproxy import AssociationProxy, association_proxy
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .associations import PaperKeyword, UserTrackedKeyword
from .base import Base

if TYPE_CHECKING:
    from .paper import Paper
    from .user import User


class Keyword(Base):
    __tablename__ = "keywords"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    value: Mapped[str] = mapped_column(Text, nullable=False, unique=True)

    paper_links: Mapped[list["PaperKeyword"]] = relationship(
        back_populates="keyword", cascade="all, delete-orphan"
    )
    tracked_by_links: Mapped[list["UserTrackedKeyword"]] = relationship(
        back_populates="keyword", cascade="all, delete-orphan"
    )
    papers: AssociationProxy[list["Paper"]] = association_proxy(
        "paper_links",
        "paper",
        creator=lambda paper: PaperKeyword(paper=paper),
    )
    tracked_by_users: AssociationProxy[list["User"]] = association_proxy(
        "tracked_by_links",
        "user",
        creator=lambda user: UserTrackedKeyword(user=user),
    )

    def __repr__(self) -> str:
        return f"Keyword(id={self.id!r}, value={self.value!r})"
