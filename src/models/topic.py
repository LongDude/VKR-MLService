from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import BigInteger, Text
from sqlalchemy.ext.associationproxy import AssociationProxy, association_proxy
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .associations import PaperTopic, UserTrackedTopic
from .base import Base

if TYPE_CHECKING:
    from .paper import Paper
    from .user import User


class Topic(Base):
    __tablename__ = "topics"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    subfield_id: Mapped[int | None] = mapped_column(BigInteger)
    openalex_id: Mapped[str | None] = mapped_column(Text, unique=True)
    name: Mapped[str] = mapped_column(Text, nullable=False)

    paper_links: Mapped[list["PaperTopic"]] = relationship(
        back_populates="topic", cascade="all, delete-orphan"
    )
    tracked_by_links: Mapped[list["UserTrackedTopic"]] = relationship(
        back_populates="topic", cascade="all, delete-orphan"
    )
    papers: AssociationProxy[list["Paper"]] = association_proxy(
        "paper_links",
        "paper",
        creator=lambda paper: PaperTopic(paper=paper),
    )
    tracked_by_users: AssociationProxy[list["User"]] = association_proxy(
        "tracked_by_links",
        "user",
        creator=lambda user: UserTrackedTopic(user=user),
    )

    def __repr__(self) -> str:
        return f"Topic(id={self.id!r}, name={self.name!r})"
