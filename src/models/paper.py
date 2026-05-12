from __future__ import annotations

from datetime import date, datetime
from typing import TYPE_CHECKING

from sqlalchemy import (
    BigInteger,
    Boolean,
    Date,
    DateTime,
    ForeignKey,
    Integer,
    SmallInteger,
    Text,
    text,
)
from sqlalchemy.ext.associationproxy import AssociationProxy, association_proxy
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .associations import PaperAuthor, PaperKeyword, PaperTopic
from .base import Base

if TYPE_CHECKING:
    from .author import Author
    from .keyword import Keyword
    from .topic import Topic
    from .associations import UserFavouritePaper
    from .user import User


class Paper(Base):
    __tablename__ = "papers"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    doi: Mapped[str | None] = mapped_column(Text, unique=True)
    publication_year: Mapped[int | None] = mapped_column(SmallInteger, index=True)
    publication_date: Mapped[date | None] = mapped_column(Date)
    type: Mapped[str | None] = mapped_column(Text)
    language: Mapped[str | None] = mapped_column(Text)
    abstract: Mapped[str | None] = mapped_column(Text)
    is_open_access: Mapped[bool | None] = mapped_column(Boolean)
    cited_by_count: Mapped[int | None] = mapped_column(
        Integer, server_default=text("0")
    )
    created_by_user_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("users.id", ondelete="SET NULL")
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )

    created_by_user: Mapped["User | None"] = relationship(
        back_populates="created_papers"
    )
    favourited_by_links: Mapped[list["UserFavouritePaper"]] = relationship(
        back_populates="paper", cascade="all, delete-orphan"
    )
    author_links: Mapped[list["PaperAuthor"]] = relationship(
        back_populates="paper", cascade="all, delete-orphan"
    )
    topic_links: Mapped[list["PaperTopic"]] = relationship(
        back_populates="paper", cascade="all, delete-orphan"
    )
    keyword_links: Mapped[list["PaperKeyword"]] = relationship(
        back_populates="paper", cascade="all, delete-orphan"
    )
    authors: AssociationProxy[list["Author"]] = association_proxy(
        "author_links",
        "author",
        creator=lambda author: PaperAuthor(author=author),
    )
    topics: AssociationProxy[list["Topic"]] = association_proxy(
        "topic_links",
        "topic",
        creator=lambda topic: PaperTopic(topic=topic),
    )
    keywords: AssociationProxy[list["Keyword"]] = association_proxy(
        "keyword_links",
        "keyword",
        creator=lambda keyword: PaperKeyword(keyword=keyword),
    )

    def __repr__(self) -> str:
        return f"Paper(id={self.id!r}, title={self.title!r})"
