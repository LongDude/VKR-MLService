from __future__ import annotations

from datetime import date, datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import (
    BigInteger,
    Boolean,
    Date,
    DateTime,
    ForeignKey,
    Integer,
    SmallInteger,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.associationproxy import AssociationProxy, association_proxy
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .associations import PaperAuthor, PaperKeyword, PaperTopic
from .base import Base

if TYPE_CHECKING:
    from .author import Author
    from .keyword import Keyword
    from .topic import Topic
    from .associations import UserFavouritePaper


class Paper(Base):
    __tablename__ = "papers"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    doi: Mapped[str | None] = mapped_column(Text, unique=True)
    publication_year: Mapped[int | None] = mapped_column(SmallInteger, index=True)
    publication_date: Mapped[date | None] = mapped_column(Date)
    language: Mapped[str | None] = mapped_column(Text)
    abstract: Mapped[str | None] = mapped_column(Text)
    is_open_access: Mapped[bool | None] = mapped_column(Boolean)
    cited_by_count: Mapped[int | None] = mapped_column(
        Integer, server_default=text("0")
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )
    openalex_id: Mapped[str | None] = mapped_column(Text)
    references_count: Mapped[int | None] = mapped_column(
        Integer, server_default=text("0")
    )
    primary_topic_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("topics.id", ondelete="SET NULL")
    )
    extracted_keywords: Mapped[Any | None] = mapped_column(JSONB)
    text_hash: Mapped[str | None] = mapped_column(Text)
    is_indexed: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default=text("false")
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )
    indexed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
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
    primary_topic: Mapped["Topic | None"] = relationship(
        back_populates="primary_papers",
        foreign_keys=[primary_topic_id],
    )
    landings: Mapped[list["Landing"]] = relationship(
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


class Landing(Base):
    __tablename__ = "landings"
    __table_args__ = (
        UniqueConstraint("paper_id", "landing_url"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    paper_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("papers.id", ondelete="CASCADE"), nullable=False, index=True
    )
    landing_url: Mapped[str] = mapped_column(Text, nullable=False)
    pdf_url: Mapped[str | None] = mapped_column(Text)
    license: Mapped[str | None] = mapped_column(Text)
    version: Mapped[str | None] = mapped_column(Text)
    is_best: Mapped[bool | None] = mapped_column(Boolean, server_default=text("false"))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )

    paper: Mapped["Paper"] = relationship(back_populates="landings")

    def __repr__(self) -> str:
        return f"Landing(id={self.id!r}, landing_url={self.landing_url!r})"
