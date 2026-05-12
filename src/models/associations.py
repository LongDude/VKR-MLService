from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING

from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    ForeignKey,
    Integer,
    Numeric,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base

if TYPE_CHECKING:
    from .author import Author
    from .institution import Institution
    from .keyword import Keyword
    from .paper import MetaSource, Paper
    from .topic import Domain, Subfield, Topic
    from .user import User


class UserFavouritePaper(Base):
    __tablename__ = "user_favourite_papers"

    user_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("users.id", ondelete="CASCADE"), primary_key=True
    )
    paper_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("papers.id", ondelete="CASCADE"),
        primary_key=True,
        index=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )

    user: Mapped["User"] = relationship(back_populates="favourite_paper_links")
    paper: Mapped["Paper"] = relationship(back_populates="favourited_by_links")


class PaperAuthor(Base):
    __tablename__ = "paper_authors"

    paper_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("papers.id", ondelete="CASCADE"), primary_key=True
    )
    author_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("authors.id", ondelete="CASCADE"),
        primary_key=True,
        index=True,
    )
    author_order: Mapped[int | None] = mapped_column(Integer)
    is_corresponding: Mapped[bool | None] = mapped_column(
        Boolean, server_default=text("false")
    )

    paper: Mapped["Paper"] = relationship(back_populates="author_links")
    author: Mapped["Author"] = relationship(back_populates="paper_links")


class AuthorInstitution(Base):
    __tablename__ = "author_institutions"

    author_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("authors.id", ondelete="CASCADE"),
        primary_key=True,
    )
    institution_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("institutions.id", ondelete="CASCADE"),
        primary_key=True,
        index=True,
    )

    author: Mapped["Author"] = relationship(back_populates="institution_links")
    institution: Mapped["Institution"] = relationship(back_populates="author_links")


class PaperMetaSource(Base):
    __tablename__ = "paper_meta_sources"
    __table_args__ = (
        UniqueConstraint("meta_source_id", "external_id"),
    )

    paper_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("papers.id", ondelete="CASCADE"),
        primary_key=True,
    )
    meta_source_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("meta_sources.id", ondelete="CASCADE"),
        primary_key=True,
    )
    external_id: Mapped[str] = mapped_column(Text, nullable=False, index=True)

    paper: Mapped["Paper"] = relationship(back_populates="meta_source_links")
    meta_source: Mapped["MetaSource"] = relationship(back_populates="paper_links")


class PaperTopic(Base):
    __tablename__ = "paper_topics"

    paper_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("papers.id", ondelete="CASCADE"), primary_key=True
    )
    topic_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("topics.id", ondelete="CASCADE"),
        primary_key=True,
        index=True,
    )
    score: Mapped[Decimal | None] = mapped_column(Numeric(6, 5))

    paper: Mapped["Paper"] = relationship(back_populates="topic_links")
    topic: Mapped["Topic"] = relationship(back_populates="paper_links")


class PaperKeyword(Base):
    __tablename__ = "paper_keywords"

    paper_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("papers.id", ondelete="CASCADE"), primary_key=True
    )
    keyword_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("keywords.id", ondelete="CASCADE"),
        primary_key=True,
        index=True,
    )
    score: Mapped[Decimal | None] = mapped_column(Numeric(6, 5))

    paper: Mapped["Paper"] = relationship(back_populates="keyword_links")
    keyword: Mapped["Keyword"] = relationship(back_populates="paper_links")


class UserTrackedTopic(Base):
    __tablename__ = "user_tracked_topics"

    user_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("users.id", ondelete="CASCADE"), primary_key=True
    )
    topic_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("topics.id", ondelete="CASCADE"), primary_key=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )

    user: Mapped["User"] = relationship(back_populates="tracked_topic_links")
    topic: Mapped["Topic"] = relationship(back_populates="tracked_by_links")


class UserTrackedDomain(Base):
    __tablename__ = "user_tracked_domains"

    user_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("users.id", ondelete="CASCADE"), primary_key=True
    )
    domain_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("domains.id", ondelete="CASCADE"), primary_key=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )

    user: Mapped["User"] = relationship(back_populates="tracked_domain_links")
    domain: Mapped["Domain"] = relationship(back_populates="tracked_by_links")


class UserTrackedSubfield(Base):
    __tablename__ = "user_tracked_subfields"

    user_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("users.id", ondelete="CASCADE"), primary_key=True
    )
    subfield_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("subfields.id", ondelete="CASCADE"), primary_key=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )

    user: Mapped["User"] = relationship(back_populates="tracked_subfield_links")
    subfield: Mapped["Subfield"] = relationship(back_populates="tracked_by_links")


class UserTrackedKeyword(Base):
    __tablename__ = "user_tracked_keywords"

    user_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("users.id", ondelete="CASCADE"), primary_key=True
    )
    keyword_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("keywords.id", ondelete="CASCADE"), primary_key=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )

    user: Mapped["User"] = relationship(back_populates="tracked_keyword_links")
    keyword: Mapped["Keyword"] = relationship(back_populates="tracked_by_links")
