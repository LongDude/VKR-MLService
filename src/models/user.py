from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import BigInteger, DateTime, Text, text
from sqlalchemy.ext.associationproxy import AssociationProxy, association_proxy
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .associations import (
    UserFavouritePaper,
    UserTrackedDomain,
    UserTrackedField,
    UserTrackedKeyword,
    UserTrackedSubfield,
    UserTrackedTopic,
)
from .base import Base

if TYPE_CHECKING:
    from .keyword import Keyword
    from .paper import Paper
    from .topic import Domain, Field, Subfield, Topic


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    email: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    password_hash: Mapped[str] = mapped_column(Text, nullable=False)
    password_salt: Mapped[str] = mapped_column(Text, nullable=False)
    name: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )

    favourite_paper_links: Mapped[list["UserFavouritePaper"]] = relationship(
        back_populates="user", cascade="all, delete-orphan"
    )
    tracked_topic_links: Mapped[list["UserTrackedTopic"]] = relationship(
        back_populates="user", cascade="all, delete-orphan"
    )
    tracked_domain_links: Mapped[list["UserTrackedDomain"]] = relationship(
        back_populates="user", cascade="all, delete-orphan"
    )
    tracked_field_links: Mapped[list["UserTrackedField"]] = relationship(
        back_populates="user", cascade="all, delete-orphan"
    )
    tracked_subfield_links: Mapped[list["UserTrackedSubfield"]] = relationship(
        back_populates="user", cascade="all, delete-orphan"
    )
    tracked_keyword_links: Mapped[list["UserTrackedKeyword"]] = relationship(
        back_populates="user", cascade="all, delete-orphan"
    )
    favourite_papers: AssociationProxy[list["Paper"]] = association_proxy(
        "favourite_paper_links",
        "paper",
        creator=lambda paper: UserFavouritePaper(paper=paper),
    )
    tracked_topics: AssociationProxy[list["Topic"]] = association_proxy(
        "tracked_topic_links",
        "topic",
        creator=lambda topic: UserTrackedTopic(topic=topic),
    )
    tracked_domains: AssociationProxy[list["Domain"]] = association_proxy(
        "tracked_domain_links",
        "domain",
        creator=lambda domain: UserTrackedDomain(domain=domain),
    )
    tracked_fields: AssociationProxy[list["Field"]] = association_proxy(
        "tracked_field_links",
        "field",
        creator=lambda field: UserTrackedField(field=field),
    )
    tracked_subfields: AssociationProxy[list["Subfield"]] = association_proxy(
        "tracked_subfield_links",
        "subfield",
        creator=lambda subfield: UserTrackedSubfield(subfield=subfield),
    )
    tracked_keywords: AssociationProxy[list["Keyword"]] = association_proxy(
        "tracked_keyword_links",
        "keyword",
        creator=lambda keyword: UserTrackedKeyword(keyword=keyword),
    )

    def __repr__(self) -> str:
        return f"User(id={self.id!r}, email={self.email!r})"
