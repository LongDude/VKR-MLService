from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import BigInteger, ForeignKey, Text
from sqlalchemy.ext.associationproxy import AssociationProxy, association_proxy
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .associations import (
    PaperTopic,
    UserTrackedDomain,
    UserTrackedSubfield,
    UserTrackedTopic,
)
from .base import Base

if TYPE_CHECKING:
    from .analytics import (
        OpenAlexMonthlyTopicStat,
        OpenAlexYearlyTopicStat,
        ResearchCluster,
    )
    from .paper import Paper
    from .user import User


class Domain(Base):
    __tablename__ = "domains"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    openalex_id: Mapped[str | None] = mapped_column(Text, unique=True)
    name: Mapped[str] = mapped_column(Text, nullable=False, unique=True)

    fields: Mapped[list["Field"]] = relationship(back_populates="domain")
    tracked_by_links: Mapped[list["UserTrackedDomain"]] = relationship(
        back_populates="domain", cascade="all, delete-orphan"
    )
    tracked_by_users: AssociationProxy[list["User"]] = association_proxy(
        "tracked_by_links",
        "user",
        creator=lambda user: UserTrackedDomain(user=user),
    )

    def __repr__(self) -> str:
        return f"Domain(id={self.id!r}, name={self.name!r})"


class Field(Base):
    __tablename__ = "fields"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    domain_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("domains.id", ondelete="SET NULL")
    )
    openalex_id: Mapped[str | None] = mapped_column(Text, unique=True)
    name: Mapped[str] = mapped_column(Text, nullable=False)

    domain: Mapped["Domain | None"] = relationship(back_populates="fields")
    subfields: Mapped[list["Subfield"]] = relationship(back_populates="field")

    def __repr__(self) -> str:
        return f"Field(id={self.id!r}, name={self.name!r})"


class Subfield(Base):
    __tablename__ = "subfields"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    field_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("fields.id", ondelete="SET NULL")
    )
    openalex_id: Mapped[str | None] = mapped_column(Text, unique=True)
    name: Mapped[str] = mapped_column(Text, nullable=False)

    field: Mapped["Field | None"] = relationship(back_populates="subfields")
    topics: Mapped[list["Topic"]] = relationship(back_populates="subfield")
    tracked_by_links: Mapped[list["UserTrackedSubfield"]] = relationship(
        back_populates="subfield", cascade="all, delete-orphan"
    )
    tracked_by_users: AssociationProxy[list["User"]] = association_proxy(
        "tracked_by_links",
        "user",
        creator=lambda user: UserTrackedSubfield(user=user),
    )

    def __repr__(self) -> str:
        return f"Subfield(id={self.id!r}, name={self.name!r})"


class Topic(Base):
    __tablename__ = "topics"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    subfield_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("subfields.id", ondelete="SET NULL")
    )
    openalex_id: Mapped[str | None] = mapped_column(Text, unique=True)
    name: Mapped[str] = mapped_column(Text, nullable=False)

    subfield: Mapped["Subfield | None"] = relationship(back_populates="topics")
    paper_links: Mapped[list["PaperTopic"]] = relationship(
        back_populates="topic", cascade="all, delete-orphan"
    )
    tracked_by_links: Mapped[list["UserTrackedTopic"]] = relationship(
        back_populates="topic", cascade="all, delete-orphan"
    )
    research_clusters: Mapped[list["ResearchCluster"]] = relationship(
        back_populates="source_topic"
    )
    openalex_monthly_stats: Mapped[list["OpenAlexMonthlyTopicStat"]] = relationship(
        back_populates="topic"
    )
    openalex_yearly_stats: Mapped[list["OpenAlexYearlyTopicStat"]] = relationship(
        back_populates="topic"
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
