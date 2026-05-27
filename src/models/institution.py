from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import BigInteger, CHAR, DateTime, Text, text
from sqlalchemy.ext.associationproxy import AssociationProxy, association_proxy
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .associations import AuthorInstitution
from .base import Base

if TYPE_CHECKING:
    from .author import Author


class Institution(Base):
    __tablename__ = "institutions"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    display_name: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    ror: Mapped[str | None] = mapped_column(Text, unique=True)
    country_code: Mapped[str | None] = mapped_column(CHAR(2))
    type: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )

    author_links: Mapped[list["AuthorInstitution"]] = relationship(
        back_populates="institution", cascade="all, delete-orphan"
    )
    authors: AssociationProxy[list["Author"]] = association_proxy(
        "author_links",
        "author",
        creator=lambda author: AuthorInstitution(author=author),
    )

    def __repr__(self) -> str:
        return f"Institution(id={self.id!r}, display_name={self.display_name!r})"
