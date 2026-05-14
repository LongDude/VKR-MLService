from __future__ import annotations

from typing import TypeVar

from sqlalchemy.orm import Session


T = TypeVar("T")


class BaseRepository:
    def __init__(self, session: Session) -> None:
        """Store the SQLAlchemy session supplied by the caller."""
        self.session = session

    def _pending_instance(self, model: type[T], **values: object) -> T | None:
        """Return a matching pending ORM instance from this session.

        SQLAlchemy does not make pending instances addressable through
        ``Session.get`` until they are flushed. Repository methods that create
        idempotent link rows need this check to avoid duplicate composite-key
        inserts when callers use ``autoflush=False``.
        """
        for instance in self.session.new:
            if not isinstance(instance, model):
                continue
            if all(getattr(instance, field, None) == value for field, value in values.items()):
                return instance
        return None


__all__ = ["BaseRepository"]
