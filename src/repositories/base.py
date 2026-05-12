from __future__ import annotations

from sqlalchemy.orm import Session


class BaseRepository:
    def __init__(self, session: Session) -> None:
        """Store the SQLAlchemy session supplied by the caller."""
        self.session = session


__all__ = ["BaseRepository"]
