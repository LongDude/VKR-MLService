from __future__ import annotations

from sqlalchemy import delete, func, select

from models import Paper, UserFavouritePaper

from .base import BaseRepository


class FavouriteRepository(BaseRepository):
    def add(self, user_id: int, paper_id: int) -> None:
        """Add a favourite paper idempotently."""
        if not self.exists(user_id, paper_id):
            self.session.add(UserFavouritePaper(user_id=user_id, paper_id=paper_id))

    def remove(self, user_id: int, paper_id: int) -> None:
        """Remove a favourite paper idempotently."""
        stmt = delete(UserFavouritePaper).where(
            UserFavouritePaper.user_id == user_id,
            UserFavouritePaper.paper_id == paper_id,
        )
        self.session.execute(stmt)

    def exists(self, user_id: int, paper_id: int) -> bool:
        """Return whether the favourite link exists."""
        stmt = select(UserFavouritePaper).where(
            UserFavouritePaper.user_id == user_id,
            UserFavouritePaper.paper_id == paper_id,
        )
        return self.session.scalar(stmt) is not None

    def list_paper_ids(self, user_id: int) -> list[int]:
        """List favourite paper ids for a user."""
        stmt = (
            select(UserFavouritePaper.paper_id)
            .where(UserFavouritePaper.user_id == user_id)
            .order_by(UserFavouritePaper.created_at.desc())
        )
        return list(self.session.scalars(stmt).all())

    def list_papers(self, user_id: int, limit: int, offset: int) -> list[Paper]:
        """List favourite papers for a user."""
        stmt = (
            select(Paper)
            .join(UserFavouritePaper, UserFavouritePaper.paper_id == Paper.id)
            .where(UserFavouritePaper.user_id == user_id)
            .order_by(UserFavouritePaper.created_at.desc())
            .limit(limit)
            .offset(offset)
        )
        return list(self.session.scalars(stmt).all())

    def count(self, user_id: int) -> int:
        """Count favourite papers for a user."""
        stmt = (
            select(func.count())
            .select_from(UserFavouritePaper)
            .where(UserFavouritePaper.user_id == user_id)
        )
        return int(self.session.scalar(stmt) or 0)


__all__ = ["FavouriteRepository"]
