from __future__ import annotations

from sqlalchemy import exists, select

from core.exceptions import EntityNotFoundError
from models import User

from .base import BaseRepository


class UserRepository(BaseRepository):
    def get_by_id(self, user_id: int) -> User | None:
        """Return a user by primary key."""
        return self.session.get(User, user_id)

    def get_by_email(self, email: str) -> User | None:
        """Return a user by email."""
        stmt = select(User).where(User.email == email)
        return self.session.scalar(stmt)

    def exists_by_email(self, email: str) -> bool:
        """Return whether a user with the email exists."""
        stmt = select(exists().where(User.email == email))
        return bool(self.session.scalar(stmt))

    def create(
        self,
        email: str,
        password_hash: str,
        password_salt: str,
        name: str | None,
    ) -> User:
        """Create a user without committing the transaction."""
        user = User(
            email=email,
            password_hash=password_hash,
            password_salt=password_salt,
            name=name,
        )
        self.session.add(user)
        return user

    def update_name(self, user_id: int, name: str) -> User:
        """Update a user name or raise when the user does not exist."""
        user = self.get_by_id(user_id)
        if user is None:
            raise EntityNotFoundError(
                "User not found",
                details={"user_id": user_id},
            )
        user.name = name
        return user


__all__ = ["UserRepository"]
