from __future__ import annotations

from typing import Any

from core.config import Settings
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker


def create_engine_from_settings(settings: Settings) -> Engine:
    """Create a synchronous SQLAlchemy engine from application settings."""
    return create_engine(settings.database_url)


def create_db_engine(database_url: str, **kwargs: Any) -> Engine:
    """Create a synchronous SQLAlchemy engine from a database URL."""
    return create_engine(database_url, **kwargs)


def create_session_factory(
    engine: Engine,
    *,
    expire_on_commit: bool = False,
) -> sessionmaker[Session]:
    """Create a synchronous SQLAlchemy session factory for the engine."""
    return sessionmaker(bind=engine, class_=Session, expire_on_commit=expire_on_commit)


__all__ = [
    "create_db_engine",
    "create_engine_from_settings",
    "create_session_factory",
]
