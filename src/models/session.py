from __future__ import annotations

from typing import Any

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker


def create_db_engine(database_url: str, **kwargs: Any) -> Engine:
    return create_engine(database_url, **kwargs)


def create_session_factory(
    engine: Engine,
    *,
    expire_on_commit: bool = False,
) -> sessionmaker[Session]:
    return sessionmaker(bind=engine, class_=Session, expire_on_commit=expire_on_commit)


__all__ = ["create_db_engine", "create_session_factory"]
