from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    database_url: str

    @classmethod
    def from_env(cls) -> "Settings":
        """Build settings from environment variables."""
        database_url = os.getenv("DATABASE_URL")
        if database_url:
            return cls(database_url=database_url)

        user = os.getenv("POSTGRES_USER", "")
        password = os.getenv("POSTGRES_PASSWORD", "")
        host = os.getenv("POSTGRES_HOST", "localhost")
        port = os.getenv("POSTGRES_PORT", "5432")
        database = os.getenv("POSTGRES_DB", "")
        auth = f"{user}:{password}@" if user or password else ""
        return cls(database_url=f"postgresql+psycopg2://{auth}{host}:{port}/{database}")


__all__ = ["Settings"]
