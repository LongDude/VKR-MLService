from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    database_url: str
    qdrant_url: str | None = None
    qdrant_host: str | None = None
    qdrant_port: int | None = None
    qdrant_api_key: str | None = None

    @classmethod
    def from_env(cls) -> "Settings":
        """Build settings from environment variables."""
        database_url = os.getenv("DATABASE_URL")
        if not database_url:
            user = os.getenv("POSTGRES_USER", "")
            password = os.getenv("POSTGRES_PASSWORD", "")
            host = os.getenv("POSTGRES_HOST", "localhost")
            port = os.getenv("POSTGRES_PORT", "5432")
            database = os.getenv("POSTGRES_DB", "")
            auth = f"{user}:{password}@" if user or password else ""
            database_url = f"postgresql+psycopg2://{auth}{host}:{port}/{database}"

        qdrant_port = os.getenv("QDRANT_PORT")
        return cls(
            database_url=database_url,
            qdrant_url=os.getenv("QDRANT_URL") or None,
            qdrant_host=os.getenv("QDRANT_HOST") or None,
            qdrant_port=int(qdrant_port)
            if qdrant_port and qdrant_port.isdigit()
            else None,
            qdrant_api_key=os.getenv("QDRANT_API_KEY") or None,
        )


__all__ = ["Settings"]
