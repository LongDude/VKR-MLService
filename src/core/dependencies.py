from __future__ import annotations

from typing import Any

from adapters import (
    LMStudioChatAdapter,
    LMStudioEmbeddingAdapter,
    OpenAlexAdapter,
    QdrantAdapter,
    RedisAdapter,
)
from core.config import Settings


def create_redis_client(
    settings: Settings,
    *,
    url: str | None = None,
    host: str | None = None,
    port: int | None = None,
    db: int | None = None,
) -> Any:
    from redis import Redis

    infrastructure = settings.infrastructure
    resolved_url = url or infrastructure.redis_url
    if resolved_url:
        return Redis.from_url(resolved_url)
    return Redis(
        host=host or infrastructure.redis_host,
        port=port if port is not None else infrastructure.redis_port,
        db=db if db is not None else infrastructure.redis_db,
        password=infrastructure.redis_password,
    )


def create_redis_adapter(settings: Settings, **overrides: Any) -> RedisAdapter:
    return RedisAdapter(create_redis_client(settings, **overrides))


def create_qdrant_adapter(
    settings: Settings,
    *,
    url: str | None = None,
    host: str | None = None,
    port: int | None = None,
    api_key: str | None = None,
) -> QdrantAdapter:
    infrastructure = settings.infrastructure
    resolved_url = url or infrastructure.qdrant_url
    resolved_api_key = api_key if api_key is not None else infrastructure.qdrant_api_key
    if resolved_url:
        return QdrantAdapter(url=resolved_url, api_key=resolved_api_key)
    return QdrantAdapter(
        host=host or infrastructure.qdrant_host,
        port=port if port is not None else infrastructure.qdrant_port,
        api_key=resolved_api_key,
    )


def create_embedding_adapter(
    settings: Settings,
    *,
    base_url: str | None = None,
) -> LMStudioEmbeddingAdapter:
    return LMStudioEmbeddingAdapter(
        base_url=base_url or settings.infrastructure.lmstudio_url,
    )


def create_chat_adapter(
    settings: Settings,
    *,
    base_url: str | None = None,
) -> LMStudioChatAdapter:
    return LMStudioChatAdapter(
        base_url=base_url or settings.infrastructure.lmstudio_url,
    )


def create_openalex_adapter(
    settings: Settings,
    *,
    base_url: str | None = None,
    api_key: str | None = None,
    mailto: str | None = None,
) -> OpenAlexAdapter:
    openalex = settings.openalex
    return OpenAlexAdapter(
        base_url=base_url or openalex.base_url,
        api_key=api_key if api_key is not None else openalex.api_key,
        mailto=mailto if mailto is not None else openalex.mailto,
    )


__all__ = [
    "create_chat_adapter",
    "create_embedding_adapter",
    "create_openalex_adapter",
    "create_qdrant_adapter",
    "create_redis_adapter",
    "create_redis_client",
]
