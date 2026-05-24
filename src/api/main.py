from __future__ import annotations

from functools import lru_cache
from typing import Iterator

from fastapi import Depends, FastAPI
from sqlalchemy.orm import Session, sessionmaker

from adapters.qdrant_adapter import QdrantAdapter
from core.config import Settings
from dto.topic_analytics import TopicAnalyticsInsightRequestDTO, TopicAnalyticsInsightResponseDTO
from ml.facades.topic_analytics import TopicAnalyticsFacade
from ml.pipelines.topic_analytics_pipeline import TopicAnalyticsPipeline
from models.session import create_engine_from_settings, create_session_factory


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings.from_env()


@lru_cache(maxsize=1)
def get_session_factory() -> sessionmaker[Session]:
    return create_session_factory(create_engine_from_settings(get_settings()))


@lru_cache(maxsize=1)
def get_qdrant_adapter() -> QdrantAdapter | None:
    settings = get_settings()
    try:
        if settings.qdrant_url:
            return QdrantAdapter(url=settings.qdrant_url, api_key=settings.qdrant_api_key)
        if settings.qdrant_host:
            return QdrantAdapter(
                host=settings.qdrant_host,
                port=settings.qdrant_port or 6333,
                api_key=settings.qdrant_api_key,
            )
    except Exception:
        return None
    return None


def get_session() -> Iterator[Session]:
    factory = get_session_factory()
    with factory() as session:
        yield session


app = FastAPI(title="VKR MLService", version="1.0.0")


@app.get("/health")
def health() -> dict[str, bool]:
    return {"ok": True}


@app.post("/v1/topic-analytics/insights", response_model=TopicAnalyticsInsightResponseDTO)
def topic_analytics_insights(
    request: TopicAnalyticsInsightRequestDTO,
    session: Session = Depends(get_session),
) -> TopicAnalyticsInsightResponseDTO:
    pipeline = TopicAnalyticsPipeline(
        TopicAnalyticsFacade(session, qdrant_adapter=get_qdrant_adapter())
    )
    return pipeline.insights(request)
