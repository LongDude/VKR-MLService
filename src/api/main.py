from __future__ import annotations

from functools import lru_cache
from typing import Iterator

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session, sessionmaker

from adapters.qdrant_adapter import QdrantAdapter
from core.config import Settings
from core.exceptions import (
    AppError,
    DuplicateEntityError,
    EntityNotFoundError,
    ExternalServiceRateLimitError,
    ExternalServiceUnavailableError,
    InsufficientUserProfileDataError,
    InvalidRequestError,
)
from dto.recommendations import (
    RecommendationRequestDTO,
    RecommendationResponseDTO,
    UserProfileDTO,
)
from dto.topic_analytics import (
    TopicAnalyticsInsightRequestDTO,
    TopicAnalyticsInsightResponseDTO,
)
from ml.facades.recommendations import RecommendationFacade
from ml.facades.topic_analytics import TopicAnalyticsFacade
from ml.facades.user_profile import UserProfileFacade
from ml.pipelines.recommendation_pipeline import RecommendationPipeline
from ml.pipelines.topic_analytics_pipeline import TopicAnalyticsPipeline
from ml.pipelines.user_profile_pipeline import UserProfilePipeline
from models.session import create_engine_from_settings, create_session_factory
from repositories.favourites import FavouriteRepository
from repositories.papers import PaperRepository
from repositories.taxonomy import TaxonomyRepository
from repositories.tracked_areas import TrackedAreaRepository
from src.ml.services.forecast_model import PublicationForecastService


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
            return QdrantAdapter(
                url=settings.qdrant_url, api_key=settings.qdrant_api_key
            )
        if settings.qdrant_host:
            return QdrantAdapter(
                host=settings.qdrant_host,
                port=settings.qdrant_port or 6333,
                api_key=settings.qdrant_api_key,
            )
    except Exception:
        return None
    return None


@lru_cache
def get_publication_forecast_service() -> PublicationForecastService:
    return PublicationForecastService(
        season_length=12,
        min_backtest_points=24,
        min_sarimax_points=36,
        max_sarimax_candidates=24,
        winsorize_count_quantile=None,
    )


def get_session() -> Iterator[Session]:
    factory = get_session_factory()
    with factory() as session:
        yield session


app = FastAPI(title="VKR MLService", version="1.0.0")


def app_error_status_code(error: AppError) -> int:
    if isinstance(error, EntityNotFoundError):
        return 404
    if isinstance(error, DuplicateEntityError):
        return 409
    if isinstance(error, InvalidRequestError):
        return 400
    if isinstance(error, InsufficientUserProfileDataError):
        return 422
    if isinstance(error, ExternalServiceRateLimitError):
        return 429
    if isinstance(error, ExternalServiceUnavailableError):
        return 503
    return 500


@app.exception_handler(AppError)
async def handle_app_error(_request: Request, error: AppError) -> JSONResponse:
    return JSONResponse(
        status_code=app_error_status_code(error),
        content=error.to_dict(),
    )


@app.get("/health")
def health() -> dict[str, bool]:
    return {"ok": True}


@app.post(
    "/v1/topic-analytics/insights", response_model=TopicAnalyticsInsightResponseDTO
)
def topic_analytics_insights(
    request: TopicAnalyticsInsightRequestDTO,
    session: Session = Depends(get_session),
    forecast_service: PublicationForecastService = Depends(
        get_publication_forecast_service
    ),
) -> TopicAnalyticsInsightResponseDTO:
    pipeline = TopicAnalyticsPipeline(
        TopicAnalyticsFacade(
            session,
            forecast_service=forecast_service,
            qdrant_adapter=get_qdrant_adapter(),
        )
    )
    return pipeline.insights(request)


@app.post("/v1/user-profiles/{user_id}/recompute", response_model=UserProfileDTO)
def recompute_user_profile(
    user_id: int,
    session: Session = Depends(get_session),
) -> UserProfileDTO:
    qdrant_adapter = get_qdrant_adapter()
    if qdrant_adapter is None:
        raise HTTPException(status_code=503, detail="Qdrant is unavailable.")
    pipeline = UserProfilePipeline(
        UserProfileFacade(
            favourite_repository=FavouriteRepository(session),
            tracked_area_repository=TrackedAreaRepository(session),
            taxonomy_repository=TaxonomyRepository(session),
            qdrant_adapter=qdrant_adapter,
        )
    )
    return pipeline.recompute_user(user_id)


@app.post("/v1/recommendations/papers", response_model=RecommendationResponseDTO)
def recommend_papers(
    request: RecommendationRequestDTO,
    session: Session = Depends(get_session),
) -> RecommendationResponseDTO:
    qdrant_adapter = get_qdrant_adapter()
    user_profile_facade = (
        UserProfileFacade(
            favourite_repository=FavouriteRepository(session),
            tracked_area_repository=TrackedAreaRepository(session),
            taxonomy_repository=TaxonomyRepository(session),
            qdrant_adapter=qdrant_adapter,
        )
        if qdrant_adapter is not None
        else None
    )
    pipeline = RecommendationPipeline(
        RecommendationFacade(
            user_profile_facade=user_profile_facade,
            qdrant_adapter=qdrant_adapter,
            paper_repository=PaperRepository(session),
            favourite_repository=FavouriteRepository(session),
            taxonomy_repository=TaxonomyRepository(session),
        )
    )
    return pipeline.recommend(request)
