from __future__ import annotations

import logging
from datetime import date

import pytest

from cli import data, ml, tasks, worker
from core.config import InfrastructureSettings, Settings
from core.exceptions import ConfigurationError
from core.logging import configure_logging, get_logger, log_event, logged_operation
from dto.recommendations import RecommendationRequestDTO
from dto.topic_analytics import TopicAnalyticsInsightRequestDTO
from ml.facades.recommendations import RecommendationFacade
from ml.facades.topic_analytics import TopicAnalyticsFacade
from ml.services.admin_coverage_tasks import AdminCoverageTaskService


def _settings(log_level: str = "INFO") -> Settings:
    return Settings(
        infrastructure=InfrastructureSettings(
            database_url="sqlite:///test.db",
            log_level=log_level,
        )
    )


def test_settings_reject_unknown_log_level() -> None:
    with pytest.raises(ConfigurationError, match="infrastructure.log_level"):
        _settings("VERBOSE")


@pytest.mark.parametrize(
    ("verbosity", "root_level", "httpx_level", "httpcore_level"),
    [
        (0, logging.ERROR, logging.WARNING, logging.WARNING),
        (1, logging.INFO, logging.INFO, logging.WARNING),
        (2, logging.DEBUG, logging.DEBUG, logging.DEBUG),
    ],
)
def test_configure_logging_resolves_application_and_dependency_levels(
    verbosity: int,
    root_level: int,
    httpx_level: int,
    httpcore_level: int,
) -> None:
    configure_logging(_settings("ERROR"), verbosity=verbosity)

    assert logging.getLogger().level == root_level
    assert logging.getLogger("httpx").level == httpx_level
    assert logging.getLogger("httpcore").level == httpcore_level
    assert logging.getLogger("qdrant_client").level == httpx_level


def test_log_event_and_logged_operation_use_key_value_format(capsys) -> None:
    configure_logging(_settings())
    logger = get_logger("tests.logging")

    log_event(logger, "sample_event", count=2, label="hello world")
    with logged_operation(logger, "sample_operation", item_id=7):
        pass

    output = capsys.readouterr().err
    assert "sample_event count=2 label=\"hello world\"" in output
    assert "sample_operation_started item_id=7" in output
    assert "sample_operation_completed elapsed_seconds=" in output


def test_logged_operation_includes_traceback_for_unexpected_failure(capsys) -> None:
    configure_logging(_settings())
    logger = get_logger("tests.logging")

    with pytest.raises(RuntimeError, match="boom"):
        with logged_operation(logger, "sample_operation"):
            raise RuntimeError("boom")

    output = capsys.readouterr().err
    assert "sample_operation_failed" in output
    assert "Traceback" in output


def test_all_cli_categories_accept_double_verbose() -> None:
    assert tasks.parse_args(["openalex-cooldown-status", "-vv"]).verbose == 2
    assert data.parse_args(["init-qdrant", "3", "-vv"]).verbose == 2
    assert ml.parse_args(["extract-keywords", "-vv"]).verbose == 2
    assert worker.parse_args(["run", "-vv"]).verbose == 2


def test_recommendation_fallback_logs_warning_without_traceback(caplog) -> None:
    class PaperRepository:
        def list_recent_indexed(self, *_args, **_kwargs):
            return []

    class FavouriteRepository:
        def list_paper_ids(self, _user_id: int):
            return []

    facade = RecommendationFacade(
        user_profile_facade=None,
        qdrant_adapter=None,
        paper_repository=PaperRepository(),  # type: ignore[arg-type]
        favourite_repository=FavouriteRepository(),  # type: ignore[arg-type]
    )

    with caplog.at_level(logging.WARNING):
        result = facade.recommend_for_user(RecommendationRequestDTO(user_id=10))

    assert result.strategy == "trending_fallback"
    assert "recommendation_fallback" in caplog.text
    assert "Traceback" not in caplog.text


def test_topic_analytics_fallback_logs_warning_without_traceback(caplog) -> None:
    class Session:
        def execute(self, *_args, **_kwargs):
            raise RuntimeError("database unavailable")

    facade = TopicAnalyticsFacade(
        Session(),  # type: ignore[arg-type]
        forecast_service=object(),  # type: ignore[arg-type]
    )
    request = TopicAnalyticsInsightRequestDTO(
        topic_id=5,
        period_start=date(2025, 1, 1),
        period_end=date(2025, 2, 1),
        sections=["activity"],
    )

    with caplog.at_level(logging.WARNING):
        result = facade.insights(request)

    assert result.errors
    assert "topic_analytics_fallback" in caplog.text
    assert "Traceback" not in caplog.text


def test_admin_worker_status_logs_warning_without_traceback(caplog) -> None:
    class RedisAdapter:
        def ping(self):
            raise RuntimeError("redis unavailable")

    service = AdminCoverageTaskService(
        None,
        RedisAdapter(),  # type: ignore[arg-type]
        _settings(),
    )

    with caplog.at_level(logging.WARNING):
        result = service.worker_status()

    assert result["redisAvailable"] is False
    assert "admin_worker_status_unavailable" in caplog.text
    assert "Traceback" not in caplog.text
