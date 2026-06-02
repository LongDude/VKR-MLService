from __future__ import annotations

import os
from dataclasses import replace
from datetime import date

import pytest

from cli import data, tasks
from cli.worker import parse_queues
from core.config import (
    InfrastructureSettings,
    OpenAlexSettings,
    Settings,
    get_settings,
    load_settings,
)
from core.exceptions import InvalidRequestError
from ml.services.admin_coverage_tasks import AdminCoverageTaskService
from ml.services.cluster_dynamics_tasks import build_cluster_dynamics_message
from ml.services.cluster_recompute_tasks import build_cluster_recompute_message
from ml.task_contracts import (
    CLUSTER_DYNAMICS_RECOMPUTE_TASK,
    COLLECT_TOPIC_STATS_TASK,
    RECOMPUTE_TOPIC_CLUSTERS_TASK,
)
from ml.workers.task_handlers import MLTaskHandler


def test_settings_precedence_and_external_toml(tmp_path, monkeypatch) -> None:
    config_file = tmp_path / "override.toml"
    config_file.write_text(
        """
[infrastructure]
database_url = "sqlite:///from-toml.db"
redis_port = 6381
log_level = "WARNING"

[worker]
idle_sleep_seconds = 4.0
""".strip(),
        encoding="utf-8",
    )
    env_file = tmp_path / "test.env"
    env_file.write_text(
        "REDIS_PORT=6382\nLOG_LEVEL=ERROR\nML_WORKER_IDLE_SLEEP_SECONDS=5.0\n",
        encoding="utf-8",
    )
    original_idle = os.environ.get("ML_WORKER_IDLE_SLEEP_SECONDS")
    for name in (
        "DATABASE_URL",
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
        "POSTGRES_HOST",
        "POSTGRES_PORT",
        "POSTGRES_DB",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("REDIS_PORT", "6383")
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")
    monkeypatch.delenv("ML_WORKER_IDLE_SLEEP_SECONDS", raising=False)
    try:
        settings = load_settings(config_file=config_file, env_file=env_file)
        assert settings.database_url == "sqlite:///from-toml.db"
        assert settings.infrastructure.redis_port == 6383
        assert settings.infrastructure.log_level == "DEBUG"
        assert settings.worker.idle_sleep_seconds == 5.0
    finally:
        if original_idle is None:
            os.environ.pop("ML_WORKER_IDLE_SLEEP_SECONDS", None)
        else:
            os.environ["ML_WORKER_IDLE_SLEEP_SECONDS"] = original_idle


def test_settings_validation_rejects_invalid_port(tmp_path, monkeypatch) -> None:
    config_file = tmp_path / "invalid.toml"
    config_file.write_text(
        """
[infrastructure]
database_url = "sqlite:///test.db"
redis_port = 0
""".strip(),
        encoding="utf-8",
    )

    monkeypatch.delenv("REDIS_PORT", raising=False)
    with pytest.raises(ValueError, match="infrastructure.redis_port"):
        load_settings(config_file=config_file, env_file=tmp_path / "missing.env")


def test_get_settings_is_cached_until_cache_clear(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("ML_ENV_FILE", str(tmp_path / "missing.env"))
    monkeypatch.setenv("REDIS_HOST", "first")
    get_settings.cache_clear()
    try:
        first = get_settings()
        monkeypatch.setenv("REDIS_HOST", "second")
        assert get_settings() is first
        assert get_settings().infrastructure.redis_host == "first"

        get_settings.cache_clear()
        assert get_settings().infrastructure.redis_host == "second"
    finally:
        get_settings.cache_clear()


def test_worker_accepts_only_full_queue_names() -> None:
    assert parse_queues("queue:paper_indexing,queue:keyword_extraction") == (
        "queue:paper_indexing",
        "queue:keyword_extraction",
    )
    with pytest.raises(ValueError, match="full queue"):
        parse_queues("paper-indexing")


def test_worker_rejects_legacy_task_types() -> None:
    handler = MLTaskHandler()

    with pytest.raises(InvalidRequestError, match="Unknown ML task type"):
        handler.handle({"task_type": "cluster_recompute"})
    with pytest.raises(InvalidRequestError, match="Unknown ML task type"):
        handler.handle({"task_type": "collect-topic-stats"})


def test_task_builders_emit_canonical_contracts() -> None:
    assert build_cluster_recompute_message([1, 2])["task_type"] == (
        RECOMPUTE_TOPIC_CLUSTERS_TASK
    )
    assert build_cluster_dynamics_message(
        ["topic:1"],
        date_from=date(2025, 1, 1),
        date_to=date(2025, 1, 31),
        granularity="month",
    )["task_type"] == CLUSTER_DYNAMICS_RECOMPUTE_TASK


def test_removed_cli_aliases_are_rejected() -> None:
    with pytest.raises(SystemExit):
        tasks.parse_args(["enqueue-indexing"])
    with pytest.raises(SystemExit):
        tasks.parse_args(["enqueue-collect-topic-stats"])
    with pytest.raises(SystemExit):
        data.parse_args(["analyze-data-coverage"])


def test_admin_task_payload_uses_injected_defaults() -> None:
    settings = Settings(
        infrastructure=InfrastructureSettings(database_url="sqlite:///test.db"),
        openalex=replace(
            OpenAlexSettings(),
            languages=("ru",),
            stats_batch_size=321,
            stats_request_workers=3,
        ),
    )
    service = AdminCoverageTaskService(None, object(), settings)  # type: ignore[arg-type]

    _queue, payload, tracked_ids = service._task_message(  # noqa: SLF001
        task_id="task-1",
        workflow_id=None,
        panel_key="monthly-stats",
        topic_ids=[10, 11],
        period={
            "date_from": date(2025, 1, 1),
            "date_to": date(2025, 1, 31),
        },
    )

    assert payload["task_type"] == COLLECT_TOPIC_STATS_TASK
    assert payload["languages"] == ["ru"]
    assert payload["batch_size"] == 321
    assert payload["request_workers"] == 3
    assert tracked_ids == [10, 11]
