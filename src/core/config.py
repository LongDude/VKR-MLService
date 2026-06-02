from __future__ import annotations

import os
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any, Mapping

import tomllib
from dotenv import load_dotenv

from core.exceptions import ConfigurationError
from ml.constants import DEFAULT_EMBEDDING_MODEL

PROJECT_DIR = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_FILE = PROJECT_DIR / "config" / "default.toml"
DEFAULT_ENV_FILE = PROJECT_DIR / ".env"


@dataclass(frozen=True)
class InfrastructureSettings:
    database_url: str
    redis_url: str | None = None
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 0
    redis_password: str | None = None
    qdrant_url: str | None = None
    qdrant_host: str = "localhost"
    qdrant_port: int = 6333
    qdrant_api_key: str | None = None
    lmstudio_url: str = "http://localhost:1234"
    embedding_model: str = DEFAULT_EMBEDDING_MODEL
    internal_api_token: str | None = None
    log_level: str = "INFO"


@dataclass(frozen=True)
class OpenAlexSettings:
    base_url: str = "https://api.openalex.org"
    api_key: str | None = None
    mailto: str | None = None
    languages: tuple[str, ...] = ("en", "ru")
    types: tuple[str, ...] = ("article",)
    stats_batch_size: int = 500
    stats_task_batch_size: int = 100
    stats_request_workers: int = 8
    stats_rate_limit_rps: float = 10.0
    stats_max_retries: int = 5
    stats_group_by_page_size: int = 200
    normalize_january_first: bool = True
    primary_topic_only: bool = True
    bootstrap_target_count: int = 20
    bootstrap_batch_size: int = 500
    bootstrap_task_batch_size: int = 100
    bootstrap_request_workers: int = 8
    bootstrap_db_workers: int = 2
    bootstrap_rate_limit_rps: float = 70.0
    bootstrap_seed: int = 42
    bootstrap_per_page: int = 100
    bootstrap_max_retries: int = 5
    cooldown_fallback_seconds: int = 900
    rate_limit_defer_after_seconds: float = 120.0


@dataclass(frozen=True)
class WorkerSettings:
    queues: tuple[str, ...] = (
        "queue:openalex_topic_stats_pending",
        "queue:openalex_bootstrap_papers_pending",
        "queue:openalex_topic_stats",
        "queue:openalex_bootstrap_papers",
        "queue:keyword_extraction",
        "queue:paper_indexing",
        "queue:entity_indexing",
        "queue:cluster_recompute",
        "queue:cluster_dynamics_recompute",
        "queue:topic_quarter_reports",
        "queue:user_profile_recompute",
    )
    idle_sleep_seconds: float = 2.0
    dequeue_timeout_seconds: int = 30
    show_progress: bool = True
    event_redis: bool = False
    event_ttl_seconds: int = 24 * 60 * 60
    keyword_extraction_batch_size: int = 128
    max_keyword_task_size: int | None = None
    paper_indexing_batch_size: int = 128
    max_paper_task_size: int | None = None
    cluster_recompute_batch_size: int = 50
    cluster_recompute_workers: int = 1
    max_cluster_task_size: int | None = None
    heartbeat_interval_seconds: float = 10.0
    heartbeat_ttl_seconds: int = 45


@dataclass(frozen=True)
class OperationDefaults:
    paper_indexing_batch_size: int = 200
    keyword_extraction_batch_size: int = 200
    keyword_extraction_top_k: int = 10
    research_entities_batch_size: int = 128
    research_entities_limit: int = 10000
    cluster_recompute_batch_size: int = 50
    cluster_dynamics_batch_size: int = 50
    topic_reports_batch_size: int = 50
    topic_report_language: str = "ru"
    user_profiles_batch_size: int = 100
    event_ttl_seconds: int = 24 * 60 * 60


@dataclass(frozen=True)
class DataDefaults:
    sample_limit: int = 20
    qdrant_distance: str = "Cosine"


@dataclass(frozen=True)
class AdminDefaults:
    tracking_ttl_seconds: int = 7 * 24 * 60 * 60
    recent_ttl_seconds: int = 24 * 60 * 60
    stale_after_hours: int = 24
    max_tracked_items: int = 5000
    result_ttl_seconds: int = 7 * 24 * 60 * 60


@dataclass(frozen=True)
class Settings:
    infrastructure: InfrastructureSettings
    openalex: OpenAlexSettings = field(default_factory=OpenAlexSettings)
    worker: WorkerSettings = field(default_factory=WorkerSettings)
    operations: OperationDefaults = field(default_factory=OperationDefaults)
    data: DataDefaults = field(default_factory=DataDefaults)
    admin: AdminDefaults = field(default_factory=AdminDefaults)

    def __post_init__(self) -> None:
        infrastructure = self.infrastructure
        _require_text(infrastructure.database_url, "infrastructure.database_url")
        _validate_port(infrastructure.redis_port, "infrastructure.redis_port")
        _validate_port(infrastructure.qdrant_port, "infrastructure.qdrant_port")
        _require_non_negative(infrastructure.redis_db, "infrastructure.redis_db")
        _validate_log_level(infrastructure.log_level)

        openalex = self.openalex
        _require_text(openalex.base_url, "openalex.base_url")
        _require_non_empty_sequence(openalex.languages, "openalex.languages")
        _require_non_empty_sequence(openalex.types, "openalex.types")
        for name in (
            "stats_batch_size",
            "stats_task_batch_size",
            "stats_request_workers",
            "stats_rate_limit_rps",
            "stats_max_retries",
            "bootstrap_target_count",
            "bootstrap_batch_size",
            "bootstrap_task_batch_size",
            "bootstrap_request_workers",
            "bootstrap_db_workers",
            "bootstrap_rate_limit_rps",
        ):
            _require_positive(getattr(openalex, name), f"openalex.{name}")

        worker = self.worker
        _require_non_empty_sequence(worker.queues, "worker.queues")
        for queue_name in worker.queues:
            if not isinstance(queue_name, str) or not queue_name.startswith("queue:"):
                raise ValueError(
                    f"worker.queues contains invalid queue name: {queue_name!r}"
                )
        for name in (
            "idle_sleep_seconds",
            "dequeue_timeout_seconds",
            "event_ttl_seconds",
            "keyword_extraction_batch_size",
            "paper_indexing_batch_size",
            "cluster_recompute_batch_size",
            "cluster_recompute_workers",
            "heartbeat_interval_seconds",
            "heartbeat_ttl_seconds",
        ):
            _require_positive(getattr(worker, name), f"worker.{name}")
        for name in (
            "max_keyword_task_size",
            "max_paper_task_size",
            "max_cluster_task_size",
        ):
            _require_optional_positive(getattr(worker, name), f"worker.{name}")

        operations = self.operations
        for name in (
            "paper_indexing_batch_size",
            "keyword_extraction_batch_size",
            "keyword_extraction_top_k",
            "research_entities_batch_size",
            "research_entities_limit",
            "cluster_recompute_batch_size",
            "cluster_dynamics_batch_size",
            "topic_reports_batch_size",
            "user_profiles_batch_size",
            "event_ttl_seconds",
        ):
            _require_positive(getattr(operations, name), f"operations.{name}")

        _require_positive(self.data.sample_limit, "data.sample_limit")
        if self.data.qdrant_distance not in {"Cosine", "Dot", "Euclid", "Manhattan"}:
            raise ValueError(
                f"data.qdrant_distance is invalid: {self.data.qdrant_distance!r}"
            )
        for name in (
            "tracking_ttl_seconds",
            "recent_ttl_seconds",
            "stale_after_hours",
            "max_tracked_items",
            "result_ttl_seconds",
        ):
            _require_positive(getattr(self.admin, name), f"admin.{name}")

    @property
    def database_url(self) -> str:
        return self.infrastructure.database_url

    @property
    def qdrant_url(self) -> str | None:
        return self.infrastructure.qdrant_url

    @property
    def qdrant_host(self) -> str:
        return self.infrastructure.qdrant_host

    @property
    def qdrant_port(self) -> int:
        return self.infrastructure.qdrant_port

    @property
    def qdrant_api_key(self) -> str | None:
        return self.infrastructure.qdrant_api_key

    @classmethod
    def from_env(cls) -> "Settings":
        """Build settings through the centralized loader."""
        return load_settings()


def load_settings(
    *,
    config_file: str | Path | None = None,
    env_file: str | Path | None = None,
) -> Settings:
    """Load settings with CLI-selected files layered under process environment."""
    selected_env_file = _selected_path(env_file, "ML_ENV_FILE", DEFAULT_ENV_FILE)
    if selected_env_file and selected_env_file.exists():
        load_dotenv(selected_env_file, override=False)

    merged: dict[str, Any] = {}
    _deep_merge(merged, _read_toml(DEFAULT_CONFIG_FILE, required=True))

    selected_config_file = _selected_path(config_file, "ML_CONFIG_FILE", None)
    if selected_config_file is not None:
        if selected_config_file.resolve() != DEFAULT_CONFIG_FILE.resolve():
            _deep_merge(merged, _read_toml(selected_config_file, required=True))

    _apply_environment(merged)
    return _settings_from_mapping(merged)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return process settings cached for API and other long-lived entrypoints."""
    return load_settings()


def _selected_path(
    explicit: str | Path | None,
    env_name: str,
    default: Path | None,
) -> Path | None:
    value = explicit or os.getenv(env_name)
    if value:
        return Path(value).expanduser()
    return default


def _read_toml(path: Path, *, required: bool) -> dict[str, Any]:
    if not path.exists():
        if required:
            raise FileNotFoundError(f"Configuration file not found: {path}")
        return {}
    with path.open("rb") as stream:
        return tomllib.load(stream)


def _deep_merge(target: dict[str, Any], source: Mapping[str, Any]) -> None:
    for key, value in source.items():
        if isinstance(value, Mapping):
            nested = target.setdefault(key, {})
            if not isinstance(nested, dict):
                nested = {}
                target[key] = nested
            _deep_merge(nested, value)
        else:
            target[key] = value


def _apply_environment(values: dict[str, Any]) -> None:
    infra = values.setdefault("infrastructure", {})
    openalex = values.setdefault("openalex", {})
    worker = values.setdefault("worker", {})

    database_url = os.getenv("DATABASE_URL")
    postgres_env_names = {
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
        "POSTGRES_HOST",
        "POSTGRES_PORT",
        "POSTGRES_DB",
    }
    if database_url:
        infra["database_url"] = database_url
    elif any(name in os.environ for name in postgres_env_names) or not infra.get(
        "database_url"
    ):
        user = os.getenv("POSTGRES_USER", "")
        password = os.getenv("POSTGRES_PASSWORD", "")
        host = os.getenv("POSTGRES_HOST", "localhost")
        port = os.getenv("POSTGRES_PORT", "5432")
        database = os.getenv("POSTGRES_DB", "")
        auth = f"{user}:{password}@" if user or password else ""
        infra["database_url"] = f"postgresql+psycopg2://{auth}{host}:{port}/{database}"

    _set_env(infra, "redis_url", "REDIS_URL", _optional_text)
    _set_env(infra, "redis_host", "REDIS_HOST", str)
    _set_env(infra, "redis_port", "REDIS_PORT", int)
    _set_env(infra, "redis_db", "REDIS_DB", int)
    _set_env(infra, "redis_password", "REDIS_PASSWORD", _optional_text)
    _set_env(infra, "qdrant_url", "QDRANT_URL", _optional_text)
    _set_env(infra, "qdrant_host", "QDRANT_HOST", str)
    _set_env(infra, "qdrant_port", "QDRANT_PORT", int)
    _set_env(infra, "qdrant_api_key", "QDRANT_API_KEY", _optional_text)
    _set_env(infra, "lmstudio_url", "LMSTUDIO_BASE_URL", str)
    _set_env(infra, "embedding_model", "EMBEDDING_MODEL", str)
    _set_env(infra, "internal_api_token", "ML_INTERNAL_API_TOKEN", _optional_text)
    _set_env(infra, "log_level", "LOG_LEVEL", str)

    _set_env(openalex, "base_url", "OPENALEX_BASE_URL", str)
    _set_env(openalex, "api_key", "OPENALEX_API_KEY", _optional_text)
    _set_env(openalex, "mailto", "OPENALEX_MAILTO", _optional_text)

    _set_env(worker, "queues", "ML_WORKER_QUEUES", _csv_tuple)
    _set_env(worker, "idle_sleep_seconds", "ML_WORKER_IDLE_SLEEP_SECONDS", float)
    _set_env(
        worker, "dequeue_timeout_seconds", "ML_WORKER_DEQUEUE_TIMEOUT_SECONDS", int
    )
    _set_env(worker, "show_progress", "ML_WORKER_SHOW_PROGRESS", _bool)
    _set_env(worker, "event_redis", "ML_WORKER_EVENT_REDIS", _bool)
    _set_env(worker, "event_ttl_seconds", "ML_WORKER_EVENT_TTL_SECONDS", int)
    _set_env(
        worker,
        "keyword_extraction_batch_size",
        "ML_WORKER_KEYWORD_EXTRACTION_BATCH_SIZE",
        int,
    )
    _set_env(
        worker,
        "max_keyword_task_size",
        "ML_WORKER_MAX_KEYWORD_TASK_SIZE",
        _optional_int,
    )
    _set_env(
        worker, "paper_indexing_batch_size", "ML_WORKER_PAPER_INDEXING_BATCH_SIZE", int
    )
    _set_env(
        worker, "max_paper_task_size", "ML_WORKER_MAX_PAPER_TASK_SIZE", _optional_int
    )
    _set_env(
        worker,
        "cluster_recompute_batch_size",
        "ML_WORKER_CLUSTER_RECOMPUTE_BATCH_SIZE",
        int,
    )
    _set_env(
        worker, "cluster_recompute_workers", "ML_WORKER_CLUSTER_RECOMPUTE_WORKERS", int
    )
    _set_env(
        worker,
        "max_cluster_task_size",
        "ML_WORKER_MAX_CLUSTER_TASK_SIZE",
        _optional_int,
    )
    _set_env(
        worker,
        "heartbeat_interval_seconds",
        "ML_WORKER_HEARTBEAT_INTERVAL_SECONDS",
        float,
    )
    _set_env(worker, "heartbeat_ttl_seconds", "ML_WORKER_HEARTBEAT_TTL_SECONDS", int)


def _set_env(
    target: dict[str, Any],
    key: str,
    env_name: str,
    parser: Any,
) -> None:
    raw_value = os.getenv(env_name)
    if raw_value is not None:
        target[key] = parser(raw_value)


def _settings_from_mapping(values: Mapping[str, Any]) -> Settings:
    return Settings(
        infrastructure=InfrastructureSettings(**dict(values.get("infrastructure", {}))),
        openalex=OpenAlexSettings(
            **_with_tuples(values.get("openalex", {}), "languages", "types")
        ),
        worker=WorkerSettings(**_with_tuples(values.get("worker", {}), "queues")),
        operations=OperationDefaults(**dict(values.get("operations", {}))),
        data=DataDefaults(**dict(values.get("data", {}))),
        admin=AdminDefaults(**dict(values.get("admin", {}))),
    )


def _with_tuples(values: Any, *keys: str) -> dict[str, Any]:
    result = dict(values or {})
    for key in keys:
        if key in result and not isinstance(result[key], tuple):
            result[key] = tuple(result[key])
    return result


def _optional_text(value: str) -> str | None:
    return value.strip() or None


def _optional_int(value: str) -> int | None:
    return int(value) if value.strip() else None


def _csv_tuple(value: str) -> tuple[str, ...]:
    return tuple(item.strip() for item in value.split(",") if item.strip())


def _bool(value: str) -> bool:
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"Expected boolean value, got {value!r}")


def _require_text(value: Any, name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")


def _require_positive(value: Any, name: str) -> None:
    if not isinstance(value, (int, float)) or isinstance(value, bool) or value <= 0:
        raise ValueError(f"{name} must be positive")


def _require_non_negative(value: Any, name: str) -> None:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValueError(f"{name} must be a non-negative integer")


def _require_optional_positive(value: Any, name: str) -> None:
    if value is not None:
        _require_positive(value, name)


def _require_non_empty_sequence(value: Any, name: str) -> None:
    if not isinstance(value, tuple) or not value:
        raise ValueError(f"{name} must contain at least one item")


def _validate_port(value: Any, name: str) -> None:
    if not isinstance(value, int) or isinstance(value, bool) or not 1 <= value <= 65535:
        raise ValueError(f"{name} must be between 1 and 65535")


def _validate_log_level(value: Any) -> None:
    allowed = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
    if not isinstance(value, str) or value.strip().upper() not in allowed:
        choices = ", ".join(sorted(allowed))
        raise ConfigurationError(
            f"infrastructure.log_level must be one of: {choices}",
            details={"log_level": value},
        )


__all__ = [
    "AdminDefaults",
    "DEFAULT_CONFIG_FILE",
    "DEFAULT_ENV_FILE",
    "DataDefaults",
    "InfrastructureSettings",
    "OpenAlexSettings",
    "OperationDefaults",
    "PROJECT_DIR",
    "Settings",
    "WorkerSettings",
    "get_settings",
    "load_settings",
]
