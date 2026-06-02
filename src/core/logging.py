from __future__ import annotations

import json
import logging
import inspect
import sys
import time
from contextlib import contextmanager
from functools import wraps
from typing import Any, Callable, Iterator, TypeVar

from core.config import Settings
from core.exceptions import ConfigurationError

LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"
VALID_LOG_LEVELS = frozenset({"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"})
T = TypeVar("T")


def configure_logging(settings: Settings, *, verbosity: int = 0) -> None:
    """Configure process logging and dependency verbosity from application settings."""
    application_level = resolve_log_level(settings.infrastructure.log_level)
    if verbosity == 1:
        application_level = logging.INFO
    elif verbosity >= 2:
        application_level = logging.DEBUG

    logging.basicConfig(
        level=application_level,
        format=LOG_FORMAT,
        stream=sys.stderr,
        force=True,
    )

    if verbosity <= 0:
        dependency_levels = {
            "httpx": logging.WARNING,
            "httpcore": logging.WARNING,
            "qdrant_client": logging.WARNING,
            "uvicorn.error": logging.WARNING,
            "uvicorn.access": logging.WARNING,
        }
    elif verbosity == 1:
        dependency_levels = {
            "httpx": logging.INFO,
            "httpcore": logging.WARNING,
            "qdrant_client": logging.INFO,
            "uvicorn.error": logging.INFO,
            "uvicorn.access": logging.INFO,
        }
    else:
        dependency_levels = {
            "httpx": logging.DEBUG,
            "httpcore": logging.DEBUG,
            "qdrant_client": logging.DEBUG,
            "uvicorn.error": logging.DEBUG,
            "uvicorn.access": logging.DEBUG,
        }

    for name, level in dependency_levels.items():
        logging.getLogger(name).setLevel(level)


def resolve_log_level(value: str) -> int:
    """Resolve one supported text log level to its logging constant."""
    normalized = str(value).strip().upper()
    if normalized not in VALID_LOG_LEVELS:
        choices = ", ".join(sorted(VALID_LOG_LEVELS))
        raise ConfigurationError(
            f"infrastructure.log_level must be one of: {choices}",
            details={"log_level": value},
        )
    return int(getattr(logging, normalized))


def get_logger(name: str) -> logging.Logger:
    """Return a namespaced process logger."""
    return logging.getLogger(name)


def log_event(
    logger: logging.Logger,
    event: str,
    *,
    level: int = logging.INFO,
    exc_info: bool = False,
    **fields: Any,
) -> None:
    """Emit one stable key=value process log event."""
    suffix = " ".join(
        f"{key}={_format_value(value)}"
        for key, value in sorted(fields.items())
        if value is not None
    )
    message = event if not suffix else f"{event} {suffix}"
    logger.log(level, message, exc_info=exc_info)


@contextmanager
def logged_operation(
    logger: logging.Logger,
    operation: str,
    **fields: Any,
) -> Iterator[None]:
    """Log start, completion, duration, and unexpected failure for an operation."""
    started_at = time.monotonic()
    log_event(logger, f"{operation}_started", **fields)
    try:
        yield
    except Exception as exc:
        log_event(
            logger,
            f"{operation}_failed",
            level=logging.ERROR,
            exc_info=True,
            elapsed_seconds=round(time.monotonic() - started_at, 3),
            error_type=exc.__class__.__name__,
            **fields,
        )
        raise
    log_event(
        logger,
        f"{operation}_completed",
        elapsed_seconds=round(time.monotonic() - started_at, 3),
        **fields,
    )


def logged_call(logger: logging.Logger, operation: str) -> Callable[[T], T]:
    """Decorate one operation boundary with standard lifecycle logging."""

    def decorator(func: T) -> T:
        if inspect.iscoroutinefunction(func):

            @wraps(func)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                with logged_operation(logger, operation):
                    return await func(*args, **kwargs)

            return async_wrapper  # type: ignore[return-value]

        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            with logged_operation(logger, operation):
                return func(*args, **kwargs)

        return wrapper  # type: ignore[return-value]

    return decorator


def _format_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str) and value and all(
        char.isalnum() or char in "._:/@+-" for char in value
    ):
        return value
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


__all__ = [
    "LOG_FORMAT",
    "VALID_LOG_LEVELS",
    "configure_logging",
    "get_logger",
    "log_event",
    "logged_call",
    "logged_operation",
    "resolve_log_level",
]
