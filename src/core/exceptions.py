from __future__ import annotations

from typing import Any


class AppError(Exception):
    code: str = "app_error"
    message: str
    details: dict[str, Any] | None

    def __init__(
        self,
        message: str,
        code: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        self.message = message
        self.code = code or self.__class__.code
        self.details = details
        super().__init__(message)

    def to_dict(self) -> dict[str, dict[str, Any]]:
        return {
            "error": {
                "code": self.code,
                "message": self.message,
                "details": self.details or {},
            }
        }


class EntityNotFoundError(AppError):
    code = "entity_not_found"


class DuplicateEntityError(AppError):
    code = "duplicate_entity"


class InvalidRequestError(AppError):
    code = "invalid_request"


class ExternalServiceUnavailableError(AppError):
    code = "external_service_unavailable"


class ExternalServiceRateLimitError(AppError):
    code = "external_service_rate_limit"


class ExternalResponseFormatError(AppError):
    code = "external_response_format"


class EmbeddingGenerationError(AppError):
    code = "embedding_generation"


class LLMGenerationError(AppError):
    code = "llm_generation"


class QdrantIndexError(AppError):
    code = "qdrant_index"


class RedisOperationError(AppError):
    code = "redis_operation"


class SnapshotReadError(AppError):
    code = "snapshot_read"


class SnapshotImportError(AppError):
    code = "snapshot_import"


class ConfigurationError(AppError):
    code = "configuration"


class InsufficientUserProfileDataError(AppError):
    code = "insufficient_user_profile_data"


__all__ = [
    "AppError",
    "ConfigurationError",
    "DuplicateEntityError",
    "EmbeddingGenerationError",
    "EntityNotFoundError",
    "ExternalResponseFormatError",
    "ExternalServiceRateLimitError",
    "ExternalServiceUnavailableError",
    "InsufficientUserProfileDataError",
    "InvalidRequestError",
    "LLMGenerationError",
    "QdrantIndexError",
    "RedisOperationError",
    "SnapshotImportError",
    "SnapshotReadError",
]
