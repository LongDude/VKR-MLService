from __future__ import annotations

import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from core.exceptions import (
    AppError,
    ConfigurationError,
    DuplicateEntityError,
    EmbeddingGenerationError,
    EntityNotFoundError,
    ExternalResponseFormatError,
    ExternalServiceRateLimitError,
    ExternalServiceUnavailableError,
    InsufficientUserProfileDataError,
    InvalidRequestError,
    LLMGenerationError,
    QdrantIndexError,
    RedisOperationError,
    SnapshotImportError,
    SnapshotReadError,
)


def test_app_error_to_dict_uses_api_error_shape() -> None:
    error = AppError("broken", code="custom_code", details={"field": "title"})

    assert str(error) == "broken"
    assert error.to_dict() == {
        "error": {
            "code": "custom_code",
            "message": "broken",
            "details": {"field": "title"},
        }
    }


def test_app_error_to_dict_uses_empty_details_by_default() -> None:
    assert EntityNotFoundError("missing").to_dict() == {
        "error": {
            "code": "entity_not_found",
            "message": "missing",
            "details": {},
        }
    }


def test_exception_subclasses_have_default_codes() -> None:
    expected_codes = {
        EntityNotFoundError: "entity_not_found",
        DuplicateEntityError: "duplicate_entity",
        InvalidRequestError: "invalid_request",
        ExternalServiceUnavailableError: "external_service_unavailable",
        ExternalServiceRateLimitError: "external_service_rate_limit",
        ExternalResponseFormatError: "external_response_format",
        EmbeddingGenerationError: "embedding_generation",
        LLMGenerationError: "llm_generation",
        QdrantIndexError: "qdrant_index",
        RedisOperationError: "redis_operation",
        SnapshotReadError: "snapshot_read",
        SnapshotImportError: "snapshot_import",
        ConfigurationError: "configuration",
        InsufficientUserProfileDataError: "insufficient_user_profile_data",
    }

    for error_type, code in expected_codes.items():
        assert error_type("message").code == code
