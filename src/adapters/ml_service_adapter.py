from __future__ import annotations

from typing import Any

import httpx

from core.exceptions import (
    ExternalResponseFormatError,
    ExternalServiceUnavailableError,
    InvalidRequestError,
)
from dto.common import BatchOperationResultDTO
from dto.papers import (
    PaperBatchIndexingRequestDTO,
    PaperIndexingRequestDTO,
    PaperIndexingResponseDTO,
)
from dto.search import SemanticSearchMLResponseDTO, SemanticSearchRequestDTO


class MLServiceAdapter:
    def __init__(
        self,
        base_url: str,
        *,
        timeout_seconds: float = 30.0,
        client: httpx.Client | None = None,
        semantic_search_path: str = "/semantic-search",
        index_paper_path: str = "/papers/index",
        index_batch_path: str = "/papers/index-batch",
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout_seconds = timeout_seconds
        self._client = client or httpx.Client(timeout=timeout_seconds)
        self._semantic_search_path = semantic_search_path
        self._index_paper_path = index_paper_path
        self._index_batch_path = index_batch_path

    def semantic_search(
        self,
        request: SemanticSearchRequestDTO,
    ) -> SemanticSearchMLResponseDTO:
        payload = self._post(self._semantic_search_path, request.model_dump(mode="json"))
        return self._validate_response(payload, SemanticSearchMLResponseDTO)

    def index_paper(
        self,
        request: PaperIndexingRequestDTO,
    ) -> PaperIndexingResponseDTO:
        payload = self._post(self._index_paper_path, request.model_dump(mode="json"))
        return self._validate_response(payload, PaperIndexingResponseDTO)

    def index_batch(
        self,
        request: PaperBatchIndexingRequestDTO,
    ) -> BatchOperationResultDTO:
        payload = self._post(self._index_batch_path, request.model_dump(mode="json"))
        return self._validate_response(payload, BatchOperationResultDTO)

    def _post(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        response = self._client.post(
            f"{self._base_url}{path}",
            json=payload,
            timeout=self._timeout_seconds,
        )
        if response.status_code >= 500:
            raise ExternalServiceUnavailableError(
                "ML service is unavailable",
                details={"status_code": response.status_code, "body": response.text},
            )
        if response.status_code >= 400:
            raise InvalidRequestError(
                "ML service rejected the request",
                details={"status_code": response.status_code, "body": response.text},
            )
        try:
            data = response.json()
        except ValueError as exc:
            raise ExternalResponseFormatError(
                "ML service response is not valid JSON",
                details={"reason": str(exc)},
            ) from exc
        if not isinstance(data, dict):
            raise ExternalResponseFormatError("ML service response must be a JSON object")
        return data

    def _validate_response(self, payload: dict[str, Any], dto_type: type[Any]) -> Any:
        try:
            return dto_type.model_validate(payload)
        except Exception as exc:
            raise ExternalResponseFormatError(
                "ML service response does not match expected DTO",
                details={"reason": str(exc)},
            ) from exc


__all__ = ["MLServiceAdapter"]
