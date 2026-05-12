from __future__ import annotations

from numbers import Real
from typing import Any

import httpx

from core.exceptions import EmbeddingGenerationError
from dto.embeddings import EmbeddingResultDTO


class LMStudioEmbeddingAdapter:
    def __init__(
        self,
        base_url: str = "http://localhost:1234",
        *,
        timeout_seconds: float = 30.0,
        client: httpx.Client | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout_seconds = timeout_seconds
        self._client = client or httpx.Client(timeout=timeout_seconds)

    def embed_text(
        self,
        text: str,
        model: str = "qwen3-embedding",
    ) -> EmbeddingResultDTO:
        return self.embed_batch([text], model=model)[0]

    def embed_batch(
        self,
        texts: list[str],
        model: str = "qwen3-embedding",
    ) -> list[EmbeddingResultDTO]:
        if not texts:
            return []
        try:
            response = self._client.post(
                f"{self._base_url}/v1/embeddings",
                json={"model": model, "input": texts if len(texts) > 1 else texts[0]},
                timeout=self._timeout_seconds,
            )
            self._raise_for_status(response)
            payload = response.json()
            data = payload.get("data")
            if not isinstance(data, list) or len(data) != len(texts):
                raise ValueError("Embedding response data length does not match input length")

            response_model = str(payload.get("model") or model)
            total_tokens = self._extract_total_tokens(payload)
            return [
                self._result_from_item(item, response_model, total_tokens)
                for item in data
            ]
        except EmbeddingGenerationError:
            raise
        except Exception as exc:
            raise EmbeddingGenerationError(
                "Failed to generate embeddings with LMStudio",
                details={"reason": str(exc)},
            ) from exc

    def _result_from_item(
        self,
        item: dict[str, Any],
        model: str,
        token_count: int | None,
    ) -> EmbeddingResultDTO:
        embedding = item.get("embedding") if isinstance(item, dict) else None
        vector = self._validate_vector(embedding)
        return EmbeddingResultDTO(
            vector=vector,
            model=model,
            dimension=len(vector),
            token_count=token_count,
        )

    def _validate_vector(self, value: Any) -> list[float]:
        if not isinstance(value, list) or not value:
            raise ValueError("Embedding vector is missing or empty")
        if not all(isinstance(item, Real) and not isinstance(item, bool) for item in value):
            raise ValueError("Embedding vector contains non-numeric values")
        return [float(item) for item in value]

    def _extract_total_tokens(self, payload: dict[str, Any]) -> int | None:
        usage = payload.get("usage")
        if not isinstance(usage, dict):
            return None
        total_tokens = usage.get("total_tokens")
        return int(total_tokens) if isinstance(total_tokens, int) else None

    def _raise_for_status(self, response: httpx.Response) -> None:
        if response.status_code >= 400:
            raise EmbeddingGenerationError(
                "LMStudio embeddings endpoint returned an error",
                details={"status_code": response.status_code, "body": response.text},
            )


__all__ = ["LMStudioEmbeddingAdapter"]
