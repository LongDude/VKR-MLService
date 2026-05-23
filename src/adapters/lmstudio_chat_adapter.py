from __future__ import annotations

import json
from typing import Any

import httpx

from core.exceptions import LLMGenerationError


class LMStudioChatAdapter:
    """Small OpenAI-compatible chat client for LMStudio local server."""

    def __init__(
        self,
        base_url: str = "http://localhost:1234",
        *,
        timeout_seconds: float = 60.0,
        client: httpx.Client | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout_seconds = timeout_seconds
        self._client = client or httpx.Client(timeout=timeout_seconds)

    def chat_completion(
        self,
        messages: list[dict[str, Any]],
        model: str = "qwen-3-8b",
        temperature: float = 0.2,
        response_format: dict[str, Any] | None = None,
    ) -> str:
        """Return assistant text from a chat completion request."""
        try:
            normalized_response_format = self._normalize_response_format(
                response_format
            )
            payload: dict[str, Any] = {
                "model": model,
                "messages": messages,
                "temperature": temperature,
            }
            if normalized_response_format is not None:
                payload["response_format"] = normalized_response_format

            response = self._client.post(
                f"{self._base_url}/v1/chat/completions",
                json=payload,
                timeout=self._timeout_seconds,
            )
            self._raise_for_status(response)
            data = response.json()
            content = self._extract_content(data)
            if self._expects_json_response(response_format):
                self._parse_json_content(content)
            return content
        except LLMGenerationError:
            raise
        except Exception as exc:
            raise LLMGenerationError(
                "Failed to generate LMStudio chat completion",
                details={"reason": str(exc)},
            ) from exc

    def summarize_cluster(self, title: str, abstracts: list[str]) -> dict[str, Any]:
        """Return a compact JSON summary for a research cluster."""
        content = self.chat_completion(
            messages=[
                {
                    "role": "system",
                    "content": "Return a compact JSON summary for a research cluster.",
                },
                {
                    "role": "user",
                    "content": (
                        f"Cluster title: {title}\n\n"
                        f"Abstracts:\n{self._join_abstracts(abstracts)}"
                    ),
                },
            ],
            response_format={"type": "json_object"},
        )
        return self._parse_json_content(content)

    def explain_recommendation(
        self,
        user_interests: list[str],
        paper_title: str,
        paper_abstract: str | None,
    ) -> str:
        """Return a concise natural-language paper recommendation explanation."""
        interests = ", ".join(user_interests) if user_interests else "not specified"
        return self.chat_completion(
            messages=[
                {
                    "role": "system",
                    "content": "Explain paper recommendations briefly and concretely.",
                },
                {
                    "role": "user",
                    "content": (
                        f"User interests: {interests}\n"
                        f"Paper title: {paper_title}\n"
                        f"Paper abstract: {paper_abstract or ''}"
                    ),
                },
            ],
        )

    def _normalize_response_format(
        self,
        response_format: dict[str, Any] | None,
    ) -> dict[str, Any] | None:
        if response_format is None:
            return None
        if response_format.get("type") != "json_object":
            return response_format

        schema = response_format.get("schema") or {
            "type": "object",
            "additionalProperties": True,
        }
        return {
            "type": "json_schema",
            "json_schema": {
                "name": str(response_format.get("name") or "json_response"),
                "schema": schema,
            },
        }

    def _expects_json_response(self, response_format: dict[str, Any] | None) -> bool:
        if response_format is None:
            return False
        return response_format.get("type") in {"json_object", "json_schema"}

    def _extract_content(self, payload: dict[str, Any]) -> str:
        choices = payload.get("choices")
        if not isinstance(choices, list) or not choices:
            raise ValueError("Chat response does not contain choices")
        first_choice = choices[0]
        if not isinstance(first_choice, dict):
            raise ValueError("Chat response choice has invalid format")
        message = first_choice.get("message")
        if not isinstance(message, dict):
            raise ValueError("Chat response choice does not contain a message")
        content = message.get("content")
        if not isinstance(content, str):
            raise ValueError("Chat response content is missing or not a string")
        return content

    def _parse_json_content(self, content: str) -> dict[str, Any]:
        try:
            parsed = json.loads(content)
        except json.JSONDecodeError as exc:
            raise LLMGenerationError(
                "LMStudio response was expected to be JSON but could not be parsed",
                details={"reason": str(exc), "content": content},
            ) from exc
        if not isinstance(parsed, dict):
            raise LLMGenerationError(
                "LMStudio response JSON must be an object",
                details={"content": content},
            )
        return parsed

    def _join_abstracts(self, abstracts: list[str]) -> str:
        return "\n\n".join(
            f"{index + 1}. {abstract}" for index, abstract in enumerate(abstracts)
        )

    def _raise_for_status(self, response: httpx.Response) -> None:
        if response.status_code >= 400:
            raise LLMGenerationError(
                "LMStudio chat endpoint returned an error",
                details={"status_code": response.status_code, "body": response.text},
            )


__all__ = ["LMStudioChatAdapter"]
