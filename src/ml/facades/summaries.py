from __future__ import annotations

import json
from typing import Any

from adapters.lmstudio_chat_adapter import LMStudioChatAdapter
from core.exceptions import LLMGenerationError
from dto.trends import ClusterSummaryDTO

from ml.constants import DEFAULT_CHAT_MODEL
from ml.services.text_preparation import TextPreparationService


class SummaryFacade:
    def __init__(
        self,
        *,
        chat_adapter: LMStudioChatAdapter,
        text_preparation_service: TextPreparationService | None = None,
        chat_model: str = DEFAULT_CHAT_MODEL,
    ) -> None:
        self.chat_adapter = chat_adapter
        self.text_preparation_service = (
            text_preparation_service or TextPreparationService()
        )
        self.chat_model = chat_model

    def summarize_cluster(
        self,
        cluster_name: str,
        paper_titles: list[str],
        abstracts: list[str],
        top_keywords: list[str],
    ) -> ClusterSummaryDTO:
        messages = self.text_preparation_service.build_cluster_summary_prompt(
            cluster_name=cluster_name,
            paper_titles=paper_titles,
            abstracts=abstracts,
            top_keywords=top_keywords,
        )
        try:
            content = self.chat_adapter.chat_completion(
                messages=messages,
                model=self.chat_model,
                temperature=0.2,
                response_format={"type": "json_object"},
            )
            payload = self._parse_json_object(content)
            return self._summary_from_payload(payload)
        except Exception:
            return self._fallback_summary(top_keywords)

    def name_cluster(
        self,
        representative_titles: list[str],
        top_keywords: list[str],
    ) -> str:
        fallback_title = self._fallback_title(top_keywords)
        try:
            content = self.chat_adapter.chat_completion(
                messages=self._build_cluster_name_prompt(
                    representative_titles,
                    top_keywords,
                ),
                model=self.chat_model,
                temperature=0.2,
                response_format={"type": "json_object"},
            )
            payload = self._parse_json_object(content)
            title = self._clean_text(payload.get("title"))
            return title or fallback_title
        except Exception:
            return fallback_title

    def explain_recommendation(
        self,
        user_interests: list[str],
        paper_title: str,
        paper_abstract: str | None,
    ) -> str:
        fallback = self._fallback_explanation(user_interests, paper_title)
        try:
            content = self.chat_adapter.chat_completion(
                messages=self._build_recommendation_explanation_prompt(
                    user_interests,
                    paper_title,
                    paper_abstract,
                ),
                model=self.chat_model,
                temperature=0.2,
                response_format={"type": "json_object"},
            )
            payload = self._parse_json_object(content)
            explanation = self._clean_text(payload.get("explanation"))
            return explanation or fallback
        except Exception:
            return fallback

    def _summary_from_payload(self, payload: dict[str, Any]) -> ClusterSummaryDTO:
        title = self._clean_text(payload.get("title"))
        summary = self._clean_text(payload.get("summary"))
        if not title or not summary:
            raise LLMGenerationError(
                "Cluster summary JSON is missing title or summary"
            )

        return ClusterSummaryDTO(
            title=title,
            summary=summary,
            key_methods=self._string_list(payload.get("key_methods")),
            key_applications=self._string_list(payload.get("key_applications")),
            limitations=self._string_list(payload.get("limitations")),
            degraded=False,
        )

    def _fallback_summary(self, top_keywords: list[str]) -> ClusterSummaryDTO:
        keywords = self._clean_list(top_keywords)
        title = self._fallback_title(keywords)
        keyword_text = ", ".join(keywords[:5])
        if keyword_text:
            summary = f"The cluster focuses on research related to {keyword_text}."
        else:
            summary = "The cluster groups related research papers around a shared trend."
        return ClusterSummaryDTO(
            title=title,
            summary=summary,
            key_methods=keywords[:3],
            key_applications=[],
            limitations=[],
            degraded=True,
        )

    def _build_cluster_name_prompt(
        self,
        representative_titles: list[str],
        top_keywords: list[str],
    ) -> list[dict[str, str]]:
        title_lines = "\n".join(
            f"{index}. {title}"
            for index, title in enumerate(self._clean_list(representative_titles), 1)
        )
        keywords = ", ".join(self._clean_list(top_keywords))
        return [
            {
                "role": "system",
                "content": (
                    "Return only a short JSON object with key title. "
                    "The title must be concise and specific."
                ),
            },
            {
                "role": "user",
                "content": (
                    f"Representative titles:\n{title_lines}\n\n"
                    f"Top keywords: {keywords}"
                ),
            },
        ]

    def _build_recommendation_explanation_prompt(
        self,
        user_interests: list[str],
        paper_title: str,
        paper_abstract: str | None,
    ) -> list[dict[str, str]]:
        interests = ", ".join(self._clean_list(user_interests)) or "not specified"
        abstract = self.text_preparation_service.truncate_text(
            paper_abstract or "",
            900,
        )
        return [
            {
                "role": "system",
                "content": (
                    "Return only a short JSON object with key explanation. "
                    "Explain the recommendation in one concise sentence."
                ),
            },
            {
                "role": "user",
                "content": (
                    f"User interests: {interests}\n"
                    f"Paper title: {self._clean_text(paper_title) or ''}\n"
                    f"Paper abstract: {abstract}"
                ),
            },
        ]

    def _parse_json_object(self, content: str) -> dict[str, Any]:
        try:
            parsed = json.loads(content)
        except json.JSONDecodeError as exc:
            raise LLMGenerationError(
                "LLM response was expected to be JSON",
                details={"reason": str(exc), "content": content},
            ) from exc
        if not isinstance(parsed, dict):
            raise LLMGenerationError(
                "LLM response JSON must be an object",
                details={"content": content},
            )
        return parsed

    def _fallback_title(self, top_keywords: list[str]) -> str:
        keywords = self._clean_list(top_keywords)
        return keywords[0] if keywords else "Research trend"

    def _fallback_explanation(
        self,
        user_interests: list[str],
        paper_title: str,
    ) -> str:
        interests = self._clean_list(user_interests)
        title = self._clean_text(paper_title) or "this paper"
        if interests:
            return f"{title} matches the user's interest in {', '.join(interests[:3])}."
        return f"{title} is recommended based on semantic similarity to the profile."

    def _string_list(self, value: Any) -> list[str]:
        if not isinstance(value, list):
            return []
        return self._clean_list([str(item) for item in value if item is not None])

    def _clean_list(self, values: list[str]) -> list[str]:
        return [
            clean_value
            for clean_value in (self._clean_text(value) for value in values)
            if clean_value
        ]

    def _clean_text(self, value: Any) -> str | None:
        if value is None:
            return None
        normalized = " ".join(str(value).split())
        return normalized or None


__all__ = ["SummaryFacade"]
