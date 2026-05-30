from __future__ import annotations

from typing import TYPE_CHECKING

from core.exceptions import InvalidRequestError

if TYPE_CHECKING:
    from ml.facades.research_entity_indexing import ResearchEntityType


class TextPreparationService:
    ABSTRACT_PROMPT_MAX_CHARS = 1200

    def build_paper_embedding_text(
        self,
        title: str,
        abstract: str | None,
        topics: list[str] | None = None,
        keywords: list[str] | None = None,
    ) -> str:
        sections: list[str] = []
        self._append_section(sections, "Title", title)
        self._append_section(sections, "Abstract", abstract)
        self._append_list_section(sections, "Topics", topics)
        self._append_list_section(sections, "Keywords", keywords)
        return "\n".join(sections)

    def build_research_entity_embedding_text(
        self,
        entity_type: ResearchEntityType,
        name: str,
        domain_name: str | None = None,
        field_name: str | None = None,
        subfield_name: str | None = None,
    ) -> str:
        sections: list[str] = []
        self._append_section(sections, "Entity type", entity_type)
        self._append_section(sections, "Name", name)
        self._append_section(sections, "Domain", domain_name)
        self._append_section(sections, "Field", field_name)
        self._append_section(sections, "Subfield", subfield_name)
        return "\n".join(sections)

    def build_cluster_summary_prompt(
        self,
        cluster_name: str,
        paper_titles: list[str],
        abstracts: list[str],
        top_keywords: list[str],
    ) -> list[dict]:
        user_parts: list[str] = []
        clean_cluster_name = self._clean_text(cluster_name)
        if clean_cluster_name:
            user_parts.append(f"Cluster name: {clean_cluster_name}")

        clean_keywords = self._clean_list(top_keywords)
        if clean_keywords:
            user_parts.append(f"Top keywords: {', '.join(clean_keywords)}")

        clean_titles = self._clean_list(paper_titles)
        if clean_titles:
            user_parts.append(
                "Paper titles:\n"
                + "\n".join(
                    f"{index}. {title}" for index, title in enumerate(clean_titles, 1)
                )
            )

        clean_abstracts = self._clean_list(abstracts)
        if clean_abstracts:
            user_parts.append(
                "Abstracts:\n"
                + "\n\n".join(
                    f"{index}. {self.truncate_text(abstract, self.ABSTRACT_PROMPT_MAX_CHARS)}"
                    for index, abstract in enumerate(clean_abstracts, 1)
                )
            )

        return [
            {
                "role": "system",
                "content": (
                    "You summarize research clusters. Return only a compact JSON "
                    "object with keys: title, summary, key_methods, "
                    "key_applications, limitations. The title must be a semantic "
                    "research-theme name, 4-8 words, not a comma-separated keyword "
                    "list. The summary must be 2-4 sentences describing the focus, "
                    "common methods or approaches, and application context. Use "
                    "short arrays of short strings for the list fields."
                ),
            },
            {
                "role": "user",
                "content": "\n\n".join(user_parts),
            },
        ]

    def truncate_text(
        self,
        text: str,
        max_chars: int,
    ) -> str:
        if max_chars < 0:
            raise InvalidRequestError(
                "max_chars must be non-negative",
                details={"max_chars": max_chars},
            )
        normalized = self._normalize_whitespace(text)
        if len(normalized) <= max_chars:
            return normalized
        if max_chars <= 3:
            return normalized[:max_chars]
        return f"{normalized[: max_chars - 3].rstrip()}..."

    def _append_section(
        self,
        sections: list[str],
        label: str,
        value: str | None,
    ) -> None:
        clean_value = self._clean_text(value)
        if clean_value:
            sections.append(f"{label}: {clean_value}")

    def _append_list_section(
        self,
        sections: list[str],
        label: str,
        values: list[str] | None,
    ) -> None:
        clean_values = self._clean_list(values)
        if clean_values:
            sections.append(f"{label}: {', '.join(clean_values)}")

    def _clean_list(self, values: list[str] | None) -> list[str]:
        if not values:
            return []
        return [
            clean_value
            for clean_value in (self._clean_text(value) for value in values)
            if clean_value
        ]

    def _clean_text(self, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = self._normalize_whitespace(value)
        return normalized or None

    def _normalize_whitespace(self, value: str) -> str:
        return " ".join(str(value).split())


__all__ = ["TextPreparationService"]
