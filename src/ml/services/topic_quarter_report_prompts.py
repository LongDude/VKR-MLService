from __future__ import annotations

import json
from typing import Any


class TopicQuarterReportPromptService:
    """Build LMStudio prompts for quarterly topic reports."""

    def build_report_prompt(
        self,
        *,
        topic: dict[str, Any],
        period: dict[str, Any],
        metrics: dict[str, Any],
        keyword_dynamics: dict[str, Any],
        openalex_counts: list[dict[str, Any]],
        representative_papers: list[dict[str, Any]],
        report_language: str = "ru",
    ) -> list[dict[str, str]]:
        """Build chat messages that request a structured JSON report."""
        language = "Russian" if report_language == "ru" else report_language
        payload = {
            "topic": topic,
            "period": period,
            "metrics": metrics,
            "keyword_dynamics": keyword_dynamics,
            "openalex_counts": openalex_counts,
            "representative_papers": representative_papers,
        }
        return [
            {
                "role": "system",
                "content": (
                    "You generate analytical reports about scientific topics. "
                    "Return only a valid JSON object. Do not invent paper ids or statistics. "
                    f"Write all narrative text in {language}."
                ),
            },
            {
                "role": "user",
                "content": (
                    "Generate a quarterly theoretical research report for the supplied topic.\n"
                    "The JSON object must contain these keys: title, summary, definition, "
                    "dynamics_summary, future_dynamics, items.\n"
                    "items must be an array of objects with keys: item_type, title, "
                    "description, maturity, evidence.\n"
                    "Allowed item_type values: research_problem, method, approach, "
                    "future_direction.\n"
                    "Allowed maturity values: emerging, growing, stable, declining, mature.\n"
                    "Use evidence.paper_ids only from representative_papers.\n\n"
                    f"Input:\n{json.dumps(payload, ensure_ascii=False, default=str)}"
                ),
            },
        ]

    def response_format(self) -> dict[str, Any]:
        """Return LMStudio JSON schema constraints for a topic report response."""
        item_types = [
            "research_problem",
            "method",
            "approach",
            "future_direction",
        ]
        maturities = [
            "emerging",
            "growing",
            "stable",
            "declining",
            "mature",
        ]
        return {
            "type": "json_schema",
            "json_schema": {
                "name": "topic_quarter_report",
                "schema": {
                    "type": "object",
                    "properties": {
                        "title": {"type": "string"},
                        "summary": {"type": "string"},
                        "definition": {"type": "string"},
                        "dynamics_summary": {"type": "string"},
                        "future_dynamics": {"type": "string"},
                        "items": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "item_type": {
                                        "type": "string",
                                        "enum": item_types,
                                    },
                                    "title": {"type": "string"},
                                    "description": {"type": "string"},
                                    "maturity": {
                                        "type": "string",
                                        "enum": maturities,
                                    },
                                    "evidence": {
                                        "type": "object",
                                        "additionalProperties": True,
                                    },
                                },
                                "required": [
                                    "item_type",
                                    "title",
                                    "description",
                                    "maturity",
                                    "evidence",
                                ],
                                "additionalProperties": False,
                            },
                        },
                    },
                    "required": [
                        "title",
                        "summary",
                        "definition",
                        "dynamics_summary",
                        "future_dynamics",
                        "items",
                    ],
                    "additionalProperties": False,
                },
            },
        }


__all__ = ["TopicQuarterReportPromptService"]
