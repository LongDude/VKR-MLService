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
        payload = {
            "topic": topic,
            "period": period,
            "metrics": metrics,
            "keyword_dynamics": keyword_dynamics,
            "openalex_counts": openalex_counts,
            "representative_papers": representative_papers,
        }
        if report_language.strip().lower() == "ru":
            return self._build_russian_prompt(payload)

        return self._build_english_prompt(payload, report_language=report_language)

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
                        "summary": {"type": "string"},
                        "period_characterization": {"type": "string"},
                        "dynamics_summary": {"type": "string"},
                        "future_dynamics": {"type": "string"},
                        "items": {
                            "type": "array",
                            "minItems": 6,
                            "maxItems": 15,
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
                        "summary",
                        "period_characterization",
                        "dynamics_summary",
                        "future_dynamics",
                        "items",
                    ],
                    "additionalProperties": False,
                },
            },
        }

    def _build_russian_prompt(self, payload: dict[str, Any]) -> list[dict[str, str]]:
        return [
            {
                "role": "system",
                "content": (
                    "Ты формируешь квартальные аналитические отчеты о научных "
                    "предметных областях. Верни только валидный JSON-объект. "
                    "Все значения текстовых полей должны быть на русском языке, "
                    "кроме общепринятых терминов и названий методов без "
                    "устойчивого русского перевода. Не выдумывай paper ids или "
                    "статистику."
                ),
            },
            {
                "role": "user",
                "content": (
                    "Сформируй квартальный теоретический отчет по указанному topic.\n"
                    "JSON-объект должен содержать ключи: summary, "
                    "period_characterization, dynamics_summary, future_dynamics, "
                    "items.\n"
                    "summary - краткий вывод по кварталу для Timeline: 1-3 "
                    "предложения о главном состоянии и изменении периода.\n"
                    "period_characterization - характеристика состояния предметной "
                    "области в этом квартале: фокус, зрелость, активные "
                    "поднаправления и характер исследований. Не давай стабильное "
                    "определение topic и не повторяй summary.\n"
                    "items должен быть массивом объектов с ключами: item_type, "
                    "title, description, maturity, evidence.\n"
                    "Допустимые item_type: research_problem, method, approach, "
                    "future_direction.\n"
                    "Допустимые maturity: emerging, growing, stable, declining, "
                    "mature.\n"
                    "Сформируй не один пункт на класс, а репрезентативный набор: "
                    "2-4 research_problem, 2-4 method, 1-3 approach и 2-4 "
                    "future_direction, если это подтверждается входными данными. "
                    "Если данных для класса мало, все равно дай минимум один "
                    "конкретный пункт для этого класса.\n"
                    "В evidence.paper_ids используй только id из representative_papers.\n\n"
                    f"Входные данные:\n{json.dumps(payload, ensure_ascii=False, default=str)}"
                ),
            },
        ]

    def _build_english_prompt(
        self,
        payload: dict[str, Any],
        *,
        report_language: str,
    ) -> list[dict[str, str]]:
        language = report_language or "English"
        return [
            {
                "role": "system",
                "content": (
                    "You generate quarterly analytical reports about scientific topics. "
                    "Return only a valid JSON object. Do not invent paper ids or "
                    "statistics. "
                    f"Write all narrative text in {language}."
                ),
            },
            {
                "role": "user",
                "content": (
                    "Generate a quarterly theoretical research report for the supplied "
                    "topic.\n"
                    "The JSON object must contain these keys: summary, "
                    "period_characterization, dynamics_summary, future_dynamics, "
                    "items.\n"
                    "summary is a concise Timeline-ready quarterly takeaway in 1-3 "
                    "sentences.\n"
                    "period_characterization describes the topic state in this quarter: "
                    "focus, maturity, active subdirections, and research character. "
                    "Do not write a stable topic description and do not repeat summary.\n"
                    "items must be an array of objects with keys: item_type, title, "
                    "description, maturity, evidence.\n"
                    "Allowed item_type values: research_problem, method, approach, "
                    "future_direction.\n"
                    "Allowed maturity values: emerging, growing, stable, declining, "
                    "mature.\n"
                    "Do not produce only one item per class. Produce a representative "
                    "set: 2-4 research_problem, 2-4 method, 1-3 approach, and "
                    "2-4 future_direction items when supported by the input data. "
                    "When evidence is sparse, still provide at least one concrete "
                    "item for each class.\n"
                    "Use evidence.paper_ids only from representative_papers.\n\n"
                    f"Input:\n{json.dumps(payload, ensure_ascii=False, default=str)}"
                ),
            },
        ]


__all__ = ["TopicQuarterReportPromptService"]
