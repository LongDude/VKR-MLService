from __future__ import annotations

from datetime import date
import sys
from pathlib import Path

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from core.exceptions import InvalidRequestError
from ml.services.quarter_periods import QuarterPeriodService
from ml.services.topic_quarter_report_prompts import TopicQuarterReportPromptService


def test_quarter_period_service_rounds_outward_to_full_quarters() -> None:
    service = QuarterPeriodService()

    periods = service.quarter_periods(date(2025, 2, 10), date(2025, 8, 20))

    assert [(period.key, period.date_from, period.date_to) for period in periods] == [
        ("2025-Q1", date(2025, 1, 1), date(2025, 3, 31)),
        ("2025-Q2", date(2025, 4, 1), date(2025, 6, 30)),
        ("2025-Q3", date(2025, 7, 1), date(2025, 9, 30)),
    ]


def test_quarter_period_service_rejects_inverted_period() -> None:
    service = QuarterPeriodService()

    with pytest.raises(InvalidRequestError):
        service.quarter_periods(date(2025, 9, 1), date(2025, 8, 31))


def test_quarter_period_key_uses_calendar_quarter() -> None:
    service = QuarterPeriodService()

    assert service.period_key(date(2026, 10, 1)) == "2026-Q4"


def test_topic_report_prompt_uses_russian_instructions_for_ru_reports() -> None:
    service = TopicQuarterReportPromptService()

    messages = service.build_report_prompt(
        topic={"id": 1, "name": "Graph Theory"},
        period={"period_key": "2025-Q1"},
        metrics={},
        keyword_dynamics={},
        openalex_counts=[],
        representative_papers=[],
        report_language="ru",
    )

    prompt_text = "\n".join(message["content"] for message in messages)
    assert "Все значения текстовых полей должны быть на русском языке" in prompt_text
    assert "summary - краткий вывод по кварталу для Timeline" in prompt_text
    assert "period_characterization - характеристика состояния" in prompt_text
    assert "2-4 research_problem" in prompt_text
    assert "2-4 method" in prompt_text


def test_topic_report_response_schema_matches_period_report_contract() -> None:
    service = TopicQuarterReportPromptService()

    schema = service.response_format()["json_schema"]["schema"]
    properties = schema["properties"]
    items_schema = properties["items"]

    assert "title" not in properties
    assert "definition" not in properties
    assert "period_characterization" in properties
    assert "period_characterization" in schema["required"]
    assert "title" in items_schema["items"]["properties"]
    assert items_schema["minItems"] == 6
    assert items_schema["maxItems"] == 15
