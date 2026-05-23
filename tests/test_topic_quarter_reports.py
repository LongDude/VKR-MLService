from __future__ import annotations

from datetime import date
import sys
from pathlib import Path

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from core.exceptions import InvalidRequestError
from ml.services.quarter_periods import QuarterPeriodService


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
