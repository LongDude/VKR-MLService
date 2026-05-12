from __future__ import annotations

from typing import Literal


class TrendStatusService:
    def determine_status(
        self,
        age_periods: int,
        recent_paper_count: int,
        growth_rate: float,
    ) -> Literal["new", "emerging", "growing", "stable", "declining"]:
        if age_periods <= 2:
            return "new"

        if growth_rate > 0.5 and recent_paper_count >= 5:
            return "emerging"

        if growth_rate > 0.2:
            return "growing"

        if growth_rate < -0.2:
            return "declining"

        return "stable"


__all__ = ["TrendStatusService"]

