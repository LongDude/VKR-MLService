from __future__ import annotations

from datetime import date
from typing import Any, Literal

from pydantic import Field

from .common import BaseDTO


class ChartPointDTO(BaseDTO):
    x: str | int | float
    y: int | float
    label: str | None = None
    meta: dict[str, Any] = Field(default_factory=dict)


class ChartSeriesDTO(BaseDTO):
    name: str
    points: list[ChartPointDTO] = Field(default_factory=list)
    color: str | None = None
    meta: dict[str, Any] = Field(default_factory=dict)


class ChartAxisDTO(BaseDTO):
    label: str | None = None
    unit: str | None = None
    min: int | float | None = None
    max: int | float | None = None


class ChartCellDTO(BaseDTO):
    x: str | int | float
    y: str | int | float
    value: int | float
    label: str | None = None
    meta: dict[str, Any] = Field(default_factory=dict)


class ChartDTO(BaseDTO):
    chart_id: str
    chart_type: Literal["line", "bar", "heatmap", "scatter", "keyword_evolution"]
    title: str
    description: str | None = None
    x_axis: ChartAxisDTO | None = None
    y_axis: ChartAxisDTO | None = None
    series: list[ChartSeriesDTO] = Field(default_factory=list)
    points: list[dict[str, Any]] = Field(default_factory=list)
    cells: list[ChartCellDTO] = Field(default_factory=list)
    meta: dict[str, Any] = Field(default_factory=dict)


class PeriodCountDTO(BaseDTO):
    period_start: date
    count: int


__all__ = [
    "ChartAxisDTO",
    "ChartCellDTO",
    "ChartDTO",
    "ChartPointDTO",
    "ChartSeriesDTO",
    "PeriodCountDTO",
]
