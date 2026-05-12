from __future__ import annotations

from datetime import date
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class BaseDTO(BaseModel):
    model_config = ConfigDict(from_attributes=True)


class PaginationDTO(BaseDTO):
    limit: int = Field(default=20, ge=1, le=100)
    offset: int = Field(default=0, ge=0)


class DateRangeDTO(BaseDTO):
    date_from: date | None = None
    date_to: date | None = None


class SortDTO(BaseDTO):
    sort_by: str | None = None
    sort_order: Literal["asc", "desc"] = "desc"


class OperationResultDTO(BaseDTO):
    success: bool
    message: str | None = None
    details: dict[str, Any] = Field(default_factory=dict)


class BatchOperationResultDTO(BaseDTO):
    total: int = Field(default=0, ge=0)
    created: int = Field(default=0, ge=0)
    updated: int = Field(default=0, ge=0)
    skipped: int = Field(default=0, ge=0)
    failed: int = Field(default=0, ge=0)
    errors: list[dict[str, Any]] = Field(default_factory=list)


__all__ = [
    "BaseDTO",
    "BatchOperationResultDTO",
    "DateRangeDTO",
    "OperationResultDTO",
    "PaginationDTO",
    "SortDTO",
]
