from __future__ import annotations

from typing import Any

from pydantic import Field

from .common import BaseDTO


class ErrorDetailDTO(BaseDTO):
    code: str
    message: str
    details: dict[str, Any] = Field(default_factory=dict)


class ErrorResponseDTO(BaseDTO):
    error: ErrorDetailDTO


__all__ = ["ErrorDetailDTO", "ErrorResponseDTO"]
