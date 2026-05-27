from __future__ import annotations

from .common import BaseDTO


class InstitutionCreateDTO(BaseDTO):
    display_name: str
    ror: str | None = None
    country_code: str | None = None
    type: str | None = None


class InstitutionDTO(InstitutionCreateDTO):
    id: int | None = None


__all__ = ["InstitutionCreateDTO", "InstitutionDTO"]
