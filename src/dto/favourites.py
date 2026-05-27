from __future__ import annotations

from pydantic import Field

from .common import BaseDTO, PaginationDTO
from .papers import PaperShortDTO


class FavouriteActionResponseDTO(BaseDTO):
    user_id: int
    paper_id: int
    is_favourite: bool
    message: str | None = None


class FavouriteListResponseDTO(BaseDTO):
    user_id: int
    papers: list[PaperShortDTO] = Field(default_factory=list)
    total: int = 0
    pagination: PaginationDTO = Field(default_factory=PaginationDTO)


__all__ = ["FavouriteActionResponseDTO", "FavouriteListResponseDTO"]
