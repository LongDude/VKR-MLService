from __future__ import annotations

from .common import BaseDTO


class AuthorCreateDTO(BaseDTO):
    display_name: str
    orcid: str | None = None


class AuthorDTO(AuthorCreateDTO):
    id: int | None = None


class PaperAuthorDTO(AuthorDTO):
    author_order: int | None = None
    is_corresponding: bool | None = None


__all__ = ["AuthorCreateDTO", "AuthorDTO", "PaperAuthorDTO"]
