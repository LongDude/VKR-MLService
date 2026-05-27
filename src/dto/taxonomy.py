from __future__ import annotations

from decimal import Decimal

from .common import BaseDTO


class DomainCreateDTO(BaseDTO):
    openalex_id: str | None = None
    name: str


class DomainDTO(DomainCreateDTO):
    id: int | None = None


class FieldCreateDTO(BaseDTO):
    domain_id: int | None = None
    openalex_id: str | None = None
    name: str


class FieldDTO(FieldCreateDTO):
    id: int | None = None


class SubfieldCreateDTO(BaseDTO):
    field_id: int | None = None
    openalex_id: str | None = None
    name: str


class SubfieldDTO(SubfieldCreateDTO):
    id: int | None = None


class TopicCreateDTO(BaseDTO):
    subfield_id: int | None = None
    openalex_id: str | None = None
    name: str


class TopicDTO(TopicCreateDTO):
    id: int | None = None


class KeywordCreateDTO(BaseDTO):
    value: str


class KeywordDTO(KeywordCreateDTO):
    id: int | None = None


class TopicWithHierarchyDTO(BaseDTO):
    topic: TopicDTO
    subfield: SubfieldDTO | None = None
    field: FieldDTO | None = None
    domain: DomainDTO | None = None


class PaperTopicDTO(BaseDTO):
    topic_id: int
    score: Decimal | None = None
    topic: TopicDTO | None = None


class PaperKeywordDTO(BaseDTO):
    keyword_id: int
    score: Decimal | None = None
    keyword: KeywordDTO | None = None


__all__ = [
    "DomainCreateDTO",
    "DomainDTO",
    "FieldCreateDTO",
    "FieldDTO",
    "KeywordCreateDTO",
    "KeywordDTO",
    "PaperKeywordDTO",
    "PaperTopicDTO",
    "SubfieldCreateDTO",
    "SubfieldDTO",
    "TopicCreateDTO",
    "TopicDTO",
    "TopicWithHierarchyDTO",
]
