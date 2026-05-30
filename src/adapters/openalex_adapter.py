from __future__ import annotations

from decimal import Decimal
from typing import Any
from urllib.parse import quote

import httpx

from core.exceptions import (
    ExternalResponseFormatError,
    ExternalServiceRateLimitError,
    ExternalServiceUnavailableError,
)
from dto.external import (
    ExternalAuthorDTO,
    ExternalInstitutionDTO,
    ExternalKeywordDTO,
    ExternalLandingDTO,
    ExternalPaperDTO,
    ExternalTopicDTO,
    OpenAlexSearchFiltersDTO,
)


class OpenAlexAdapter:
    def __init__(
        self,
        base_url: str = "https://api.openalex.org",
        *,
        timeout_seconds: float = 30.0,
        client: httpx.Client | None = None,
        api_key: str | None = None,
        mailto: str | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout_seconds = timeout_seconds
        self._api_key = api_key.strip() if api_key and api_key.strip() else None
        self._mailto = mailto.strip() if mailto and mailto.strip() else None
        self._client = client or httpx.Client(timeout=timeout_seconds)

    def get_work_by_doi(self, doi: str) -> ExternalPaperDTO | None:
        payload = self._get_json(
            f"/works/doi:{quote(doi, safe='')}", allow_not_found=True
        )
        if payload is None:
            return None
        return self.normalize_work(payload)

    def get_work_by_external_id(
        self,
        external_id: str,
    ) -> ExternalPaperDTO | None:
        work_id = self._normalize_work_identifier(external_id)
        payload = self._get_json(
            f"/works/{quote(work_id, safe='')}", allow_not_found=True
        )
        if payload is None:
            return None
        return self.normalize_work(payload)

    def count_works(
        self,
        filters: OpenAlexSearchFiltersDTO,
        *,
        topic_external_id: str | None = None,
        primary_topic_only: bool = False,
    ) -> int:
        """Return OpenAlex ``meta.count`` for works matching the supplied filters.

        OpenAlex exposes total result counts in the list response metadata, so callers
        can collect publication statistics without paginating through all works.
        """
        params: dict[str, Any] = {"page": 1, "per-page": 1, "select": "id"}
        filter_value = self._build_filter_param(filters)
        filter_parts = [filter_value] if filter_value else []
        if topic_external_id:
            topic_id = self._normalize_work_identifier(topic_external_id)
            topic_filter = "primary_topic.id" if primary_topic_only else "topics.id"
            filter_parts.append(f"{topic_filter}:{topic_id}")
        if filter_parts:
            params["filter"] = ",".join(filter_parts)

        payload = self._get_json("/works", params=params, allow_not_found=False)
        if not isinstance(payload, dict):
            raise ExternalResponseFormatError(
                "OpenAlex/OpenAlex count response is not an object"
            )
        meta = payload.get("meta")
        if not isinstance(meta, dict):
            raise ExternalResponseFormatError(
                "OpenAlex/OpenAlex count response has no meta"
            )
        try:
            return int(meta.get("count") or 0)
        except (TypeError, ValueError) as exc:
            raise ExternalResponseFormatError(
                "OpenAlex/OpenAlex count response has invalid meta.count",
                details={"count": meta.get("count")},
            ) from exc

    def group_works(
        self,
        filters: OpenAlexSearchFiltersDTO,
        *,
        group_by: str,
        extra_filter_parts: list[str] | None = None,
        cursor: str = "*",
        per_page: int = 200,
    ) -> tuple[list[dict[str, Any]], str | None]:
        """Return one OpenAlex ``group_by`` page and the next cursor."""
        params: dict[str, Any] = {
            "group_by": group_by,
            "cursor": cursor,
            "per-page": max(1, min(200, per_page)),
        }
        filter_value = self._build_filter_param(filters)
        filter_parts = [filter_value] if filter_value else []
        filter_parts.extend(extra_filter_parts or [])
        if filter_parts:
            params["filter"] = ",".join(filter_parts)

        payload = self._get_json("/works", params=params, allow_not_found=False)
        if not isinstance(payload, dict):
            raise ExternalResponseFormatError(
                "OpenAlex/OpenAlex group response is not an object"
            )
        groups = payload.get("group_by")
        if not isinstance(groups, list):
            raise ExternalResponseFormatError(
                "OpenAlex/OpenAlex group_by payload is not a list"
            )
        meta = payload.get("meta") if isinstance(payload.get("meta"), dict) else {}
        next_cursor = meta.get("next_cursor") if isinstance(meta, dict) else None
        return [item for item in groups if isinstance(item, dict)], (
            str(next_cursor) if next_cursor else None
        )

    def _get_json(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        allow_not_found: bool,
    ) -> dict[str, Any] | None:
        response = self._client.get(
            f"{self._base_url}{path}",
            params=self._with_auth_params(params),
            timeout=self._timeout_seconds,
        )
        if response.status_code == 404 and allow_not_found:
            return None
        if response.status_code == 429:
            raise ExternalServiceRateLimitError(
                "OpenAlex/OpenAlex rate limit exceeded",
                details={
                    "status_code": response.status_code,
                    "body": response.text,
                    "retry_after": response.headers.get("Retry-After"),
                    "rate_limit_remaining": response.headers.get(
                        "X-RateLimit-Remaining"
                    ),
                    "rate_limit_reset": response.headers.get("X-RateLimit-Reset"),
                    "rate_limit_credits_used": response.headers.get(
                        "X-RateLimit-Credits-Used"
                    ),
                },
            )
        if response.status_code >= 500:
            raise ExternalServiceUnavailableError(
                "OpenAlex/OpenAlex service is unavailable",
                details={
                    "status_code": response.status_code,
                    "body": response.text,
                    "retry_after": response.headers.get("Retry-After"),
                },
            )
        if response.status_code >= 400:
            raise ExternalResponseFormatError(
                "OpenAlex/OpenAlex returned an unexpected client error",
                details={"status_code": response.status_code, "body": response.text},
            )
        try:
            return response.json()
        except ValueError as exc:
            raise ExternalResponseFormatError(
                "OpenAlex/OpenAlex response is not valid JSON",
                details={"reason": str(exc)},
            ) from exc

    def _with_auth_params(
        self,
        params: dict[str, Any] | None,
    ) -> dict[str, Any] | None:
        if not self._api_key and not self._mailto:
            return params
        result = dict(params or {})
        if self._api_key:
            result.setdefault("api_key", self._api_key)
        if self._mailto:
            result.setdefault("mailto", self._mailto)
        return result

    @classmethod
    def normalize_work(cls, data: dict[str, Any]) -> ExternalPaperDTO:

        authors, institutions_from_authors = cls._normalize_authors(data)
        institutions = cls._dedupe_institutions(
            [
                *institutions_from_authors,
                *cls._normalize_top_level_institutions(data.get("institutions")),
            ]
        )
        return ExternalPaperDTO(
            external_id=cls._string_or_none(
                data.get("external_id") or data.get("openalex_id") or data.get("id")
            ),
            doi=cls._string_or_none(data.get("doi")),
            title=str(
                data.get("title") or data.get("display_name") or data.get("name") or ""
            ),
            abstract=cls._normalize_abstract(data),
            publication_year=cls._int_or_none(data.get("publication_year")),
            publication_date=data.get("publication_date"),
            type=cls._string_or_none(data.get("type")),
            language=cls._string_or_none(data.get("language")),
            is_open_access=cls._normalize_open_access(data),
            cited_by_count=cls._int_or_none(data.get("cited_by_count")),
            references_count=cls._int_or_none(
                cls._first_present(
                    data,
                    "referenced_works_count",
                    "references_count",
                    "referencesCount",
                )
            ),
            authors=authors,
            institutions=institutions,
            topics=cls._normalize_topics(data.get("topics")),
            keywords=cls._normalize_keywords(data),
            landings=cls._normalize_landings(data),
            raw=data,
        )

    @staticmethod
    def _normalize_abstract(data: dict[str, Any]) -> str | None:
        abstract = data.get("abstract")
        if isinstance(abstract, str):
            return abstract
        inverted = data.get("abstract_inverted_index")
        if not isinstance(inverted, dict):
            return None

        positions: dict[int, str] = {}
        for token, token_positions in inverted.items():
            if not isinstance(token_positions, list):
                continue
            for position in token_positions:
                if isinstance(position, int):
                    positions[position] = str(token)
        return " ".join(positions[index] for index in sorted(positions))

    @classmethod
    def _normalize_authors(
        cls,
        data: dict[str, Any],
    ) -> tuple[list[ExternalAuthorDTO], list[ExternalInstitutionDTO]]:
        raw_authors = data.get("authorships") or data.get("authors") or []
        authors: list[ExternalAuthorDTO] = []
        institutions: list[ExternalInstitutionDTO] = []
        if not isinstance(raw_authors, list):
            return authors, institutions

        for index, item in enumerate(raw_authors):
            if not isinstance(item, dict):
                continue
            author_data = cls._dict_or_default(item.get("author"), item)
            author_institutions = cls._normalize_top_level_institutions(
                item.get("institutions")
            )
            institutions.extend(author_institutions)
            authors.append(
                ExternalAuthorDTO(
                    external_id=cls._string_or_none(
                        author_data.get("id") or author_data.get("external_id")
                    ),
                    display_name=str(
                        author_data.get("display_name") or author_data.get("name") or ""
                    ),
                    orcid=cls._string_or_none(author_data.get("orcid")),
                    author_order=cls._int_or_none(item.get("author_order"))
                    or index + 1,
                    is_corresponding=cls._bool_or_none(item.get("is_corresponding")),
                    institutions=author_institutions,
                    raw=item,
                )
            )
        return authors, institutions

    @classmethod
    def _normalize_top_level_institutions(
        cls,
        raw_institutions: Any,
    ) -> list[ExternalInstitutionDTO]:
        if not isinstance(raw_institutions, list):
            return []
        institutions: list[ExternalInstitutionDTO] = []
        for item in raw_institutions:
            if not isinstance(item, dict):
                continue
            institutions.append(
                ExternalInstitutionDTO(
                    external_id=cls._string_or_none(
                        item.get("id") or item.get("external_id")
                    ),
                    display_name=str(
                        item.get("display_name") or item.get("name") or ""
                    ),
                    ror=cls._string_or_none(item.get("ror")),
                    country_code=cls._string_or_none(item.get("country_code")),
                    type=cls._string_or_none(item.get("type")),
                    raw=item,
                )
            )
        return institutions

    @staticmethod
    def _dedupe_institutions(
        institutions: list[ExternalInstitutionDTO],
    ) -> list[ExternalInstitutionDTO]:
        seen: set[str] = set()
        result: list[ExternalInstitutionDTO] = []
        for institution in institutions:
            key = institution.external_id or institution.ror or institution.display_name
            if key in seen:
                continue
            seen.add(key)
            result.append(institution)
        return result

    @classmethod
    def _normalize_topics(cls, raw_topics: Any) -> list[ExternalTopicDTO]:
        if not isinstance(raw_topics, list):
            return []
        topics: list[ExternalTopicDTO] = []
        for item in raw_topics:
            if not isinstance(item, dict):
                continue

            domain = cls._dict_or_default(item.get("domain"))
            field = cls._dict_or_default(item.get("field"))
            subfield = cls._dict_or_default(item.get("subfield"))

            topics.append(
                ExternalTopicDTO(
                    external_id=cls._string_or_none(item.get("id")),
                    name=str(item.get("display_name") or item.get("name") or ""),
                    score=cls._decimal_or_none(item.get("score")),
                    domain_name=cls._string_or_none(
                        domain.get("display_name") or domain.get("name")
                    ),
                    field_name=cls._string_or_none(
                        field.get("display_name") or field.get("name")
                    ),
                    subfield_name=cls._string_or_none(
                        subfield.get("display_name") or subfield.get("name")
                    ),
                    raw=item,
                )
            )
        return topics

    @classmethod
    def _normalize_keywords(cls, data: dict[str, Any]) -> list[ExternalKeywordDTO]:
        raw_keywords = data.get("keywords")
        if raw_keywords is None:
            raw_keywords = data.get("concepts")
        if not isinstance(raw_keywords, list):
            return []

        keywords: list[ExternalKeywordDTO] = []
        for item in raw_keywords:
            if isinstance(item, str):
                keywords.append(ExternalKeywordDTO(value=item))
            elif isinstance(item, dict):
                keywords.append(
                    ExternalKeywordDTO(
                        value=str(
                            item.get("keyword")
                            or item.get("display_name")
                            or item.get("name")
                            or item.get("value")
                            or ""
                        ),
                        score=cls._decimal_or_none(item.get("score")),
                        raw=item,
                    )
                )
        return keywords

    @classmethod
    def _normalize_landings(cls, data: dict[str, Any]) -> list[ExternalLandingDTO]:
        candidates: list[tuple[dict[str, Any], bool | None]] = []
        for key, is_best in (
            ("primary_location", True),
            ("best_oa_location", True),
        ):
            value = data.get(key)
            if isinstance(value, dict):
                candidates.append((value, is_best))
        locations = data.get("locations")
        if isinstance(locations, list):
            candidates.extend(
                (item, None) for item in locations if isinstance(item, dict)
            )

        landings: list[ExternalLandingDTO] = []
        seen: set[str] = set()
        for item, default_is_best in candidates:
            landing_url = item.get("landing_page_url") or item.get("landing_url")
            pdf_url = item.get("pdf_url")
            if landing_url is None and pdf_url is not None:
                landing_url = pdf_url
            if not landing_url:
                continue
            landing_url = str(landing_url)
            if landing_url in seen:
                continue
            seen.add(landing_url)
            explicit_is_best = cls._bool_or_none(item.get("is_best"))
            landings.append(
                ExternalLandingDTO(
                    landing_url=landing_url,
                    pdf_url=cls._string_or_none(pdf_url),
                    license=cls._string_or_none(item.get("license")),
                    version=cls._string_or_none(item.get("version")),
                    is_best=explicit_is_best
                    if explicit_is_best is not None
                    else default_is_best,
                    raw=item,
                )
            )
        return landings

    @classmethod
    def _normalize_open_access(cls, data: dict[str, Any]) -> bool | None:
        value = data.get("is_open_access")
        if isinstance(value, bool):
            return value
        open_access = data.get("open_access")
        if isinstance(open_access, dict):
            return cls._bool_or_none(open_access.get("is_oa"))
        return None

    @staticmethod
    def _build_filter_param(filters: OpenAlexSearchFiltersDTO) -> str | None:
        values: list[str] = []
        if filters.date_from is not None:
            values.append(f"from_publication_date:{filters.date_from.isoformat()}")
        elif filters.publication_year_from is not None:
            values.append(
                f"from_publication_date:{filters.publication_year_from}-01-01"
            )
        if filters.date_to is not None:
            values.append(f"to_publication_date:{filters.date_to.isoformat()}")
        elif filters.publication_year_to is not None:
            values.append(f"to_publication_date:{filters.publication_year_to}-12-31")
        if filters.type:
            values.append(f"type:{filters.type}")
        if filters.language:
            values.append(f"language:{filters.language}")
        if filters.is_open_access is not None:
            values.append(f"is_oa:{str(filters.is_open_access).lower()}")
        return ",".join(values) if values else None

    @staticmethod
    def _normalize_work_identifier(external_id: str) -> str:
        value = external_id.rstrip("/")
        if "/" in value:
            return value.rsplit("/", 1)[-1]
        return value

    @staticmethod
    def _dict_or_default(value: Any, default: Any = {}) -> dict[str, Any]:
        return value if isinstance(value, dict) else default

    @staticmethod
    def _string_or_none(value: Any) -> str | None:
        if value is None:
            return None
        return str(value)

    @staticmethod
    def _first_present(data: dict[str, Any], *keys: str) -> Any:
        for key in keys:
            if key in data:
                return data.get(key)
        return None

    @staticmethod
    def _int_or_none(value: Any) -> int | None:
        if isinstance(value, bool) or value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _bool_or_none(value: Any) -> bool | None:
        return value if isinstance(value, bool) else None

    @staticmethod
    def _decimal_or_none(value: Any) -> Decimal | None:
        if value is None or isinstance(value, bool):
            return None
        try:
            return Decimal(str(value))
        except Exception:
            return None


__all__ = ["OpenAlexAdapter"]
