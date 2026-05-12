from __future__ import annotations

from datetime import date
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
    ExternalSearchResultDTO,
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
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout_seconds = timeout_seconds
        self._client = client or httpx.Client(timeout=timeout_seconds)

    def get_work_by_doi(self, doi: str) -> ExternalPaperDTO | None:
        payload = self._get_json(f"/works/doi:{quote(doi, safe='')}", allow_not_found=True)
        if payload is None:
            return None
        return self._normalize_work(payload)

    def get_work_by_external_id(
        self,
        external_id: str,
    ) -> ExternalPaperDTO | None:
        work_id = self._normalize_work_identifier(external_id)
        payload = self._get_json(f"/works/{quote(work_id, safe='')}", allow_not_found=True)
        if payload is None:
            return None
        return self._normalize_work(payload)

    def search_works(
        self,
        query: str,
        filters: OpenAlexSearchFiltersDTO,
        page: int = 1,
        per_page: int = 25,
    ) -> ExternalSearchResultDTO:
        params: dict[str, Any] = {"page": page, "per-page": per_page}
        search_query = query or filters.query
        if search_query:
            params["search"] = search_query
        filter_value = self._build_filter_param(filters)
        if filter_value:
            params["filter"] = filter_value

        payload = self._get_json("/works", params=params, allow_not_found=False)
        return self._normalize_search_result(payload)

    def list_recent_works(
        self,
        date_from: date,
        date_to: date,
        page: int = 1,
        per_page: int = 25,
    ) -> ExternalSearchResultDTO:
        filters = OpenAlexSearchFiltersDTO(date_from=date_from, date_to=date_to)
        return self.search_works("", filters, page=page, per_page=per_page)

    def _get_json(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        allow_not_found: bool,
    ) -> dict[str, Any] | list[Any] | None:
        response = self._client.get(
            f"{self._base_url}{path}",
            params=params,
            timeout=self._timeout_seconds,
        )
        if response.status_code == 404 and allow_not_found:
            return None
        if response.status_code == 429:
            raise ExternalServiceRateLimitError(
                "OpenAlex/OpenAlex rate limit exceeded",
                details={"status_code": response.status_code, "body": response.text},
            )
        if response.status_code >= 500:
            raise ExternalServiceUnavailableError(
                "OpenAlex/OpenAlex service is unavailable",
                details={"status_code": response.status_code, "body": response.text},
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

    def _normalize_search_result(
        self,
        payload: dict[str, Any] | list[Any] | None,
    ) -> ExternalSearchResultDTO:
        if isinstance(payload, list):
            results = payload
            total = len(results)
        elif isinstance(payload, dict):
            results = payload.get("results") or payload.get("items") or []
            meta = payload.get("meta") if isinstance(payload.get("meta"), dict) else {}
            total = (
                meta.get("count")
                or payload.get("total")
                or payload.get("total_count")
                or len(results)
            )
        else:
            raise ExternalResponseFormatError("OpenAlex/OpenAlex search response is empty")

        if not isinstance(results, list):
            raise ExternalResponseFormatError(
                "OpenAlex/OpenAlex search results are not a list"
            )

        return ExternalSearchResultDTO(
            source_name="openalex",
            total=int(total) if isinstance(total, int) else None,
            items=[
                self._normalize_work(item)
                for item in results
                if isinstance(item, dict)
            ],
            raw=payload if isinstance(payload, dict) else {"results": payload},
        )

    def _normalize_work(self, data: dict[str, Any]) -> ExternalPaperDTO:
        authors, institutions_from_authors = self._normalize_authors(data)
        institutions = self._dedupe_institutions(
            [
                *institutions_from_authors,
                *self._normalize_top_level_institutions(data.get("institutions")),
            ]
        )
        return ExternalPaperDTO(
            external_id=self._string_or_none(
                data.get("external_id") or data.get("openalex_id") or data.get("id")
            ),
            doi=self._string_or_none(data.get("doi")),
            title=str(
                data.get("title")
                or data.get("display_name")
                or data.get("name")
                or ""
            ),
            abstract=self._normalize_abstract(data),
            publication_year=self._int_or_none(data.get("publication_year")),
            publication_date=data.get("publication_date"),
            type=self._string_or_none(data.get("type")),
            language=self._string_or_none(data.get("language")),
            is_open_access=self._normalize_open_access(data),
            cited_by_count=self._int_or_none(data.get("cited_by_count")),
            authors=authors,
            institutions=institutions,
            topics=self._normalize_topics(data.get("topics")),
            keywords=self._normalize_keywords(data),
            landings=self._normalize_landings(data),
            raw=data,
        )

    def _normalize_abstract(self, data: dict[str, Any]) -> str | None:
        abstract = data.get("abstract")
        if isinstance(abstract, str):
            return abstract
        inverted = data.get("abstract_inverted_index")
        if isinstance(inverted, dict):
            return self._restore_inverted_abstract(inverted)
        return None

    def _restore_inverted_abstract(self, inverted: dict[str, Any]) -> str:
        positions: dict[int, str] = {}
        for token, token_positions in inverted.items():
            if not isinstance(token_positions, list):
                continue
            for position in token_positions:
                if isinstance(position, int):
                    positions[position] = str(token)
        return " ".join(positions[index] for index in sorted(positions))

    def _normalize_authors(
        self,
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
            author_data = item.get("author") if isinstance(item.get("author"), dict) else item
            author_institutions = self._normalize_top_level_institutions(
                item.get("institutions")
            )
            institutions.extend(author_institutions)
            authors.append(
                ExternalAuthorDTO(
                    external_id=self._string_or_none(
                        author_data.get("id") or author_data.get("external_id")
                    ),
                    display_name=str(
                        author_data.get("display_name")
                        or author_data.get("name")
                        or ""
                    ),
                    orcid=self._string_or_none(author_data.get("orcid")),
                    author_order=self._int_or_none(item.get("author_order")) or index + 1,
                    is_corresponding=self._bool_or_none(item.get("is_corresponding")),
                    institutions=author_institutions,
                    raw=item,
                )
            )
        return authors, institutions

    def _normalize_top_level_institutions(
        self,
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
                    external_id=self._string_or_none(
                        item.get("id") or item.get("external_id")
                    ),
                    display_name=str(
                        item.get("display_name")
                        or item.get("name")
                        or ""
                    ),
                    ror=self._string_or_none(item.get("ror")),
                    country_code=self._string_or_none(item.get("country_code")),
                    type=self._string_or_none(item.get("type")),
                    raw=item,
                )
            )
        return institutions

    def _dedupe_institutions(
        self,
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

    def _normalize_topics(self, raw_topics: Any) -> list[ExternalTopicDTO]:
        if not isinstance(raw_topics, list):
            return []
        topics: list[ExternalTopicDTO] = []
        for item in raw_topics:
            if not isinstance(item, dict):
                continue
            domain = item.get("domain") if isinstance(item.get("domain"), dict) else {}
            field = item.get("field") if isinstance(item.get("field"), dict) else {}
            subfield = item.get("subfield") if isinstance(item.get("subfield"), dict) else {}
            topics.append(
                ExternalTopicDTO(
                    external_id=self._string_or_none(item.get("id")),
                    name=str(item.get("display_name") or item.get("name") or ""),
                    score=self._decimal_or_none(item.get("score")),
                    domain_name=self._string_or_none(
                        domain.get("display_name") or domain.get("name")
                    ),
                    field_name=self._string_or_none(
                        field.get("display_name") or field.get("name")
                    ),
                    subfield_name=self._string_or_none(
                        subfield.get("display_name") or subfield.get("name")
                    ),
                    raw=item,
                )
            )
        return topics

    def _normalize_keywords(self, data: dict[str, Any]) -> list[ExternalKeywordDTO]:
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
                        score=self._decimal_or_none(item.get("score")),
                        raw=item,
                    )
                )
        return keywords

    def _normalize_landings(self, data: dict[str, Any]) -> list[ExternalLandingDTO]:
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
            candidates.extend((item, None) for item in locations if isinstance(item, dict))

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
            explicit_is_best = self._bool_or_none(item.get("is_best"))
            landings.append(
                ExternalLandingDTO(
                    landing_url=landing_url,
                    pdf_url=self._string_or_none(pdf_url),
                    license=self._string_or_none(item.get("license")),
                    version=self._string_or_none(item.get("version")),
                    is_best=explicit_is_best
                    if explicit_is_best is not None
                    else default_is_best,
                    raw=item,
                )
            )
        return landings

    def _normalize_open_access(self, data: dict[str, Any]) -> bool | None:
        value = data.get("is_open_access")
        if isinstance(value, bool):
            return value
        open_access = data.get("open_access")
        if isinstance(open_access, dict):
            return self._bool_or_none(open_access.get("is_oa"))
        return None

    def _build_filter_param(self, filters: OpenAlexSearchFiltersDTO) -> str | None:
        values: list[str] = []
        if filters.date_from is not None:
            values.append(f"from_publication_date:{filters.date_from.isoformat()}")
        elif filters.publication_year_from is not None:
            values.append(f"from_publication_date:{filters.publication_year_from}-01-01")
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

    def _normalize_work_identifier(self, external_id: str) -> str:
        value = external_id.rstrip("/")
        if "/" in value:
            return value.rsplit("/", 1)[-1]
        return value

    def _string_or_none(self, value: Any) -> str | None:
        if value is None:
            return None
        return str(value)

    def _int_or_none(self, value: Any) -> int | None:
        if isinstance(value, bool) or value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    def _bool_or_none(self, value: Any) -> bool | None:
        return value if isinstance(value, bool) else None

    def _decimal_or_none(self, value: Any) -> Decimal | None:
        if value is None or isinstance(value, bool):
            return None
        try:
            return Decimal(str(value))
        except Exception:
            return None


__all__ = ["OpenAlexAdapter"]
