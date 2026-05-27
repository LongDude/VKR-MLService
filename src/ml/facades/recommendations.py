from __future__ import annotations

import json
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any

from adapters.qdrant_adapter import QdrantAdapter
from adapters.redis_adapter import RedisAdapter
from core.exceptions import InsufficientUserProfileDataError
from dto.papers import PaperShortDTO
from dto.qdrant import QdrantSearchHitDTO
from dto.recommendations import (
    RecommendationItemDTO,
    RecommendationRequestDTO,
    RecommendationResponseDTO,
    RecommendationScoreDetailsDTO,
)
from repositories.favourites import FavouriteRepository
from repositories.papers import PaperRepository
from repositories.taxonomy import TaxonomyRepository
from utils.hashing import calculate_text_hash

from ml.constants import PAPERS_COLLECTION, TREND_CLUSTERS_COLLECTION
from ml.facades.summaries import SummaryFacade
from ml.facades.user_profile import UserProfileFacade
from ml.services.scoring import ScoringService


RECOMMENDATION_CACHE_TTL_SECONDS = 300
FALLBACK_TRENDING_WINDOW_DAYS = 365 * 3


class RecommendationFacade:
    def __init__(
        self,
        *,
        user_profile_facade: UserProfileFacade | None,
        qdrant_adapter: QdrantAdapter | None,
        paper_repository: PaperRepository,
        favourite_repository: FavouriteRepository,
        taxonomy_repository: TaxonomyRepository | None = None,
        scoring_service: ScoringService | None = None,
        summary_facade: SummaryFacade | None = None,
        redis_adapter: RedisAdapter | None = None,
        papers_collection: str = PAPERS_COLLECTION,
        trend_clusters_collection: str = TREND_CLUSTERS_COLLECTION,
        cache_ttl_seconds: int = RECOMMENDATION_CACHE_TTL_SECONDS,
    ) -> None:
        self.user_profile_facade = user_profile_facade
        self.qdrant_adapter = qdrant_adapter
        self.paper_repository = paper_repository
        self.favourite_repository = favourite_repository
        self.taxonomy_repository = taxonomy_repository
        self.scoring_service = scoring_service or ScoringService()
        self.summary_facade = summary_facade
        self.redis_adapter = redis_adapter
        self.papers_collection = papers_collection
        self.trend_clusters_collection = trend_clusters_collection
        self.cache_ttl_seconds = cache_ttl_seconds

    def recommend_for_user(
        self,
        request: RecommendationRequestDTO,
    ) -> RecommendationResponseDTO:
        if request.user_id is None:
            raise InsufficientUserProfileDataError(
                "user_id is required for profile-based recommendations"
            )

        cache_key = self._cache_key(request)
        cached = self._get_cached_response(cache_key)
        if cached is not None:
            return cached

        tag_topic_sets = self._topic_sets_by_selected_tags(request)
        try:
            if self.user_profile_facade is None or self.qdrant_adapter is None:
                raise InsufficientUserProfileDataError("Qdrant profile search is unavailable")
            profile_vector = self.user_profile_facade.get_user_profile_vector(
                request.user_id,
                recompute_if_missing=True,
            )
        except InsufficientUserProfileDataError as exc:
            response = self._fallback_trending_response(
                request,
                tag_topic_sets=tag_topic_sets,
                errors=[str(exc)],
            )
            self._set_cached_response(cache_key, response)
            return response

        favourite_ids = self._favourite_ids(request)
        try:
            qdrant_hits = self.qdrant_adapter.search(
                self.papers_collection,
                profile_vector,
                top_k=self._candidate_limit(request),
                filters=self._build_qdrant_filter(request, tag_topic_sets.get("all", [])),
            )
        except Exception as exc:
            response = self._fallback_trending_response(
                request,
                tag_topic_sets=tag_topic_sets,
                errors=[str(exc)],
            )
            self._set_cached_response(cache_key, response)
            return response
        candidate_ids = [
            paper_id
            for paper_id in (self._paper_id_from_hit(hit) for hit in qdrant_hits)
            if paper_id is not None
        ]
        paper_by_id = self._paper_by_id(candidate_ids)
        cluster_scores = self._cluster_trend_scores(qdrant_hits)

        items: list[RecommendationItemDTO] = []
        for hit in qdrant_hits:
            paper_id = self._paper_id_from_hit(hit)
            if paper_id is None:
                continue
            if request.exclude_favourites and paper_id in favourite_ids:
                continue

            payload = hit.payload
            paper = self._paper_short(paper_id, payload, paper_by_id.get(paper_id))
            trend_score = self._trend_score(payload, cluster_scores)
            recency_score = self.scoring_service.calculate_recency_score(
                self._payload_date(payload.get("publication_date"))
                or getattr(paper_by_id.get(paper_id), "publication_date", None)
            )
            citation_score = self.scoring_service.calculate_citation_score(
                self._payload_int(payload.get("cited_by_count"))
                if payload.get("cited_by_count") is not None
                else getattr(paper_by_id.get(paper_id), "cited_by_count", None)
            )
            tag_match_score = self._tag_match_score(payload, tag_topic_sets)
            score = self.scoring_service.calculate_recommendation_score(
                semantic_score=hit.score,
                trend_score=trend_score,
                recency_score=recency_score,
                citation_score=citation_score,
                tag_match_score=tag_match_score,
            )
            explanation = self._explanation(request, paper, payload)
            items.append(
                RecommendationItemDTO(
                    paper=paper,
                    score=score,
                    reason=explanation,
                    score_details=RecommendationScoreDetailsDTO(
                        semantic_score=hit.score,
                        profile_score=hit.score,
                        tag_match_score=tag_match_score,
                        trend_score=trend_score,
                        recency_score=recency_score,
                        citation_score=citation_score,
                        meta={"payload": payload},
                    ),
                )
            )

        items.sort(key=lambda item: item.score, reverse=True)
        response = RecommendationResponseDTO(
            items=items[: request.limit],
            total=len(items),
            strategy=request.strategy,
        )
        self._set_cached_response(cache_key, response)
        return response

    def _fallback_trending_response(
        self,
        request: RecommendationRequestDTO,
        *,
        tag_topic_sets: dict[str, list[int]] | None = None,
        errors: list[str] | None = None,
    ) -> RecommendationResponseDTO:
        date_from = request.date_from or date.today() - timedelta(days=FALLBACK_TRENDING_WINDOW_DAYS)
        papers = self.paper_repository.list_recent_indexed(
            date_from,
            limit=self._candidate_limit(request),
            domain_ids=request.domain_ids,
            field_ids=request.field_ids,
            subfield_ids=request.subfield_ids,
            topic_ids=request.topic_ids,
        )
        favourite_ids = self._favourite_ids(request)
        tag_topic_sets = tag_topic_sets or self._topic_sets_by_selected_tags(request)
        items: list[RecommendationItemDTO] = []
        for paper in papers:
            paper_id = int(getattr(paper, "id"))
            if request.exclude_favourites and paper_id in favourite_ids:
                continue
            if not self._paper_matches_request(paper, request):
                continue
            recency_score = self.scoring_service.calculate_recency_score(
                getattr(paper, "publication_date", None)
            )
            citation_score = self.scoring_service.calculate_citation_score(
                getattr(paper, "cited_by_count", None)
            )
            tag_match_score = self._tag_match_score(
                {"topic_ids": self._paper_topic_ids_from_model(paper)},
                tag_topic_sets,
            )
            score = self.scoring_service.calculate_recommendation_score(
                semantic_score=0.0,
                recency_score=recency_score,
                citation_score=citation_score,
                tag_match_score=tag_match_score,
            )
            paper_short = self._paper_short_from_model(paper)
            items.append(
                RecommendationItemDTO(
                    paper=paper_short,
                    score=score,
                    reason=self._explanation(request, paper_short, {}),
                    score_details=RecommendationScoreDetailsDTO(
                        semantic_score=0.0,
                        profile_score=0.0,
                        tag_match_score=tag_match_score,
                        recency_score=recency_score,
                        citation_score=citation_score,
                        meta={"fallback": "trending_recent"},
                    ),
                )
            )

        items.sort(key=lambda item: item.score, reverse=True)
        return RecommendationResponseDTO(
            items=items[: request.limit],
            total=len(items),
            strategy="trending_fallback",
            errors=errors or [],
        )

    def _build_qdrant_filter(
        self,
        request: RecommendationRequestDTO,
        topic_filter_ids: list[int] | None = None,
    ) -> dict[str, Any] | None:
        must: list[dict[str, Any]] = []
        if request.date_from is not None:
            must.append(
                {
                    "key": "publication_date",
                    "range": {"gte": request.date_from.isoformat()},
                }
            )
        if request.language is not None:
            must.append({"key": "language", "match": {"value": request.language}})
        if request.is_open_access is not None:
            must.append(
                {
                    "key": "is_open_access",
                    "match": {"value": request.is_open_access},
                }
            )
        self._append_any_filter(must, "topic_ids", topic_filter_ids or request.topic_ids)
        self._append_any_filter(must, "keyword_ids", request.keyword_ids)
        return {"must": must} if must else None

    def _append_any_filter(
        self,
        must: list[dict[str, Any]],
        field_name: str,
        values: list[int],
    ) -> None:
        if values:
            must.append({"key": field_name, "match": {"any": values}})

    def _topic_sets_by_selected_tags(
        self,
        request: RecommendationRequestDTO,
    ) -> dict[str, list[int]]:
        empty = {"domains": [], "fields": [], "subfields": [], "topics": [], "all": []}
        if not self._has_selected_taxonomy(request):
            return empty
        if self.taxonomy_repository is None:
            topic_ids = sorted(set(int(topic_id) for topic_id in request.topic_ids))
            return {
                "domains": [],
                "fields": [],
                "subfields": [],
                "topics": topic_ids,
                "all": topic_ids,
            }
        return self.taxonomy_repository.list_topic_ids_for_filters(
            domain_ids=request.domain_ids,
            field_ids=request.field_ids,
            subfield_ids=request.subfield_ids,
            topic_ids=request.topic_ids,
        )

    def _has_selected_taxonomy(self, request: RecommendationRequestDTO) -> bool:
        return bool(
            request.domain_ids
            or request.field_ids
            or request.subfield_ids
            or request.topic_ids
        )

    def _tag_match_score(
        self,
        payload: dict[str, Any],
        tag_topic_sets: dict[str, list[int]],
    ) -> float | None:
        if not tag_topic_sets.get("all"):
            return None
        paper_topic_ids = self._payload_int_set(payload.get("topic_ids", []))
        if not paper_topic_ids:
            return 0.0
        weighted_sets = [
            ("topics", 1.0),
            ("subfields", 0.70),
            ("fields", 0.45),
            ("domains", 0.25),
        ]
        score = 0.0
        for key, weight in weighted_sets:
            if paper_topic_ids.intersection(tag_topic_sets.get(key, [])):
                score = max(score, weight)
        return score

    def _paper_topic_ids_from_model(self, paper: Any) -> list[int]:
        topic_ids: set[int] = set()
        primary_topic_id = getattr(paper, "primary_topic_id", None)
        if primary_topic_id is not None:
            try:
                topic_ids.add(int(primary_topic_id))
            except (TypeError, ValueError):
                pass
        for link in getattr(paper, "topic_links", []) or []:
            topic_id = getattr(link, "topic_id", None)
            try:
                topic_ids.add(int(topic_id))
            except (TypeError, ValueError):
                continue
        return sorted(topic_ids)

    def _cluster_trend_scores(
        self,
        hits: list[QdrantSearchHitDTO],
    ) -> dict[int, float]:
        if self.qdrant_adapter is None:
            return {}
        topic_ids: set[int] = set()
        for hit in hits:
            for topic_id in hit.payload.get("topic_ids", []):
                try:
                    topic_ids.add(int(topic_id))
                except (TypeError, ValueError):
                    continue
        if not topic_ids:
            return {}

        point_ids = [f"topic:{topic_id}" for topic_id in sorted(topic_ids)]
        points = self.qdrant_adapter.retrieve(
            self.trend_clusters_collection,
            point_ids,
            with_vectors=False,
        )
        scores: dict[int, float] = {}
        for point in points:
            try:
                topic_id = int(str(point.id).removeprefix("topic:"))
            except ValueError:
                continue
            scores[topic_id] = self._payload_float(point.payload.get("trend_score"))
        return scores

    def _trend_score(
        self,
        payload: dict[str, Any],
        cluster_scores: dict[int, float],
    ) -> float:
        if payload.get("trend_score") is not None:
            return self._payload_float(payload.get("trend_score"))
        scores: list[float] = []
        for topic_id in payload.get("topic_ids", []):
            try:
                score = cluster_scores.get(int(topic_id))
            except (TypeError, ValueError):
                score = None
            if score is not None:
                scores.append(score)
        return max(scores) if scores else 0.0

    def _paper_short(
        self,
        paper_id: int,
        payload: dict[str, Any],
        paper: Any | None,
    ) -> PaperShortDTO:
        if paper is not None:
            return self._paper_short_from_model(paper, payload)
        return PaperShortDTO(
            id=paper_id,
            title=str(payload.get("title") or f"Paper {paper_id}"),
            doi=payload.get("doi"),
            publication_year=self._payload_int(payload.get("publication_year")),
            publication_date=self._payload_date(payload.get("publication_date")),
            language=payload.get("language"),
            is_open_access=payload.get("is_open_access"),
            cited_by_count=self._payload_int(payload.get("cited_by_count")) or 0,
            openalex_id=payload.get("openalex_id"),
            references_count=self._payload_int(payload.get("references_count")) or 0,
            primary_topic_id=self._payload_int(payload.get("primary_topic_id")),
            extracted_keywords=payload.get("extracted_keywords"),
        )

    def _paper_short_from_model(
        self,
        paper: Any,
        payload: dict[str, Any] | None = None,
    ) -> PaperShortDTO:
        payload = payload or {}
        return PaperShortDTO(
            id=int(getattr(paper, "id")),
            title=str(getattr(paper, "title", None) or payload.get("title") or "Untitled paper"),
            doi=getattr(paper, "doi", None) or payload.get("doi"),
            publication_year=getattr(paper, "publication_year", None)
            or self._payload_int(payload.get("publication_year")),
            publication_date=getattr(paper, "publication_date", None)
            or self._payload_date(payload.get("publication_date")),
            language=getattr(paper, "language", None) or payload.get("language"),
            is_open_access=getattr(paper, "is_open_access", None)
            if getattr(paper, "is_open_access", None) is not None
            else payload.get("is_open_access"),
            cited_by_count=getattr(paper, "cited_by_count", None)
            if getattr(paper, "cited_by_count", None) is not None
            else self._payload_int(payload.get("cited_by_count")) or 0,
            openalex_id=getattr(paper, "openalex_id", None) or payload.get("openalex_id"),
            references_count=getattr(paper, "references_count", None)
            if getattr(paper, "references_count", None) is not None
            else self._payload_int(payload.get("references_count")) or 0,
            primary_topic_id=getattr(paper, "primary_topic_id", None)
            or self._payload_int(payload.get("primary_topic_id")),
            extracted_keywords=getattr(paper, "extracted_keywords", None)
            or payload.get("extracted_keywords"),
        )

    def _paper_matches_request(
        self,
        paper: Any,
        request: RecommendationRequestDTO,
    ) -> bool:
        if request.language is not None and getattr(paper, "language", None) != request.language:
            return False
        if (
            request.is_open_access is not None
            and getattr(paper, "is_open_access", None) != request.is_open_access
        ):
            return False
        if request.date_from is not None:
            publication_date = getattr(paper, "publication_date", None)
            if publication_date is None or publication_date < request.date_from:
                return False
        return True

    def _explanation(
        self,
        request: RecommendationRequestDTO,
        paper: PaperShortDTO,
        payload: dict[str, Any],
    ) -> str | None:
        if not request.include_explanations or self.summary_facade is None:
            return None
        try:
            return self.summary_facade.explain_recommendation(
                user_interests=self._interests_from_request(request),
                paper_title=paper.title,
                paper_abstract=payload.get("abstract"),
            )
        except Exception:
            return None

    def _interests_from_request(self, request: RecommendationRequestDTO) -> list[str]:
        interests: list[str] = []
        interests.extend(f"domain:{domain_id}" for domain_id in request.domain_ids)
        interests.extend(f"field:{field_id}" for field_id in request.field_ids)
        interests.extend(f"subfield:{subfield_id}" for subfield_id in request.subfield_ids)
        interests.extend(f"topic:{topic_id}" for topic_id in request.topic_ids)
        interests.extend(f"keyword:{keyword_id}" for keyword_id in request.keyword_ids)
        return interests

    def _paper_by_id(self, paper_ids: list[int]) -> dict[int, Any]:
        if not paper_ids:
            return {}
        papers = self.paper_repository.get_by_ids(list(dict.fromkeys(paper_ids)))
        return {int(getattr(paper, "id")): paper for paper in papers}

    def _favourite_ids(self, request: RecommendationRequestDTO) -> set[int]:
        if request.user_id is None or not request.exclude_favourites:
            return set()
        return set(self.favourite_repository.list_paper_ids(request.user_id))

    def _paper_id_from_hit(self, hit: QdrantSearchHitDTO) -> int | None:
        value = hit.payload.get("paper_id") or hit.id
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    def _candidate_limit(self, request: RecommendationRequestDTO) -> int:
        return min(max(request.limit * 3, request.limit), 100)

    def _payload_date(self, value: Any) -> date | None:
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        if isinstance(value, str):
            try:
                return date.fromisoformat(value[:10])
            except ValueError:
                return None
        return None

    def _payload_int(self, value: Any) -> int | None:
        if value is None or isinstance(value, bool):
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    def _payload_int_set(self, value: Any) -> set[int]:
        if not isinstance(value, list | tuple | set):
            value = [value]
        result: set[int] = set()
        for item in value:
            parsed = self._payload_int(item)
            if parsed is not None:
                result.add(parsed)
        return result

    def _payload_float(self, value: Any) -> float:
        if isinstance(value, Decimal):
            return float(value)
        if value is None or isinstance(value, bool):
            return 0.0
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0

    def _cache_key(self, request: RecommendationRequestDTO) -> str:
        payload = request.model_dump(mode="json")
        serialized = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        return f"ml:recommendations:{calculate_text_hash(serialized)}"

    def _get_cached_response(
        self,
        cache_key: str,
    ) -> RecommendationResponseDTO | None:
        if self.redis_adapter is None:
            return None
        cached = self.redis_adapter.get_json(cache_key)
        if cached is None:
            return None
        return RecommendationResponseDTO.model_validate(cached)

    def _set_cached_response(
        self,
        cache_key: str,
        response: RecommendationResponseDTO,
    ) -> None:
        if self.redis_adapter is None:
            return
        self.redis_adapter.set_json(
            cache_key,
            response.model_dump(mode="json"),
            ttl_seconds=self.cache_ttl_seconds,
        )


__all__ = ["RecommendationFacade"]
