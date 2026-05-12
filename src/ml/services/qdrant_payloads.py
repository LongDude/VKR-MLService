from __future__ import annotations

from collections.abc import Mapping
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Any


class QdrantPayloadBuilder:
    def build_paper_payload(self, paper: Any = None, **overrides: Any) -> dict[str, Any]:
        data = self._merge_source(paper, overrides)
        topics = data.get("topics")
        keywords = data.get("keywords")

        payload = {
            "paper_id": self._first(data, "paper_id", "id"),
            "title": data.get("title"),
            "doi": data.get("doi"),
            "publication_year": data.get("publication_year"),
            "publication_date": data.get("publication_date"),
            "type": data.get("type"),
            "language": data.get("language"),
            "is_open_access": data.get("is_open_access"),
            "cited_by_count": data.get("cited_by_count"),
            "created_by_user_id": data.get("created_by_user_id"),
            "topic_ids": data.get("topic_ids")
            or self._extract_related_ids(topics, "topic_id", "topic"),
            "topic_names": data.get("topic_names")
            or self._extract_related_names(topics, "topic", "name"),
            "keyword_ids": data.get("keyword_ids")
            or self._extract_related_ids(keywords, "keyword_id", "keyword"),
            "keywords": data.get("keywords_values")
            or data.get("keyword_values")
            or self._extract_related_names(keywords, "keyword", "value"),
        }
        return self._compact_json_dict(payload)

    def build_research_entity_payload(
        self,
        entity: Any = None,
        *args: Any,
        **overrides: Any,
    ) -> dict[str, Any]:
        if isinstance(entity, str):
            overrides.setdefault("entity_type", entity)
            if args:
                overrides.setdefault("entity_id", args[0])
            if len(args) > 1:
                overrides.setdefault("name", args[1])
            entity = None

        data = self._merge_source(entity, overrides)
        entity_type = data.get("entity_type") or self._infer_entity_type(entity)
        payload = {
            "entity_id": self._first(data, "entity_id", "id", f"{entity_type}_id"),
            "entity_type": entity_type,
            "name": data.get("name") or data.get("value"),
            "openalex_id": data.get("openalex_id"),
            "domain_id": data.get("domain_id"),
            "domain_name": data.get("domain_name"),
            "field_id": data.get("field_id"),
            "field_name": data.get("field_name"),
            "subfield_id": data.get("subfield_id"),
            "subfield_name": data.get("subfield_name"),
        }
        return self._compact_json_dict(payload)

    def build_trend_cluster_payload(
        self,
        cluster: Any = None,
        **overrides: Any,
    ) -> dict[str, Any]:
        data = self._merge_source(cluster, overrides)
        metrics = self._as_mapping(data.get("metrics"))

        payload = {
            "cluster_id": self._first(data, "cluster_id", "id"),
            "cluster_key": data.get("cluster_key"),
            "cluster_type": data.get("cluster_type"),
            "name": data.get("name"),
            "summary": data.get("summary"),
            "status": data.get("status"),
            "source_topic_id": data.get("source_topic_id"),
            "paper_count": self._first(data, "paper_count", source=metrics),
            "previous_paper_count": self._first(
                data,
                "previous_paper_count",
                source=metrics,
            ),
            "growth_rate": self._first(data, "growth_rate", source=metrics),
            "trend_score": self._first(data, "trend_score", source=metrics),
            "semantic_drift": self._first(data, "semantic_drift", source=metrics),
            "citation_count_sum": self._first(data, "citation_count_sum", source=metrics),
            "avg_cited_by_count": self._first(data, "avg_cited_by_count", source=metrics),
            "top_keywords": data.get("top_keywords"),
            "representative_paper_ids": data.get("representative_paper_ids"),
        }
        return self._compact_json_dict(payload)

    def build_cluster_period_payload(
        self,
        period: Any = None,
        **overrides: Any,
    ) -> dict[str, Any]:
        data = self._merge_source(period, overrides)
        payload = {
            "period_id": self._first(data, "period_id", "id"),
            "cluster_id": data.get("cluster_id"),
            "cluster_key": data.get("cluster_key"),
            "cluster_name": data.get("cluster_name"),
            "period_start": data.get("period_start"),
            "period_end": data.get("period_end"),
            "paper_count": data.get("paper_count"),
            "previous_paper_count": data.get("previous_paper_count"),
            "growth_rate": data.get("growth_rate"),
            "trend_score": data.get("trend_score"),
            "semantic_drift": data.get("semantic_drift"),
            "citation_count_sum": data.get("citation_count_sum"),
            "avg_cited_by_count": data.get("avg_cited_by_count"),
            "top_keywords": data.get("top_keywords"),
            "representative_paper_ids": data.get("representative_paper_ids"),
        }
        return self._compact_json_dict(payload)

    def build_user_profile_payload(
        self,
        profile: Any = None,
        **overrides: Any,
    ) -> dict[str, Any]:
        data = self._merge_source(profile, overrides)
        payload = {
            "user_id": data.get("user_id"),
            "profile_id": self._first(data, "profile_id", "id"),
            "profile_version": data.get("profile_version"),
            "embedding_model": data.get("embedding_model"),
            "source": data.get("source"),
            "source_counts": data.get("source_counts"),
            "tracked_domain_ids": data.get("tracked_domain_ids"),
            "tracked_subfield_ids": data.get("tracked_subfield_ids"),
            "tracked_topic_ids": data.get("tracked_topic_ids"),
            "tracked_keyword_ids": data.get("tracked_keyword_ids"),
            "favourite_paper_ids": data.get("favourite_paper_ids"),
            "updated_at": data.get("updated_at"),
            "created_at": data.get("created_at"),
        }
        return self._compact_json_dict(payload)

    def _merge_source(self, source: Any, overrides: Mapping[str, Any]) -> dict[str, Any]:
        data = self._as_mapping(source)
        data.update(dict(overrides))
        return data

    def _as_mapping(self, value: Any) -> dict[str, Any]:
        if value is None:
            return {}
        if isinstance(value, Mapping):
            return dict(value)
        if hasattr(value, "model_dump"):
            return value.model_dump()
        if hasattr(value, "__dict__"):
            return {
                key: item
                for key, item in vars(value).items()
                if not key.startswith("_")
            }
        return {}

    def _first(
        self,
        data: Mapping[str, Any],
        *keys: str,
        source: Mapping[str, Any] | None = None,
    ) -> Any:
        lookups = (data, source) if source is not None else (data,)
        for lookup in lookups:
            for key in keys:
                value = lookup.get(key)
                if value is not None:
                    return value
        return None

    def _extract_related_ids(
        self,
        items: Any,
        direct_key: str,
        nested_key: str,
    ) -> list[Any]:
        result: list[Any] = []
        if not isinstance(items, list):
            return result
        for item in items:
            item_data = self._as_mapping(item)
            value = item_data.get(direct_key)
            if value is None:
                value = self._as_mapping(item_data.get(nested_key)).get("id")
            if value is not None:
                result.append(value)
        return result

    def _extract_related_names(
        self,
        items: Any,
        nested_key: str,
        name_key: str,
    ) -> list[str]:
        result: list[str] = []
        if not isinstance(items, list):
            return result
        for item in items:
            item_data = self._as_mapping(item)
            nested_data = self._as_mapping(item_data.get(nested_key))
            value = nested_data.get(name_key) or item_data.get(name_key)
            if value:
                result.append(str(value))
        return result

    def _infer_entity_type(self, entity: Any) -> str | None:
        if entity is None:
            return None
        class_name = entity.__class__.__name__.lower()
        for suffix in ("dto", "create"):
            if class_name.endswith(suffix):
                class_name = class_name[: -len(suffix)]
        return class_name or None

    def _compact_json_dict(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        compact: dict[str, Any] = {}
        for key, value in payload.items():
            json_value = self._to_jsonable(value)
            if self._is_empty(json_value):
                continue
            compact[key] = json_value
        return compact

    def _to_jsonable(self, value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, date):
            return value.isoformat()
        if isinstance(value, Decimal):
            return float(value)
        if isinstance(value, Enum):
            return value.value
        if isinstance(value, Mapping):
            return {
                str(key): self._to_jsonable(item)
                for key, item in value.items()
                if not self._is_empty(self._to_jsonable(item))
            }
        if isinstance(value, (list, tuple)):
            return [
                item
                for item in (self._to_jsonable(item) for item in value)
                if not self._is_empty(item)
            ]
        if isinstance(value, set):
            return [
                item
                for item in (
                    self._to_jsonable(item) for item in sorted(value, key=str)
                )
                if not self._is_empty(item)
            ]
        if hasattr(value, "model_dump"):
            return self._to_jsonable(value.model_dump())
        if isinstance(value, (str, int, float, bool)):
            return value
        return str(value)

    def _is_empty(self, value: Any) -> bool:
        if value is None:
            return True
        if isinstance(value, str) and value == "":
            return True
        if isinstance(value, (list, dict)) and not value:
            return True
        return False


__all__ = ["QdrantPayloadBuilder"]
