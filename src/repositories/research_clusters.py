from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from sqlalchemy import delete, select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from core.exceptions import InvalidRequestError
from models import ResearchCluster, ResearchClusterPeriodStat

from .base import BaseRepository


class ResearchClusterRepository(BaseRepository):
    """Persist cluster read-model rows derived from ML Qdrant payloads."""

    def get_by_cluster_key(self, cluster_key: str) -> ResearchCluster | None:
        """Return one cluster by stable ML cluster key."""
        stmt = select(ResearchCluster).where(ResearchCluster.cluster_key == cluster_key)
        return self.session.scalar(stmt)

    def upsert_cluster_from_payload(
        self,
        payload: dict[str, Any],
    ) -> tuple[ResearchCluster, bool]:
        """Create or update a cluster from a Qdrant trend cluster payload."""
        values = self._cluster_values(payload)
        existing = self.get_by_cluster_key(str(values["cluster_key"]))
        created = existing is None

        if self._is_postgresql():
            stmt = pg_insert(ResearchCluster).values(values)
            update_values = {
                "cluster_type": stmt.excluded.cluster_type,
                "source_topic_id": stmt.excluded.source_topic_id,
                "name": stmt.excluded.name,
                "summary": stmt.excluded.summary,
                "status": stmt.excluded.status,
                "trend_score": stmt.excluded.trend_score,
                "updated_at": stmt.excluded.updated_at,
            }
            cluster_id = self.session.scalar(
                stmt.on_conflict_do_update(
                    index_elements=[ResearchCluster.cluster_key],
                    set_=update_values,
                ).returning(ResearchCluster.id)
            )
            cluster = self.session.get(ResearchCluster, int(cluster_id))
            if cluster is None:
                raise InvalidRequestError(
                    "Research cluster upsert did not return a row",
                    details={"cluster_key": values["cluster_key"]},
                )
            return cluster, created

        if existing is None:
            cluster = ResearchCluster(**values)
            self.session.add(cluster)
            self.session.flush()
            return cluster, True

        for field, value in values.items():
            if field == "cluster_key":
                continue
            setattr(existing, field, value)
        return existing, False

    def upsert_period_from_payload(
        self,
        payload: dict[str, Any],
    ) -> tuple[ResearchClusterPeriodStat, bool]:
        """Create or update one cluster period stat from a Qdrant payload."""
        cluster = self._cluster_for_period_payload(payload)
        values = self._period_values(payload, cluster_id=int(cluster.id))
        existing = self._get_period_stat(
            int(cluster.id),
            values["period_start"],
            values["period_end"],
        )
        created = existing is None

        if self._is_postgresql():
            stmt = pg_insert(ResearchClusterPeriodStat).values(values)
            update_values = {
                "paper_count": stmt.excluded.paper_count,
                "previous_paper_count": stmt.excluded.previous_paper_count,
                "growth_rate": stmt.excluded.growth_rate,
                "trend_score": stmt.excluded.trend_score,
                "semantic_drift": stmt.excluded.semantic_drift,
                "citation_count_sum": stmt.excluded.citation_count_sum,
                "avg_cited_by_count": stmt.excluded.avg_cited_by_count,
                "top_keywords": stmt.excluded.top_keywords,
                "representative_paper_ids": stmt.excluded.representative_paper_ids,
                "summary": stmt.excluded.summary,
            }
            stat_id = self.session.scalar(
                stmt.on_conflict_do_update(
                    index_elements=[
                        ResearchClusterPeriodStat.cluster_id,
                        ResearchClusterPeriodStat.period_start,
                        ResearchClusterPeriodStat.period_end,
                    ],
                    set_=update_values,
                ).returning(ResearchClusterPeriodStat.id)
            )
            stat = self.session.get(ResearchClusterPeriodStat, int(stat_id))
            if stat is None:
                raise InvalidRequestError(
                    "Research cluster period stat upsert did not return a row",
                    details={
                        "cluster_key": cluster.cluster_key,
                        "period_start": values["period_start"],
                        "period_end": values["period_end"],
                    },
                )
            return stat, created

        if existing is None:
            stat = ResearchClusterPeriodStat(**values)
            self.session.add(stat)
            self.session.flush()
            return stat, True

        for field, value in values.items():
            if field in {"cluster_id", "period_start", "period_end"}:
                continue
            setattr(existing, field, value)
        return existing, False

    def delete_clusters_not_in_keys(self, cluster_keys: set[str]) -> int:
        """Delete clusters whose keys were not seen in the Qdrant sync source."""
        stmt = delete(ResearchCluster)
        if cluster_keys:
            stmt = stmt.where(ResearchCluster.cluster_key.notin_(cluster_keys))
        result = self.session.execute(stmt)
        return int(result.rowcount or 0)

    def delete_clusters_by_keys(self, cluster_keys: set[str]) -> int:
        """Delete clusters whose keys are explicitly listed."""
        if not cluster_keys:
            return 0
        result = self.session.execute(
            delete(ResearchCluster).where(ResearchCluster.cluster_key.in_(cluster_keys))
        )
        return int(result.rowcount or 0)

    def delete_period_stats_not_in_keys(
        self,
        period_keys: set[tuple[str, date, date]],
        *,
        cluster_key: str | None = None,
    ) -> int:
        """Delete period stats whose identity was not seen in Qdrant."""
        existing_stmt = (
            select(
                ResearchClusterPeriodStat.id,
                ResearchCluster.cluster_key,
                ResearchClusterPeriodStat.period_start,
                ResearchClusterPeriodStat.period_end,
            )
            .join(ResearchCluster, ResearchCluster.id == ResearchClusterPeriodStat.cluster_id)
        )
        if cluster_key is not None:
            existing_stmt = existing_stmt.where(ResearchCluster.cluster_key == cluster_key)

        ids_to_delete = [
            int(stat_id)
            for stat_id, existing_cluster_key, period_start, period_end in self.session.execute(
                existing_stmt
            )
            if (str(existing_cluster_key), period_start, period_end) not in period_keys
        ]
        if not ids_to_delete:
            return 0
        result = self.session.execute(
            delete(ResearchClusterPeriodStat).where(
                ResearchClusterPeriodStat.id.in_(ids_to_delete)
            )
        )
        return int(result.rowcount or 0)

    def _cluster_for_period_payload(
        self,
        payload: dict[str, Any],
    ) -> ResearchCluster:
        cluster_key = self._cluster_key(payload)
        cluster = self.get_by_cluster_key(cluster_key)
        if cluster is not None:
            return cluster
        cluster_payload = {
            "cluster_key": cluster_key,
            "cluster_id": cluster_key,
            "cluster_type": payload.get("cluster_type") or "topic",
            "source_topic_id": payload.get("source_topic_id"),
            "name": payload.get("cluster_name") or payload.get("name") or cluster_key,
            "summary": payload.get("summary"),
            "status": payload.get("status"),
            "trend_score": payload.get("trend_score"),
        }
        cluster, _ = self.upsert_cluster_from_payload(cluster_payload)
        return cluster

    def _get_period_stat(
        self,
        cluster_id: int,
        period_start: date,
        period_end: date,
    ) -> ResearchClusterPeriodStat | None:
        stmt = select(ResearchClusterPeriodStat).where(
            ResearchClusterPeriodStat.cluster_id == cluster_id,
            ResearchClusterPeriodStat.period_start == period_start,
            ResearchClusterPeriodStat.period_end == period_end,
        )
        return self.session.scalar(stmt)

    def _cluster_values(self, payload: dict[str, Any]) -> dict[str, Any]:
        cluster_key = self._cluster_key(payload)
        name = payload.get("name") or payload.get("cluster_name") or cluster_key
        now = datetime.now(timezone.utc)
        return {
            "cluster_key": cluster_key,
            "cluster_type": str(payload.get("cluster_type") or "topic"),
            "source_topic_id": self._source_topic_id(payload, cluster_key),
            "name": str(name),
            "summary": payload.get("summary"),
            "status": payload.get("status"),
            "trend_score": self._decimal_or_none(payload.get("trend_score")),
            "updated_at": now,
        }

    def _period_values(
        self,
        payload: dict[str, Any],
        *,
        cluster_id: int,
    ) -> dict[str, Any]:
        period_start = self._date_value(payload.get("period_start"), "period_start")
        period_end = self._date_value(payload.get("period_end"), "period_end")
        return {
            "cluster_id": cluster_id,
            "period_start": period_start,
            "period_end": period_end,
            "paper_count": self._int_value(payload.get("paper_count")),
            "previous_paper_count": self._int_value(payload.get("previous_paper_count")),
            "growth_rate": self._decimal_or_none(payload.get("growth_rate")),
            "trend_score": self._decimal_or_none(payload.get("trend_score")),
            "semantic_drift": self._decimal_or_none(payload.get("semantic_drift")),
            "citation_count_sum": self._optional_int(payload.get("citation_count_sum")),
            "avg_cited_by_count": self._decimal_or_none(payload.get("avg_cited_by_count")),
            "top_keywords": payload.get("top_keywords"),
            "representative_paper_ids": payload.get("representative_paper_ids"),
            "summary": payload.get("summary"),
        }

    def _cluster_key(self, payload: dict[str, Any]) -> str:
        value = payload.get("cluster_key") or payload.get("cluster_id")
        if value is None or not str(value).strip():
            raise InvalidRequestError(
                "Cluster payload does not contain cluster_key or cluster_id",
                details={"payload_keys": sorted(payload)},
            )
        return str(value).strip()

    def _source_topic_id(self, payload: dict[str, Any], cluster_key: str) -> int | None:
        value = payload.get("source_topic_id")
        if value is not None:
            return self._optional_int(value)
        prefix = "topic:"
        if cluster_key.startswith(prefix):
            return self._optional_int(cluster_key.removeprefix(prefix))
        return None

    def _date_value(self, value: Any, field: str) -> date:
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        if isinstance(value, str):
            try:
                return date.fromisoformat(value[:10])
            except ValueError as exc:
                raise InvalidRequestError(
                    "Cluster period payload date is invalid",
                    details={"field": field, "value": value},
                ) from exc
        raise InvalidRequestError(
            "Cluster period payload date is required",
            details={"field": field, "value": value},
        )

    def _decimal_or_none(self, value: Any) -> Decimal | None:
        if value is None:
            return None
        try:
            return Decimal(str(value))
        except (InvalidOperation, ValueError) as exc:
            raise InvalidRequestError(
                "Cluster payload numeric value is invalid",
                details={"value": value},
            ) from exc

    def _int_value(self, value: Any) -> int:
        return int(value or 0)

    def _optional_int(self, value: Any) -> int | None:
        if value is None:
            return None
        return int(value)


__all__ = ["ResearchClusterRepository"]
