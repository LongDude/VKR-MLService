from __future__ import annotations

from datetime import date, datetime
from typing import Any, Literal

from adapters.qdrant_adapter import QdrantAdapter
from core.exceptions import AppError, InvalidRequestError
from core.logging import get_logger, logged_call
from dto.common import OperationResultDTO
from dto.qdrant import QdrantPointDTO
from ml.constants import TREND_CLUSTER_PERIODS_COLLECTION, TREND_CLUSTERS_COLLECTION
from ml.services.events import EventSink, MLEvent, NoopEventSink
from repositories.research_clusters import ResearchClusterRepository

SyncScope = Literal["all", "clusters", "periods"]
logger = get_logger(__name__)


class ClusterDbSyncFacade:
    """Synchronize cluster read-model rows in PostgreSQL from Qdrant payloads."""

    def __init__(
        self,
        *,
        qdrant_adapter: QdrantAdapter,
        research_cluster_repository: ResearchClusterRepository,
        event_sink: EventSink | None = None,
        clusters_collection: str = TREND_CLUSTERS_COLLECTION,
        periods_collection: str = TREND_CLUSTER_PERIODS_COLLECTION,
    ) -> None:
        self.qdrant_adapter = qdrant_adapter
        self.research_cluster_repository = research_cluster_repository
        self.event_sink = event_sink or NoopEventSink()
        self.clusters_collection = clusters_collection
        self.periods_collection = periods_collection

    @logged_call(logger, "cluster_db_sync_all")
    def sync_all(
        self,
        *,
        batch_size: int = 256,
        cluster_id: str | None = None,
        prune_missing: bool = True,
        dry_run: bool = False,
    ) -> OperationResultDTO:
        """Synchronize clusters and period stats from Qdrant into PostgreSQL."""
        cluster_result = self.sync_clusters_from_qdrant(
            batch_size=batch_size,
            cluster_id=cluster_id,
            prune_missing=prune_missing,
            dry_run=dry_run,
        )
        period_result = self.sync_periods_from_qdrant(
            batch_size=batch_size,
            cluster_id=cluster_id,
            prune_missing=prune_missing,
            dry_run=dry_run,
        )
        cluster_details = cluster_result.details
        period_details = period_result.details
        failed = int(cluster_details["failed"]) + int(period_details["failed"])
        return OperationResultDTO(
            success=failed == 0,
            message="Cluster DB sync completed",
            details={
                "scope": "all",
                "dry_run": dry_run,
                "clusters": cluster_details,
                "periods": period_details,
                "created": int(cluster_details["created"])
                + int(period_details["created"]),
                "updated": int(cluster_details["updated"])
                + int(period_details["updated"]),
                "skipped": int(cluster_details["skipped"])
                + int(period_details["skipped"]),
                "failed": failed,
                "pruned_clusters": cluster_details.get("pruned_clusters", 0),
                "pruned_period_stats": period_details.get("pruned_period_stats", 0),
            },
        )

    @logged_call(logger, "cluster_db_sync_clusters")
    def sync_clusters_from_qdrant(
        self,
        *,
        batch_size: int = 256,
        cluster_id: str | None = None,
        prune_missing: bool = True,
        dry_run: bool = False,
    ) -> OperationResultDTO:
        """Synchronize trend cluster rows from ``trend_clusters_v1``."""
        self._validate_batch_size(batch_size)
        self._emit(
            "cluster_db_sync_started",
            entity_id=cluster_id or "clusters",
            stage="clusters",
            message="Starting cluster DB sync",
            payload={"dry_run": dry_run, "prune_missing": prune_missing},
        )
        points = self._cluster_points(batch_size=batch_size, cluster_id=cluster_id)
        counters = self._empty_counters(scope="clusters", dry_run=dry_run)
        seen_cluster_keys: set[str] = set()

        for index, point in enumerate(points, start=1):
            counters["total"] += 1
            try:
                payload = self._payload_with_cluster_key(point)
                cluster_key = self._cluster_key(payload)
                seen_cluster_keys.add(cluster_key)
                if dry_run:
                    counters["skipped"] += 1
                else:
                    _, created = (
                        self.research_cluster_repository.upsert_cluster_from_payload(
                            payload
                        )
                    )
                    if created:
                        counters["created"] += 1
                    else:
                        counters["updated"] += 1
            except AppError as exc:
                counters["failed"] += 1
                counters["errors"].append(self._error_payload(point, exc))
            self._emit(
                "cluster_db_sync_progress",
                entity_id=cluster_id or "clusters",
                stage="clusters",
                current=index,
                total=len(points),
                message=(
                    f"clusters done={index} created={counters['created']} "
                    f"updated={counters['updated']} failed={counters['failed']}"
                ),
            )

        pruned = 0
        if prune_missing and not dry_run:
            if cluster_id is None:
                pruned = self.research_cluster_repository.delete_clusters_not_in_keys(
                    seen_cluster_keys
                )
            elif cluster_id not in seen_cluster_keys:
                pruned = self.research_cluster_repository.delete_clusters_by_keys(
                    {cluster_id}
                )
        counters["pruned_clusters"] = pruned
        return self._sync_result("clusters", counters, cluster_id=cluster_id)

    @logged_call(logger, "cluster_db_sync_periods")
    def sync_periods_from_qdrant(
        self,
        *,
        batch_size: int = 256,
        cluster_id: str | None = None,
        prune_missing: bool = True,
        dry_run: bool = False,
    ) -> OperationResultDTO:
        """Synchronize cluster period stats from ``trend_cluster_periods_v1``."""
        self._validate_batch_size(batch_size)
        self._emit(
            "cluster_db_sync_started",
            entity_id=cluster_id or "periods",
            stage="periods",
            message="Starting cluster period DB sync",
            payload={"dry_run": dry_run, "prune_missing": prune_missing},
        )
        points = self._period_points(batch_size=batch_size, cluster_id=cluster_id)
        counters = self._empty_counters(scope="periods", dry_run=dry_run)
        seen_period_keys: set[tuple[str, date, date]] = set()

        for index, point in enumerate(points, start=1):
            counters["total"] += 1
            try:
                payload = self._payload_with_period_defaults(point)
                period_key = self._period_key(payload)
                seen_period_keys.add(period_key)
                if dry_run:
                    counters["skipped"] += 1
                else:
                    _, created = (
                        self.research_cluster_repository.upsert_period_from_payload(
                            payload
                        )
                    )
                    if created:
                        counters["created"] += 1
                    else:
                        counters["updated"] += 1
            except AppError as exc:
                counters["failed"] += 1
                counters["errors"].append(self._error_payload(point, exc))
            self._emit(
                "cluster_db_sync_progress",
                entity_id=cluster_id or "periods",
                stage="periods",
                current=index,
                total=len(points),
                message=(
                    f"periods done={index} created={counters['created']} "
                    f"updated={counters['updated']} failed={counters['failed']}"
                ),
            )

        pruned = 0
        if prune_missing and not dry_run:
            pruned = self.research_cluster_repository.delete_period_stats_not_in_keys(
                seen_period_keys,
                cluster_key=cluster_id,
            )
        counters["pruned_period_stats"] = pruned
        return self._sync_result("periods", counters, cluster_id=cluster_id)

    def _cluster_points(
        self,
        *,
        batch_size: int,
        cluster_id: str | None,
    ) -> list[QdrantPointDTO]:
        if cluster_id is not None:
            return self.qdrant_adapter.retrieve(
                self.clusters_collection,
                [cluster_id],
                with_vectors=False,
            )
        return self.qdrant_adapter.scroll_points(
            self.clusters_collection,
            batch_size=batch_size,
            with_vectors=False,
        )

    def _period_points(
        self,
        *,
        batch_size: int,
        cluster_id: str | None,
    ) -> list[QdrantPointDTO]:
        points = self.qdrant_adapter.scroll_points(
            self.periods_collection,
            batch_size=batch_size,
            with_vectors=False,
        )
        if cluster_id is None:
            return points
        return [
            point for point in points if self._point_cluster_key(point) == cluster_id
        ]

    def _payload_with_cluster_key(self, point: QdrantPointDTO) -> dict[str, Any]:
        payload = dict(point.payload)
        payload.setdefault("cluster_key", payload.get("cluster_id") or point.id)
        payload.setdefault("cluster_id", payload["cluster_key"])
        return payload

    def _payload_with_period_defaults(self, point: QdrantPointDTO) -> dict[str, Any]:
        payload = dict(point.payload)
        cluster_key = self._point_cluster_key(point)
        if cluster_key is None:
            raise InvalidRequestError(
                "Cluster period payload does not contain cluster id",
                details={"point_id": point.id},
            )
        payload.setdefault("cluster_key", cluster_key)
        payload.setdefault("cluster_id", cluster_key)
        payload.setdefault("period_id", point.id)
        return payload

    def _point_cluster_key(self, point: QdrantPointDTO) -> str | None:
        value = point.payload.get("cluster_key") or point.payload.get("cluster_id")
        return str(value) if value else None

    def _cluster_key(self, payload: dict[str, Any]) -> str:
        value = payload.get("cluster_key") or payload.get("cluster_id")
        if value is None or not str(value).strip():
            raise InvalidRequestError("Cluster payload is missing cluster key")
        return str(value)

    def _period_key(self, payload: dict[str, Any]) -> tuple[str, date, date]:
        return (
            self._cluster_key(payload),
            self._date_value(payload.get("period_start"), "period_start"),
            self._date_value(payload.get("period_end"), "period_end"),
        )

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

    def _sync_result(
        self,
        scope: str,
        counters: dict[str, Any],
        *,
        cluster_id: str | None,
    ) -> OperationResultDTO:
        self._emit(
            "cluster_db_sync_completed",
            entity_id=cluster_id or scope,
            stage=scope,
            message=(
                f"{scope} DB sync completed: created={counters['created']} "
                f"updated={counters['updated']} failed={counters['failed']}"
            ),
            payload=counters,
        )
        return OperationResultDTO(
            success=int(counters["failed"]) == 0,
            message=f"Cluster DB sync completed for {scope}",
            details=counters,
        )

    def _empty_counters(self, *, scope: str, dry_run: bool) -> dict[str, Any]:
        return {
            "scope": scope,
            "dry_run": dry_run,
            "total": 0,
            "created": 0,
            "updated": 0,
            "skipped": 0,
            "failed": 0,
            "errors": [],
        }

    def _error_payload(self, point: QdrantPointDTO, exc: AppError) -> dict[str, Any]:
        return {
            "point_id": point.id,
            "code": exc.code,
            "message": exc.message,
            "details": exc.details or {},
        }

    def _validate_batch_size(self, batch_size: int) -> None:
        if batch_size <= 0:
            raise InvalidRequestError(
                "batch_size must be positive",
                details={"batch_size": batch_size},
            )

    def _emit(
        self,
        event_type: str,
        *,
        entity_id: str | int | None = None,
        stage: str | None = None,
        current: int | None = None,
        total: int | None = None,
        message: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> None:
        self.event_sink.emit(
            MLEvent(
                event_type=event_type,
                task_type="cluster_db_sync",
                entity_id=entity_id,
                stage=stage,
                current=current,
                total=total,
                message=message,
                payload=payload or {},
            )
        )


__all__ = ["ClusterDbSyncFacade"]
