from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Literal

from adapters.qdrant_adapter import QdrantAdapter
from core.exceptions import EntityNotFoundError, InvalidRequestError
from dto.charts import ChartAxisDTO, ChartDTO, ChartPointDTO, ChartSeriesDTO
from dto.common import BatchOperationResultDTO
from dto.qdrant import QdrantPointDTO
from dto.trends import ClusterChartsResponseDTO, TrendClusterDTO, TrendMetricsDTO
from repositories.graph import PaperGraphRepository
from repositories.research_clusters import ResearchClusterRepository
from repositories.taxonomy import TaxonomyRepository

from ml.constants import PAPERS_COLLECTION, TREND_CLUSTER_PERIODS_COLLECTION
from ml.services.chart_builder import ChartBuilderService
from ml.services.events import EventSink, MLEvent, NoopEventSink
from ml.services.qdrant_payloads import QdrantPayloadBuilder
from ml.services.scoring import ScoringService
from ml.services.vector_math import VectorMathService


Granularity = Literal["week", "month"]
REPRESENTATIVE_PAPER_LIMIT = 5
TOP_KEYWORD_LIMIT = 10


@dataclass(frozen=True)
class PeriodWindow:
    period_start: date
    period_end: date
    effective_start: date
    effective_end: date


class ClusterDynamicsFacade:
    def __init__(
        self,
        *,
        taxonomy_repository: TaxonomyRepository,
        paper_graph_repository: PaperGraphRepository,
        qdrant_adapter: QdrantAdapter,
        research_cluster_repository: ResearchClusterRepository | None = None,
        vector_math_service: VectorMathService | None = None,
        scoring_service: ScoringService | None = None,
        chart_builder_service: ChartBuilderService | None = None,
        payload_builder: QdrantPayloadBuilder | None = None,
        event_sink: EventSink | None = None,
        papers_collection: str = PAPERS_COLLECTION,
        period_collection: str = TREND_CLUSTER_PERIODS_COLLECTION,
    ) -> None:
        self.taxonomy_repository = taxonomy_repository
        self.paper_graph_repository = paper_graph_repository
        self.qdrant_adapter = qdrant_adapter
        self.research_cluster_repository = research_cluster_repository
        self.vector_math_service = vector_math_service or VectorMathService()
        self.scoring_service = scoring_service or ScoringService()
        self.chart_builder_service = chart_builder_service or ChartBuilderService()
        self.payload_builder = payload_builder or QdrantPayloadBuilder()
        self.event_sink = event_sink or NoopEventSink()
        self.papers_collection = papers_collection
        self.period_collection = period_collection

    def recompute_cluster_periods(
        self,
        cluster_id: str,
        date_from: date,
        date_to: date,
        granularity: Granularity = "month",
    ) -> BatchOperationResultDTO:
        self._validate_period_request(date_from, date_to, granularity)
        topic_id = self._parse_topic_cluster_id(cluster_id)
        topic = self.taxonomy_repository.get_topic_by_id(topic_id)
        if topic is None:
            raise EntityNotFoundError(
                "Topic not found",
                details={"cluster_id": cluster_id, "topic_id": topic_id},
            )

        windows = self._period_windows(date_from, date_to, granularity)
        self._emit(
            "cluster_dynamics_started",
            entity_id=cluster_id,
            stage="periods",
            current=0,
            total=len(windows),
            message=f"Starting cluster dynamics recompute for {len(windows)} periods",
            payload={
                "date_from": date_from,
                "date_to": date_to,
                "granularity": granularity,
            },
        )
        all_paper_ids = self.taxonomy_repository.list_paper_ids_by_topic(topic_id)
        self._emit(
            "cluster_dynamics_paper_ids_loaded",
            entity_id=cluster_id,
            stage="paper_ids",
            current=len(all_paper_ids),
            total=len(all_paper_ids),
            message=f"Loaded {len(all_paper_ids)} paper ids",
        )
        all_points = self.qdrant_adapter.retrieve(
            self.papers_collection,
            all_paper_ids,
            with_vectors=True,
        )
        self._emit(
            "cluster_dynamics_vectors_loaded",
            entity_id=cluster_id,
            stage="qdrant_vectors",
            current=len(all_points),
            total=len(all_paper_ids),
            message=f"Loaded {len(all_points)} indexed paper vectors",
        )
        count_map = self._period_count_map(topic_id, date_from, date_to, granularity)
        previous_count_map = self._period_count_map(
            topic_id,
            self._previous_start(windows[0], granularity),
            date_to,
            granularity,
        )

        result = BatchOperationResultDTO(total=len(windows))
        previous_centroid: list[float] | None = None
        for period_index, window in enumerate(windows, start=1):
            period_points = self._points_for_window(all_points, window)
            vectors = [point.vector for point in period_points if point.vector]
            paper_count = count_map.get(
                window.period_start,
                self._count_points_for_window(all_points, window),
            )
            previous_start = self._previous_period_start(window.period_start, granularity)
            previous_paper_count = previous_count_map.get(
                previous_start,
                self._count_points_between(
                    all_points,
                    previous_start,
                    self._previous_period_end(window.period_start),
                ),
            )
            growth_rate = self.scoring_service.calculate_growth_rate(
                paper_count,
                previous_paper_count,
            )
            trend_score = self.scoring_service.calculate_trend_score(
                growth_rate=growth_rate,
                recent_paper_count=paper_count,
            )

            if not vectors:
                result.skipped += 1
                previous_centroid = None
                self._emit(
                    "cluster_dynamics_progress",
                    entity_id=cluster_id,
                    stage="periods",
                    current=period_index,
                    total=len(windows),
                    message=f"{window.period_start}: skipped, no vectors",
                )
                continue

            centroid = self.vector_math_service.mean_vector(vectors)
            semantic_drift = (
                self.vector_math_service.semantic_drift(centroid, previous_centroid)
                if previous_centroid is not None
                else None
            )
            previous_centroid = centroid
            representative_paper_ids = self._representative_paper_ids(
                period_points,
                centroid,
            )
            keyword_counts = self._keyword_counts(period_points)
            top_keywords = [
                keyword
                for keyword, _ in keyword_counts.most_common(TOP_KEYWORD_LIMIT)
            ]

            period_id = self._period_point_id(
                cluster_id,
                granularity,
                window.period_start,
            )
            payload = self.payload_builder.build_cluster_period_payload(
                period_id=period_id,
                cluster_id=cluster_id,
                cluster_key=cluster_id,
                cluster_name=self._topic_name(topic),
                granularity=granularity,
                period_start=window.period_start,
                period_end=window.period_end,
                paper_count=paper_count,
                previous_paper_count=previous_paper_count,
                growth_rate=growth_rate,
                trend_score=trend_score,
                semantic_drift=semantic_drift,
                centroid_dimension=len(centroid),
                top_keywords=top_keywords,
                keyword_counts=dict(keyword_counts),
                representative_paper_ids=representative_paper_ids,
                indexed_paper_ids=[
                    paper_id
                    for paper_id in (
                        self._paper_id_from_point(point) for point in period_points
                    )
                    if paper_id is not None
                ],
                indexed_at=datetime.now(timezone.utc),
            )
            self.qdrant_adapter.upsert_point(
                self.period_collection,
                period_id,
                centroid,
                payload,
            )
            if self.research_cluster_repository is not None:
                self._emit(
                    "db_cluster_period_upsert_started",
                    entity_id=period_id,
                    stage="db_upsert",
                    message="Upserting cluster period row",
                    payload={"cluster_id": cluster_id},
                )
                try:
                    self.research_cluster_repository.upsert_period_from_payload(payload)
                except Exception as exc:
                    self._emit(
                        "db_cluster_period_upsert_failed",
                        entity_id=period_id,
                        stage="db_upsert",
                        message=str(exc),
                        payload={
                            "cluster_id": cluster_id,
                            "error_type": exc.__class__.__name__,
                        },
                    )
                    raise
                self._emit(
                    "db_cluster_period_upsert_completed",
                    entity_id=period_id,
                    stage="db_upsert",
                    message="Cluster period row upserted",
                    payload={"cluster_id": cluster_id},
                )
            result.updated += 1
            self._emit(
                "cluster_dynamics_progress",
                entity_id=cluster_id,
                stage="periods",
                current=period_index,
                total=len(windows),
                message=f"{window.period_start}: papers={paper_count}",
                payload={"period_id": period_id, "paper_count": paper_count},
            )

        self._emit(
            "cluster_dynamics_completed",
            entity_id=cluster_id,
            stage="completed",
            current=len(windows),
            total=len(windows),
            message=(
                f"Cluster dynamics completed: updated={result.updated} "
                f"skipped={result.skipped} failed={result.failed}"
            ),
            payload=result.model_dump(mode="json"),
        )
        return result

    def get_cluster_periods(
        self,
        cluster_id: str,
        date_from: date,
        date_to: date,
        granularity: Granularity = "month",
    ) -> list[dict]:
        self._validate_period_request(date_from, date_to, granularity)
        self._parse_topic_cluster_id(cluster_id)
        point_ids = [
            self._period_point_id(cluster_id, granularity, window.period_start)
            for window in self._period_windows(date_from, date_to, granularity)
        ]
        points = self.qdrant_adapter.retrieve(
            self.period_collection,
            point_ids,
            with_vectors=False,
        )
        payloads = [dict(point.payload) for point in points]
        payloads.sort(key=lambda item: str(item.get("period_start", "")))
        return payloads

    def build_cluster_charts(
        self,
        cluster_id: str,
        date_from: date,
        date_to: date,
        granularity: Granularity = "month",
    ) -> ClusterChartsResponseDTO:
        topic = self._get_topic(cluster_id)
        periods = self.get_cluster_periods(cluster_id, date_from, date_to, granularity)
        charts = [
            self.chart_builder_service.build_cluster_activity_line_chart(
                cluster_id,
                self._topic_name(topic),
                periods,
            ),
            self.chart_builder_service.build_keyword_evolution_chart(
                cluster_id,
                self._topic_name(topic),
                periods,
            ),
        ]
        drift_chart = self._build_semantic_drift_chart(
            cluster_id,
            self._topic_name(topic),
            periods,
        )
        if drift_chart is not None:
            charts.append(drift_chart)
        return ClusterChartsResponseDTO(cluster_id=cluster_id, charts=charts)

    def build_dashboard_charts(
        self,
        cluster_ids: list[str],
        date_from: date,
        date_to: date,
        granularity: Granularity = "month",
    ) -> list[ChartDTO]:
        self._validate_period_request(date_from, date_to, granularity)
        clusters: list[TrendClusterDTO] = []
        period_matrix: list[dict[str, Any]] = []
        for cluster_id in cluster_ids:
            topic = self._get_topic(cluster_id)
            periods = self.get_cluster_periods(
                cluster_id,
                date_from,
                date_to,
                granularity,
            )
            clusters.append(self._cluster_from_periods(cluster_id, topic, periods))
            for period in periods:
                period_matrix.append(
                    {
                        **period,
                        "cluster_id": self._parse_topic_cluster_id(cluster_id),
                        "cluster_name": self._topic_name(topic),
                    }
                )

        return [
            self.chart_builder_service.build_cluster_activity_heatmap(
                clusters,
                period_matrix,
            ),
            self.chart_builder_service.build_growth_vs_total_scatter(clusters),
        ]

    def _period_count_map(
        self,
        topic_id: int,
        date_from: date,
        date_to: date,
        granularity: Granularity,
    ) -> dict[date, int]:
        if date_from > date_to:
            return {}
        points = self.paper_graph_repository.count_papers_by_topic_and_period(
            topic_id,
            date_from,
            date_to,
            granularity,
        )
        return {point.period_start: int(point.count) for point in points}

    def _period_windows(
        self,
        date_from: date,
        date_to: date,
        granularity: Granularity,
    ) -> list[PeriodWindow]:
        windows: list[PeriodWindow] = []
        current = self._period_floor(date_from, granularity)
        while current <= date_to:
            period_end = self._period_end(current, granularity)
            windows.append(
                PeriodWindow(
                    period_start=current,
                    period_end=period_end,
                    effective_start=max(current, date_from),
                    effective_end=min(period_end, date_to),
                )
            )
            current = self._next_period_start(current, granularity)
        return windows

    def _points_for_window(
        self,
        points: list[QdrantPointDTO],
        window: PeriodWindow,
    ) -> list[QdrantPointDTO]:
        return [
            point
            for point in points
            if self._point_date_in_window(point, window.effective_start, window.effective_end)
        ]

    def _count_points_for_window(
        self,
        points: list[QdrantPointDTO],
        window: PeriodWindow,
    ) -> int:
        return len(self._points_for_window(points, window))

    def _count_points_between(
        self,
        points: list[QdrantPointDTO],
        start: date,
        end: date,
    ) -> int:
        return sum(1 for point in points if self._point_date_in_window(point, start, end))

    def _point_date_in_window(
        self,
        point: QdrantPointDTO,
        start: date,
        end: date,
    ) -> bool:
        publication_date = self._publication_date(point)
        return publication_date is not None and start <= publication_date <= end

    def _publication_date(self, point: QdrantPointDTO) -> date | None:
        value = point.payload.get("publication_date")
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

    def _representative_paper_ids(
        self,
        points: list[QdrantPointDTO],
        centroid: list[float],
    ) -> list[int]:
        scored_points = [
            (
                self._paper_id_from_point(point),
                self.vector_math_service.cosine_similarity(point.vector, centroid),
            )
            for point in points
            if point.vector
        ]
        scored_points.sort(key=lambda item: item[1], reverse=True)
        return [
            paper_id
            for paper_id, _ in scored_points[:REPRESENTATIVE_PAPER_LIMIT]
            if paper_id is not None
        ]

    def _keyword_counts(self, points: list[QdrantPointDTO]) -> Counter[str]:
        counter: Counter[str] = Counter()
        for point in points:
            paper_id = self._paper_id_from_point(point)
            if paper_id is not None:
                try:
                    for keyword in self.taxonomy_repository.list_keywords_by_paper(paper_id):
                        value = getattr(keyword, "value", None)
                        if value:
                            counter[str(value)] += 1
                except Exception:
                    pass
            if not counter:
                for keyword in point.payload.get("keywords", []):
                    if keyword:
                        counter[str(keyword)] += 1
        return counter

    def _build_semantic_drift_chart(
        self,
        cluster_id: str,
        cluster_name: str,
        periods: list[dict],
    ) -> ChartDTO | None:
        drift_points = [
            ChartPointDTO(
                x=period.get("period_start"),
                y=float(period["semantic_drift"]),
                meta={"cluster_id": cluster_id},
            )
            for period in periods
            if period.get("semantic_drift") is not None
        ]
        if not drift_points:
            return None
        return ChartDTO(
            chart_id=f"cluster-{cluster_id}-semantic-drift",
            chart_type="line",
            title=f"{cluster_name}: semantic drift",
            x_axis=ChartAxisDTO(label="Period"),
            y_axis=ChartAxisDTO(label="Semantic drift"),
            series=[ChartSeriesDTO(name="Semantic drift", points=drift_points)],
            meta={"cluster_id": cluster_id, "cluster_name": cluster_name},
        )

    def _cluster_from_periods(
        self,
        cluster_id: str,
        topic: Any,
        periods: list[dict],
    ) -> TrendClusterDTO:
        paper_count = sum(int(period.get("paper_count", 0) or 0) for period in periods)
        previous_count = sum(
            int(period.get("previous_paper_count", 0) or 0) for period in periods
        )
        growth_rate = self.scoring_service.calculate_growth_rate(
            paper_count,
            previous_count,
        )
        return TrendClusterDTO(
            id=self._parse_topic_cluster_id(cluster_id),
            cluster_key=cluster_id,
            cluster_type="topic",
            name=self._topic_name(topic),
            source_topic_id=self._parse_topic_cluster_id(cluster_id),
            metrics=TrendMetricsDTO(
                paper_count=paper_count,
                previous_paper_count=previous_count,
                growth_rate=Decimal(str(growth_rate)),
            ),
        )

    def _get_topic(self, cluster_id: str) -> Any:
        topic_id = self._parse_topic_cluster_id(cluster_id)
        topic = self.taxonomy_repository.get_topic_by_id(topic_id)
        if topic is None:
            raise EntityNotFoundError(
                "Topic not found",
                details={"cluster_id": cluster_id, "topic_id": topic_id},
            )
        return topic

    def _paper_id_from_point(self, point: QdrantPointDTO) -> int | None:
        paper_id = point.payload.get("paper_id") or point.id
        try:
            return int(paper_id)
        except (TypeError, ValueError):
            return None

    def _period_point_id(
        self,
        cluster_id: str,
        granularity: Granularity,
        period_start: date,
    ) -> str:
        return f"{cluster_id}:{granularity}:{period_start.isoformat()}"

    def _period_floor(self, value: date, granularity: Granularity) -> date:
        if granularity == "week":
            return value - timedelta(days=value.weekday())
        return value.replace(day=1)

    def _period_end(self, value: date, granularity: Granularity) -> date:
        if granularity == "week":
            return value + timedelta(days=6)
        next_month = self._next_month_start(value)
        return next_month - timedelta(days=1)

    def _next_period_start(self, value: date, granularity: Granularity) -> date:
        if granularity == "week":
            return value + timedelta(days=7)
        return self._next_month_start(value)

    def _previous_period_start(self, value: date, granularity: Granularity) -> date:
        if granularity == "week":
            return value - timedelta(days=7)
        previous_month_end = value.replace(day=1) - timedelta(days=1)
        return previous_month_end.replace(day=1)

    def _previous_period_end(self, value: date) -> date:
        return value - timedelta(days=1)

    def _previous_start(
        self,
        first_window: PeriodWindow,
        granularity: Granularity,
    ) -> date:
        return self._previous_period_start(first_window.period_start, granularity)

    def _next_month_start(self, value: date) -> date:
        if value.month == 12:
            return value.replace(year=value.year + 1, month=1, day=1)
        return value.replace(month=value.month + 1, day=1)

    def _parse_topic_cluster_id(self, cluster_id: str) -> int:
        prefix = "topic:"
        if not cluster_id.startswith(prefix):
            raise InvalidRequestError(
                "Unsupported cluster_id format",
                details={"cluster_id": cluster_id, "expected": "topic:{topic_id}"},
            )
        try:
            return int(cluster_id.removeprefix(prefix))
        except ValueError as exc:
            raise InvalidRequestError(
                "Invalid topic cluster id",
                details={"cluster_id": cluster_id},
            ) from exc

    def _topic_name(self, topic: Any) -> str:
        name = getattr(topic, "name", None)
        if not name:
            raise InvalidRequestError(
                "Topic name is required for cluster dynamics",
                details={"topic_id": getattr(topic, "id", None)},
            )
        return str(name)

    def _validate_period_request(
        self,
        date_from: date,
        date_to: date,
        granularity: Granularity,
    ) -> None:
        if granularity not in {"week", "month"}:
            raise InvalidRequestError(
                "granularity must be 'week' or 'month'",
                details={"granularity": granularity},
            )
        if date_from > date_to:
            raise InvalidRequestError(
                "date_from must be less than or equal to date_to",
                details={"date_from": date_from, "date_to": date_to},
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
                task_type="cluster_dynamics_recompute",
                entity_id=entity_id,
                stage=stage,
                current=current,
                total=total,
                message=message,
                payload=payload or {},
            )
        )


__all__ = ["ClusterDynamicsFacade"]
