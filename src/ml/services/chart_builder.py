from __future__ import annotations

from collections.abc import Mapping
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from dto.charts import (
    ChartAxisDTO,
    ChartCellDTO,
    ChartDTO,
    ChartPointDTO,
    ChartSeriesDTO,
)
from dto.trends import TrendClusterDTO


class ChartBuilderService:
    def build_cluster_activity_line_chart(
        self,
        cluster_id: str,
        cluster_name: str,
        period_points: list[dict],
    ) -> ChartDTO:
        points = [
            ChartPointDTO(
                x=self._period_value(point),
                y=self._number(self._first(point, "paper_count", "count", "value")),
                label=self._label(point),
                meta=self._meta(point),
            )
            for point in period_points
        ]
        return ChartDTO(
            chart_id=f"cluster-{cluster_id}-activity",
            chart_type="line",
            title=f"{cluster_name}: activity",
            x_axis=ChartAxisDTO(label="Period"),
            y_axis=ChartAxisDTO(label="Paper count"),
            series=[ChartSeriesDTO(name="Paper count", points=points)],
            meta={"cluster_id": cluster_id, "cluster_name": cluster_name},
        )

    def build_top_growing_clusters_bar_chart(
        self,
        clusters: list[TrendClusterDTO],
    ) -> ChartDTO:
        ordered_clusters = sorted(
            clusters,
            key=lambda cluster: self._metric(cluster, "growth_rate"),
            reverse=True,
        )
        points = [
            ChartPointDTO(
                x=cluster.name,
                y=self._metric(cluster, "growth_rate"),
                label=cluster.name,
                meta={
                    "cluster_id": cluster.id,
                    "cluster_key": cluster.cluster_key,
                    "status": cluster.status,
                    "trend_score": self._metric(cluster, "trend_score"),
                },
            )
            for cluster in ordered_clusters
        ]
        return ChartDTO(
            chart_id="top-growing-clusters",
            chart_type="bar",
            title="Top growing clusters",
            x_axis=ChartAxisDTO(label="Cluster"),
            y_axis=ChartAxisDTO(label="Growth rate"),
            series=[ChartSeriesDTO(name="Growth rate", points=points)],
        )

    def build_cluster_activity_heatmap(
        self,
        clusters: list[TrendClusterDTO],
        period_matrix: list[dict],
    ) -> ChartDTO:
        cluster_names = {str(cluster.id): cluster.name for cluster in clusters}
        cells = [
            ChartCellDTO(
                x=self._period_value(item),
                y=self._cluster_name(item, cluster_names),
                value=self._number(self._first(item, "paper_count", "count", "value")),
                label=self._label(item),
                meta=self._meta(item),
            )
            for item in period_matrix
        ]
        return ChartDTO(
            chart_id="cluster-activity-heatmap",
            chart_type="heatmap",
            title="Cluster activity heatmap",
            x_axis=ChartAxisDTO(label="Period"),
            y_axis=ChartAxisDTO(label="Cluster"),
            cells=cells,
        )

    def build_growth_vs_total_scatter(
        self,
        clusters: list[TrendClusterDTO],
    ) -> ChartDTO:
        points = [
            {
                "x": self._metric(cluster, "growth_rate"),
                "y": self._metric(cluster, "paper_count"),
                "label": cluster.name,
                "meta": {
                    "cluster_id": cluster.id,
                    "cluster_key": cluster.cluster_key,
                    "status": cluster.status,
                    "trend_score": self._metric(cluster, "trend_score"),
                },
            }
            for cluster in clusters
        ]
        return ChartDTO(
            chart_id="growth-vs-total",
            chart_type="scatter",
            title="Growth vs total activity",
            x_axis=ChartAxisDTO(label="Growth rate"),
            y_axis=ChartAxisDTO(label="Paper count"),
            points=points,
        )

    def build_keyword_evolution_chart(
        self,
        cluster_id: str,
        cluster_name: str,
        periods: list[dict],
    ) -> ChartDTO:
        period_keyword_counts = [
            (self._period_value(period), self._keyword_counts(period))
            for period in periods
        ]
        keyword_order = self._keyword_order(
            keyword_counts for _, keyword_counts in period_keyword_counts
        )
        series = [
            ChartSeriesDTO(
                name=keyword,
                points=[
                    ChartPointDTO(
                        x=period,
                        y=keyword_counts.get(keyword, 0.0),
                        label=keyword,
                    )
                    for period, keyword_counts in period_keyword_counts
                ],
            )
            for keyword in keyword_order
        ]
        return ChartDTO(
            chart_id=f"cluster-{cluster_id}-keyword-evolution",
            chart_type="keyword_evolution",
            title=f"{cluster_name}: keyword evolution",
            x_axis=ChartAxisDTO(label="Period"),
            y_axis=ChartAxisDTO(label="Keyword weight"),
            series=series,
            meta={"cluster_id": cluster_id, "cluster_name": cluster_name},
        )

    def _metric(self, cluster: TrendClusterDTO, name: str) -> float:
        return self._number(getattr(cluster.metrics, name, None))

    def _period_value(self, point: Mapping[str, Any]) -> str | int | float:
        value = self._first(point, "period", "period_start", "date", "x")
        return self._axis_value(value)

    def _cluster_name(
        self,
        point: Mapping[str, Any],
        cluster_names: dict[str, str],
    ) -> str:
        value = self._first(point, "cluster_name", "cluster", "y")
        if value is not None:
            return str(value)
        cluster_id = self._first(point, "cluster_id", "id")
        return cluster_names.get(str(cluster_id), str(cluster_id))

    def _keyword_counts(self, period: Mapping[str, Any]) -> dict[str, float]:
        raw_keywords = self._first(
            period,
            "keyword_counts",
            "keywords",
            "top_keywords",
        )
        if raw_keywords is None:
            return {}
        if isinstance(raw_keywords, Mapping):
            return {
                str(keyword): self._number(value)
                for keyword, value in raw_keywords.items()
            }
        if not isinstance(raw_keywords, list):
            return {}

        result: dict[str, float] = {}
        fallback_weight = len(raw_keywords)
        for index, item in enumerate(raw_keywords):
            if isinstance(item, str):
                result[item] = float(max(fallback_weight - index, 1))
                continue
            if not isinstance(item, Mapping):
                continue
            keyword = self._first(item, "keyword", "name", "value")
            if keyword is None:
                continue
            count = self._first(item, "count", "paper_count", "weight", "score")
            if count is None:
                count = max(fallback_weight - index, 1)
            result[str(keyword)] = self._number(count)
        return result

    def _keyword_order(self, keyword_counts: Any) -> list[str]:
        ordered: list[str] = []
        seen: set[str] = set()
        for counts in keyword_counts:
            for keyword in counts:
                if keyword not in seen:
                    seen.add(keyword)
                    ordered.append(keyword)
        return ordered

    def _first(self, data: Mapping[str, Any], *keys: str) -> Any:
        for key in keys:
            value = data.get(key)
            if value is not None:
                return value
        return None

    def _number(self, value: Any) -> float:
        if value is None:
            return 0.0
        if isinstance(value, Decimal):
            return float(value)
        return float(value)

    def _axis_value(self, value: Any) -> str | int | float:
        if isinstance(value, (datetime, date)):
            return value.isoformat()
        if isinstance(value, (str, int, float)):
            return value
        if isinstance(value, Decimal):
            return float(value)
        return str(value)

    def _label(self, point: Mapping[str, Any]) -> str | None:
        value = self._first(point, "label", "name")
        return str(value) if value is not None else None

    def _meta(self, point: Mapping[str, Any]) -> dict[str, Any]:
        return {
            key: self._axis_value(value)
            for key, value in point.items()
            if key not in {"x", "y", "value", "label"}
        }


__all__ = ["ChartBuilderService"]
