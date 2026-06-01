from __future__ import annotations

from collections import OrderedDict
from datetime import date
from typing import Any

import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session

from adapters.qdrant_adapter import QdrantAdapter
from dto.topic_analytics import (
    ForecastQualityDTO,
    ForecastQualityGroupDTO,
    ForecastQualityModelDTO,
    RelatedTopicDTO,
    TopicAnalyticsInsightRequestDTO,
    TopicAnalyticsInsightResponseDTO,
    TopicDecompositionMetricDTO,
    TopicForecastPointDTO,
)
from ml.constants import RESEARCH_ENTITIES_COLLECTION
from ml.services import PublicationForecastService


class TopicAnalyticsFacade:
    """Analytics calculations for one OpenAlex Topic."""

    def __init__(
        self,
        session: Session,
        *,
        forecast_service: PublicationForecastService,
        qdrant_adapter: QdrantAdapter | None = None,
    ) -> None:
        self.session = session
        self.qdrant_adapter = qdrant_adapter
        self._forecast_service = forecast_service

    def insights(
        self, request: TopicAnalyticsInsightRequestDTO
    ) -> TopicAnalyticsInsightResponseDTO:
        errors: list[str] = []
        forecast: list[TopicForecastPointDTO] = []
        forecast_quality = ForecastQualityDTO()
        if self._includes(request, "activity"):
            try:
                monthly = self._monthly_series(
                    request.topic_id, request.period_start, request.period_end
                )
                forecast, forecast_quality = self._forecast(
                    monthly, request.forecast_months
                )
            except Exception as exc:
                errors.append(f"Forecast unavailable: {exc}")

        decomposition: list[TopicDecompositionMetricDTO] = []
        if self._includes(request, "trend-decomposition"):
            try:
                decomposition = self._decomposition(request)
            except Exception as exc:
                errors.append(f"Trend decomposition ML metrics unavailable: {exc}")

        related: list[RelatedTopicDTO] = []
        if self._includes(request, "related-topics"):
            try:
                related = self._related_topics(request, errors)
            except Exception as exc:
                errors.append(f"Related topics unavailable: {exc}")

        return TopicAnalyticsInsightResponseDTO(
            forecast=forecast,
            forecast_quality=forecast_quality,
            decomposition=decomposition,
            related_topics=related,
            errors=errors,
        )

    def _monthly_series(
        self, topic_id: int, period_start: date, period_end: date
    ) -> list[dict[str, Any]]:
        rows = self.session.execute(
            text(
                """
                WITH topic_meta AS (
                    SELECT id, subfield_id
                    FROM topics
                    WHERE id = :topic_id
                ),
                months AS (
                    SELECT generate_series(
                        date_trunc('month', CAST(:period_start AS date)),
                        date_trunc('month', CAST(:period_end AS date)),
                        interval '1 month'
                    )::date AS period_start
                ),
                topic_counts AS (
                    SELECT period_start, SUM(works_count)::bigint AS topic_count
                    FROM openalex_montly_topic_stats
                    WHERE topic_id = :topic_id
                      AND period_start BETWEEN :period_start AND :period_end
                    GROUP BY period_start
                ),
                subfield_counts AS (
                    SELECT s.period_start, SUM(s.works_count)::bigint AS subfield_count
                    FROM openalex_montly_topic_stats s
                    JOIN topics t ON t.id = s.topic_id
                    JOIN topic_meta tm ON tm.subfield_id = t.subfield_id
                    WHERE s.period_start BETWEEN :period_start AND :period_end
                    GROUP BY s.period_start
                )
                SELECT
                    m.period_start,
                    (sc.period_start IS NOT NULL) AS is_observed,
                    COALESCE(tc.topic_count, 0)::bigint AS topic_count,
                    COALESCE(sc.subfield_count, 0)::bigint AS subfield_count
                FROM months m
                LEFT JOIN topic_counts tc ON tc.period_start = m.period_start
                LEFT JOIN subfield_counts sc ON sc.period_start = m.period_start
                ORDER BY m.period_start ASC
                """
            ),
            {
                "topic_id": topic_id,
                "period_start": period_start,
                "period_end": period_end,
            },
        ).mappings()
        result = []
        for row in rows:
            subfield_count = int(row["subfield_count"] or 0)
            topic_count = int(row["topic_count"] or 0)
            result.append(
                {
                    "period_start": row["period_start"],
                    "is_observed": bool(row["is_observed"]),
                    "topic_count": topic_count,
                    "subfield_count": subfield_count,
                    "share": topic_count / subfield_count
                    if subfield_count > 0
                    else 0.0,
                }
            )
        return result

    def _forecast(
        self, rows: list[dict[str, Any]], horizon: int
    ) -> tuple[list[TopicForecastPointDTO], ForecastQualityDTO]:
        observed_rows = [row for row in rows if row.get("is_observed", True)]
        if not observed_rows:
            return [], ForecastQualityDTO()
        share_series = pd.Series(
            [float(row["share"]) for row in observed_rows],
            index=pd.to_datetime([row["period_start"] for row in observed_rows]),
        )
        topic_series = pd.Series(
            [float(row["topic_count"]) for row in observed_rows],
            index=pd.to_datetime([row["period_start"] for row in observed_rows]),
        )
        share_result = self._forecast_service.forecast_series_with_quality(
            share_series, horizon, kind="share"
        )

        topic_result = self._forecast_service.forecast_series_with_quality(
            topic_series, horizon, kind="count"
        )
        share_forecast = share_result["forecast"]
        topic_forecast = topic_result["forecast"]

        # share_forecast = self._forecast_series(share_series, horizon)
        # topic_forecast = self._forecast_series(subfield_series, horizon)
        result: list[TopicForecastPointDTO] = []
        for index in range(min(len(share_forecast), len(topic_forecast))):
            share = max(0.0, min(1.0, float(share_forecast[index]["forecast"])))
            lower_share = max(
                0.0, min(1.0, float(share_forecast[index]["lower_bound"]))
            )
            upper_share = max(
                0.0, min(1.0, float(share_forecast[index]["upper_bound"]))
            )
            topic = max(0.0, float(topic_forecast[index]["forecast"]))
            lower_topic = max(0.0, float(topic_forecast[index]["lower_bound"]))
            upper_topic = max(0.0, float(topic_forecast[index]["upper_bound"]))
            result.append(
                TopicForecastPointDTO(
                    period_start=share_forecast[index]["period_start"],
                    forecast_count=topic,
                    lower_bound=lower_topic,
                    upper_bound=upper_topic,
                    forecast_share=share,
                    lower_share=lower_share,
                    upper_share=upper_share,
                    model_name=f"{share_forecast[index]['model_name']} x {topic_forecast[index]['model_name']}",
                    share_model_name=share_forecast[index]["model_name"],
                    count_model_name=topic_forecast[index]["model_name"],
                    backtest_error_mae=topic_forecast[index].get("backtest_error_mae"),
                    backtest_error_mape=topic_forecast[index].get(
                        "backtest_error_mape"
                    ),
                    backtest_error_smape=topic_forecast[index].get(
                        "backtest_error_smape"
                    ),
                )
            )
        return (
            result,
            ForecastQualityDTO(
                activity=ForecastQualityGroupDTO(
                    primary_metric="SMAPE",
                    models=[
                        ForecastQualityModelDTO(**item)
                        for item in topic_result["quality"]
                    ],
                ),
                share=ForecastQualityGroupDTO(
                    primary_metric="MAE",
                    models=[
                        ForecastQualityModelDTO(**item)
                        for item in share_result["quality"]
                    ],
                ),
            ),
        )

    def _includes(
        self, request: TopicAnalyticsInsightRequestDTO, section: str
    ) -> bool:
        return request.sections is None or section in request.sections

    # ! Deprecated
    # def _forecast_series(
    #     self, series: Any, horizon: int, season_length: int = 12
    # ) -> list[dict[str, Any]]:
    #     clean = (
    #         pd.to_numeric(series, errors="coerce").fillna(0).astype(float).sort_index()
    #     )
    #     if clean.empty:
    #         return []
    #     residual_std = (
    #         float(clean.std(ddof=0))
    #         if len(clean) > 1
    #         else max(1.0, float(clean.mean()))
    #     )

    #     def seasonal_naive(train: Any, steps: int) -> Any:
    #         if len(train) >= season_length:
    #             base = train.iloc[-season_length:].to_numpy(dtype=float)
    #             return np.array([base[index % season_length] for index in range(steps)])
    #         value = train.tail(min(6, len(train))).mean() if len(train) else 0.0
    #         return np.repeat(float(value), steps)

    #     def fit_predict(train: Any, steps: int) -> tuple[Any, str]:
    #         train = train.astype(float)
    #         if len(train) >= 60:
    #             try:
    #                 from statsmodels.tsa.statespace.sarimax import SARIMAX

    #                 with warnings.catch_warnings():
    #                     warnings.simplefilter("ignore")
    #                     model = SARIMAX(
    #                         train,
    #                         order=(0, 1, 1),
    #                         seasonal_order=(0, 1, 1, season_length),
    #                         enforce_stationarity=False,
    #                         enforce_invertibility=False,
    #                     ).fit(disp=False)
    #                 return model.forecast(steps).to_numpy(
    #                     dtype=float
    #                 ), "SARIMA(0,1,1)(0,1,1,12)"
    #             except Exception:
    #                 pass
    #         try:
    #             from statsmodels.tsa.holtwinters import ExponentialSmoothing

    #             print("y")
    #             seasonal = "add" if len(train) >= season_length * 2 else None
    #             with warnings.catch_warnings():
    #                 warnings.simplefilter("ignore")
    #                 model = ExponentialSmoothing(
    #                     train,
    #                     trend="add",
    #                     seasonal=seasonal,
    #                     seasonal_periods=season_length if seasonal else None,
    #                     initialization_method="estimated",
    #                 ).fit(optimized=True)
    #             return model.forecast(steps).to_numpy(dtype=float), "ETS/Holt-Winters"
    #         except Exception:
    #             fallback = (
    #                 "seasonal_naive" if len(train) >= season_length else "rolling_mean"
    #             )
    #             return seasonal_naive(train, steps), fallback

    #     backtest_metrics = {"MAE": None, "MAPE": None, "SMAPE": None}
    #     if len(clean) >= 24:
    #         backtest_window = min(12, max(6, len(clean) // 5))
    #         train = clean.iloc[:-backtest_window]
    #         actual = clean.iloc[-backtest_window:]
    #         predicted, _ = fit_predict(train, backtest_window)
    #         predicted = np.clip(predicted, 0, None)
    #         errors = actual.to_numpy(dtype=float) - predicted
    #         residual_std = float(np.std(errors)) if len(errors) else residual_std
    #         backtest_metrics = {
    #             "MAE": float(np.mean(np.abs(errors))),
    #             "MAPE": float(
    #                 np.mean(
    #                     np.abs(errors) / np.maximum(actual.to_numpy(dtype=float), 1)
    #                 )
    #                 * 100
    #             ),
    #             "SMAPE": float(
    #                 np.mean(
    #                     2
    #                     * np.abs(errors)
    #                     / np.maximum(
    #                         np.abs(actual.to_numpy(dtype=float)) + np.abs(predicted), 1
    #                     )
    #                 )
    #                 * 100
    #             ),
    #         }

    #     values, model_name = fit_predict(clean, horizon)
    #     values = np.clip(values, 0, None)
    #     forecast_index = pd.date_range(
    #         clean.index.max() + pd.DateOffset(months=1), periods=horizon, freq="MS"
    #     )
    #     lower = np.clip(values - 1.96 * residual_std, 0, None)
    #     upper = values + 1.96 * residual_std
    #     return [
    #         {
    #             "period_start": forecast_index[index].date(),
    #             "forecast": float(values[index]),
    #             "lower_bound": float(lower[index]),
    #             "upper_bound": float(upper[index]),
    #             "model_name": model_name,
    #             "backtest_error_mae": backtest_metrics["MAE"],
    #             "backtest_error_mape": backtest_metrics["MAPE"],
    #             "backtest_error_smape": backtest_metrics["SMAPE"],
    #         }
    #         for index in range(horizon)
    #     ]

    def _decomposition(
        self, request: TopicAnalyticsInsightRequestDTO
    ) -> list[TopicDecompositionMetricDTO]:
        keyphrase_novelty = self._keyphrase_novelty(request)
        semantic_drift = self._semantic_drift(
            request.topic_id, request.period_start, request.period_end
        )
        return [
            self._metric(
                "keyphrase_novelty",
                "Keyphrase novelty",
                keyphrase_novelty,
                "percent",
                keyphrase_novelty,
            ),
            self._metric(
                "semantic_drift",
                "Semantic drift",
                semantic_drift,
                "score",
                None if semantic_drift is None else min(1.0, semantic_drift / 0.30),
            ),
        ]

    def _keyphrase_novelty(
        self, request: TopicAnalyticsInsightRequestDTO
    ) -> float | None:
        rows = self.session.execute(
            text(
                """
                WITH bounds AS (
                    SELECT
                        CAST(:period_end AS date) AS period_end,
                        (CAST(:period_end AS date) - (CAST(:window_months AS integer) - 1) * interval '1 month')::date AS current_start,
                        (CAST(:period_end AS date) - (CAST(:window_months AS integer)) * interval '1 month')::date AS previous_end,
                        (CAST(:period_end AS date) - (2 * CAST(:window_months AS integer) - 1) * interval '1 month')::date AS previous_start
                )
                SELECT
                    k.value,
                    CASE
                        WHEN p.publication_date BETWEEN b.current_start AND b.period_end THEN 'current'
                        WHEN p.publication_date BETWEEN b.previous_start AND b.previous_end THEN 'previous'
                    END AS window_name
                FROM paper_topics pt
                JOIN papers p ON p.id = pt.paper_id
                JOIN paper_keywords pk ON pk.paper_id = p.id
                JOIN keywords k ON k.id = pk.keyword_id
                CROSS JOIN bounds b
                WHERE pt.topic_id = :topic_id
                  AND p.publication_date BETWEEN b.previous_start AND b.period_end
                """
            ),
            {
                "topic_id": request.topic_id,
                "period_end": request.period_end,
                "window_months": request.comparison_window_months,
            },
        ).mappings()
        current: set[str] = set()
        previous: set[str] = set()
        for row in rows:
            if row["window_name"] == "current":
                current.add(str(row["value"]))
            elif row["window_name"] == "previous":
                previous.add(str(row["value"]))
        if not current:
            return None
        return len(current - previous) / len(current)

    def _semantic_drift(
        self, topic_id: int, period_start: date, period_end: date
    ) -> float | None:
        value = self.session.execute(
            text(
                """
                SELECT AVG(ps.semantic_drift)::float AS semantic_drift
                FROM research_clusters c
                JOIN research_cluster_period_stats ps ON ps.cluster_id = c.id
                WHERE c.cluster_key = :cluster_key
                  AND ps.period_start <= :period_end
                  AND ps.period_end >= :period_start
                  AND ps.semantic_drift IS NOT NULL
                """
            ),
            {
                "cluster_key": f"topic:{topic_id}",
                "period_start": period_start,
                "period_end": period_end,
            },
        ).scalar()
        return None if value is None else float(value)

    def _related_topics(
        self, request: TopicAnalyticsInsightRequestDTO, errors: list[str]
    ) -> list[RelatedTopicDTO]:
        related: OrderedDict[int, RelatedTopicDTO] = OrderedDict()
        for item in self._same_subfield_related(request):
            related[item.topic_id] = item
        for item in self._shared_keyphrase_related(request):
            related[item.topic_id] = item
        try:
            for item in self._embedding_related(request):
                existing = related.get(item.topic_id)
                if existing is None or (item.similarity or 0) > (
                    existing.similarity or 0
                ):
                    related[item.topic_id] = item
        except Exception as exc:
            errors.append(f"Embedding similarity unavailable: {exc}")

        return list(related.values())[: request.max_related]

    def _same_subfield_related(
        self, request: TopicAnalyticsInsightRequestDTO
    ) -> list[RelatedTopicDTO]:
        rows = self.session.execute(
            text(
                """
                WITH topic_meta AS (
                    SELECT subfield_id FROM topics WHERE id = :topic_id
                ),
                recent AS (
                    SELECT s.topic_id, SUM(s.works_count)::bigint AS papers
                    FROM openalex_montly_topic_stats s
                    WHERE s.period_start BETWEEN (CAST(:period_end AS date) - interval '11 months')::date AND :period_end
                    GROUP BY s.topic_id
                )
                SELECT t.id, t.name, COALESCE(r.papers, 0)::bigint AS papers
                FROM topics t
                JOIN topic_meta tm ON tm.subfield_id = t.subfield_id
                LEFT JOIN recent r ON r.topic_id = t.id
                WHERE t.id <> :topic_id
                ORDER BY COALESCE(r.papers, 0) DESC, t.name ASC
                LIMIT :limit
                """
            ),
            {
                "topic_id": request.topic_id,
                "period_end": request.period_end,
                "limit": request.max_related,
            },
        ).mappings()
        return [
            RelatedTopicDTO(
                topic_id=int(row["id"]),
                name=str(row["name"]),
                relation_type="same subfield",
                similarity=None,
                trend_status=None,
            )
            for row in rows
        ]

    def _shared_keyphrase_related(
        self, request: TopicAnalyticsInsightRequestDTO
    ) -> list[RelatedTopicDTO]:
        rows = self.session.execute(
            text(
                """
                WITH selected_papers AS MATERIALIZED (
                    SELECT p.id, COALESCE(p.cited_by_count, 0) AS cited_by_count
                    FROM paper_topics pt
                    JOIN papers p ON p.id = pt.paper_id
                    WHERE pt.topic_id = :topic_id
                      AND p.publication_date BETWEEN :period_start AND :period_end
                ),
                selected_keywords AS MATERIALIZED (
                    SELECT DISTINCT pk.keyword_id
                    FROM selected_papers sp
                    JOIN paper_keywords pk ON pk.paper_id = sp.id
                ),
                matches AS MATERIALIZED (
                    SELECT DISTINCT
                        pt.topic_id,
                        pk.keyword_id,
                        p.id AS paper_id,
                        COALESCE(p.cited_by_count, 0) AS cited_by_count
                    FROM selected_keywords sk
                    JOIN paper_keywords pk ON pk.keyword_id = sk.keyword_id
                    JOIN papers p ON p.id = pk.paper_id
                    JOIN paper_topics pt ON pt.paper_id = p.id
                    WHERE pt.topic_id <> :topic_id
                      AND p.publication_date BETWEEN :period_start AND :period_end
                ),
                related AS MATERIALIZED (
                    SELECT
                        topic_id,
                        COUNT(DISTINCT keyword_id)::int AS shared_count,
                        COUNT(DISTINCT paper_id)::int AS common_papers
                    FROM matches
                    GROUP BY topic_id
                    ORDER BY shared_count DESC, common_papers DESC
                    LIMIT :limit
                ),
                related_citations AS (
                    SELECT selected.topic_id, SUM(selected.cited_by_count)::int AS common_citations
                    FROM (
                        SELECT DISTINCT m.topic_id, m.paper_id, m.cited_by_count
                        FROM matches m
                        JOIN related r ON r.topic_id = m.topic_id
                    ) selected
                    GROUP BY selected.topic_id
                )
                SELECT
                    t.id,
                    t.name,
                    r.shared_count,
                    r.common_papers,
                    COALESCE(rc.common_citations, 0)::int AS common_citations,
                    COALESCE(keywords.values, ARRAY[]::text[]) AS shared_keyphrases
                FROM related r
                JOIN topics t ON t.id = r.topic_id
                LEFT JOIN related_citations rc ON rc.topic_id = r.topic_id
                LEFT JOIN LATERAL (
                    SELECT array_agg(k.value ORDER BY k.value) AS values
                    FROM (
                        SELECT DISTINCT m.keyword_id
                        FROM matches m
                        WHERE m.topic_id = r.topic_id
                        LIMIT 8
                    ) ids
                    JOIN keywords k ON k.id = ids.keyword_id
                ) keywords ON TRUE
                ORDER BY r.shared_count DESC, r.common_papers DESC, t.name ASC
                LIMIT :limit
                """
            ),
            {
                "topic_id": request.topic_id,
                "period_start": request.period_start,
                "period_end": request.period_end,
                "limit": request.max_related,
            },
        ).mappings()
        return [
            RelatedTopicDTO(
                topic_id=int(row["id"]),
                name=str(row["name"]),
                relation_type="shared keyphrases",
                similarity=min(1.0, float(row["shared_count"] or 0) / 10.0),
                shared_keyphrases=list(row["shared_keyphrases"] or []),
                common_papers=int(row["common_papers"] or 0),
                common_citations=int(row["common_citations"] or 0),
                trend_status=None,
            )
            for row in rows
        ]

    def _embedding_related(
        self, request: TopicAnalyticsInsightRequestDTO
    ) -> list[RelatedTopicDTO]:
        if self.qdrant_adapter is None:
            return []
        point = self.qdrant_adapter.retrieve(
            RESEARCH_ENTITIES_COLLECTION,
            [f"topic:{request.topic_id}"],
            with_vectors=True,
        )
        if not point or not point[0].vector:
            return []
        hits = self.qdrant_adapter.search(
            RESEARCH_ENTITIES_COLLECTION,
            point[0].vector,
            top_k=request.max_related + 10,
            filters={"must": [{"key": "entity_type", "match": {"value": "topic"}}]},
        )
        result: list[RelatedTopicDTO] = []
        for hit in hits:
            payload = hit.payload or {}
            topic_id = payload.get("entity_id")
            if topic_id is None:
                continue
            topic_id = int(topic_id)
            if topic_id == request.topic_id:
                continue
            result.append(
                RelatedTopicDTO(
                    topic_id=topic_id,
                    name=str(payload.get("name") or f"Topic {topic_id}"),
                    relation_type="embedding similarity",
                    similarity=float(hit.score),
                    trend_status=None,
                )
            )
        return result[: request.max_related]

    def _metric(
        self,
        key: str,
        label: str,
        value: float | None,
        unit: str,
        normalized: float | None,
    ) -> TopicDecompositionMetricDTO:
        score = None if normalized is None else max(0.0, min(1.0, float(normalized)))
        level = (
            None
            if score is None
            else ("high" if score >= 0.66 else "medium" if score >= 0.33 else "low")
        )
        return TopicDecompositionMetricDTO(
            key=key,
            label=label,
            value=value,
            unit=unit,
            normalized=score,
            level=level,
        )


__all__ = ["TopicAnalyticsFacade"]
