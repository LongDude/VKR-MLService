import pandas as pd

from ml.services.forecast_model import (
    CandidateScore,
    ForecastCandidate,
    PublicationForecastService,
)


def score(
    family: str,
    *,
    mae: float,
    mape: float,
    smape: float,
    order: tuple[int, int, int] | None = None,
) -> CandidateScore:
    return CandidateScore(
        candidate=ForecastCandidate(family=family, order=order),  # type: ignore[arg-type]
        mae=mae,
        mape=mape,
        smape=smape,
        residual_std=1.0,
        folds=2,
    )


def test_count_quality_is_deduplicated_sorted_and_limited() -> None:
    service = PublicationForecastService()

    quality = service._quality_scores(  # noqa: SLF001
        [
            score("sarimax", mae=4.0, mape=8.0, smape=7.0, order=(0, 0, 1)),
            score("sarimax", mae=3.0, mape=6.0, smape=5.0, order=(1, 0, 1)),
            score("ets", mae=2.0, mape=5.0, smape=4.0),
            score("rolling_mean", mae=6.0, mape=10.0, smape=9.0),
            score("linear_trend", mae=5.0, mape=9.0, smape=8.0),
        ],
        "count",
    )

    assert [item["family"] for item in quality] == [
        "ETS",
        "SARIMAX",
        "linear_trend",
    ]
    assert quality[1]["smape"] == 5.0
    assert all("(" not in str(item["family"]) for item in quality)


def test_share_quality_uses_mae_as_primary_metric() -> None:
    service = PublicationForecastService()

    quality = service._quality_scores(  # noqa: SLF001
        [
            score("ets", mae=0.10, mape=4.0, smape=3.0),
            score("linear_trend", mae=0.02, mape=8.0, smape=7.0),
            score("rolling_mean", mae=0.05, mape=2.0, smape=1.0),
        ],
        "share",
    )

    assert [item["family"] for item in quality] == [
        "linear_trend",
        "rolling_mean",
        "ETS",
    ]


def test_quality_includes_selected_fallback_without_backtest_metrics() -> None:
    service = PublicationForecastService()
    selected = CandidateScore(
        candidate=ForecastCandidate(family="rolling_mean"),
        mae=None,
        mape=None,
        smape=None,
        residual_std=1.0,
        folds=0,
    )

    quality = service._quality_scores([], "count", selected)  # noqa: SLF001

    assert quality == [
        {
            "family": "rolling_mean",
            "mae": None,
            "mape": None,
            "smape": None,
            "selected": True,
        }
    ]


def test_quality_keeps_selected_family_within_top_three() -> None:
    service = PublicationForecastService()
    scores = [
        score("ets", mae=2.0, mape=5.0, smape=4.0),
        score("sarimax", mae=3.0, mape=6.0, smape=5.0),
        score("linear_trend", mae=5.0, mape=9.0, smape=8.0),
        score("rolling_mean", mae=6.0, mape=10.0, smape=9.0),
    ]

    quality = service._quality_scores(scores, "count", scores[-1])  # noqa: SLF001

    assert len(quality) == 3
    assert quality[-1]["family"] == "rolling_mean"
    assert quality[-1]["selected"] is True


def test_short_series_exposes_used_fallback_model() -> None:
    service = PublicationForecastService()
    series = pd.Series(
        [2.0, 3.0, 4.0],
        index=pd.date_range("2025-01-01", periods=3, freq="MS"),
    )

    result = service.forecast_series_with_quality(series, horizon=2)

    assert result["quality"] == [
        {
            "family": "rolling_mean",
            "mae": None,
            "mape": None,
            "smape": None,
            "selected": True,
        }
    ]
    assert {point["model_name"] for point in result["forecast"]} == {"rolling_mean"}
