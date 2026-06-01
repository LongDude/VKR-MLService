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
