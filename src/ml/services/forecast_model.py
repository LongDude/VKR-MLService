from __future__ import annotations

import math
import warnings
from dataclasses import dataclass
from typing import Any, Literal, cast

import numpy as np
import pandas as pd

SeriesKind = Literal["count", "share"]
TransformKind = Literal["none", "log1p", "logit"]
ModelFamily = Literal[
    "sarimax",
    "ets",
    "seasonal_naive",
    "rolling_mean",
    "seasonal_drift",
    "linear_trend",
]


@dataclass(frozen=True)
class ForecastCandidate:
    family: ModelFamily
    transform: TransformKind = "none"
    order: tuple[int, int, int] | None = None
    seasonal_order: tuple[int, int, int, int] | None = None
    use_exog: bool = False
    seasonal: bool = False
    damped_trend: bool = True

    @property
    def name(self) -> str:
        if self.family == "sarimax":
            order = self.order or (0, 0, 0)
            seasonal_order = self.seasonal_order or (0, 0, 0, 0)
            exog = "+exog(q2,q4)" if self.use_exog else ""
            transform = f"+{self.transform}" if self.transform != "none" else ""
            return f"SARIMAX{order}{seasonal_order}{transform}{exog}"

        if self.family == "ets":
            seasonal = "+seasonal" if self.seasonal else ""
            damped = "+damped" if self.damped_trend else ""
            transform = f"+{self.transform}" if self.transform != "none" else ""
            return f"ETS{seasonal}{damped}{transform}"

        if self.family == "seasonal_drift":
            return "seasonal_drift"

        if self.family == "linear_trend":
            return "linear_trend"

        return self.family


@dataclass(frozen=True)
class CandidateScore:
    candidate: ForecastCandidate
    mae: float | None
    mape: float | None
    smape: float | None
    residual_std: float
    folds: int


@dataclass(frozen=True)
class ForecastValues:
    mean: Any
    lower: Any | None = None
    upper: Any | None = None


class PublicationForecastService:
    """
    Прогнозирование месячных рядов публикационной активности.

    kind="count":
        неотрицательный ряд количества публикаций.
        Для SARIMAX/ETS по умолчанию используется log1p.

    kind="share":
        доля topic_count / subfield_count в диапазоне [0, 1].
        Для SARIMAX/ETS используется logit, чтобы прогноз не выходил
        за допустимый интервал.
    """

    def __init__(
        self,
        season_length: int = 12,
        min_backtest_points: int = 24,
        min_sarimax_points: int = 36,
        max_sarimax_candidates: int = 24,
        winsorize_count_quantile: float | None = None,
        share_epsilon: float = 1e-4,
        near_tie_relative_tolerance: float = 0.05,
    ) -> None:
        self.season_length = season_length
        self.min_backtest_points = min_backtest_points
        self.min_sarimax_points = min_sarimax_points
        self.max_sarimax_candidates = max_sarimax_candidates
        self.winsorize_count_quantile = winsorize_count_quantile
        self.share_epsilon = share_epsilon
        self.near_tie_relative_tolerance = near_tie_relative_tolerance

    def forecast_series(
        self,
        series: Any,
        horizon: int,
        kind: SeriesKind = "count",
    ) -> list[dict[str, Any]]:
        if horizon <= 0:
            return []

        clean = self._prepare_series(series, kind)
        if clean.empty:
            return []

        selected = self._select_candidate(clean, kind, horizon)

        try:
            forecast = self._fit_forecast(
                train=clean,
                steps=horizon,
                candidate=selected.candidate,
                kind=kind,
                interval_residual_std=selected.residual_std,
                with_interval=True,
            )
        except Exception:
            selected = self._fallback_score(clean, kind)
            forecast = self._fit_forecast(
                train=clean,
                steps=horizon,
                candidate=selected.candidate,
                kind=kind,
                interval_residual_std=selected.residual_std,
                with_interval=True,
            )

        values = self._constrain(forecast.mean, kind)
        lower = self._constrain(forecast.lower, kind)
        upper = self._constrain(forecast.upper, kind)

        forecast_index = pd.date_range(
            clean.index.max() + pd.DateOffset(months=1),
            periods=horizon,
            freq="MS",
        )

        return [
            {
                "period_start": forecast_index[index].date(),
                "forecast": float(values[index]),
                "lower_bound": float(lower[index]),
                "upper_bound": float(upper[index]),
                "model_name": selected.candidate.name,
                "backtest_error_mae": selected.mae,
                "backtest_error_mape": selected.mape,
                "backtest_error_smape": selected.smape,
            }
            for index in range(horizon)
        ]

    def _forecast_series(
        self,
        series: Any,
        horizon: int,
        season_length: int | None = None,
        kind: SeriesKind = "count",
    ) -> list[dict[str, Any]]:
        if season_length is not None and season_length != self.season_length:
            local = PublicationForecastService(
                season_length=season_length,
                min_backtest_points=self.min_backtest_points,
                min_sarimax_points=self.min_sarimax_points,
                max_sarimax_candidates=self.max_sarimax_candidates,
                winsorize_count_quantile=self.winsorize_count_quantile,
                share_epsilon=self.share_epsilon,
            )
            return local.forecast_series(series, horizon, kind=kind)

        return self.forecast_series(series, horizon, kind=kind)

    def _prepare_series(self, series: Any, kind: SeriesKind) -> Any:

        raw = pd.Series(series).copy()
        if raw.empty:
            return pd.Series(dtype=float)

        index = pd.to_datetime(raw.index, errors="coerce")
        valid_index = ~pd.isna(index)

        raw = raw.loc[valid_index]
        raw.index = index[valid_index]

        values = pd.to_numeric(raw, errors="coerce")
        if values.dropna().empty:
            return pd.Series(dtype=float)

        # clarification for PyLance
        values.index = pd.DatetimeIndex(values.index).to_period("M").to_timestamp()

        if kind == "count":
            monthly = values.groupby(level=0).sum(min_count=1).astype(float)
        else:
            monthly = values.groupby(level=0).mean().astype(float)

        if monthly.dropna().empty:
            return pd.Series(dtype=float)

        monthly = monthly.sort_index()

        full_index = pd.date_range(
            monthly.index.min(),
            monthly.index.max(),
            freq="MS",
        )
        monthly = monthly.reindex(full_index)

        if kind == "count":
            monthly = monthly.fillna(0.0).clip(lower=0.0)

            if (
                self.winsorize_count_quantile is not None
                and len(monthly) >= self.season_length * 2
            ):
                upper = float(monthly.quantile(self.winsorize_count_quantile))
                if np.isfinite(upper) and upper > 0:
                    monthly = monthly.clip(upper=upper)
        else:
            monthly = monthly.interpolate(method="time").ffill().bfill()
            monthly = monthly.clip(lower=0.0, upper=1.0)

        return monthly.astype(float)

    def _select_candidate(
        self,
        clean: Any,
        kind: SeriesKind,
        horizon: int,
    ) -> CandidateScore:
        fallback = self._fallback_score(clean, kind)

        if len(clean) < self.min_backtest_points:
            return fallback

        splits = self._backtest_splits(len(clean), horizon)
        if not splits:
            return fallback

        scores: list[CandidateScore] = []

        for candidate in self._candidate_grid(len(clean), kind):
            score = self._backtest_candidate(clean, candidate, splits, kind)
            if score is not None:
                primary = self._primary_error(score, kind)
                if np.isfinite(primary):
                    scores.append(score)

        if not scores:
            return fallback

        best_primary = min(self._primary_error(score, kind) for score in scores)

        # Не даем rolling_mean/seasonal_naive выигрывать микроскопические tie.
        eligible = [
            score
            for score in scores
            if self._primary_error(score, kind)
            <= best_primary * (1.0 + self.near_tie_relative_tolerance)
        ]

        return sorted(
            eligible,
            key=lambda score: (
                self._model_priority(score.candidate, kind),
                self._primary_error(score, kind),
                self._candidate_complexity(score.candidate),
            ),
        )[0]

    def _candidate_grid(
        self,
        n_points: int,
        kind: SeriesKind,
    ) -> list[ForecastCandidate]:
        candidates: list[ForecastCandidate] = []

        default_transform: TransformKind = "logit" if kind == "share" else "log1p"

        # Локальный тренд нужен, чтобы share не превращался в константу.
        candidates.append(
            ForecastCandidate(
                family="linear_trend",
                transform=default_transform,
            )
        )

        # Сезонность с поправкой на изменение уровня.
        if n_points >= self.season_length * 2:
            candidates.append(
                ForecastCandidate(
                    family="seasonal_drift",
                    transform="none",
                )
            )

        # ETS без damped_trend важен для share с ростом.
        candidates.append(
            ForecastCandidate(
                family="ets",
                transform=default_transform,
                seasonal=False,
                damped_trend=False,
            )
        )

        candidates.append(
            ForecastCandidate(
                family="ets",
                transform=default_transform,
                seasonal=n_points >= self.season_length * 2,
                damped_trend=True,
            )
        )

        if kind == "count":
            candidates.append(
                ForecastCandidate(
                    family="ets",
                    transform="none",
                    seasonal=n_points >= self.season_length * 2,
                    damped_trend=True,
                )
            )

        candidates.append(ForecastCandidate(family="seasonal_naive"))
        candidates.append(ForecastCandidate(family="rolling_mean"))

        if n_points < self.min_sarimax_points:
            return candidates

        orders = [
            (0, 1, 1),
            (1, 1, 0),
            (1, 1, 1),
            (1, 0, 1),
        ]

        seasonal_orders = [
            (0, 1, 1, self.season_length),
            (1, 1, 0, self.season_length),
            (0, 0, 1, self.season_length),
            (1, 0, 1, self.season_length),
        ]

        added = 0

        for order in orders:
            for seasonal_order in seasonal_orders:
                for use_exog in [True, False]:
                    candidates.append(
                        ForecastCandidate(
                            family="sarimax",
                            transform=default_transform,
                            order=order,
                            seasonal_order=seasonal_order,
                            use_exog=use_exog,
                        )
                    )

                    added += 1
                    if added >= self.max_sarimax_candidates:
                        return candidates

        return candidates

    def _backtest_splits(
        self,
        n_points: int,
        horizon: int,
    ) -> list[tuple[int, int]]:
        validation_size = max(3, min(12, int(horizon)))

        min_train = max(
            self.season_length + validation_size,
            self.min_backtest_points - validation_size,
        )

        splits: list[tuple[int, int]] = []

        train_end = n_points - validation_size

        while train_end >= min_train and len(splits) < 4:
            validation_end = train_end + validation_size
            splits.append((train_end, validation_end))
            train_end -= validation_size

        return list(reversed(splits))

    def _backtest_candidate(
        self,
        clean: Any,
        candidate: ForecastCandidate,
        splits: list[tuple[int, int]],
        kind: SeriesKind,
    ) -> CandidateScore | None:

        fold_mae: list[float] = []
        fold_mape: list[float] = []
        fold_smape: list[float] = []
        residuals: list[float] = []

        for train_end, validation_end in splits:
            train = clean.iloc[:train_end]
            actual = clean.iloc[train_end:validation_end]
            steps = len(actual)

            if steps <= 0 or not self._can_fit(candidate, len(train)):
                continue

            try:
                predicted = self._fit_forecast(
                    train=train,
                    steps=steps,
                    candidate=candidate,
                    kind=kind,
                    interval_residual_std=None,
                    with_interval=False,
                ).mean
            except Exception:
                continue

            predicted = self._constrain(predicted, kind)
            actual_values = actual.to_numpy(dtype=float)

            if len(predicted) != len(actual_values):
                continue

            metrics = self._metrics(actual_values, predicted, kind)

            fold_mae.append(metrics["MAE"])
            fold_mape.append(metrics["MAPE"])
            fold_smape.append(metrics["SMAPE"])
            residuals.extend((actual_values - predicted).tolist())

        if not fold_mae:
            return None

        residual_std = float(np.std(np.asarray(residuals, dtype=float), ddof=0))

        if not np.isfinite(residual_std) or residual_std <= 0:
            residual_std = self._default_residual_std(clean, kind)

        return CandidateScore(
            candidate=candidate,
            mae=float(np.mean(fold_mae)),
            mape=float(np.mean(fold_mape)),
            smape=float(np.mean(fold_smape)),
            residual_std=residual_std,
            folds=len(fold_mae),
        )

    def _fit_forecast(
        self,
        train: Any,
        steps: int,
        candidate: ForecastCandidate,
        kind: SeriesKind,
        interval_residual_std: float | None,
        with_interval: bool,
    ) -> ForecastValues:

        if steps <= 0:
            empty = np.asarray([], dtype=float)
            return ForecastValues(empty, empty, empty)

        if candidate.family == "sarimax":
            return self._fit_sarimax(
                train=train,
                steps=steps,
                candidate=candidate,
                kind=kind,
                interval_residual_std=interval_residual_std,
                with_interval=with_interval,
            )

        if candidate.family == "ets":
            return self._fit_ets(
                train=train,
                steps=steps,
                candidate=candidate,
                kind=kind,
                interval_residual_std=interval_residual_std,
                with_interval=with_interval,
            )

        if candidate.family == "seasonal_drift":
            mean = self._seasonal_drift(train, steps, kind)
        elif candidate.family == "linear_trend":
            mean = self._linear_trend(train, steps, kind)
        elif candidate.family == "seasonal_naive":
            mean = self._seasonal_naive(train, steps)
        else:
            mean = self._rolling_mean(train, steps)

        lower, upper = self._residual_interval(
            mean,
            interval_residual_std,
            train,
            kind,
        )

        return ForecastValues(
            mean=mean,
            lower=lower if with_interval else None,
            upper=upper if with_interval else None,
        )

    def _fit_sarimax(
        self,
        train: Any,
        steps: int,
        candidate: ForecastCandidate,
        kind: SeriesKind,
        interval_residual_std: float | None,
        with_interval: bool,
    ) -> ForecastValues:

        from statsmodels.tsa.statespace.sarimax import SARIMAX

        transformed = self._transform(
            train.to_numpy(dtype=float),
            candidate.transform,
        )
        y = pd.Series(transformed, index=train.index)

        future_index = self._future_index(train.index, steps)

        exog_train = self._calendar_exog(train.index) if candidate.use_exog else None
        exog_future = self._calendar_exog(future_index) if candidate.use_exog else None

        fitted: SARIMAX | None = None
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")

            fitted = SARIMAX(  # type: ignore
                y,
                exog=exog_train,
                order=candidate.order or (0, 1, 1),
                seasonal_order=candidate.seasonal_order
                or (0, 1, 1, self.season_length),
                enforce_stationarity=False,
                enforce_invertibility=False,
                concentrate_scale=True,
            ).fit(disp=False, maxiter=200)  # type: ignore

        if fitted is None:
            raise RuntimeError("Unable to initialize forecast model")

        if with_interval:
            prediction = fitted.get_forecast(  # type: ignore
                steps=steps,
                exog=exog_future,
            )

            mean_t = cast(np.ndarray, prediction.predicted_mean.to_numpy(dtype=float))  # type: ignore

            try:
                conf_int = prediction.conf_int(alpha=0.05)
                lower_t = cast(np.ndarray, conf_int.iloc[:, 0].to_numpy(dtype=float))  # type: ignore
                upper_t = cast(np.ndarray, conf_int.iloc[:, 1].to_numpy(dtype=float))  # type: ignore

                if not np.all(np.isfinite(mean_t)):
                    raise ValueError("Non-finite SARIMAX forecast")
                if not np.all(np.isfinite(lower_t)):
                    raise ValueError("Non-finite SARIMAX forecast")
                if not np.all(np.isfinite(upper_t)):
                    raise ValueError("Non-finite SARIMAX forecast")

                mean = self._inverse_transform(mean_t, candidate.transform)
                lower = self._inverse_transform(lower_t, candidate.transform)
                upper = self._inverse_transform(upper_t, candidate.transform)

                return ForecastValues(
                    mean=self._constrain(mean, kind),
                    lower=self._constrain(lower, kind),
                    upper=self._constrain(upper, kind),
                )

            except Exception:
                mean = self._inverse_transform(mean_t, candidate.transform)
                lower, upper = self._residual_interval(
                    mean,
                    interval_residual_std,
                    train,
                    kind,
                )
                return ForecastValues(mean, lower, upper)

        forecast_t = fitted.forecast(  # type: ignore
            steps=steps,
            exog=exog_future,
        ).to_numpy(dtype=float)

        if not np.all(np.isfinite(forecast_t)):
            raise ValueError("Non-finite SARIMAX forecast")

        mean = self._inverse_transform(forecast_t, candidate.transform)

        if not np.all(np.isfinite(mean)):
            raise ValueError("Non-finite inverse-transformed SARIMAX forecast")

        return ForecastValues(mean=mean)

    def _fit_ets(
        self,
        train: Any,
        steps: int,
        candidate: ForecastCandidate,
        kind: SeriesKind,
        interval_residual_std: float | None,
        with_interval: bool,
    ) -> ForecastValues:

        from statsmodels.tsa.holtwinters import ExponentialSmoothing

        transformed = self._transform(
            train.to_numpy(dtype=float),
            candidate.transform,
        )
        y = pd.Series(transformed, index=train.index)

        seasonal = (
            "add"
            if candidate.seasonal and len(train) >= self.season_length * 2
            else None
        )
        trend = "add" if len(train) >= max(8, self.season_length) else None
        damped_trend = bool(candidate.damped_trend and trend is not None)

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")

            fitted = ExponentialSmoothing(
                y,
                trend=trend,
                damped_trend=damped_trend,
                seasonal=seasonal,
                seasonal_periods=self.season_length if seasonal else None,
                initialization_method="estimated",
            ).fit(optimized=True, use_brute=False)

        forecast_t = fitted.forecast(steps).to_numpy(dtype=float)
        mean = self._inverse_transform(forecast_t, candidate.transform)

        if not with_interval:
            return ForecastValues(mean=mean)

        lower, upper = self._residual_interval(
            mean,
            interval_residual_std,
            train,
            kind,
        )

        return ForecastValues(mean=mean, lower=lower, upper=upper)

    def _seasonal_drift(
        self,
        train: Any,
        steps: int,
        kind: SeriesKind,
    ) -> Any:
        if len(train) < self.season_length * 2:
            return self._seasonal_naive(train, steps)

        base = self._seasonal_naive(train, steps)

        window = min(6, self.season_length, len(train) - self.season_length)

        recent = train.iloc[-window:].to_numpy(dtype=float)
        previous = train.iloc[
            -self.season_length - window : -self.season_length
        ].to_numpy(dtype=float)

        if len(recent) != len(previous) or len(recent) == 0:
            return base

        if kind == "share":
            delta = float(np.mean(recent - previous))
            adjusted = base + 0.65 * delta

            return self._constrain(adjusted, kind)

        previous_mean = max(float(np.mean(previous)), 1.0)
        recent_mean = float(np.mean(recent))

        ratio = recent_mean / previous_mean
        ratio = float(np.clip(ratio, 0.65, 1.35))

        # shrinkage: не переносим весь последний шок в будущее
        ratio = 1.0 + 0.65 * (ratio - 1.0)

        adjusted = base * ratio

        return self._constrain(adjusted, kind)

    def _linear_trend(
        self,
        train: Any,
        steps: int,
        kind: SeriesKind,
    ) -> Any:
        if len(train) < 6:
            return self._rolling_mean(train, steps)

        window = min(12, len(train))
        values = train.tail(window).to_numpy(dtype=float)

        transform: TransformKind = "logit" if kind == "share" else "log1p"

        y = self._transform(values, transform)
        x = np.arange(window, dtype=float)

        mask = np.isfinite(x) & np.isfinite(y)
        if np.sum(mask) < 3:
            return self._rolling_mean(train, steps)

        slope, intercept = np.polyfit(x[mask], y[mask], deg=1)

        # Защита от переэкстраполяции по одному резкому кварталу.
        if kind == "share":
            slope = float(np.clip(slope, -0.20, 0.20))
            damp = 0.55
        else:
            slope = float(np.clip(slope, -0.08, 0.08))
            damp = 0.45

        future_x = np.arange(window, window + steps, dtype=float)
        forecast_t = intercept + damp * slope * future_x

        mean = self._inverse_transform(forecast_t, transform)

        return self._constrain(mean, kind)

    def _can_fit(self, candidate: ForecastCandidate, n_points: int) -> bool:
        if candidate.family in {"rolling_mean", "seasonal_naive"}:
            return n_points >= 1

        if candidate.family == "seasonal_drift":
            return n_points >= self.season_length * 2

        if candidate.family == "linear_trend":
            return n_points >= 6

        if candidate.family == "ets":
            if candidate.seasonal:
                return n_points >= self.season_length * 2

            return n_points >= 4

        if candidate.family == "sarimax":
            return n_points >= self.min_sarimax_points

        return False

    def _fallback_score(self, clean: Any, kind: SeriesKind) -> CandidateScore:
        candidate = ForecastCandidate(
            family="seasonal_naive"
            if len(clean) >= self.season_length
            else "rolling_mean"
        )

        return CandidateScore(
            candidate=candidate,
            mae=None,
            mape=None,
            smape=None,
            residual_std=self._default_residual_std(clean, kind),
            folds=0,
        )

    def _default_residual_std(self, clean: Any, kind: SeriesKind) -> float:

        values = clean.to_numpy(dtype=float)

        if len(values) >= self.season_length + 1:
            residuals = values[self.season_length :] - values[: -self.season_length]
            std = float(np.std(residuals, ddof=0))
        elif len(values) > 1:
            std = float(np.std(np.diff(values), ddof=0))
        else:
            std = float(abs(values[0])) if len(values) else 0.0

        if np.isfinite(std) and std > 0:
            return std

        if kind == "share":
            return 0.05

        return max(1.0, float(np.mean(values)) if len(values) else 1.0)

    def _seasonal_naive(self, train: Any, steps: int) -> Any:

        if len(train) >= self.season_length:
            base = train.iloc[-self.season_length :].to_numpy(dtype=float)

            return np.asarray(
                [base[index % self.season_length] for index in range(steps)],
                dtype=float,
            )

        return self._rolling_mean(train, steps)

    def _rolling_mean(self, train: Any, steps: int) -> Any:

        if len(train) == 0:
            return np.zeros(steps, dtype=float)

        window = min(6, len(train))
        value = float(train.tail(window).mean())

        return np.repeat(value, steps).astype(float)

    def _residual_interval(
        self,
        mean: Any,
        residual_std: float | None,
        train: Any,
        kind: SeriesKind,
    ) -> tuple[Any, Any]:

        values = np.asarray(mean, dtype=float)
        std = residual_std

        if std is None or not np.isfinite(std) or std <= 0:
            std = self._default_residual_std(train, kind)

        horizon_scale = np.sqrt(
            1.0 + np.arange(len(values), dtype=float) / max(1, self.season_length)
        )
        margin = 1.96 * float(std) * horizon_scale

        lower = values - margin
        upper = values + margin

        return self._constrain(lower, kind), self._constrain(upper, kind)

    def _metrics(
        self,
        actual: Any,
        predicted: Any,
        kind: SeriesKind,
    ) -> dict[str, float]:

        actual = np.asarray(actual, dtype=float)
        predicted = np.asarray(predicted, dtype=float)

        mask = np.isfinite(actual) & np.isfinite(predicted)

        if not np.any(mask):
            return {
                "MAE": math.inf,
                "MAPE": math.inf,
                "SMAPE": math.inf,
            }

        actual = actual[mask]
        predicted = predicted[mask]

        errors = actual - predicted

        zero_guard = 0.01 if kind == "share" else 1.0

        mae = float(np.mean(np.abs(errors)))
        denominator_mape = np.maximum(
            np.abs(actual),
            zero_guard,
        )
        mape = float(np.mean(np.abs(errors) / denominator_mape) * 100.0)

        denominator_smape = np.maximum(
            np.abs(actual) + np.abs(predicted),
            zero_guard,
        )

        smape_values = (2.0 * np.abs(errors)) / denominator_smape

        smape_values = smape_values[np.isfinite(smape_values)]

        if len(smape_values) == 0:
            smape = math.inf
        else:
            smape = float(np.mean(smape_values) * 100.0)

        return {
            "MAE": mae,
            "MAPE": mape,
            "SMAPE": smape,
        }

    def _calendar_exog(self, index: Any) -> Any:

        month = index.month
        quarter = ((month - 1) // 3) + 1

        return pd.DataFrame(
            {
                "q2": (quarter == 2).astype(float),
                "q4": (quarter == 4).astype(float),
                # "jan": (month == 1).astype(float),
                # "may_jun": ((month == 5) | (month == 6)).astype(float),
                # "dec": (month == 12).astype(float),
            },
            index=index,
        )

    def _transform(self, values: Any, transform: TransformKind) -> Any:

        arr = np.asarray(values, dtype=float)

        if transform == "log1p":
            return np.log1p(np.clip(arr, 0.0, None))

        if transform == "logit":
            clipped = np.clip(
                arr,
                self.share_epsilon,
                1.0 - self.share_epsilon,
            )

            return np.log(clipped / (1.0 - clipped))

        return arr

    def _inverse_transform(self, values: Any, transform: TransformKind) -> Any:

        arr = np.asarray(values, dtype=float)

        if transform == "log1p":
            # float64 safety bound
            arr = np.clip(arr, -50.0, 50.0)
            return np.expm1(arr)

        if transform == "logit":
            clipped = np.clip(arr, -50.0, 50.0)
            return 1.0 / (1.0 + np.exp(-clipped))

        return arr

    def _constrain(self, values: Any, kind: SeriesKind) -> Any:

        arr = np.asarray(values, dtype=float)

        if kind == "share":
            return np.clip(arr, 0.0, 1.0)

        return np.clip(arr, 0.0, None)

    def _future_index(self, index: Any, steps: int) -> Any:

        return pd.date_range(
            index.max() + pd.DateOffset(months=1),
            periods=steps,
            freq="MS",
        )

    def _quarter_exog(self, index: Any) -> Any:

        month = index.month
        quarter = ((month - 1) // 3) + 1

        return pd.DataFrame(
            {
                "q2": (quarter == 2).astype(float),
                "q4": (quarter == 4).astype(float),
            },
            index=index,
        )

    def _candidate_complexity(self, candidate: ForecastCandidate) -> int:
        if candidate.family == "sarimax":
            order_complexity = sum(candidate.order or (0, 0, 0))
            seasonal_complexity = sum((candidate.seasonal_order or (0, 0, 0, 0))[:3])
            exog_complexity = 2 if candidate.use_exog else 0

            return 10 + order_complexity + seasonal_complexity + exog_complexity

        if candidate.family == "ets":
            return 5 + int(candidate.seasonal) + int(candidate.damped_trend)

        if candidate.family == "seasonal_naive":
            return 2

        return 1

    def _primary_error(self, score: CandidateScore, kind: SeriesKind) -> float:
        value = score.mae if kind == "share" else score.smape

        if value is None or not np.isfinite(value):
            return math.inf

        return float(value)

    def _model_priority(
        self,
        candidate: ForecastCandidate,
        kind: SeriesKind,
    ) -> int:
        if kind == "share":
            priority = {
                "linear_trend": 0,
                "ets": 1,
                "sarimax": 2,
                "seasonal_drift": 3,
                "seasonal_naive": 4,
                "rolling_mean": 5,
            }
        else:
            priority = {
                "sarimax": 0,
                "ets": 1,
                "seasonal_drift": 2,
                "seasonal_naive": 3,
                "linear_trend": 4,
                "rolling_mean": 5,
            }

        return priority.get(candidate.family, 99)


__all__ = ["PublicationForecastService"]
