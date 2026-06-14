"""Temporal multi-horizon price forecast with leakage-safe feature building."""
from __future__ import annotations

import math
import os
from datetime import date, datetime, timedelta
from typing import Any, Optional

import joblib
import numpy as np


FEATURES = [
    "horizon_days",
    "log_anchor_price",
    "history_points_log",
    "history_span_days_log",
    "last_gap_days",
    "latest_listing_count_log",
    "listing_count_change",
    "ratio_lag_1",
    "ratio_lag_2",
    "ratio_lag_3",
    "ratio_lag_7",
    "mean_ratio_3",
    "mean_ratio_7",
    "median_ratio_7",
    "median_ratio_14",
    "std_ratio_7",
    "std_ratio_14",
    "return_1",
    "return_2",
    "return_7",
    "slope_pct_per_day_7",
    "slope_pct_per_day_14",
]

HORIZON_BUCKETS = ((1, 3), (4, 7), (8, 14), (15, 30))


def horizon_bucket(day: int) -> str:
    for lower, upper in HORIZON_BUCKETS:
        if lower <= int(day) <= upper:
            return f"{lower}-{upper}"
    return "31+"


def _as_date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return datetime.fromisoformat(str(value)).date()


def normalize_history(history: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for raw in history:
        price = float(raw.get("avg_price", raw.get("avg_price_vnd", 0)) or 0)
        if price <= 0:
            continue
        rows.append({
            "record_date": _as_date(raw["record_date"]),
            "avg_price": price,
            "listing_count": float(raw.get("listing_count") or 0),
        })
    rows.sort(key=lambda row: row["record_date"])
    by_date = {row["record_date"]: row for row in rows}
    return [by_date[key] for key in sorted(by_date)]


def _ratio_at(prices: list[float], lag: int, anchor: float) -> float:
    if len(prices) <= lag or anchor <= 0:
        return 1.0
    return float(prices[-1 - lag] / anchor)


def _window_ratio(prices: list[float], window: int, anchor: float, fn) -> float:
    values = prices[-window:]
    return float(fn(values) / anchor) if values and anchor > 0 else 1.0


def _return(prices: list[float], lag: int) -> float:
    if len(prices) <= lag or prices[-1 - lag] <= 0:
        return 0.0
    return float(prices[-1] / prices[-1 - lag] - 1.0)


def _slope_pct(rows: list[dict[str, Any]], window: int) -> float:
    sample = rows[-window:]
    if len(sample) < 2:
        return 0.0
    slopes = []
    for left in range(len(sample)):
        for right in range(left + 1, len(sample)):
            days = (sample[right]["record_date"] - sample[left]["record_date"]).days
            base = sample[left]["avg_price"]
            if days > 0 and base > 0:
                slopes.append((sample[right]["avg_price"] / base - 1.0) / days)
    return float(np.median(slopes)) if slopes else 0.0


def build_feature_row(
    history: list[dict[str, Any]],
    *,
    horizon_days: int,
) -> dict[str, float]:
    rows = normalize_history(history)
    if not rows:
        raise ValueError("Forecast features require at least one history row.")
    prices = [row["avg_price"] for row in rows]
    anchor = prices[-1]
    counts = [row["listing_count"] for row in rows]
    span = max((rows[-1]["record_date"] - rows[0]["record_date"]).days, 0)
    last_gap = (
        max((rows[-1]["record_date"] - rows[-2]["record_date"]).days, 0)
        if len(rows) > 1 else 0
    )
    count_change = 0.0
    if len(counts) > 1:
        count_change = (counts[-1] - counts[-2]) / max(counts[-2], 1.0)

    ratios_7 = np.asarray(prices[-7:], dtype=float) / anchor
    ratios_14 = np.asarray(prices[-14:], dtype=float) / anchor
    return {
        "horizon_days": float(horizon_days),
        "log_anchor_price": float(math.log1p(anchor)),
        "history_points_log": float(math.log1p(len(rows))),
        "history_span_days_log": float(math.log1p(span)),
        "last_gap_days": float(last_gap),
        "latest_listing_count_log": float(math.log1p(max(counts[-1], 0))),
        "listing_count_change": float(np.clip(count_change, -5.0, 5.0)),
        "ratio_lag_1": _ratio_at(prices, 1, anchor),
        "ratio_lag_2": _ratio_at(prices, 2, anchor),
        "ratio_lag_3": _ratio_at(prices, 3, anchor),
        "ratio_lag_7": _ratio_at(prices, 7, anchor),
        "mean_ratio_3": _window_ratio(prices, 3, anchor, np.mean),
        "mean_ratio_7": _window_ratio(prices, 7, anchor, np.mean),
        "median_ratio_7": _window_ratio(prices, 7, anchor, np.median),
        "median_ratio_14": _window_ratio(prices, 14, anchor, np.median),
        "std_ratio_7": float(np.std(ratios_7)),
        "std_ratio_14": float(np.std(ratios_14)),
        "return_1": _return(prices, 1),
        "return_2": _return(prices, 2),
        "return_7": _return(prices, 7),
        "slope_pct_per_day_7": _slope_pct(rows, 7),
        "slope_pct_per_day_14": _slope_pct(rows, 14),
    }


class TemporalPriceForecaster:
    def __init__(self, artifact: dict[str, Any]):
        self.artifact = artifact
        self.model = artifact["model"]
        self.lower_model = artifact.get("lower_model")
        self.upper_model = artifact.get("upper_model")
        self.features = artifact.get("features") or FEATURES
        self.metadata = artifact.get("metadata") or {}

    @classmethod
    def load(cls, path: str) -> "TemporalPriceForecaster":
        return cls(joblib.load(path))

    def predict(
        self,
        history: list[dict[str, Any]],
        *,
        horizon_days: int,
        anchor_date: date,
        max_change_pct: float = 12.0,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        rows = normalize_history(history)
        anchor = float(rows[-1]["avg_price"])
        matrix = np.asarray([
            [build_feature_row(rows, horizon_days=day)[name] for name in self.features]
            for day in range(1, horizon_days + 1)
        ])
        center_log_ratio = self.model.predict(matrix)
        lower_log_ratio = (
            self.lower_model.predict(matrix) if self.lower_model is not None
            else center_log_ratio
        )
        upper_log_ratio = (
            self.upper_model.predict(matrix) if self.upper_model is not None
            else center_log_ratio
        )
        conformal = self.artifact.get("conformal_log_residuals") or {}
        cap = max_change_pct / 100.0
        lower_ratio_cap = max(0.01, 1.0 - cap)
        upper_ratio_cap = 1.0 + cap
        output = []
        for day, center_raw, lower_raw, upper_raw in zip(
            range(1, horizon_days + 1),
            center_log_ratio,
            lower_log_ratio,
            upper_log_ratio,
        ):
            correction = conformal.get(horizon_bucket(day)) or conformal.get("global")
            if correction:
                lower_raw = min(
                    float(lower_raw),
                    float(center_raw) + float(correction["lower"]),
                )
                upper_raw = max(
                    float(upper_raw),
                    float(center_raw) + float(correction["upper"]),
                )
            center_ratio = float(np.clip(np.exp(center_raw), lower_ratio_cap, upper_ratio_cap))
            lower_ratio = float(np.clip(np.exp(lower_raw), lower_ratio_cap, center_ratio))
            upper_ratio = float(np.clip(np.exp(upper_raw), center_ratio, upper_ratio_cap))
            output.append({
                "forecast_date": (anchor_date + timedelta(days=day)).isoformat(),
                "day_offset": day,
                "predicted_price_vnd": round(anchor * center_ratio, 2),
                "lower_price_vnd": round(anchor * lower_ratio, 2),
                "upper_price_vnd": round(anchor * upper_ratio, 2),
            })
        return output, {
            "method_detail": self.metadata.get(
                "method", "hist_gradient_boosting_direct_horizon"
            ),
            "trained_at": self.metadata.get("trained_at"),
            "training_metrics": self.metadata.get("metrics"),
            "quality_gate": self.metadata.get("quality_gate"),
            "interval_method": self.metadata.get(
                "interval_method", "quantile_gradient_boosting"
            ),
            "max_horizon_change_pct": max_change_pct,
        }


_FORECASTER: Optional[TemporalPriceForecaster] = None
_FORECASTER_PATH: Optional[str] = None


def load_forecaster(path: str, *, force_reload: bool = False) -> TemporalPriceForecaster:
    global _FORECASTER, _FORECASTER_PATH
    absolute = os.path.abspath(path)
    if not force_reload and _FORECASTER is not None and _FORECASTER_PATH == absolute:
        return _FORECASTER
    if not os.path.exists(absolute):
        raise FileNotFoundError(absolute)
    _FORECASTER = TemporalPriceForecaster.load(absolute)
    _FORECASTER_PATH = absolute
    return _FORECASTER
