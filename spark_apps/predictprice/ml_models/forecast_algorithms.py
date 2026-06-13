"""Pure statistical forecast algorithms shared by the API and backtests."""
from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any

import numpy as np
import pandas as pd


def flat_rolling_median(
    history: list[dict[str, Any]],
    *,
    horizon_days: int,
    anchor_date: date,
    window_days: int = 7,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    prices = [float(row["avg_price"]) for row in history[-window_days:]]
    level = float(np.median(prices))
    series = [
        {
            "forecast_date": (anchor_date + timedelta(days=day)).isoformat(),
            "day_offset": day,
            "predicted_price_vnd": round(level, 2),
        }
        for day in range(1, horizon_days + 1)
    ]
    return series, {
        "method_detail": "flat_rolling_median",
        "window_days": window_days,
        "points_used": len(prices),
        "median_price_vnd": round(level, 2),
    }


def damped_rolling_median_trend(
    history: list[dict[str, Any]],
    *,
    horizon_days: int,
    anchor_date: date,
    window_days: int = 7,
    slope_lookback_points: int = 14,
    damping_days: float = 14.0,
    max_daily_change_pct: float = 0.5,
    max_horizon_change_pct: float = 8.0,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """
    Fit a robust slope to rolling medians, then exponentially damp it.

    The forecast starts at the latest observed price. This preserves a visible
    local direction without extending a noisy short-term slope for 30 days.
    """
    normalized = []
    for row in history:
        raw_date = row["record_date"]
        if isinstance(raw_date, datetime):
            raw_date = raw_date.date()
        normalized.append((raw_date, float(row["avg_price"])))
    normalized.sort(key=lambda item: item[0])
    if not normalized:
        return [], {"method_detail": "damped_rolling_median_trend", "points_used": 0}

    frame = pd.DataFrame(normalized, columns=["record_date", "price"])
    frame["smooth"] = frame["price"].rolling(
        window=max(2, window_days),
        min_periods=2,
    ).median()
    smooth = frame.dropna(subset=["smooth"]).tail(slope_lookback_points)
    anchor = float(frame.iloc[-1]["price"])

    slopes = []
    values = list(smooth[["record_date", "smooth"]].itertuples(index=False, name=None))
    for left in range(len(values)):
        for right in range(left + 1, len(values)):
            days = (values[right][0] - values[left][0]).days
            if days > 0:
                slopes.append((float(values[right][1]) - float(values[left][1])) / days)
    raw_slope = float(np.median(slopes)) if slopes else 0.0
    daily_cap = anchor * max_daily_change_pct / 100.0
    slope = float(np.clip(raw_slope, -daily_cap, daily_cap))

    lower = anchor * (1.0 - max_horizon_change_pct / 100.0)
    upper = anchor * (1.0 + max_horizon_change_pct / 100.0)
    predicted = anchor
    series = []
    for day in range(1, horizon_days + 1):
        damp = float(np.exp(-(day - 1) / max(damping_days, 1.0)))
        predicted = float(np.clip(predicted + slope * damp, lower, upper))
        series.append({
            "forecast_date": (anchor_date + timedelta(days=day)).isoformat(),
            "day_offset": day,
            "predicted_price_vnd": round(predicted, 2),
        })

    return series, {
        "method_detail": "damped_rolling_median_trend",
        "window_days": window_days,
        "slope_lookback_points": slope_lookback_points,
        "damping_days": damping_days,
        "points_used": int(len(smooth)),
        "raw_slope_vnd_per_day": round(raw_slope, 2),
        "applied_slope_vnd_per_day": round(slope, 2),
        "max_horizon_change_pct": max_horizon_change_pct,
    }


def converging_rolling_median(
    history: list[dict[str, Any]],
    *,
    horizon_days: int,
    anchor_date: date,
    window_days: int = 7,
    convergence_days: float = 3.0,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Smoothly converge from the latest observed price to the robust median."""
    prices = [float(row["avg_price"]) for row in history[-window_days:]]
    if not prices:
        return [], {"method_detail": "converging_rolling_median", "points_used": 0}
    anchor = float(history[-1]["avg_price"])
    level = float(np.median(prices))
    series = []
    for day in range(1, horizon_days + 1):
        weight = float(np.exp(-day / max(convergence_days, 0.1)))
        predicted = level + (anchor - level) * weight
        series.append({
            "forecast_date": (anchor_date + timedelta(days=day)).isoformat(),
            "day_offset": day,
            "predicted_price_vnd": round(predicted, 2),
        })
    return series, {
        "method_detail": "converging_rolling_median",
        "window_days": window_days,
        "convergence_days": convergence_days,
        "points_used": len(prices),
        "anchor_price_vnd": round(anchor, 2),
        "median_price_vnd": round(level, 2),
    }
