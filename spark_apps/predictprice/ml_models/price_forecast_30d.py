"""
Dự báo giá 30 ngày: chỉ xu hướng price_history (tối thiểu 14 ngày).
"""
from __future__ import annotations

import json
import os
from datetime import date, datetime, timedelta
from typing import Any, Optional

import numpy as np
import pandas as pd

from ml_models.depreciation_curve import (
    DEFAULT_YEN_TO_VND,
    aggregate_baseline_from_listings,
    build_baseline_row,
    load_curve_config,
    resolve_market_anchor_vnd,
)
from ml_models.prediction_quality import compute_prediction_quality
from ml_models.forecast_algorithms import converging_rolling_median
from ml_models.temporal_price_forecaster import load_forecaster
from NLP.title_nlp import resolve_product_ml_identity

_METHOD_LABEL_VI = {
    "history_trend": "xu hướng lịch sử (chỉ giảm hoặc giữ nguyên)",
    "converging_median": "hội tụ dần về trung vị giá 7 ngày",
    "temporal_ml": "mô hình ML chuỗi thời gian đa chân trời",
    "none": "chưa đủ dữ liệu",
}

_APP_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_FORECAST_CONFIG_PATH = os.path.join(_APP_ROOT, "config", "price_forecast_defaults.json")
DEFAULT_TEMPORAL_MODEL_PATH = os.path.join(
    _APP_ROOT, "models", "temporal_price_forecaster.pkl"
)


def load_forecast_config(path: Optional[str] = None) -> dict:
    cfg_path = path or DEFAULT_FORECAST_CONFIG_PATH
    with open(cfg_path, "r", encoding="utf-8") as f:
        return json.load(f)


def _normalize_history(history: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for h in history:
        rd = h["record_date"]
        if isinstance(rd, datetime):
            rd = rd.date()
        out.append({**h, "record_date": rd})
    return out


def _clip_horizon_change(
    series: list[dict[str, Any]],
    *,
    anchor_price: float,
    min_pct: float,
    max_pct: float,
) -> list[dict[str, Any]]:
    if not series or anchor_price <= 0:
        return series
    lo = anchor_price * (1.0 + min_pct / 100.0)
    hi = anchor_price * (1.0 + max_pct / 100.0)
    out = []
    for pt in series:
        p = float(np.clip(pt["predicted_price_vnd"], lo, hi))
        out.append({**pt, "predicted_price_vnd": round(p, 2)})
    return out


def _enforce_non_increasing_from_anchor(
    series: list[dict[str, Any]],
    *,
    anchor_price: float,
) -> list[dict[str, Any]]:
    """Dự báo không cao hơn giá hôm nay; chỉ giữ nguyên hoặc giảm dần."""
    if not series or anchor_price <= 0:
        return series
    out: list[dict[str, Any]] = []
    prev = float(anchor_price)
    for pt in series:
        p = min(float(pt["predicted_price_vnd"]), prev, float(anchor_price))
        p = max(0.0, p)
        out.append({**pt, "predicted_price_vnd": round(p, 2)})
        prev = p
    return out


def _robust_downward_trend_forecast(
    history: list[dict[str, Any]],
    *,
    horizon_days: int,
    anchor_date: date,
    anchor_price_vnd: float,
    max_daily_change_pct: float,
    max_upward_pct_per_day: float,
    lookback_days: int,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """
    Median % thay đổi ngày (lookback gần nhất), ép slope ≤ 0 — không dự báo tăng giá.
    """
    dates = [h["record_date"] for h in history]
    prices = [float(h["avg_price"]) for h in history]

    meta: dict[str, Any] = {"points_used": len(prices), "method_detail": "median_daily_pct"}
    cap = max_daily_change_pct / 100.0
    max_up = max_upward_pct_per_day / 100.0

    daily_pcts: list[float] = []
    for i in range(1, len(prices)):
        if prices[i - 1] > 0:
            daily_pcts.append((prices[i] - prices[i - 1]) / prices[i - 1])

    if daily_pcts:
        window = daily_pcts[-lookback_days:] if lookback_days > 0 else daily_pcts
        slope_pct = float(np.median(window))
    else:
        slope_pct = 0.0

    slope_pct = min(slope_pct, max_up)

    meta.update({
        "median_daily_pct": round(slope_pct * 100, 4),
        "lookback_days": lookback_days,
        "slope_vnd_per_day": round(anchor_price_vnd * slope_pct, 4) if anchor_price_vnd else 0.0,
    })

    series: list[dict[str, Any]] = []
    prev = float(anchor_price_vnd)
    for day in range(1, horizon_days + 1):
        raw_pred = prev * (1.0 + slope_pct)
        if prev > 0:
            lo, hi = prev * (1.0 - cap), prev * (1.0 + cap)
            raw_pred = float(np.clip(raw_pred, lo, hi))
        raw_pred = min(raw_pred, float(anchor_price_vnd))
        raw_pred = max(0.0, raw_pred)
        fd = anchor_date + timedelta(days=day)
        series.append({
            "forecast_date": fd.isoformat(),
            "day_offset": day,
            "predicted_price_vnd": round(raw_pred, 2),
        })
        prev = raw_pred

    return series, meta


def _rolling_median_forecast(
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
        "method_detail": "rolling_median",
        "window_days": window_days,
        "points_used": len(prices),
        "median_price_vnd": round(level, 2),
    }


def build_product_baseline_row(
    product: dict[str, Any],
    listings_df: pd.DataFrame,
) -> dict[str, Any]:
    curve_cfg = load_curve_config()
    specs = product.get("base_specs") or {}
    if isinstance(specs, str):
        try:
            specs = json.loads(specs)
        except json.JSONDecodeError:
            specs = {}
    storage, ram = _specs_storage_ram(specs)
    identity = resolve_product_ml_identity(product)

    agg = aggregate_baseline_from_listings(listings_df)
    if "condition_rank" in agg:
        agg["condition"] = agg.pop("condition_rank")

    return build_baseline_row(
        model_line=identity["model_line"],
        model_number=identity["model_number"],
        variant=identity["variant"],
        storage=storage,
        ram=ram,
        overrides=agg,
        config=curve_cfg,
    )


def _specs_storage_ram(specs: dict[str, Any]) -> tuple[str, str]:
    storage = specs.get("storage")
    ram = specs.get("ram")
    s = "" if storage is None else str(storage).replace("GB", "").replace("gb", "").strip()
    if s.upper().endswith("TB"):
        s = s[:-2].strip()
    r = "" if ram is None else str(ram).replace("GB", "").replace("gb", "").strip()
    return s or "64", r or "4"


def compute_price_forecast_30d(
    *,
    product: dict[str, Any],
    history: list[dict[str, Any]],
    listings_df: pd.DataFrame,
    stored_forecast: Optional[dict[str, Any]] = None,
    horizon_days: Optional[int] = None,
    config: Optional[dict] = None,
) -> dict[str, Any]:
    cfg = config or load_forecast_config()
    horizon = int(horizon_days or cfg.get("horizon_days", 30))
    min_trend = int(cfg.get("min_history_points_trend", 14))
    anchor_date = date.today()
    yen_to_vnd = float(cfg.get("yen_to_vnd", DEFAULT_YEN_TO_VND))

    history_norm = _normalize_history(history)
    n_hist = len(history_norm)

    anchor_info = resolve_market_anchor_vnd(history_norm, listings_df)
    prediction_quality = compute_prediction_quality(
        product=product,
        history=history_norm,
        listings_df=listings_df,
    )

    history_out = [
        {
            "record_date": h["record_date"].isoformat()
            if hasattr(h["record_date"], "isoformat")
            else str(h["record_date"]),
            "avg_price_vnd": float(h["avg_price"]),
            "listing_count": int(h.get("listing_count") or 0),
        }
        for h in history_norm
    ]

    base = {
        "product_id": product.get("product_id"),
        "product_name": product.get("name"),
        "brand": product.get("brand"),
        "model_series": product.get("model_series"),
        "horizon_days": horizon,
        "anchor_date": anchor_date.isoformat(),
        "history": history_out,
        "yen_to_vnd": yen_to_vnd,
        "disclaimer": cfg.get("disclaimer", ""),
        "diagnostics": {
            "history_points": n_hist,
            "min_history_points_required": min_trend,
            "anchor": anchor_info,
            "stored_forecast_date": (
                stored_forecast.get("forecast_date").isoformat()
                if stored_forecast and stored_forecast.get("forecast_date")
                and hasattr(stored_forecast["forecast_date"], "isoformat")
                else (str(stored_forecast.get("forecast_date")) if stored_forecast else None)
            ),
        },
        "prediction_quality": prediction_quality,
    }

    if n_hist < min_trend:
        return {
            **base,
            "status": "insufficient_data",
            "status_message_vi": (
                f"Cần ít nhất {min_trend} ngày price_history để dự báo 30 ngày "
                f"(hiện có {n_hist})."
            ),
            "anchor_price_vnd": round(anchor_info["anchor_price_vnd"], 2)
            if anchor_info.get("ok")
            else None,
            "anchor_source": anchor_info.get("anchor_source"),
            "method": "none",
            "method_label_vi": _METHOD_LABEL_VI["none"],
            "confidence": prediction_quality["score"],
            "forecasts": [],
            "summary": {
                "trend_pct_over_horizon": None,
                "min_forecast_vnd": None,
                "max_forecast_vnd": None,
                "forecast_at_last_day_vnd": None,
            },
            "model_version": None,
        }

    anchor_price_vnd = float(history_norm[-1]["avg_price"])
    method = "converging_median"
    model_version = None
    try:
        temporal = load_forecaster(
            str(cfg.get("temporal_model_path") or DEFAULT_TEMPORAL_MODEL_PATH)
        )
        forecasts, trend_meta = temporal.predict(
            history_norm,
            horizon_days=horizon,
            anchor_date=anchor_date,
            max_change_pct=float(cfg.get("temporal_max_horizon_change_pct", 12.0)),
        )
        method = "temporal_ml"
        model_version = trend_meta.get("method_detail")
    except (FileNotFoundError, KeyError, ValueError, TypeError) as error:
        rolling_window = int(cfg.get("rolling_median_window_days", 7))
        forecasts, trend_meta = converging_rolling_median(
            history_norm,
            horizon_days=horizon,
            anchor_date=anchor_date,
            window_days=rolling_window,
            convergence_days=float(cfg.get("median_convergence_days", 3.0)),
            max_target_change_pct=float(
                cfg.get("median_max_target_change_pct", 8.0)
            ),
        )
        trend_meta["fallback_reason"] = str(error)

    confidence = prediction_quality["score"]

    prices = [f["predicted_price_vnd"] for f in forecasts]
    day_last = forecasts[-1]["predicted_price_vnd"] if forecasts else anchor_price_vnd
    trend_pct = round((day_last - anchor_price_vnd) / anchor_price_vnd * 100, 2) if anchor_price_vnd > 0 else 0.0

    return {
        **base,
        "status": "ok",
        "status_message_vi": None,
        "anchor_price_vnd": round(anchor_price_vnd, 2),
        "anchor_source": "price_history",
        "method": method,
        "method_label_vi": _METHOD_LABEL_VI[method],
        "confidence": round(float(confidence), 3),
        "forecasts": forecasts,
        "summary": {
            "trend_pct_over_horizon": trend_pct,
            "min_forecast_vnd": round(min(prices), 2) if prices else None,
            "max_forecast_vnd": round(max(prices), 2) if prices else None,
            "forecast_at_last_day_vnd": round(day_last, 2),
        },
        "diagnostics": {
            **base["diagnostics"],
            "trend": trend_meta,
        },
        "model_version": model_version,
    }
