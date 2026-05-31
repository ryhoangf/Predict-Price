"""
Dự báo giá 30 ngày: history trend + ML depreciation fallback.
"""
from __future__ import annotations

import json
import os
from datetime import date, datetime, timedelta
from typing import Any, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from ml_models.smart_price_predictor import SmartPricePredictor

import numpy as np
import pandas as pd

from ml_models.depreciation_curve import (
    DEFAULT_YEN_TO_VND,
    aggregate_baseline_from_listings,
    build_baseline_row,
    load_curve_config,
    load_predictor,
)
from ml_models.depreciation_curve import get_model_version
from NLP.title_nlp import resolve_product_ml_identity

_METHOD_LABEL_VI = {
    "history_trend": "xu hướng lịch sử",
    "model_depreciation": "mô hình ML (khấu hao)",
    "hybrid": "lịch sử + ML",
}

_APP_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_FORECAST_CONFIG_PATH = os.path.join(_APP_ROOT, "config", "price_forecast_defaults.json")


def load_forecast_config(path: Optional[str] = None) -> dict:
    cfg_path = path or DEFAULT_FORECAST_CONFIG_PATH
    with open(cfg_path, "r", encoding="utf-8") as f:
        return json.load(f)


def _linear_trend_forecast(
    history: list[dict[str, Any]],
    *,
    horizon_days: int,
    anchor_date: date,
    max_daily_change_pct: float,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """OLS trên avg_price theo ngày; trả về điểm D+1..D+horizon."""
    dates = [h["record_date"] for h in history]
    if isinstance(dates[0], datetime):
        dates = [d.date() if hasattr(d, "date") else d for d in dates]
    prices = [float(h["avg_price"]) for h in history]

    t0 = dates[0]
    x = np.array([(d - t0).days for d in dates], dtype=float)
    y = np.array(prices, dtype=float)

    meta: dict[str, Any] = {"points_used": len(x)}
    if len(x) < 2:
        flat = float(y[-1]) if len(y) else 0.0
        meta.update({"slope_vnd_per_day": 0.0, "r2": None, "method_detail": "flat_single_point"})
        series = _flat_series(anchor_date, flat, horizon_days, max_daily_change_pct)
        return series, meta

    coef = np.polyfit(x, y, 1)
    slope, intercept = float(coef[0]), float(coef[1])
    y_hat = intercept + slope * x
    ss_res = float(np.sum((y - y_hat) ** 2))
    ss_tot = float(np.sum((y - np.mean(y)) ** 2))
    r2 = 1.0 - ss_res / ss_tot if ss_tot > 1e-9 else 0.0

    last_x = float(x[-1])
    anchor_price = float(y[-1])
    cap = max_daily_change_pct / 100.0

    series: list[dict[str, Any]] = []
    prev = anchor_price
    for day in range(1, horizon_days + 1):
        xi = last_x + day
        raw_pred = intercept + slope * xi
        fd = anchor_date + timedelta(days=day)
        # Giới hạn biến động ngày để tránh spike
        if prev > 0:
            lo, hi = prev * (1 - cap), prev * (1 + cap)
            raw_pred = float(np.clip(raw_pred, lo, hi))
        raw_pred = max(0.0, raw_pred)
        series.append({
            "forecast_date": fd.isoformat(),
            "day_offset": day,
            "predicted_price_vnd": round(raw_pred, 2),
        })
        prev = raw_pred

    meta.update({
        "slope_vnd_per_day": round(slope, 4),
        "r2": round(r2, 4),
        "method_detail": "linear_regression",
    })
    return series, meta


def _flat_series(
    anchor_date: date,
    anchor_price: float,
    horizon_days: int,
    max_daily_change_pct: float,
    *,
    daily_pct: float = 0.0,
) -> list[dict[str, Any]]:
    series = []
    prev = anchor_price
    cap = max_daily_change_pct / 100.0
    for day in range(1, horizon_days + 1):
        nxt = prev * (1 + daily_pct)
        if prev > 0:
            nxt = float(np.clip(nxt, prev * (1 - cap), prev * (1 + cap)))
        nxt = max(0.0, nxt)
        series.append({
            "forecast_date": (anchor_date + timedelta(days=day)).isoformat(),
            "day_offset": day,
            "predicted_price_vnd": round(nxt, 2),
        })
        prev = nxt
    return series


def _predict_yen_at_device_age(
    predictor: Any,
    raw_row: dict[str, Any],
    device_age_years: float,
    reference_year: int,
) -> float:
    """Một điểm trên đường cong ML tại tuổi thiết bị cụ thể."""
    eng = predictor.engineer_features(pd.DataFrame([raw_row]))
    cols = predictor.feature_columns
    row = eng.iloc[0].copy()
    row["device_age_years"] = float(device_age_years)
    row["release_year"] = float(int(reference_year - round(device_age_years)))
    row["age_condition_interaction"] = row["device_age_years"] * row["condition_score"]
    X = pd.DataFrame([row[list(cols)].values.astype(float)], columns=cols)
    return float(predictor.model.predict(X)[0])


def _model_ratio_path_vnd(
    raw_row: dict[str, Any],
    *,
    anchor_price_vnd: float,
    horizon_days: int,
    config: dict,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """
    Neo giá thị trường (anchor) × tỷ lệ giá ML theo tuổi máy (+N ngày).
    Tránh đường phẳng khi slope age 0→0.25 bị round(age)=0.
    """
    curve_cfg = load_curve_config()
    pred = load_predictor()
    ref_year = int(curve_cfg.get("reference_year", 2026))
    eng = pred.engineer_features(pd.DataFrame([raw_row]))
    age0 = float(eng.iloc[0]["device_age_years"])

    yen0 = _predict_yen_at_device_age(pred, raw_row, age0, ref_year)
    # +30 ngày thường không đổi round(age) → dùng tỷ lệ 1 năm rồi scale về horizon
    yen_plus_1y = _predict_yen_at_device_age(pred, raw_row, age0 + 1.0, ref_year)
    yen_horizon = _predict_yen_at_device_age(
        pred, raw_row, age0 + horizon_days / 365.0, ref_year
    )

    daily_pct_floor = float(config.get("default_phone_depreciation_pct_per_day", -0.12)) / 100.0
    min_horizon_pct = float(config.get("min_horizon_change_pct", -6.0)) / 100.0
    max_horizon_pct = float(config.get("max_horizon_change_pct", 10.0)) / 100.0
    cap = float(config.get("max_daily_change_pct", 3.0)) / 100.0

    annual_ratio = (yen_plus_1y / yen0) if yen0 > 1e-6 else 1.0
    ratio_from_horizon_age = (yen_horizon / yen0) if yen0 > 1e-6 else 1.0
    ratio_end = annual_ratio ** (horizon_days / 365.0)
    if abs(ratio_from_horizon_age - 1.0) >= 1e-4:
        ratio_end = ratio_from_horizon_age
    if abs(ratio_end - 1.0) < 1e-4:
        ratio_end = (1.0 + daily_pct_floor) ** horizon_days
    ratio_end = float(np.clip(ratio_end, 1.0 + min_horizon_pct, 1.0 + max_horizon_pct))

    anchor_date = date.today()
    series: list[dict[str, Any]] = []
    prev = anchor_price_vnd

    for day in range(1, horizon_days + 1):
        t = day / float(horizon_days)
        # Nội suy tỷ lệ theo ngày (mượt), không hằng số cộng vnd/day
        ratio_d = 1.0 + (ratio_end - 1.0) * t
        if abs(ratio_d - 1.0) < 1e-5:
            ratio_d = (1.0 + daily_pct_floor) ** day
        nxt = anchor_price_vnd * ratio_d
        if prev > 0:
            nxt = float(np.clip(nxt, prev * (1 - cap), prev * (1 + cap)))
        nxt = max(0.0, nxt)
        series.append({
            "forecast_date": (anchor_date + timedelta(days=day)).isoformat(),
            "day_offset": day,
            "predicted_price_vnd": round(nxt, 2),
        })
        prev = nxt

    return series, {
        "method_detail": "model_age_ratio",
        "device_age_years_start": round(age0, 3),
        "yen_anchor": round(yen0, 2),
        "yen_plus_1y": round(yen_plus_1y, 2),
        "yen_horizon_age": round(yen_horizon, 2),
        "annual_ratio": round(annual_ratio, 5),
        "ratio_end": round(ratio_end, 5),
        "implied_horizon_change_pct": round((ratio_end - 1.0) * 100, 2),
        "model_version": get_model_version(predictor=pred),
    }


def _blend_series(
    trend: list[dict[str, Any]],
    model: list[dict[str, Any]],
    weight_trend: float,
) -> list[dict[str, Any]]:
    w = float(np.clip(weight_trend, 0.0, 1.0))
    out = []
    for t, m in zip(trend, model):
        p = w * t["predicted_price_vnd"] + (1.0 - w) * m["predicted_price_vnd"]
        out.append({
            "forecast_date": t["forecast_date"],
            "day_offset": t["day_offset"],
            "predicted_price_vnd": round(p, 2),
        })
    return out


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
    min_trend = int(cfg.get("min_history_points_trend", 4))
    yen_to_vnd = float(cfg.get("yen_to_vnd", DEFAULT_YEN_TO_VND))
    anchor_date = date.today()

    history_norm = []
    for h in history:
        rd = h["record_date"]
        if isinstance(rd, datetime):
            rd = rd.date()
        history_norm.append({**h, "record_date": rd})

    # Anchor: ưu tiên giá thị trường gần nhất
    if history_norm:
        anchor_price_vnd = float(history_norm[-1]["avg_price"])
    elif stored_forecast and stored_forecast.get("predicted_price") is not None:
        anchor_price_vnd = float(stored_forecast["predicted_price"])
    else:
        anchor_price_vnd = 0.0

    raw_row = build_product_baseline_row(product, listings_df)

    if anchor_price_vnd <= 0:
        try:
            pred = load_predictor()
            anchor_yen = float(pred.predict(pd.DataFrame([raw_row]))[0])
            anchor_price_vnd = anchor_yen * yen_to_vnd
        except Exception:
            anchor_price_vnd = 0.0

    trend_series, trend_meta = [], {"method_detail": "none"}
    if len(history_norm) >= min_trend:
        trend_series, trend_meta = _linear_trend_forecast(
            history_norm,
            horizon_days=horizon,
            anchor_date=anchor_date,
            max_daily_change_pct=float(cfg.get("max_daily_change_pct", 3.0)),
        )
        method = "history_trend"
        confidence = min(0.95, 0.45 + 0.05 * len(history_norm) + 0.25 * (trend_meta.get("r2") or 0))
    else:
        method = "model_depreciation"
        confidence = 0.4

    model_series, model_meta = _model_ratio_path_vnd(
        raw_row,
        anchor_price_vnd=anchor_price_vnd,
        horizon_days=horizon,
        config=cfg,
    )

    if trend_series:
        cap_days = float(cfg.get("history_blend_weight_cap_days", 14))
        w_trend = min(1.0, len(history_norm) / cap_days)
        min_high = int(cfg.get("min_history_points_high_confidence", 10))
        if len(history_norm) >= min_high:
            w_trend = max(w_trend, 0.7)
        sparse_w = float(cfg.get("model_blend_weight_when_sparse_history", 0.35))
        if len(history_norm) < min_trend:
            w_trend = 0.0
        elif len(history_norm) < min_high:
            w_trend = max(w_trend, 1.0 - sparse_w)

        forecasts = _blend_series(trend_series, model_series, w_trend)
        method = "hybrid" if w_trend < 1.0 and w_trend > 0 else ("history_trend" if w_trend >= 1 else "model_depreciation")
        trend_meta["blend_weight_history"] = round(w_trend, 3)
    else:
        forecasts = model_series
        trend_meta = model_meta

    prices = [f["predicted_price_vnd"] for f in forecasts]
    day30 = forecasts[-1]["predicted_price_vnd"] if forecasts else anchor_price_vnd
    trend_pct = 0.0
    if anchor_price_vnd > 0:
        trend_pct = round((day30 - anchor_price_vnd) / anchor_price_vnd * 100, 2)

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

    mv = model_meta.get("model_version")
    if not mv:
        try:
            mv = get_model_version(predictor=load_predictor())
        except Exception:
            mv = "unknown"

    return {
        "product_id": product.get("product_id"),
        "product_name": product.get("name"),
        "brand": product.get("brand"),
        "model_series": product.get("model_series"),
        "horizon_days": horizon,
        "anchor_date": anchor_date.isoformat(),
        "anchor_price_vnd": round(anchor_price_vnd, 2),
        "method": method,
        "method_label_vi": _METHOD_LABEL_VI.get(method, method),
        "confidence": round(float(confidence), 3),
        "history": history_out,
        "forecasts": forecasts,
        "summary": {
            "trend_pct_over_horizon": trend_pct,
            "min_forecast_vnd": round(min(prices), 2) if prices else None,
            "max_forecast_vnd": round(max(prices), 2) if prices else None,
            "forecast_at_last_day_vnd": round(day30, 2),
        },
        "diagnostics": {
            "history_points": len(history_norm),
            "trend": trend_meta,
            "model": model_meta,
            "stored_forecast_date": (
                stored_forecast.get("forecast_date").isoformat()
                if stored_forecast and stored_forecast.get("forecast_date")
                and hasattr(stored_forecast["forecast_date"], "isoformat")
                else (str(stored_forecast.get("forecast_date")) if stored_forecast else None)
            ),
        },
        "model_version": mv,
        "yen_to_vnd": yen_to_vnd,
        "disclaimer": cfg.get("disclaimer", ""),
    }
