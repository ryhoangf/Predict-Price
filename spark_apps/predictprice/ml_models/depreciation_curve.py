"""Đường cong trượt giá: neo giá thị trường + hình khấu hao ML (tỷ lệ)."""
from __future__ import annotations

import hashlib
import json
import os
import re
from datetime import date, datetime
from typing import Any, Optional

import joblib
import numpy as np
import pandas as pd
from sklearn.isotonic import IsotonicRegression

from ml_models.smart_price_predictor import SmartPricePredictor

# Thư mục spark_apps/predictprice
_APP_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_MODEL_PATH = os.path.join(_APP_ROOT, "models", "smart_price_predictor.pkl")
DEFAULT_CURVE_CONFIG_PATH = os.path.join(_APP_ROOT, "config", "depreciation_curve_defaults.json")
DEFAULT_DEPRECIATION_MODEL_PATH = os.path.join(
    _APP_ROOT, "models", "depreciation_model.pkl"
)
# Đồng bộ với etl.YEN_TO_VND_RATE — tránh import etl (side effect load model)
DEFAULT_YEN_TO_VND = 175

_predictor: Optional[SmartPricePredictor] = None
_predictor_path: Optional[str] = None
_DEPRECIATION_BRANDS = [
    "apple", "samsung", "google", "sony", "sharp", "xiaomi",
    "oppo", "motorola", "huawei", "asus",
]


def _depreciation_storage_gb(value: object) -> float:
    match = re.search(
        r"(\d+(?:\.\d+)?)\s*(TB|GB)?",
        str(value or ""),
        re.IGNORECASE,
    )
    if not match:
        return 64.0
    amount = float(match.group(1))
    if (match.group(2) or "").upper() == "TB":
        amount *= 1024.0
    return amount if 1 <= amount <= 2048 else 64.0


def _depreciation_feature_row(
    *,
    device_age_years: float,
    storage: object,
    listing_count: float,
    brand: str,
) -> dict[str, float]:
    normalized_brand = str(brand or "").strip().lower()
    return {
        "device_age_years": float(max(device_age_years, 0.0)),
        "storage_log": float(np.log1p(_depreciation_storage_gb(storage))),
        "listing_count_log": float(np.log1p(max(float(listing_count or 0), 0.0))),
        **{
            f"brand_{known}": float(normalized_brand == known)
            for known in _DEPRECIATION_BRANDS
        },
    }


def get_default_model_path() -> str:
    return DEFAULT_MODEL_PATH


def load_predictor(model_path: Optional[str] = None, *, force_reload: bool = False) -> SmartPricePredictor:
    """Lazy-load SmartPricePredictor; dùng chung cho API / batch."""
    global _predictor, _predictor_path
    path = os.path.abspath(model_path or DEFAULT_MODEL_PATH)
    if not os.path.exists(path):
        raise FileNotFoundError(f"Không tìm thấy model: {path}")
    if not force_reload and _predictor is not None and _predictor_path == path:
        return _predictor
    p = SmartPricePredictor()
    p.load(path)
    _predictor = p
    _predictor_path = path
    return p


def clear_predictor_cache() -> None:
    global _predictor, _predictor_path
    _predictor = None
    _predictor_path = None


def get_model_version(
    predictor: Optional[SmartPricePredictor] = None,
    model_path: Optional[str] = None,
) -> str:
    if predictor is not None and getattr(predictor, "train_stats_", None):
        r2 = predictor.train_stats_.get("test_r2", 0) or 0
        fingerprint = (
            getattr(predictor, "model_metadata_", {}) or {}
        ).get("dataset_sha256_16")
        suffix = f"_{fingerprint[:8]}" if fingerprint else ""
        return f"smart_v2_r2_{r2:.3f}{suffix}"
    path = os.path.abspath(model_path or DEFAULT_MODEL_PATH)
    data = joblib.load(path)
    stats = data.get("train_stats") or {}
    fingerprint = (data.get("model_metadata") or {}).get("dataset_sha256_16")
    suffix = f"_{fingerprint[:8]}" if fingerprint else ""
    return f"smart_v2_r2_{stats.get('test_r2', 0):.3f}{suffix}"


def load_curve_config(path: Optional[str] = None) -> dict:
    cfg_path = path or DEFAULT_CURVE_CONFIG_PATH
    with open(cfg_path, "r", encoding="utf-8") as f:
        return json.load(f)


def baseline_dict_fingerprint(d: dict) -> str:
    blob = json.dumps(d, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()[:16]


def build_baseline_row(
    *,
    model_line: str,
    storage: str,
    ram: str,
    model_number: str = "",
    variant: str = "",
    overrides: Optional[dict[str, Any]] = None,
    config: Optional[dict] = None,
) -> dict[str, Any]:
    """
    Ghép baseline_defaults từ config JSON + identity + storage/ram.
    `config`: toàn bộ dict từ load_curve_config(); nếu None chỉ dùng overrides + các field truyền vào.
    """
    defaults = dict((config or {}).get("baseline_defaults") or {})
    row: dict[str, Any] = {**defaults}
    row["model_line"] = model_line
    row["model_number"] = model_number or ""
    row["variant"] = variant or ""
    row["storage"] = str(storage)
    row["ram"] = str(ram)
    if overrides:
        row.update(overrides)
    return row


def aggregate_baseline_from_listings(
    listings_df: pd.DataFrame,
    *,
    numeric_cols: tuple[str, ...] = ("battery_percentage",),
    mode_cols: tuple[str, ...] = (
        "condition_rank",
        "screen_condition",
        "body_condition",
        "platform",
    ),
    bool_cols: tuple[str, ...] = (
        "has_box",
        "has_charger",
        "is_sim_free",
        "fully_functional",
        "has_scratches",
        "has_damage",
        "has_issues",
    ),
) -> dict[str, Any]:
    """listings_df: các hàng active_listings cùng product_id (đã join specs nếu cần)."""
    out: dict[str, Any] = {}
    if listings_df.empty:
        return out
    for c in numeric_cols:
        if c in listings_df.columns and listings_df[c].notna().any():
            out[c] = float(listings_df[c].median())
    for c in mode_cols:
        if c in listings_df.columns:
            m = listings_df[c].mode(dropna=True)
            if len(m):
                out[c] = m.iloc[0]
    for c in bool_cols:
        if c in listings_df.columns:
            m = listings_df[c].mode(dropna=True)
            v = m.iloc[0] if len(m) else 0
            out[c] = int(bool(v))
    if "condition_rank" in out:
        out["condition"] = out.pop("condition_rank")
    return out


def resolve_market_anchor_vnd(
    history: list[dict[str, Any]] | None,
    listings_df: pd.DataFrame | None,
    *,
    min_listings: int = 3,
) -> dict[str, Any]:
    """
    Giá neo thị trường (VND): price_history gần nhất → median active_listings.
    """
    history = history or []
    listings_df = listings_df if listings_df is not None else pd.DataFrame()

    if history:
        last = history[-1]
        price = float(last.get("avg_price") or 0)
        if price > 0:
            rd = last.get("record_date")
            if isinstance(rd, datetime):
                rd = rd.date()
            return {
                "ok": True,
                "anchor_price_vnd": price,
                "anchor_source": "price_history",
                "anchor_date": rd.isoformat() if hasattr(rd, "isoformat") else str(rd),
                "history_points": len(history),
                "listing_count": int(last.get("listing_count") or 0),
            }

    if not listings_df.empty and "price" in listings_df.columns:
        prices = pd.to_numeric(listings_df["price"], errors="coerce").dropna()
        if len(prices) >= min_listings:
            med = float(prices.median())
            if med > 0:
                return {
                    "ok": True,
                    "anchor_price_vnd": med,
                    "anchor_source": "active_listings_median",
                    "anchor_date": date.today().isoformat(),
                    "history_points": 0,
                    "listing_count": int(len(prices)),
                }

    return {
        "ok": False,
        "anchor_price_vnd": 0.0,
        "anchor_source": None,
        "anchor_date": None,
        "history_points": len(history),
        "listing_count": 0,
    }


def predict_yen_at_device_age(
    predictor: SmartPricePredictor,
    raw_row: dict[str, Any],
    device_age_years: float,
    reference_year: int,
) -> float:
    """Một điểm giá YEN trên đường cong ML tại tuổi thiết bị cụ thể."""
    eng = predictor.engineer_features(pd.DataFrame([raw_row]))
    cols = predictor.feature_columns
    if not cols:
        raise ValueError("Model chưa load hoặc chưa có feature_columns")
    row = eng.iloc[0].copy()
    row["device_age_years"] = float(device_age_years)
    row["release_year"] = float(int(reference_year - round(device_age_years)))
    row["age_condition_interaction"] = row["device_age_years"] * row["condition_score"]
    X = pd.DataFrame([row[list(cols)].values.astype(float)], columns=cols)
    return float(predictor.model.predict(X)[0])


def _fit_monotonic_decreasing(ages: list[float], prices: list[float]) -> list[float]:
    """Closest non-increasing curve to the model predictions."""
    if not prices:
        return prices
    if len(prices) == 1:
        return [float(prices[0])]
    fitted = IsotonicRegression(
        increasing=False,
        out_of_bounds="clip",
    ).fit_transform(
        np.asarray(ages, dtype=float),
        np.asarray(prices, dtype=float),
    )
    return [float(value) for value in fitted]


def _pin_anchor_at_age(
    ages: list[float],
    prices: list[float],
    *,
    age_now: float,
    anchor_vnd: float,
) -> list[float]:
    if not ages or anchor_vnd <= 0:
        return prices
    idx = min(range(len(ages)), key=lambda i: abs(ages[i] - age_now))
    out = list(prices)
    out[idx] = float(anchor_vnd)
    return out


def market_anchored_curve_vnd(
    predictor: SmartPricePredictor,
    raw_row: dict[str, Any],
    *,
    anchor_vnd: float,
    age_now: float,
    age_min: float,
    age_max: float,
    age_step: float,
    reference_year: int,
) -> tuple[list[float], list[float], list[float]]:
    """
    prices_vnd[age] = anchor × model-relative ratio(age).

    Isotonic regression removes local upward noise without imposing a fixed
    annual depreciation percentage.
    Trả về (ages, prices_vnd, prices_yen_ml_raw).
    """
    ages, yen_raw = predict_depreciation_curve_yen(
        predictor,
        raw_row,
        age_min=age_min,
        age_max=age_max,
        age_step=age_step,
        reference_year=reference_year,
    )
    yen_now = predict_yen_at_device_age(predictor, raw_row, age_now, reference_year)
    if yen_now <= 1e-6:
        ml_ratios = [1.0] * len(yen_raw)
    else:
        ml_ratios = [float(y) / yen_now for y in yen_raw]

    model_vnd = [anchor_vnd * ratio for ratio in ml_ratios]
    vnd = _fit_monotonic_decreasing(ages, model_vnd)
    anchor_index = min(range(len(ages)), key=lambda i: abs(ages[i] - age_now))
    fitted_anchor = float(vnd[anchor_index])
    if fitted_anchor > 1e-6:
        scale = float(anchor_vnd) / fitted_anchor
        vnd = [float(value) * scale for value in vnd]
    return ages, vnd, yen_raw


def dedicated_depreciation_curve_vnd(
    artifact: dict[str, Any],
    *,
    brand: str,
    storage: object,
    listing_count: float,
    anchor_vnd: float,
    age_now: float,
    age_min: float,
    age_max: float,
    age_step: float,
    max_annual_drop_pct: float = 20.0,
) -> tuple[list[float], list[float], dict[str, float]]:
    ages: list[float] = []
    rows: list[dict[str, float]] = []
    age = float(age_min)
    while age <= age_max + 1e-9:
        ages.append(age)
        rows.append(_depreciation_feature_row(
            device_age_years=age,
            storage=storage,
            listing_count=listing_count,
            brand=brand,
        ))
        age += age_step
    matrix = pd.DataFrame(rows)[artifact["features"]]
    raw_log_prices = np.asarray(artifact["model"].predict(matrix), dtype=float)
    slopes = []
    for left in range(len(ages)):
        for right in range(left + 1, len(ages)):
            delta_age = ages[right] - ages[left]
            if delta_age > 0:
                slopes.append(
                    (raw_log_prices[right] - raw_log_prices[left]) / delta_age
                )
    raw_slope = float(np.median(slopes)) if slopes else 0.0
    min_log_slope = float(np.log1p(-max_annual_drop_pct / 100.0))
    applied_slope = float(np.clip(raw_slope, min_log_slope, 0.0))
    prices = [
        float(anchor_vnd) * float(np.exp(applied_slope * (age - age_now)))
        for age in ages
    ]
    return ages, prices, {
        "raw_annual_depreciation_pct": round(
            (1.0 - float(np.exp(raw_slope))) * 100.0, 3
        ),
        "annual_depreciation_pct": round(
            (1.0 - float(np.exp(applied_slope))) * 100.0, 3
        ),
        "max_annual_drop_safety_pct": max_annual_drop_pct,
    }


def predict_depreciation_curve_yen(
    predictor: SmartPricePredictor,
    raw_row: dict[str, Any],
    *,
    age_min: float = 0,
    age_max: float = 8,
    age_step: float = 1,
    reference_year: int = 2026,
) -> tuple[list[float], list[float]]:
    """
    Trả về (ages, prices_yen). Ghi đè device_age_years + release_year + age_condition_interaction
    sau engineer_features để khớp cặp (release_year, age) như lúc train.
    """
    df0 = pd.DataFrame([raw_row])
    eng = predictor.engineer_features(df0)
    cols = predictor.feature_columns
    if not cols:
        raise ValueError("Model chưa load hoặc chưa có feature_columns")

    ages_list: list[float] = []
    prices: list[float] = []
    age = float(age_min)
    while age <= age_max + 1e-9:
        row = eng.iloc[0].copy()
        ry = int(reference_year - round(age))
        row["device_age_years"] = float(age)
        row["release_year"] = float(ry)
        row["age_condition_interaction"] = row["device_age_years"] * row["condition_score"]

        X = pd.DataFrame([row[list(cols)].values.astype(float)], columns=cols)
        y_hat = float(predictor.model.predict(X)[0])
        ages_list.append(float(age))
        prices.append(y_hat)
        age += age_step

    return ages_list, prices


def curve_to_vnd(prices_yen: list[float], yen_to_vnd: float) -> list[float]:
    return [float(p) * float(yen_to_vnd) for p in prices_yen]


def build_cache_key(
    product_id: str,
    model_version: str,
    baseline_fp: str,
    grid: dict,
    fx: float,
) -> str:
    g = json.dumps(grid, sort_keys=True)
    return f"depcurve:{product_id}:{model_version}:{baseline_fp}:{g}:fx{fx}"


def compute_depreciation_curve_response(
    raw_row: dict[str, Any],
    *,
    product_id: str = "",
    history: list[dict[str, Any]] | None = None,
    listings_df: pd.DataFrame | None = None,
    yen_to_vnd: float = DEFAULT_YEN_TO_VND,
    config: Optional[dict] = None,
    model_path: Optional[str] = None,
    predictor: Optional[SmartPricePredictor] = None,
) -> dict[str, Any]:
    """
    Đường cong khấu hao neo giá thị trường; ML chỉ định hình tỷ lệ theo tuổi máy.
    """
    cfg = config or load_curve_config()
    pred = predictor or load_predictor(model_path)
    mv = get_model_version(predictor=pred)
    ref_year = int(cfg.get("reference_year", 2026))
    fp = baseline_dict_fingerprint(raw_row)
    grid = {
        "age_min": cfg.get("age_min"),
        "age_max": cfg.get("age_max"),
        "age_step": cfg.get("age_step"),
        "reference_year": ref_year,
    }
    min_listings = int(cfg.get("min_listings_for_anchor", 3))

    eng = pred.engineer_features(pd.DataFrame([raw_row]))
    age_now = float(eng.iloc[0]["device_age_years"])

    anchor_info = resolve_market_anchor_vnd(
        history,
        listings_df,
        min_listings=min_listings,
    )

    if not anchor_info["ok"]:
        return {
            "status": "insufficient_data",
            "status_message_vi": (
                "Chưa có price_history hoặc đủ listing đang bán "
                f"(cần ≥1 ngày history hoặc ≥{min_listings} listing) để neo giá thị trường."
            ),
            "product_id": product_id,
            "ages_years": [],
            "prices_yen": [],
            "prices_vnd": [],
            "yen_to_vnd": yen_to_vnd,
            "model_version": mv,
            "baseline_fingerprint": fp,
            "cache_key": build_cache_key(product_id or fp, mv, fp, grid, yen_to_vnd),
            "disclaimer": cfg.get("disclaimer", ""),
            "reference_year": ref_year,
            "anchor_source": None,
            "anchor_price_vnd": None,
            "device_age_years_now": age_now,
            "history_points": anchor_info.get("history_points", 0),
        }

    depreciation_artifact = None
    try:
        depreciation_path = str(
            cfg.get("depreciation_model_path") or DEFAULT_DEPRECIATION_MODEL_PATH
        )
        if not os.path.exists(depreciation_path):
            depreciation_path = DEFAULT_DEPRECIATION_MODEL_PATH
        depreciation_artifact = joblib.load(depreciation_path)
        ages, vnd, depreciation_meta = dedicated_depreciation_curve_vnd(
            depreciation_artifact,
            brand=str(raw_row.get("brand") or ""),
            storage=raw_row.get("storage"),
            listing_count=float(anchor_info.get("listing_count") or 0),
            anchor_vnd=float(anchor_info["anchor_price_vnd"]),
            age_now=age_now,
            age_min=float(cfg.get("age_min", 0)),
            age_max=float(cfg.get("age_max", 8)),
            age_step=float(cfg.get("age_step", 1)),
            max_annual_drop_pct=float(
                cfg.get("max_annual_depreciation_safety_pct", 20.0)
            ),
        )
        yen_ml_raw = [float(price) / float(yen_to_vnd) for price in vnd]
        curve_method = depreciation_artifact.get(
            "method", "monotonic_hedonic_depreciation_v2"
        )
    except (FileNotFoundError, KeyError, TypeError, ValueError):
        ages, vnd, yen_ml_raw = market_anchored_curve_vnd(
            pred,
            raw_row,
            anchor_vnd=float(anchor_info["anchor_price_vnd"]),
            age_now=age_now,
            age_min=float(cfg.get("age_min", 0)),
            age_max=float(cfg.get("age_max", 8)),
            age_step=float(cfg.get("age_step", 1)),
            reference_year=ref_year,
        )
        curve_method = "market_anchored_ml_ratio_isotonic_fallback"
        depreciation_meta = {}
    fx = float(cfg.get("yen_to_vnd", yen_to_vnd))
    prices_yen_chart = [round(float(p) / fx, 2) for p in vnd]

    return {
        "status": "ok",
        "product_id": product_id,
        "ages_years": ages,
        "prices_vnd": vnd,
        "prices_yen": prices_yen_chart,
        "prices_yen_ml_raw": yen_ml_raw,
        "yen_to_vnd": yen_to_vnd,
        "model_version": mv,
        "baseline_fingerprint": fp,
        "cache_key": build_cache_key(product_id or fp, mv, fp, grid, yen_to_vnd),
        "disclaimer": cfg.get("disclaimer", ""),
        "reference_year": ref_year,
        "anchor_source": anchor_info["anchor_source"],
        "anchor_price_vnd": round(float(anchor_info["anchor_price_vnd"]), 2),
        "anchor_date": anchor_info.get("anchor_date"),
        "device_age_years_now": age_now,
        "history_points": anchor_info.get("history_points", 0),
        "listing_count": anchor_info.get("listing_count", 0),
        "curve_method": curve_method,
        "fixed_annual_depreciation_pct": None,
        "depreciation_model_quality_gate": (
            depreciation_artifact.get("quality_gate")
            if depreciation_artifact else None
        ),
        "depreciation_diagnostics": depreciation_meta,
    }
