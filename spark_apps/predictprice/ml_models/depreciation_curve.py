"""Đường cong trượt giá (mô hình): quét device_age_years, giữ baseline cố định."""
from __future__ import annotations

import hashlib
import json
import os
from typing import Any, Optional

import joblib
import pandas as pd

from ml_models.smart_price_predictor import SmartPricePredictor

# Thư mục spark_apps/predictprice
_APP_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_MODEL_PATH = os.path.join(_APP_ROOT, "models", "smart_price_predictor.pkl")
DEFAULT_CURVE_CONFIG_PATH = os.path.join(_APP_ROOT, "config", "depreciation_curve_defaults.json")
# Đồng bộ với etl.YEN_TO_VND_RATE — tránh import etl (side effect load model)
DEFAULT_YEN_TO_VND = 175

_predictor: Optional[SmartPricePredictor] = None
_predictor_path: Optional[str] = None


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
        return f"smart_v1_r2_{r2:.3f}"
    path = os.path.abspath(model_path or DEFAULT_MODEL_PATH)
    data = joblib.load(path)
    stats = data.get("train_stats") or {}
    return f"smart_v1_r2_{stats.get('test_r2', 0):.3f}"


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
    yen_to_vnd: float = DEFAULT_YEN_TO_VND,
    config: Optional[dict] = None,
    model_path: Optional[str] = None,
    predictor: Optional[SmartPricePredictor] = None,
) -> dict[str, Any]:
    """
    Payload gợi ý cho API: ages, giá yen/vnd, disclaimer, model_version, fingerprint baseline.
    """
    cfg = config or load_curve_config()
    pred = predictor or load_predictor(model_path)
    mv = get_model_version(predictor=pred)

    ref_year = int(cfg.get("reference_year", 2026))
    ages, yen = predict_depreciation_curve_yen(
        pred,
        raw_row,
        age_min=float(cfg.get("age_min", 0)),
        age_max=float(cfg.get("age_max", 8)),
        age_step=float(cfg.get("age_step", 1)),
        reference_year=ref_year,
    )
    vnd = curve_to_vnd(yen, yen_to_vnd)
    fp = baseline_dict_fingerprint(raw_row)
    grid = {
        "age_min": cfg.get("age_min"),
        "age_max": cfg.get("age_max"),
        "age_step": cfg.get("age_step"),
        "reference_year": ref_year,
    }

    return {
        "product_id": product_id,
        "ages_years": ages,
        "prices_yen": yen,
        "prices_vnd": vnd,
        "yen_to_vnd": yen_to_vnd,
        "model_version": mv,
        "baseline_fingerprint": fp,
        "cache_key": build_cache_key(product_id or fp, mv, fp, grid, yen_to_vnd),
        "disclaimer": cfg.get("disclaimer", ""),
        "reference_year": ref_year,
    }
