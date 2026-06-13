"""Đường cong trượt giá: neo giá thị trường + hình khấu hao ML (tỷ lệ)."""
from __future__ import annotations

import hashlib
import json
import os
from datetime import date, datetime
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


def _enforce_monotonic_decreasing(ages: list[float], prices: list[float]) -> list[float]:
    """Tuổi máy tăng → giá không tăng (khấu hao)."""
    if not prices:
        return prices
    out = [float(prices[0])]
    for i in range(1, len(prices)):
        if ages[i] > ages[i - 1]:
            out.append(min(float(prices[i]), out[-1]))
        else:
            out.append(float(prices[i]))
    return out


def _min_depreciation_ratios(
    ages: list[float],
    *,
    age_now: float,
    min_annual_dep_pct: float,
) -> list[float]:
    """
    Sàn khấu hao: tuổi > age_now thì mỗi năm giảm tối thiểu min_annual_dep_pct%.
    Tuổi < age_now được phép cao hơn anchor (máy mới hơn).
    """
    dep = float(min_annual_dep_pct) / 100.0
    ratios: list[float] = []
    for age in ages:
        if age >= age_now:
            years_older = float(age - age_now)
            ratios.append((1.0 - dep) ** years_older)
        else:
            years_younger = float(age_now - age)
            ratios.append((1.0 + dep) ** years_younger)
    return ratios


def _merge_ml_and_floor_ratios(
    ml_ratios: list[float],
    floor_ratios: list[float],
    ages: list[float],
    *,
    age_now: float,
) -> list[float]:
    """Tuổi lớn hơn: lấy ratio thấp hơn (khấu hao mạnh hơn). Tuổi nhỏ hơn: lấy ratio cao hơn."""
    out: list[float] = []
    for ml_r, floor_r, age in zip(ml_ratios, floor_ratios, ages):
        if age >= age_now:
            out.append(min(float(ml_r), float(floor_r)))
        else:
            out.append(max(float(ml_r), float(floor_r)))
    return out


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
    min_annual_dep_pct: float = 8.0,
) -> tuple[list[float], list[float], list[float]]:
    """
    prices_vnd[age] = anchor × ratio(age); ratio = min(ML, sàn khấu hao năm).
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

    floor_ratios = _min_depreciation_ratios(
        ages, age_now=age_now, min_annual_dep_pct=min_annual_dep_pct
    )
    ratios = _merge_ml_and_floor_ratios(ml_ratios, floor_ratios, ages, age_now=age_now)

    vnd = [anchor_vnd * r for r in ratios]
    vnd = _pin_anchor_at_age(ages, vnd, age_now=age_now, anchor_vnd=anchor_vnd)
    vnd = _enforce_monotonic_decreasing(ages, vnd)
    return ages, vnd, yen_raw


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

    ages, vnd, yen_ml_raw = market_anchored_curve_vnd(
        pred,
        raw_row,
        anchor_vnd=float(anchor_info["anchor_price_vnd"]),
        age_now=age_now,
        age_min=float(cfg.get("age_min", 0)),
        age_max=float(cfg.get("age_max", 8)),
        age_step=float(cfg.get("age_step", 1)),
        reference_year=ref_year,
        min_annual_dep_pct=float(cfg.get("min_annual_depreciation_pct", 8.0)),
    )
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
    }
