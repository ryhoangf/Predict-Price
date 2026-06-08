from __future__ import annotations

import json
import re
from typing import Any

import pandas as pd


GENERIC_NAME_RE = re.compile(
    r"^(android|iphone|galaxy|pixel|xperia|aquos|redmi|xiaomi|oppo|sony|samsung|motorola)(\s+\d+\s*(gb|tb))?$",
    re.IGNORECASE,
)
FUTURE_LIKE_RE = re.compile(
    r"\b(iPhone\s*1[89]|Pixel\s*1[1-9]|Galaxy\s*S\s*2[7-9])\b",
    re.IGNORECASE,
)


def _parse_specs(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if not raw:
        return {}
    try:
        return json.loads(str(raw))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}


def _present(v: Any) -> bool:
    if v is None:
        return False
    try:
        if pd.isna(v):
            return False
    except (TypeError, ValueError):
        pass
    return str(v).strip() not in ("", "null", "None")


def _price_cv_pct(listings_df: pd.DataFrame) -> float | None:
    if listings_df is None or listings_df.empty or "price" not in listings_df.columns:
        return None
    prices = pd.to_numeric(listings_df["price"], errors="coerce").dropna()
    if len(prices) < 2:
        return None
    mean = float(prices.mean())
    if mean <= 0:
        return None
    return float(prices.std(ddof=0) / mean * 100.0)


def _missing_pct(listings_df: pd.DataFrame, column: str) -> float | None:
    if listings_df is None or listings_df.empty or column not in listings_df.columns:
        return None
    return float(listings_df[column].isna().sum() / len(listings_df) * 100.0)


def compute_prediction_quality(
    *,
    product: dict[str, Any],
    history: list[dict[str, Any]] | None,
    listings_df: pd.DataFrame | None,
    segment_mape_pct: float | None = None,
) -> dict[str, Any]:
    """
    Case-level confidence for price/trend outputs.

    This is intentionally not the model's global R2. It reflects whether this
    product has enough clean, specific market evidence to show a strong signal.
    """
    history = history or []
    listings_df = listings_df if listings_df is not None else pd.DataFrame()
    specs = _parse_specs(product.get("base_specs"))
    name = str(product.get("name") or "")
    brand = str(product.get("brand") or "")

    listing_count = int(len(listings_df))
    history_days = len({str(h.get("record_date")) for h in history if h.get("record_date")})
    cv_pct = _price_cv_pct(listings_df)
    battery_missing_pct = _missing_pct(listings_df, "battery_percentage")
    condition_missing_pct = _missing_pct(listings_df, "condition_rank")

    storage_known = _present(specs.get("storage"))
    brand_known = _present(brand) and brand.lower() not in {"null", "needs review"}
    generic_name = bool(GENERIC_NAME_RE.match(name.strip()))
    future_like = bool(FUTURE_LIKE_RE.search(name))

    score = 1.0
    reasons: list[str] = []

    if not storage_known:
        score -= 0.25
        reasons.append("storage_missing")
    if not brand_known:
        score -= 0.12
        reasons.append("brand_missing")
    if generic_name:
        score -= 0.25
        reasons.append("generic_product_name")
    if future_like:
        score -= 0.25
        reasons.append("future_like_product_name")

    if history_days < 7:
        score -= 0.25
        reasons.append("history_lt_7_days")
    elif history_days < 14:
        score -= 0.14
        reasons.append("history_lt_14_days")

    if listing_count < 10:
        score -= 0.22
        reasons.append("listing_count_lt_10")
    elif listing_count < 50:
        score -= 0.12
        reasons.append("listing_count_lt_50")

    if cv_pct is not None:
        if cv_pct > 50:
            score -= 0.25
            reasons.append("price_cv_gt_50_pct")
        elif cv_pct > 25:
            score -= 0.12
            reasons.append("price_cv_gt_25_pct")
    else:
        score -= 0.08
        reasons.append("price_variance_unknown")

    if battery_missing_pct is not None and battery_missing_pct > 70:
        score -= 0.08
        reasons.append("battery_mostly_missing")
    if condition_missing_pct is not None and condition_missing_pct > 20:
        score -= 0.08
        reasons.append("condition_missing_high")

    if segment_mape_pct is not None:
        if segment_mape_pct > 50:
            score -= 0.18
            reasons.append("segment_mape_gt_50_pct")
        elif segment_mape_pct > 25:
            score -= 0.08
            reasons.append("segment_mape_gt_25_pct")

    score = max(0.0, min(0.95, score))
    if (
        storage_known
        and history_days >= 14
        and listing_count >= 50
        and cv_pct is not None
        and cv_pct < 25
        and (segment_mape_pct is None or segment_mape_pct < 20)
        and not generic_name
        and not future_like
    ):
        level = "high"
    elif score >= 0.55:
        level = "medium"
    else:
        level = "low"

    return {
        "level": level,
        "score": round(float(score), 3),
        "reasons": reasons,
        "metrics": {
            "listing_count": listing_count,
            "history_days": history_days,
            "price_cv_pct": round(cv_pct, 2) if cv_pct is not None else None,
            "battery_missing_pct": round(battery_missing_pct, 2)
            if battery_missing_pct is not None
            else None,
            "condition_missing_pct": round(condition_missing_pct, 2)
            if condition_missing_pct is not None
            else None,
            "storage_known": storage_known,
            "brand_known": brand_known,
            "generic_name": generic_name,
            "future_like": future_like,
            "segment_mape_pct": segment_mape_pct,
        },
    }
