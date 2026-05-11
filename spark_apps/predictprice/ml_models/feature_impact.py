"""
Phân tích tác động linh kiện — counterfactual (Hướng A).
delta_yen > 0: nâng yếu tố lên mức tham chiếu làm giá dự báo tăng.
"""
from __future__ import annotations

import copy
from dataclasses import dataclass
from typing import Any, Callable

import pandas as pd

from ml_models.smart_price_predictor import SmartPricePredictor

DEFAULT_YEN_TO_VND = 175


def _as_int_flag(v: Any) -> int:
    """0/1 an toàn: tránh bool('0') == True khi client gửi chuỗi."""
    if v is None:
        return 0
    if isinstance(v, bool):
        return int(v)
    if isinstance(v, (int, float)):
        return 1 if int(v) else 0
    s = str(v).strip().lower()
    if s in ("0", "false", "no", "", "off"):
        return 0
    if s in ("1", "true", "yes", "on"):
        return 1
    try:
        return 1 if int(float(s)) else 0
    except ValueError:
        return 0


def _as_bool(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(int(v))
    s = str(v).strip().lower()
    if s in ("0", "false", "no", "", "off"):
        return False
    if s in ("1", "true", "yes", "on"):
        return True
    return bool(s)


def predict_yen(predictor: SmartPricePredictor, raw_row: dict[str, Any]) -> float:
    return float(predictor.predict(pd.DataFrame([raw_row]))[0])


@dataclass(frozen=True)
class _Scenario:
    id: str
    label_vi: str
    field: str
    reference: Any
    should_run: Callable[[dict[str, Any]], bool]


def _default_scenarios() -> list[_Scenario]:
    def _num(r: dict, key: str, default: float) -> float:
        v = r.get(key)
        if v is None:
            return default
        try:
            return float(v)
        except (TypeError, ValueError):
            return default

    return [
        _Scenario("battery_to_100", "Pin (so với 100%)", "battery_percentage", 100.0,
                  lambda r: _num(r, "battery_percentage", 80.0) < 99.5),
        _Scenario("has_box_true", "Có hộp", "has_box", True,
                  lambda r: not bool(r.get("has_box"))),
        _Scenario("has_charger_true", "Có sạc", "has_charger", True,
                  lambda r: not bool(r.get("has_charger"))),
        _Scenario("screen_clean", "Màn hình (so với clean)", "screen_condition", "clean",
                  lambda r: str(r.get("screen_condition") or "clean").lower() != "clean"),
        _Scenario("body_good", "Khung/vỏ (so với good)", "body_condition", "good",
                  lambda r: str(r.get("body_condition") or "good").lower() != "good"),
        _Scenario("no_scratches", "Không trầy (has_scratches)", "has_scratches", 0,
                  lambda r: _as_int_flag(r.get("has_scratches")) == 1),
        _Scenario("no_damage", "Không hư hỏng (has_damage)", "has_damage", 0,
                  lambda r: _as_int_flag(r.get("has_damage")) == 1),
    ]


def counterfactual_impact_report(
    predictor: SmartPricePredictor,
    raw_row: dict[str, Any],
    *,
    yen_to_vnd: float = DEFAULT_YEN_TO_VND,
    include_all_scenarios: bool = False,
) -> dict[str, Any]:
    base = {k: v for k, v in raw_row.items() if v is not None}
    base_yen = predict_yen(predictor, base)
    impacts = []
    for sc in _default_scenarios():
        if not include_all_scenarios and not sc.should_run(base):
            continue
        before_val = base.get(sc.field)
        alt = copy.deepcopy(base)
        if isinstance(sc.reference, bool):
            alt[sc.field] = bool(sc.reference)
        elif sc.field in ("has_scratches", "has_damage", "has_issues"):
            alt[sc.field] = int(sc.reference)
        else:
            alt[sc.field] = sc.reference

        alt_yen = predict_yen(predictor, alt)
        delta_yen = float(alt_yen - base_yen)
        impacts.append({
            "id": sc.id, "label_vi": sc.label_vi, "field": sc.field,
            "value_before": before_val, "value_reference": sc.reference,
            "delta_yen": round(delta_yen, 2), "delta_vnd": round(delta_yen * float(yen_to_vnd), 2),
        })

    return {
        "method": "counterfactual",
        "baseline_prediction_yen": round(base_yen, 2),
        "baseline_prediction_vnd": round(base_yen * float(yen_to_vnd), 2),
        "yen_to_vnd": float(yen_to_vnd),
        "impacts": impacts,
        "disclaimer": "Mỗi impact chỉ đổi một yếu tố; không cộng tuyến tính khi đổi nhiều yếu tố.",
    }


def raw_listing_from_flat_json(data: dict[str, Any]) -> dict[str, Any]:
    out = dict(data)
    for k in ("has_box", "has_charger"):
        if k in out:
            out[k] = _as_bool(out[k])
    for k in ("is_sim_free", "fully_functional", "has_scratches", "has_damage", "has_issues"):
        if k in out:
            out[k] = _as_int_flag(out[k])
    if "battery_percentage" in out and out["battery_percentage"] is not None:
        try:
            out["battery_percentage"] = float(out["battery_percentage"])
        except (TypeError, ValueError):
            out["battery_percentage"] = 80.0
    return out