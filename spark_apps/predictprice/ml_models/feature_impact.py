"""
Phân tích tác động linh kiện — counterfactual (Hướng A).
delta_yen > 0: nâng yếu tố lên mức tham chiếu làm giá dự báo tăng.
"""
from __future__ import annotations

import copy
import os
from dataclasses import dataclass
from typing import Any, Callable

import pandas as pd
import joblib

from ml_models.smart_price_predictor import SmartPricePredictor

DEFAULT_YEN_TO_VND = 175
DEFAULT_EFFECTS_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "models",
    "counterfactual_effects.pkl",
)


def _load_effects(path: str | None = None) -> dict[str, Any] | None:
    artifact_path = path or DEFAULT_EFFECTS_PATH
    if not os.path.exists(artifact_path):
        return None
    try:
        return joblib.load(artifact_path)
    except (OSError, ValueError, TypeError, KeyError):
        return None


def _evidence_for(
    artifact: dict[str, Any] | None,
    scenario_id: str,
    brand: object,
) -> dict[str, Any] | None:
    if not artifact:
        return None
    scenario = (artifact.get("effects") or {}).get(scenario_id)
    if not scenario:
        return None
    normalized_brand = str(brand or "").strip().lower()
    return (
        (scenario.get("by_brand") or {}).get(normalized_brand)
        or scenario.get("global")
    )


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
    effects_path: str | None = None,
) -> dict[str, Any]:
    base = {k: v for k, v in raw_row.items() if v is not None}
    base_yen = predict_yen(predictor, base)
    evidence_artifact = _load_effects(effects_path)
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
        model_delta_yen = float(alt_yen - base_yen)
        evidence = _evidence_for(
            evidence_artifact, sc.id, base.get("brand")
        )
        stability = float((evidence or {}).get("stable_direction") or 0.5)
        if (
            evidence
            and int(evidence.get("matched_groups") or 0) >= 8
            and stability >= 0.65
        ):
            empirical_delta = float(evidence["effect_yen"])
            if sc.id == "battery_to_100":
                battery_gap = max(0.0, 100.0 - float(before_val or 80.0))
                empirical_delta *= min(1.0, battery_gap / 20.0)
            empirical_weight = min(0.80, max(0.50, stability))
            delta_yen = (
                empirical_weight * empirical_delta
                + (1.0 - empirical_weight) * model_delta_yen
            )
            lower_yen = float(evidence["lower_yen"])
            upper_yen = float(evidence["upper_yen"])
            support = "strong" if evidence["matched_groups"] >= 30 else "moderate"
        elif evidence:
            delta_yen = model_delta_yen
            lower_yen = float(evidence["lower_yen"])
            upper_yen = float(evidence["upper_yen"])
            support = "uncertain"
        else:
            delta_yen = model_delta_yen
            lower_yen = None
            upper_yen = None
            support = "model_only"
        delta_vnd = round(delta_yen * float(yen_to_vnd), 2)
        # delta > 0: nâng yếu tố lên chuẩn → giá tăng → trạng thái hiện tại đang "thiệt" |delta|
        deficit_vnd = round(max(0.0, delta_vnd), 2)
        gain_if_fixed_vnd = deficit_vnd
        impacts.append({
            "id": sc.id,
            "label_vi": sc.label_vi,
            "field": sc.field,
            "value_before": before_val,
            "value_reference": sc.reference,
            "delta_yen": round(delta_yen, 2),
            "model_delta_yen": round(model_delta_yen, 2),
            "delta_vnd": delta_vnd,
            "deficit_vnd": deficit_vnd,
            "gain_if_fixed_vnd": gain_if_fixed_vnd,
            "evidence_support": support,
            "matched_groups": int(evidence.get("matched_groups") or 0)
            if evidence else 0,
            "evidence_direction_agreement": round(stability, 3)
            if evidence else None,
            "interval_vnd": (
                {
                    "lower": round(lower_yen * float(yen_to_vnd), 2),
                    "upper": round(upper_yen * float(yen_to_vnd), 2),
                }
                if lower_yen is not None and upper_yen is not None else None
            ),
            "message_vi": _impact_message_vi(
                sc.label_vi, before_val, sc.reference, deficit_vnd, delta_vnd
            ),
        })

    return {
        "method": "counterfactual",
        "estimator": (
            "matched_evidence_plus_model_counterfactual_v2"
            if evidence_artifact else "model_counterfactual_v1"
        ),
        "baseline_prediction_yen": round(base_yen, 2),
        "baseline_prediction_vnd": round(base_yen * float(yen_to_vnd), 2),
        "yen_to_vnd": float(yen_to_vnd),
        "impacts": impacts,
        "evidence_trained_at": (
            evidence_artifact.get("trained_at") if evidence_artifact else None
        ),
        "disclaimer": (
            "Mỗi dòng chỉ đổi một yếu tố so với mức tham chiếu; "
            "deficit_vnd = mức thiệt so với chuẩn (không cộng tuyến tính khi sửa nhiều yếu tố cùng lúc)."
        ),
    }


def _impact_message_vi(
    label_vi: str,
    value_before: Any,
    value_reference: Any,
    deficit_vnd: float,
    delta_vnd: float,
) -> str:
    if deficit_vnd <= 0 and delta_vnd <= 0:
        if value_before == value_reference:
            return f"{label_vi}: đã đạt mức tham chiếu ({value_reference})."
        return f"{label_vi}: mô hình không ước lượng chênh lệch đáng kể so với chuẩn."
    if "Pin" in label_vi and value_before is not None:
        return (
            f"Pin còn {value_before}% (so với {value_reference}%): "
            f"ước tính thiệt ~{deficit_vnd:,.0f} ₫"
        ).replace(",", ".")
    if label_vi.startswith("Có hộp") and not value_before:
        return f"Thiếu hộp phụ kiện: ước tính thiệt ~{deficit_vnd:,.0f} ₫".replace(",", ".")
    if label_vi.startswith("Có sạc") and not value_before:
        return f"Thiếu sạc: ước tính thiệt ~{deficit_vnd:,.0f} ₫".replace(",", ".")
    return (
        f"{label_vi} ({value_before} → {value_reference}): "
        f"ước tính thiệt ~{deficit_vnd:,.0f} ₫"
    ).replace(",", ".")


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
