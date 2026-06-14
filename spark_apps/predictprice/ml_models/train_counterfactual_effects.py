"""Train matched, observational feature-effect evidence for explanations."""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd


SCENARIOS = {
    "has_box_true": ("has_box", True),
    "has_charger_true": ("has_charger", True),
    "no_scratches": ("has_scratches", False),
    "no_damage": ("has_damage", False),
    "screen_clean": ("screen_condition", "clean"),
    "body_good": ("body_condition", "good"),
}


def _model_key(frame: pd.DataFrame) -> pd.Series:
    return (
        frame["brand"].fillna("").astype(str).str.lower().str.strip()
        + "|"
        + frame["model_line"].fillna("").astype(str).str.lower().str.strip()
        + "|"
        + frame["model_number"].fillna("").astype(str).str.lower().str.strip()
        + "|"
        + frame["variant"].fillna("").astype(str).str.lower().str.strip()
        + "|"
        + frame["storage"].fillna("").astype(str).str.lower().str.strip()
    )


def _boolean(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _summary(values: list[float]) -> dict[str, Any] | None:
    clean = np.asarray([value for value in values if np.isfinite(value)], dtype=float)
    if len(clean) < 8:
        return None
    return {
        "effect_yen": float(np.median(clean)),
        "lower_yen": float(np.quantile(clean, 0.10)),
        "upper_yen": float(np.quantile(clean, 0.90)),
        "matched_groups": int(len(clean)),
        "stable_direction": float(max(np.mean(clean >= 0), np.mean(clean <= 0))),
    }


def _matched_effects(
    frame: pd.DataFrame,
    *,
    field: str,
    reference: Any,
) -> pd.DataFrame:
    rows = frame[["_model_key", "brand", "price", field]].dropna(
        subset=["_model_key", "price", field]
    ).copy()
    if isinstance(reference, bool):
        rows["_reference"] = rows[field].map(_boolean) == reference
    else:
        rows["_reference"] = (
            rows[field].astype(str).str.lower().str.strip()
            == str(reference).lower()
        )
    aggregates = rows.groupby(["_model_key", "brand", "_reference"])["price"].agg(
        ["median", "count"]
    ).reset_index()
    pivot = aggregates.pivot_table(
        index=["_model_key", "brand"],
        columns="_reference",
        values=["median", "count"],
        aggfunc="first",
    )
    required = [
        ("median", False), ("median", True), ("count", False), ("count", True)
    ]
    if any(column not in pivot.columns for column in required):
        return pd.DataFrame(columns=["brand", "effect_yen"])
    pivot = pivot.dropna(subset=required)
    pivot = pivot[
        (pivot[("count", False)] >= 3) & (pivot[("count", True)] >= 3)
    ]
    return pd.DataFrame({
        "brand": pivot.index.get_level_values("brand").astype(str).str.lower(),
        "effect_yen": (
            pivot[("median", True)] - pivot[("median", False)]
        ).to_numpy(dtype=float),
    })


def train(frame: pd.DataFrame) -> tuple[dict[str, Any], dict[str, Any]]:
    data = frame.copy()
    data["price"] = pd.to_numeric(data["price"], errors="coerce")
    data = data[(data["price"] >= 5000) & (data["price"] <= 300000)]
    data["_model_key"] = _model_key(data)
    data["brand"] = data["brand"].fillna("").astype(str).str.lower().str.strip()
    effects: dict[str, Any] = {}
    for scenario_id, (field, reference) in SCENARIOS.items():
        if field not in data.columns:
            continue
        matched = _matched_effects(data, field=field, reference=reference)
        global_summary = _summary(matched["effect_yen"].tolist())
        if not global_summary:
            continue
        by_brand = {}
        for brand, part in matched.groupby("brand"):
            summary = _summary(part["effect_yen"].tolist())
            if summary:
                by_brand[str(brand)] = summary
        effects[scenario_id] = {
            "field": field,
            "reference": reference,
            "global": global_summary,
            "by_brand": by_brand,
        }

    if "battery_percentage" in data.columns:
        battery = data.copy()
        battery["_battery"] = pd.to_numeric(
            battery["battery_percentage"], errors="coerce"
        )
        battery = battery[battery["_battery"].between(50, 100)]
        battery["_reference"] = battery["_battery"] >= 90
        aggregates = battery.groupby(
            ["_model_key", "brand", "_reference"]
        )["price"].agg(["median", "count"]).reset_index()
        pivot = aggregates.pivot_table(
            index=["_model_key", "brand"],
            columns="_reference",
            values=["median", "count"],
            aggfunc="first",
        )
        required = [
            ("median", False), ("median", True), ("count", False), ("count", True)
        ]
        if all(column in pivot.columns for column in required):
            pivot = pivot.dropna(subset=required)
            pivot = pivot[
                (pivot[("count", False)] >= 3) & (pivot[("count", True)] >= 3)
            ]
            matched = pd.DataFrame({
                "brand": pivot.index.get_level_values("brand").astype(str).str.lower(),
                "effect_yen": (
                    pivot[("median", True)] - pivot[("median", False)]
                ).to_numpy(dtype=float),
            })
            global_summary = _summary(matched["effect_yen"].tolist())
            if global_summary:
                effects["battery_to_100"] = {
                    "field": "battery_percentage",
                    "reference": 100.0,
                    "comparison": "90plus_vs_below90",
                    "global": global_summary,
                    "by_brand": {
                        str(brand): summary
                        for brand, part in matched.groupby("brand")
                        if (summary := _summary(part["effect_yen"].tolist()))
                    },
                }

    report = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "rows": int(len(data)),
        "model_groups": int(data["_model_key"].nunique()),
        "scenarios": {
            key: value["global"] for key, value in effects.items()
        },
        "passed": len(effects) >= 4,
    }
    artifact = {
        "method": "matched_model_variant_effects_v1",
        "trained_at": report["created_at"],
        "effects": effects,
        "report": report,
    }
    return artifact, report


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", default="book_data/training_data.csv")
    parser.add_argument(
        "--output", default="spark_apps/predictprice/models/counterfactual_effects.pkl"
    )
    parser.add_argument(
        "--report-output",
        default="spark_apps/predictprice/models/counterfactual_effects_report.json",
    )
    args = parser.parse_args()
    artifact, report = train(pd.read_csv(args.data))
    report_path = Path(args.report_output)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    if report["passed"]:
        output = Path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        joblib.dump(artifact, output)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if report["passed"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
