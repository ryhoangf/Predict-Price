"""Train a dedicated monotonic retained-value model from longitudinal history."""
from __future__ import annotations

import argparse
import json
import math
import re
from datetime import datetime, timezone
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, r2_score
from sqlalchemy import create_engine, text

import config as cfg
from ml_models.smart_price_predictor import SmartPricePredictor


MYSQL_URI = (
    f"mysql+pymysql://{cfg.MYSQL_USER}:{cfg.MYSQL_PASSWORD}"
    f"@{cfg.MYSQL_HOST}:{cfg.MYSQL_PORT}/{cfg.MYSQL_DB}"
)
FEATURES = [
    "elapsed_years",
    "device_age_years",
    "storage_log",
    "listing_count_log",
]


def _parse_specs(value) -> dict:
    if isinstance(value, dict):
        return value
    try:
        return json.loads(value or "{}")
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}


def _storage_gb(value) -> float:
    text_value = str(value or "")
    match = re.search(r"(\d+(?:\.\d+)?)\s*(TB|GB)?", text_value, re.IGNORECASE)
    if not match:
        return 64.0
    amount = float(match.group(1))
    if (match.group(2) or "").upper() == "TB":
        amount *= 1024.0
    return amount if 1 <= amount <= 2048 else 64.0


def prepare_panel(history: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    predictor = SmartPricePredictor(n_estimators=1, max_depth=1)
    products = history[
        ["product_id", "name", "model_series", "base_specs"]
    ].drop_duplicates("product_id")
    identity = products.assign(
        model_line=products["model_series"].fillna(products["name"]),
        model_number="",
        variant="",
        condition="Good",
        battery_percentage=85,
        screen_condition="clean",
        body_condition="good",
        storage="64",
        ram="4",
        platform="Mercari",
        has_box=True,
        has_charger=True,
        is_sim_free=True,
        fully_functional=True,
        has_scratches=False,
        has_damage=False,
        has_issues=False,
    )
    engineered = predictor.engineer_features(identity)
    release_year = pd.Series(
        engineered["release_year"].to_numpy(),
        index=products["product_id"],
    )

    rows = history.copy()
    rows["record_date"] = pd.to_datetime(rows["record_date"])
    rows["release_year"] = rows["product_id"].map(release_year)
    rows["storage_gb"] = rows["base_specs"].map(
        lambda raw: _storage_gb(_parse_specs(raw).get("storage"))
    )
    rows["first_date"] = rows.groupby("product_id")["record_date"].transform("min")
    rows["last_date"] = rows.groupby("product_id")["record_date"].transform("max")
    rows["history_points"] = rows.groupby("product_id")["record_date"].transform("count")
    rows["history_span_days"] = (
        rows["last_date"] - rows["first_date"]
    ).dt.days
    rows["baseline_price"] = rows.groupby("product_id")["avg_price"].transform("first")
    rows["retained_value"] = rows["avg_price"] / rows["baseline_price"]
    rows["elapsed_years"] = (
        rows["record_date"] - rows["first_date"]
    ).dt.days / 365.25
    rows["device_age_years"] = (
        rows["record_date"].dt.year - rows["release_year"]
    ).clip(lower=0)
    rows["storage_log"] = np.log1p(rows["storage_gb"])
    rows["listing_count_log"] = np.log1p(
        pd.to_numeric(rows["listing_count"], errors="coerce").fillna(0)
    )
    rows = rows.replace([np.inf, -np.inf], np.nan).dropna(
        subset=FEATURES + ["retained_value"]
    )
    report = {
        "products": int(rows["product_id"].nunique()),
        "rows": int(len(rows)),
        "date_min": rows["record_date"].min().isoformat() if len(rows) else None,
        "date_max": rows["record_date"].max().isoformat() if len(rows) else None,
        "max_history_span_days": int(rows["history_span_days"].max()) if len(rows) else 0,
        "median_history_span_days": float(rows.groupby("product_id")[
            "history_span_days"
        ].max().median()) if len(rows) else 0,
    }
    return rows, report


def train(
    panel: pd.DataFrame,
    *,
    min_points: int = 8,
    min_span_days: int = 30,
) -> tuple[dict | None, dict]:
    eligible = panel[
        (panel["history_points"] >= min_points)
        & (panel["history_span_days"] >= min_span_days)
    ].copy()
    sufficiency = {
        "min_points_per_product": min_points,
        "min_span_days": min_span_days,
        "eligible_products": int(eligible["product_id"].nunique()),
        "eligible_rows": int(len(eligible)),
    }
    if eligible["product_id"].nunique() < 30 or len(eligible) < 500:
        return None, {
            **sufficiency,
            "passed": False,
            "reason": "Need at least 30 products and 500 longitudinal rows.",
        }

    cutoff = eligible["record_date"].quantile(0.80)
    train_rows = eligible[eligible["record_date"] < cutoff]
    test_rows = eligible[eligible["record_date"] >= cutoff]
    model = HistGradientBoostingRegressor(
        learning_rate=0.05,
        max_iter=300,
        max_leaf_nodes=31,
        l2_regularization=1.0,
        monotonic_cst=[-1, -1, 0, 0],
        random_state=42,
    )
    model.fit(train_rows[FEATURES], train_rows["retained_value"])
    prediction = model.predict(test_rows[FEATURES])
    prediction = np.clip(prediction, 0.0, 1.5)
    metrics = {
        "mae_retained_value": float(
            mean_absolute_error(test_rows["retained_value"], prediction)
        ),
        "r2_retained_value": float(
            r2_score(test_rows["retained_value"], prediction)
        ),
        "n_train": int(len(train_rows)),
        "n_test": int(len(test_rows)),
        "cutoff": pd.Timestamp(cutoff).isoformat(),
    }
    quality_passed = bool(
        metrics["r2_retained_value"] >= 0.15
        and metrics["mae_retained_value"] <= 0.20
    )
    quality = {
        "min_r2_retained_value": 0.15,
        "max_mae_retained_value": 0.20,
        "passed": quality_passed,
    }
    if not quality_passed:
        return None, {
            **sufficiency,
            "passed": False,
            "reason": "Longitudinal model failed the predictive quality gate.",
            "metrics": metrics,
            "quality_gate": quality,
        }
    artifact = {
        "model": model,
        "features": FEATURES,
        "metrics": metrics,
        "trained_at": datetime.now(timezone.utc).isoformat(),
        "method": "monotonic_retained_value_v1",
        "quality_gate": quality,
    }
    return artifact, {
        **sufficiency,
        "passed": True,
        "metrics": metrics,
        "quality_gate": quality,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default="models/depreciation_model.pkl")
    parser.add_argument("--report-output", default="models/depreciation_model_report.json")
    parser.add_argument("--min-points", type=int, default=8)
    parser.add_argument("--min-span-days", type=int, default=30)
    args = parser.parse_args()
    engine = create_engine(MYSQL_URI, pool_pre_ping=True)
    history = pd.read_sql(
        text(
            """
            SELECT
                ph.product_id, ph.record_date, ph.avg_price, ph.listing_count,
                p.name, p.model_series, p.base_specs
            FROM price_history ph
            JOIN products p ON p.product_id = ph.product_id
            WHERE ph.avg_price IS NOT NULL AND ph.avg_price > 0
            ORDER BY ph.product_id, ph.record_date
            """
        ),
        engine,
    )
    panel, data_report = prepare_panel(history)
    artifact, training_report = train(
        panel,
        min_points=args.min_points,
        min_span_days=args.min_span_days,
    )
    report = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "data": data_report,
        "training": training_report,
    }
    report_path = Path(args.report_output)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    if artifact is not None:
        output = Path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        joblib.dump(artifact, output)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if artifact is not None else 2


if __name__ == "__main__":
    raise SystemExit(main())
