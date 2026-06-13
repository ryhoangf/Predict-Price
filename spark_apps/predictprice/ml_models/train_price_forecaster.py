"""Train and temporal-backtest the direct multi-horizon price forecaster."""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingRegressor, HistGradientBoostingRegressor
from sqlalchemy import create_engine, text

import config as cfg
from ml_models.temporal_price_forecaster import FEATURES, build_feature_row


MYSQL_URI = (
    f"mysql+pymysql://{cfg.MYSQL_USER}:{cfg.MYSQL_PASSWORD}"
    f"@{cfg.MYSQL_HOST}:{cfg.MYSQL_PORT}/{cfg.MYSQL_DB}"
)


def build_supervised_rows(
    history: pd.DataFrame,
    *,
    min_history: int = 14,
    max_horizon: int = 30,
) -> pd.DataFrame:
    samples = []
    for product_id, part in history.groupby("product_id"):
        part = part.sort_values("record_date").drop_duplicates("record_date", keep="last")
        records = [
            {
                "record_date": pd.Timestamp(row.record_date).date(),
                "avg_price": float(row.avg_price),
                "listing_count": float(row.listing_count or 0),
            }
            for row in part.itertuples()
            if float(row.avg_price or 0) > 0
        ]
        for origin_index in range(min_history - 1, len(records) - 1):
            origin = records[origin_index]
            prefix = records[: origin_index + 1]
            anchor = origin["avg_price"]
            for target in records[origin_index + 1:]:
                horizon = (target["record_date"] - origin["record_date"]).days
                if horizon <= 0:
                    continue
                if horizon > max_horizon:
                    break
                feature = build_feature_row(prefix, horizon_days=horizon)
                samples.append({
                    "product_id": product_id,
                    "origin_date": origin["record_date"],
                    "target_date": target["record_date"],
                    "anchor_price": anchor,
                    "actual_price": target["avg_price"],
                    "target_log_ratio": float(np.log(target["avg_price"] / anchor)),
                    **feature,
                })
    return pd.DataFrame(samples)


def _metrics(actual: np.ndarray, predicted: np.ndarray) -> dict[str, float]:
    error = np.abs(actual - predicted)
    pct = error / np.maximum(np.abs(actual), 1.0)
    return {
        "n": int(len(actual)),
        "mae_vnd": float(error.mean()),
        "median_ae_vnd": float(np.median(error)),
        "mape_pct": float(pct.mean() * 100.0),
        "within_10pct": float(np.mean(pct <= 0.10)),
        "within_20pct": float(np.mean(pct <= 0.20)),
    }


def train(
    samples: pd.DataFrame,
    *,
    test_fraction: float = 0.20,
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    if len(samples) < 500:
        return None, {"passed": False, "reason": "Need at least 500 temporal samples."}
    samples = samples.sort_values(["target_date", "product_id", "origin_date"]).reset_index(drop=True)
    cutoff_index = max(1, int(len(samples) * (1.0 - test_fraction)))
    cutoff = samples.iloc[cutoff_index]["target_date"]
    train_rows = samples[samples["target_date"] < cutoff]
    test_rows = samples[samples["target_date"] >= cutoff]
    if len(train_rows) < 300 or len(test_rows) < 100:
        return None, {"passed": False, "reason": "Temporal split is too small."}

    model = HistGradientBoostingRegressor(
        loss="absolute_error",
        learning_rate=0.04,
        max_iter=350,
        max_leaf_nodes=31,
        min_samples_leaf=20,
        l2_regularization=2.0,
        random_state=42,
    )
    lower_model = GradientBoostingRegressor(
        loss="quantile",
        alpha=0.10,
        n_estimators=250,
        learning_rate=0.04,
        max_depth=3,
        min_samples_leaf=20,
        random_state=42,
    )
    upper_model = GradientBoostingRegressor(
        loss="quantile",
        alpha=0.90,
        n_estimators=250,
        learning_rate=0.04,
        max_depth=3,
        min_samples_leaf=20,
        random_state=42,
    )
    x_train = train_rows[FEATURES]
    y_train = train_rows["target_log_ratio"]
    model.fit(x_train, y_train)
    lower_model.fit(x_train, y_train)
    upper_model.fit(x_train, y_train)

    x_test = test_rows[FEATURES]
    anchor = test_rows["anchor_price"].to_numpy(dtype=float)
    actual = test_rows["actual_price"].to_numpy(dtype=float)
    predicted = anchor * np.exp(model.predict(x_test))
    last_value = anchor
    median_7 = anchor * test_rows["median_ratio_7"].to_numpy(dtype=float)
    model_metrics = _metrics(actual, predicted)
    last_metrics = _metrics(actual, last_value)
    median_metrics = _metrics(actual, median_7)

    lower = anchor * np.exp(lower_model.predict(x_test))
    upper = anchor * np.exp(upper_model.predict(x_test))
    interval_coverage = float(np.mean((actual >= lower) & (actual <= upper)))
    best_baseline_mae = min(last_metrics["mae_vnd"], median_metrics["mae_vnd"])
    mae_ratio = model_metrics["mae_vnd"] / max(best_baseline_mae, 1.0)
    quality_gate = {
        "max_mae_ratio_vs_best_baseline": 0.99,
        "min_within_20pct": 0.70,
        "min_interval_coverage": 0.70,
        "actual_mae_ratio_vs_best_baseline": float(mae_ratio),
        "actual_within_20pct": model_metrics["within_20pct"],
        "actual_interval_coverage": interval_coverage,
    }
    quality_gate["passed"] = bool(
        mae_ratio <= quality_gate["max_mae_ratio_vs_best_baseline"]
        and model_metrics["within_20pct"] >= quality_gate["min_within_20pct"]
        and interval_coverage >= quality_gate["min_interval_coverage"]
    )
    report = {
        "passed": quality_gate["passed"],
        "cutoff": str(cutoff),
        "n_train": int(len(train_rows)),
        "n_test": int(len(test_rows)),
        "products": int(samples["product_id"].nunique()),
        "date_min": str(samples["target_date"].min()),
        "date_max": str(samples["target_date"].max()),
        "metrics": {
            "model": model_metrics,
            "last_value": last_metrics,
            "median_7": median_metrics,
            "interval_80_coverage": interval_coverage,
        },
        "quality_gate": quality_gate,
    }
    if not quality_gate["passed"]:
        report["reason"] = "Temporal model did not beat the best baseline quality gate."
        return None, report
    metadata = {
        "method": "hist_gradient_boosting_direct_horizon_v1",
        "trained_at": datetime.now(timezone.utc).isoformat(),
        "metrics": report["metrics"],
        "quality_gate": quality_gate,
    }
    return {
        "model": model,
        "lower_model": lower_model,
        "upper_model": upper_model,
        "features": FEATURES,
        "metadata": metadata,
    }, report


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default="models/temporal_price_forecaster.pkl")
    parser.add_argument("--report-output", default="models/temporal_price_forecaster_report.json")
    parser.add_argument("--min-history", type=int, default=14)
    parser.add_argument("--max-horizon", type=int, default=30)
    args = parser.parse_args()
    engine = create_engine(MYSQL_URI, pool_pre_ping=True)
    history = pd.read_sql(
        text(
            """
            SELECT product_id, record_date, avg_price, listing_count
            FROM price_history
            WHERE avg_price IS NOT NULL AND avg_price > 0
            ORDER BY product_id, record_date
            """
        ),
        engine,
    )
    samples = build_supervised_rows(
        history,
        min_history=args.min_history,
        max_horizon=args.max_horizon,
    )
    artifact, training = train(samples)
    report = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "samples": int(len(samples)),
        "training": training,
    }
    report_path = Path(args.report_output)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    if artifact is not None:
        output = Path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        joblib.dump(artifact, output)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if artifact is not None else 2


if __name__ == "__main__":
    raise SystemExit(main())
