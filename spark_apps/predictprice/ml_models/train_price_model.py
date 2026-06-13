"""Leakage-safe training and evaluation entrypoint for SmartPricePredictor."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.metrics import mean_absolute_error, r2_score

from ml_models.smart_price_predictor import SmartPricePredictor


def _json_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_value(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(v) for v in value]
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return float(value)
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()
    return value


def dataset_fingerprint(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()[:16]


def read_dataset(path: str) -> pd.DataFrame:
    if str(path).lower().endswith(".parquet"):
        return pd.read_parquet(path)
    return pd.read_csv(path)


def clean_training_frame(df: pd.DataFrame, target_col: str = "price") -> pd.DataFrame:
    required = {
        target_col,
        "model_line",
        "model_number",
        "variant",
        "condition",
    }
    missing = sorted(required - set(df.columns))
    if missing:
        raise ValueError(f"Missing required training columns: {missing}")

    out = df.copy()
    initial_rows = len(out)
    out[target_col] = pd.to_numeric(out[target_col], errors="coerce")
    out = out.dropna(subset=[target_col])
    out = out[(out[target_col] >= 5000) & (out[target_col] <= 300000)]
    for col in ("brand", "model_line", "model_number", "variant"):
        out[col] = out[col].astype("string").str.strip()
        out.loc[out[col].isin(["", "nan", "None", "<NA>"]), col] = pd.NA

    brand_aliases = {
        "APPLE": "Apple",
        "SAMSUNG": "Samsung",
        "SHARP": "Sharp",
        "SONY": "Sony",
        "GOOGLE": "Google",
        "XIAOMI": "Xiaomi",
        "OPPO": "Oppo",
        "MOTOROLA": "Motorola",
        "HUAWEI": "Huawei",
        "ASUS": "Asus",
        "REALME": "Realme",
    }
    out["brand"] = out["brand"].str.upper().map(brand_aliases).fillna(out["brand"])
    valid_identity = (
        out["brand"].notna()
        & out["model_line"].notna()
        & out["model_number"].notna()
    )
    missing_identity_rows = int((~valid_identity).sum())
    out = out.loc[valid_identity].copy()
    cleaning_report = {
        "initial_rows": int(initial_rows),
        "kept_rows": int(len(out)),
        "dropped_rows": int(initial_rows - len(out)),
        "dropped_missing_identity": missing_identity_rows,
    }
    out = out.reset_index(drop=True)
    out.attrs["cleaning_report"] = cleaning_report
    return out


def median_baseline(train_df: pd.DataFrame, test_df: pd.DataFrame) -> dict[str, float]:
    names_train = SmartPricePredictor._full_model_name(train_df)
    names_test = SmartPricePredictor._full_model_name(test_df)
    medians = train_df.assign(_model=names_train).groupby("_model")["price"].median()
    global_median = float(train_df["price"].median())
    predictions = names_test.map(medians).fillna(global_median).to_numpy()
    actual = test_df["price"].to_numpy()
    return {
        "mae": float(mean_absolute_error(actual, predictions)),
        "r2": float(r2_score(actual, predictions)),
        "global_median_yen": global_median,
    }


def metrics_by_group(
    predictor: SmartPricePredictor,
    test_df: pd.DataFrame,
    group_col: str,
    *,
    min_samples: int = 30,
) -> list[dict[str, Any]]:
    if group_col not in test_df.columns:
        return []
    rows = test_df.copy()
    rows["_prediction"] = predictor.predict(rows)
    rows["_abs_error"] = np.abs(rows["price"] - rows["_prediction"])
    result = []
    for group, part in rows.groupby(group_col, dropna=False):
        if len(part) < min_samples:
            continue
        result.append({
            "group": str(group),
            "n_samples": int(len(part)),
            "mae": float(part["_abs_error"].mean()),
            "median_ae": float(part["_abs_error"].median()),
        })
    return sorted(result, key=lambda item: item["mae"], reverse=True)


def cold_start_audit(
    df: pd.DataFrame,
    *,
    test_size: float,
    random_state: int,
    n_estimators: int,
    max_depth: int,
) -> dict[str, Any]:
    train_df, test_df, split = SmartPricePredictor.split_data(
        df,
        test_size=test_size,
        random_state=random_state,
        strategy="group",
    )
    predictor = SmartPricePredictor(
        n_estimators=n_estimators,
        max_depth=max_depth,
        random_state=random_state,
    )
    predictor.fit(train_df, verbose=False)
    return {
        "split": split,
        "model": predictor.evaluate(test_df),
        "baseline": median_baseline(train_df, test_df),
    }


def train_and_evaluate(
    data_path: str,
    model_path: str,
    report_path: str,
    *,
    split_strategy: str = "auto",
    time_col: str | None = None,
    test_size: float = 0.2,
    random_state: int = 42,
    n_estimators: int = 200,
    max_depth: int = 25,
    max_mae_ratio_vs_baseline: float = 1.0,
    min_within_20pct: float = 0.60,
    min_interval_coverage: float = 0.85,
    run_cold_start: bool = True,
) -> dict[str, Any]:
    source = clean_training_frame(read_dataset(data_path))
    cleaning_report = source.attrs.get("cleaning_report", {})
    train_df, test_df, split = SmartPricePredictor.split_data(
        source,
        test_size=test_size,
        random_state=random_state,
        strategy=split_strategy,
        time_col=time_col,
    )
    fit_df, calibration_df, calibration_split = SmartPricePredictor.split_data(
        train_df,
        test_size=0.15,
        random_state=random_state,
        strategy="temporal" if split["strategy"] == "temporal" else "random",
        time_col=split.get("time_col"),
    )
    predictor = SmartPricePredictor(
        n_estimators=n_estimators,
        max_depth=max_depth,
        random_state=random_state,
    )
    predictor.fit(fit_df, verbose=True)
    interval_calibration = predictor.calibrate_prediction_interval(
        calibration_df,
        coverage=0.90,
    )
    model_metrics = predictor.evaluate(test_df)
    interval_test = predictor.predict_interval(test_df)
    actual_test = test_df["price"].to_numpy()
    interval_metrics = {
        "coverage": float(np.mean(
            (actual_test >= interval_test["lower"].to_numpy())
            & (actual_test <= interval_test["upper"].to_numpy())
        )),
        "mean_width_yen": float(np.mean(
            interval_test["upper"] - interval_test["lower"]
        )),
        "median_width_yen": float(np.median(
            interval_test["upper"] - interval_test["lower"]
        )),
        **interval_calibration,
    }
    baseline_metrics = median_baseline(train_df, test_df)
    mae_ratio = model_metrics["mae"] / max(baseline_metrics["mae"], 1.0)

    quality_gate = {
        "max_mae_ratio_vs_baseline": max_mae_ratio_vs_baseline,
        "min_within_20pct": min_within_20pct,
        "actual_mae_ratio_vs_baseline": mae_ratio,
        "actual_within_20pct": model_metrics["within_20pct"],
        "min_interval_coverage": min_interval_coverage,
        "actual_interval_coverage": interval_metrics["coverage"],
    }
    quality_gate["passed"] = bool(
        mae_ratio <= max_mae_ratio_vs_baseline
        and model_metrics["within_20pct"] >= min_within_20pct
        and interval_metrics["coverage"] >= min_interval_coverage
    )

    report = {
        "schema_version": 2,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "dataset": {
            "path": os.path.abspath(data_path),
            "sha256_16": dataset_fingerprint(data_path),
            "n_rows": int(len(source)),
            "cleaning": cleaning_report,
        },
        "model": {
            "type": "RandomForestRegressor",
            "n_estimators": n_estimators,
            "max_depth": max_depth,
            "feature_count": int(len(predictor.feature_columns or [])),
        },
        "split": split,
        "calibration_split": calibration_split,
        "metrics": model_metrics,
        "prediction_interval": interval_metrics,
        "median_baseline": baseline_metrics,
        "mae_improvement_vs_baseline_pct": float((1.0 - mae_ratio) * 100.0),
        "metrics_by_brand": metrics_by_group(predictor, test_df, "brand"),
        "quality_gate": quality_gate,
    }
    if run_cold_start:
        report["cold_start_audit"] = cold_start_audit(
            source,
            test_size=test_size,
            random_state=random_state,
            n_estimators=n_estimators,
            max_depth=max_depth,
        )

    predictor.train_stats_.update({
        "test_mae": model_metrics["mae"],
        "test_median_ae": model_metrics["median_ae"],
        "test_rmse": model_metrics["rmse"],
        "test_r2": model_metrics["r2"],
        "test_smape": model_metrics["smape"],
        "test_within_10pct": model_metrics["within_10pct"],
        "test_within_20pct": model_metrics["within_20pct"],
        "n_test": model_metrics["n_samples"],
        "split": split,
        "baseline_mae": baseline_metrics["mae"],
        "quality_gate_passed": quality_gate["passed"],
    })
    predictor.model_metadata_ = {
        "schema_version": 2,
        "trained_at": report["created_at"],
        "dataset_sha256_16": report["dataset"]["sha256_16"],
        "split": split,
        "calibration_split": calibration_split,
        "prediction_interval": interval_metrics,
        "quality_gate": quality_gate,
    }

    Path(report_path).parent.mkdir(parents=True, exist_ok=True)
    with open(report_path, "w", encoding="utf-8") as handle:
        json.dump(_json_value(report), handle, ensure_ascii=False, indent=2)

    if quality_gate["passed"]:
        predictor.save(model_path)
    return report


def parse_args() -> argparse.Namespace:
    app_root = Path(__file__).resolve().parents[1]
    project_root = app_root.parents[1]
    parser = argparse.ArgumentParser(
        description="Train and evaluate the production price model without target leakage."
    )
    parser.add_argument(
        "--data",
        default=str(project_root / "book_data" / "training_data.csv"),
    )
    parser.add_argument(
        "--model-output",
        default=str(app_root / "models" / "smart_price_predictor.pkl"),
    )
    parser.add_argument(
        "--report-output",
        default=str(app_root / "models" / "smart_price_model_report.json"),
    )
    parser.add_argument(
        "--split-strategy",
        choices=["auto", "temporal", "random", "group"],
        default="auto",
    )
    parser.add_argument("--time-col")
    parser.add_argument("--test-size", type=float, default=0.2)
    parser.add_argument("--random-state", type=int, default=42)
    parser.add_argument("--n-estimators", type=int, default=200)
    parser.add_argument("--max-depth", type=int, default=25)
    parser.add_argument("--max-mae-ratio-vs-baseline", type=float, default=1.0)
    parser.add_argument("--min-within-20pct", type=float, default=0.60)
    parser.add_argument("--min-interval-coverage", type=float, default=0.85)
    parser.add_argument("--skip-cold-start", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = train_and_evaluate(
        args.data,
        args.model_output,
        args.report_output,
        split_strategy=args.split_strategy,
        time_col=args.time_col,
        test_size=args.test_size,
        random_state=args.random_state,
        n_estimators=args.n_estimators,
        max_depth=args.max_depth,
        max_mae_ratio_vs_baseline=args.max_mae_ratio_vs_baseline,
        min_within_20pct=args.min_within_20pct,
        min_interval_coverage=args.min_interval_coverage,
        run_cold_start=not args.skip_cold_start,
    )
    print(json.dumps(_json_value(report), ensure_ascii=False, indent=2))
    if not report["quality_gate"]["passed"]:
        print("Quality gate failed; production model artifact was not replaced.")
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
