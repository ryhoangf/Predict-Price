"""Train a dedicated monotonic hedonic depreciation model."""
from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.model_selection import GroupShuffleSplit
from sqlalchemy import create_engine, text

import config as cfg
from ml_models.smart_price_predictor import SmartPricePredictor


MYSQL_URI = (
    f"mysql+pymysql://{cfg.MYSQL_USER}:{cfg.MYSQL_PASSWORD}"
    f"@{cfg.MYSQL_HOST}:{cfg.MYSQL_PORT}/{cfg.MYSQL_DB}"
)
BRANDS = [
    "apple", "samsung", "google", "sony", "sharp", "xiaomi",
    "oppo", "motorola", "huawei", "asus",
]
FEATURES = [
    "device_age_years",
    "storage_log",
    "listing_count_log",
    *[f"brand_{brand}" for brand in BRANDS],
]
MODEL_FAMILIES = [
    "iphone", "galaxy_s", "galaxy_a", "pixel", "xperia", "aquos",
    "redmi", "xiaomi", "oppo", "moto", "huawei", "zenfone", "other",
]


def model_family(value: object) -> str:
    text_value = str(value or "").strip().lower()
    rules = [
        ("iphone", "iphone"),
        ("galaxy s", "galaxy_s"),
        ("galaxy a", "galaxy_a"),
        ("pixel", "pixel"),
        ("xperia", "xperia"),
        ("aquos", "aquos"),
        ("redmi", "redmi"),
        ("xiaomi", "xiaomi"),
        ("oppo", "oppo"),
        ("moto", "moto"),
        ("huawei", "huawei"),
        ("zenfone", "zenfone"),
    ]
    return next((family for token, family in rules if token in text_value), "other")


def _parse_specs(value) -> dict:
    if isinstance(value, dict):
        return value
    try:
        return json.loads(value or "{}")
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}


def storage_gb(value) -> float:
    match = re.search(r"(\d+(?:\.\d+)?)\s*(TB|GB)?", str(value or ""), re.IGNORECASE)
    if not match:
        return 64.0
    amount = float(match.group(1))
    if (match.group(2) or "").upper() == "TB":
        amount *= 1024.0
    return amount if 1 <= amount <= 2048 else 64.0


def feature_row(
    *,
    device_age_years: float,
    storage: object,
    listing_count: float,
    brand: str,
    model_series: object = "",
) -> dict[str, float]:
    normalized_brand = str(brand or "").strip().lower()
    family = model_family(model_series)
    return {
        "device_age_years": float(max(device_age_years, 0.0)),
        "storage_log": float(np.log1p(storage_gb(storage))),
        "listing_count_log": float(np.log1p(max(float(listing_count or 0), 0.0))),
        **{
            f"brand_{known}": float(normalized_brand == known)
            for known in BRANDS
        },
        **{
            f"family_{known}": float(family == known)
            for known in MODEL_FAMILIES
        },
    }


FEATURES.extend([f"family_{family}" for family in MODEL_FAMILIES])


def prepare_panel(history: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    predictor = SmartPricePredictor(n_estimators=1, max_depth=1)
    products = history[
        ["product_id", "name", "model_series", "base_specs", "brand"]
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
    rows = rows.sort_values(["product_id", "record_date"]).drop_duplicates(
        "product_id", keep="last"
    )
    rows["release_year"] = rows["product_id"].map(release_year)
    rows["device_age_years"] = (
        rows["record_date"].dt.year - rows["release_year"]
    ).clip(lower=0, upper=12)
    rows["storage"] = rows["base_specs"].map(
        lambda raw: _parse_specs(raw).get("storage")
    )
    feature_frame = pd.DataFrame([
        feature_row(
            device_age_years=row.device_age_years,
            storage=row.storage,
            listing_count=row.listing_count,
            brand=row.brand,
            model_series=row.model_series,
        )
        for row in rows.itertuples()
    ], index=rows.index)
    rows[FEATURES] = feature_frame[FEATURES]
    rows["target_log_price"] = np.log(
        pd.to_numeric(rows["avg_price"], errors="coerce")
    )
    rows = rows.replace([np.inf, -np.inf], np.nan).dropna(
        subset=FEATURES + ["target_log_price"]
    )
    return rows, {
        "products": int(rows["product_id"].nunique()),
        "rows": int(len(rows)),
        "date_min": rows["record_date"].min().isoformat() if len(rows) else None,
        "date_max": rows["record_date"].max().isoformat() if len(rows) else None,
        "age_min": float(rows["device_age_years"].min()) if len(rows) else None,
        "age_max": float(rows["device_age_years"].max()) if len(rows) else None,
        "brands": int(rows["brand"].nunique()) if len(rows) else 0,
    }


def _metrics(actual_log: np.ndarray, predicted_log: np.ndarray) -> dict:
    actual = np.exp(actual_log)
    predicted = np.exp(predicted_log)
    absolute = np.abs(actual - predicted)
    pct = absolute / np.maximum(actual, 1.0)
    return {
        "mae_vnd": float(absolute.mean()),
        "mape_pct": float(pct.mean() * 100.0),
        "within_20pct": float(np.mean(pct <= 0.20)),
        "r2_log_price": float(r2_score(actual_log, predicted_log)),
    }


def train(panel: pd.DataFrame, **_ignored) -> tuple[dict | None, dict]:
    if len(panel) < 500 or panel["product_id"].nunique() < 100:
        return None, {
            "passed": False,
            "reason": "Need at least 100 products and 500 catalog snapshots.",
        }
    groups = panel["model_series"].fillna(panel["name"]).astype(str)
    splitter = GroupShuffleSplit(n_splits=1, test_size=0.20, random_state=42)
    train_index, test_index = next(splitter.split(panel, groups=groups))
    train_rows = panel.iloc[train_index]
    test_rows = panel.iloc[test_index]
    model = HistGradientBoostingRegressor(
        loss="absolute_error",
        learning_rate=0.04,
        max_iter=400,
        max_leaf_nodes=31,
        min_samples_leaf=20,
        l2_regularization=2.0,
        monotonic_cst=[-1, *([0] * (len(FEATURES) - 1))],
        random_state=42,
    )
    model.fit(train_rows[FEATURES], train_rows["target_log_price"])
    prediction = model.predict(test_rows[FEATURES])
    model_metrics = _metrics(test_rows["target_log_price"].to_numpy(), prediction)

    brand_medians = train_rows.groupby("brand")["target_log_price"].median()
    global_median = float(train_rows["target_log_price"].median())
    baseline = test_rows["brand"].map(brand_medians).fillna(global_median).to_numpy()
    baseline_metrics = _metrics(test_rows["target_log_price"].to_numpy(), baseline)
    mae_ratio = model_metrics["mae_vnd"] / max(baseline_metrics["mae_vnd"], 1.0)
    quality_gate = {
        "max_mae_ratio_vs_brand_baseline": 0.95,
        "min_r2_log_price": 0.30,
        "min_within_20pct": 0.25,
        "actual_mae_ratio_vs_brand_baseline": float(mae_ratio),
        "actual_r2_log_price": model_metrics["r2_log_price"],
        "actual_within_20pct": model_metrics["within_20pct"],
    }
    quality_gate["passed"] = bool(
        mae_ratio <= quality_gate["max_mae_ratio_vs_brand_baseline"]
        and model_metrics["r2_log_price"] >= quality_gate["min_r2_log_price"]
        and model_metrics["within_20pct"] >= quality_gate["min_within_20pct"]
    )
    report = {
        "passed": quality_gate["passed"],
        "n_train": int(len(train_rows)),
        "n_test": int(len(test_rows)),
        "split": "group_holdout_by_model_series",
        "train_model_series": int(groups.iloc[train_index].nunique()),
        "test_model_series": int(groups.iloc[test_index].nunique()),
        "metrics": {
            "model": model_metrics,
            "brand_median_baseline": baseline_metrics,
        },
        "quality_gate": quality_gate,
    }
    if not quality_gate["passed"]:
        report["reason"] = "Hedonic depreciation model failed the quality gate."
        return None, report
    return {
        "model": model,
        "features": FEATURES,
        "brands": BRANDS,
        "model_families": MODEL_FAMILIES,
        "metrics": report["metrics"],
        "trained_at": datetime.now(timezone.utc).isoformat(),
        "method": "monotonic_hedonic_depreciation_v2",
        "quality_gate": quality_gate,
    }, report


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default="models/depreciation_model.pkl")
    parser.add_argument("--report-output", default="models/depreciation_model_report.json")
    args = parser.parse_args()
    engine = create_engine(MYSQL_URI, pool_pre_ping=True)
    history = pd.read_sql(
        text(
            """
            SELECT
                ph.product_id, ph.record_date, ph.avg_price, ph.listing_count,
                p.name, p.brand, p.model_series, p.base_specs
            FROM price_history ph
            JOIN products p ON p.product_id = ph.product_id
            WHERE ph.avg_price IS NOT NULL AND ph.avg_price > 0
            ORDER BY ph.product_id, ph.record_date
            """
        ),
        engine,
    )
    panel, data_report = prepare_panel(history)
    artifact, training_report = train(panel)
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
