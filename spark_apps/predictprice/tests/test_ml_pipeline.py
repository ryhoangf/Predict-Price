import os
import sys
import tempfile
import unittest

import joblib
import numpy as np
import pandas as pd


APP_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if APP_ROOT not in sys.path:
    sys.path.insert(0, APP_ROOT)

from ml_models.smart_price_predictor import SmartPricePredictor
from ml_models.backtest_price_forecast import backtest
from ml_models.forecast_algorithms import converging_rolling_median
from ml_models.train_depreciation_model import train as train_depreciation
from ml_models.temporal_price_forecaster import (
    FEATURES as FORECAST_FEATURES,
    TemporalPriceForecaster,
    build_feature_row,
)
from ml_models.train_price_forecaster import build_supervised_rows


def sample_frame():
    rows = []
    models = [
        ("iPhone", "13", "", 50000),
        ("iPhone", "13", "Pro", 65000),
        ("Galaxy S", "22", "", 42000),
        ("Pixel", "7", "", 38000),
    ]
    for model_line, model_number, variant, base_price in models:
        for index in range(8):
            rows.append({
                "price": base_price + index * 250,
                "model_line": model_line,
                "model_number": model_number,
                "variant": variant,
                "condition": "Good",
                "battery_percentage": 85 - index % 3,
                "screen_condition": "clean",
                "body_condition": "good",
                "storage": "128GB",
                "ram": "6GB",
                "platform": "Mercari",
                "has_box": index % 2 == 0,
                "has_charger": True,
                "is_sim_free": True,
                "fully_functional": True,
                "has_scratches": False,
                "has_damage": False,
                "has_issues": False,
                "brand": "Apple" if model_line == "iPhone" else "Android",
            })
    return pd.DataFrame(rows)


class MlPipelineTests(unittest.TestCase):
    def test_group_split_has_no_model_overlap(self):
        frame = sample_frame()
        train_df, test_df, info = SmartPricePredictor.split_data(
            frame,
            strategy="group",
            test_size=0.25,
        )
        train_models = set(SmartPricePredictor._full_model_name(train_df))
        test_models = set(SmartPricePredictor._full_model_name(test_df))
        self.assertEqual(info["strategy"], "group")
        self.assertFalse(train_models & test_models)

    def test_target_encoding_is_fit_on_train_only(self):
        frame = sample_frame()
        train_df = frame[frame["model_number"] != "7"].copy()
        held_out = frame[frame["model_number"] == "7"].copy()
        predictor = SmartPricePredictor(n_estimators=5, max_depth=4)
        predictor.fit(train_df, verbose=False)
        held_out_name = SmartPricePredictor._full_model_name(held_out).iloc[0]
        self.assertNotIn(held_out_name, predictor.model_price_map)

    def test_auto_split_prefers_timestamp(self):
        frame = sample_frame()
        frame["ingested_at"] = pd.date_range(
            "2026-01-01",
            periods=len(frame),
            freq="h",
        )
        train_df, test_df, info = SmartPricePredictor.split_data(
            frame,
            strategy="auto",
            test_size=0.25,
        )
        self.assertEqual(info["strategy"], "temporal")
        self.assertLessEqual(
            pd.to_datetime(train_df["ingested_at"]).max(),
            pd.to_datetime(test_df["ingested_at"]).min(),
        )

    def test_metadata_round_trip(self):
        predictor = SmartPricePredictor(n_estimators=5, max_depth=4)
        predictor.fit(sample_frame(), verbose=False)
        predictor.model_metadata_ = {"dataset_sha256_16": "abc123"}
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "model.pkl")
            predictor.save(path)
            loaded = SmartPricePredictor(n_estimators=5, max_depth=4)
            loaded.load(path)
        self.assertEqual(
            loaded.model_metadata_["dataset_sha256_16"],
            "abc123",
        )

    def test_unknown_model_gets_lower_quality_score(self):
        frame = sample_frame()
        predictor = SmartPricePredictor(n_estimators=5, max_depth=4)
        predictor.fit(frame, verbose=False)
        predictor.train_stats_["test_within_20pct"] = 0.60
        known = frame.iloc[[0]].copy()
        unknown = known.copy()
        unknown["model_number"] = "999"
        known_score = predictor.prediction_quality_scores(known)[0]
        unknown_score = predictor.prediction_quality_scores(unknown)[0]
        self.assertLess(unknown_score, known_score)

    def test_prediction_interval_round_trip(self):
        frame = sample_frame()
        predictor = SmartPricePredictor(n_estimators=10, max_depth=5)
        predictor.fit(frame.iloc[:24], verbose=False)
        result = predictor.calibrate_prediction_interval(
            frame.iloc[8:],
            coverage=0.90,
        )
        intervals = predictor.predict_interval(frame.iloc[:2])
        self.assertGreater(result["radius_yen"], 0)
        self.assertTrue(np.all(intervals["lower"] <= intervals["prediction"]))
        self.assertTrue(np.all(intervals["prediction"] <= intervals["upper"]))

    def test_forecast_backtest_compares_baselines(self):
        dates = pd.date_range("2026-01-01", periods=50, freq="D")
        history = pd.DataFrame({
            "product_id": ["p1"] * len(dates),
            "record_date": dates,
            "avg_price": np.linspace(10000000, 9000000, len(dates)),
            "listing_count": 10,
        })
        report = backtest(
            history,
            min_history=14,
            horizons=(1, 7, 30),
            origin_step=3,
        )
        self.assertIn("history_trend", report["metrics"])
        self.assertIn("last_value", report["metrics"])
        self.assertGreater(report["metrics"]["history_trend"]["1"]["n"], 0)

    def test_converging_median_moves_without_inventing_long_term_slope(self):
        history = [
            {
                "record_date": day.date(),
                "avg_price": price,
            }
            for day, price in zip(
                pd.date_range("2026-01-01", periods=7, freq="D"),
                [100, 100, 100, 100, 100, 100, 130],
            )
        ]
        forecast, _ = converging_rolling_median(
            history,
            horizon_days=30,
            anchor_date=pd.Timestamp("2026-01-07").date(),
            convergence_days=3,
        )
        prices = [row["predicted_price_vnd"] for row in forecast]
        self.assertGreater(prices[0], prices[-1])
        self.assertAlmostEqual(prices[-1], 119.6, delta=0.02)

    def test_converging_median_clips_extreme_target(self):
        history = [
            {"record_date": pd.Timestamp(f"2026-01-0{day}").date(), "avg_price": 100}
            for day in range(1, 7)
        ] + [{"record_date": pd.Timestamp("2026-01-07").date(), "avg_price": 10}]
        forecast, diagnostics = converging_rolling_median(
            history,
            horizon_days=30,
            anchor_date=pd.Timestamp("2026-01-07").date(),
            convergence_days=3,
            max_target_change_pct=8,
        )
        self.assertTrue(diagnostics["target_was_clipped"])
        self.assertLessEqual(forecast[-1]["predicted_price_vnd"], 10.8)

    def test_depreciation_gate_rejects_short_history(self):
        panel = pd.DataFrame({
            "product_id": ["p1"] * 4,
            "record_date": pd.date_range("2026-01-01", periods=4, freq="D"),
            "history_points": [4] * 4,
            "history_span_days": [3] * 4,
            "retained_value": [1.0, 0.99, 0.98, 0.97],
            "elapsed_years": [0.0, 0.01, 0.02, 0.03],
            "device_age_years": [3.0] * 4,
            "storage_log": [4.8] * 4,
            "listing_count_log": [2.0] * 4,
        })
        artifact, report = train_depreciation(panel)
        self.assertIsNone(artifact)
        self.assertFalse(report["passed"])

    def test_temporal_forecast_features_only_use_history_prefix(self):
        history = [
            {
                "record_date": day.date(),
                "avg_price": 100 - index,
                "listing_count": 10 + index,
            }
            for index, day in enumerate(pd.date_range("2026-01-01", periods=15))
        ]
        features = build_feature_row(history, horizon_days=7)
        self.assertEqual(set(features), set(FORECAST_FEATURES))
        self.assertEqual(features["horizon_days"], 7)
        self.assertLess(features["return_7"], 0)

    def test_temporal_supervised_rows_respect_target_horizon(self):
        dates = pd.date_range("2026-01-01", periods=20, freq="2D")
        history = pd.DataFrame({
            "product_id": ["p1"] * len(dates),
            "record_date": dates,
            "avg_price": np.linspace(100, 80, len(dates)),
            "listing_count": 10,
        })
        rows = build_supervised_rows(history, min_history=5, max_horizon=7)
        self.assertGreater(len(rows), 0)
        self.assertTrue((rows["horizon_days"] <= 7).all())
        self.assertTrue((rows["target_date"] > rows["origin_date"]).all())

    def test_temporal_forecaster_returns_ordered_interval(self):
        class ConstantModel:
            def __init__(self, value):
                self.value = value

            def predict(self, matrix):
                return np.full(len(matrix), self.value)

        forecaster = TemporalPriceForecaster({
            "model": ConstantModel(0.0),
            "lower_model": ConstantModel(-0.05),
            "upper_model": ConstantModel(0.05),
            "features": FORECAST_FEATURES,
            "metadata": {"method": "test_temporal"},
        })
        history = [
            {
                "record_date": day.date(),
                "avg_price": 100,
                "listing_count": 10,
            }
            for day in pd.date_range("2026-01-01", periods=14)
        ]
        points, meta = forecaster.predict(
            history,
            horizon_days=3,
            anchor_date=pd.Timestamp("2026-01-14").date(),
        )
        self.assertEqual(meta["method_detail"], "test_temporal")
        self.assertEqual(len(points), 3)
        self.assertLess(points[0]["lower_price_vnd"], points[0]["predicted_price_vnd"])
        self.assertLess(points[0]["predicted_price_vnd"], points[0]["upper_price_vnd"])

    def test_old_artifact_without_metadata_still_loads(self):
        predictor = SmartPricePredictor(n_estimators=5, max_depth=4)
        predictor.fit(sample_frame(), verbose=False)
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "legacy.pkl")
            joblib.dump({
                "model": predictor.model,
                "feature_columns": predictor.feature_columns,
                "feature_importance": predictor.feature_importance_,
                "train_stats": predictor.train_stats_,
                "model_price_map": predictor.model_price_map,
            }, path)
            loaded = SmartPricePredictor(n_estimators=5, max_depth=4)
            loaded.load(path)
        self.assertEqual(loaded.model_metadata_, {})


if __name__ == "__main__":
    unittest.main()
