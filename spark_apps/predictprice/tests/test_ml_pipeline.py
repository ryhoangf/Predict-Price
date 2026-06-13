import os
import sys
import tempfile
import unittest

import joblib
import pandas as pd


APP_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if APP_ROOT not in sys.path:
    sys.path.insert(0, APP_ROOT)

from ml_models.smart_price_predictor import SmartPricePredictor


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
