"""NLP + junk detection — Spark post-ingest (main.py) and local CLI."""

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd

_nlp_cache: dict | None = None


def _model_dir() -> str:
    docker = "/opt/spark/apps/predictprice/NLP/models"
    if os.path.isdir(docker):
        return docker
    return str(Path(__file__).resolve().parent.parent / "NLP" / "models")


def _get_nlp_cache() -> dict:
    global _nlp_cache
    if _nlp_cache is not None:
        return _nlp_cache

    from NLP.title_nlp import PhoneInfoExtractor
    from NLP.item_explanation import ItemExplanationExtractor

    cache: dict = {
        "phone_nlp": PhoneInfoExtractor(),
        "item_nlp": ItemExplanationExtractor(),
        "junk_ready": False,
    }

    try:
        import joblib
        import lightgbm as lgb

        model_dir = _model_dir()
        cache["tfidf"] = joblib.load(f"{model_dir}/tfidf_junk_v1.pkl")
        cache["junk_model"] = lgb.Booster(model_file=f"{model_dir}/lgbm_junk_v1.txt")
        cache["junk_ready"] = True
    except Exception as exc:
        cache["junk_error"] = str(exc)

    _nlp_cache = cache
    return cache


def run_nlp_pipeline(df: pd.DataFrame, source_name: str) -> pd.DataFrame:
    """Layer 2 NLP + Layer 1 junk model. Returns enriched DataFrame."""
    if df is None or df.empty:
        return df

    cache = _get_nlp_cache()
    print(f"[{source_name}] NLP pipeline for {len(df)} row(s)...")
    df = cache["phone_nlp"].process_dataframe(df, title_column="name")
    df = cache["item_nlp"].process_dataframe(df, explanation_column="explanation")

    if cache.get("junk_ready"):
        try:
            import re
            import scipy.sparse as sp

            df = df.copy()
            df["name_clean"] = df["name"].fillna("")
            df["expl_clean"] = df["explanation"].fillna("")
            combined_text = df["name_clean"] + " " + df["expl_clean"]

            def clean_price(p):
                if pd.isna(p):
                    return 0
                nums = re.sub(r"[^\d]", "", str(p))
                return int(nums) if nums else 0

            prices_array = df["price"].apply(clean_price).values.reshape(-1, 1)
            X_text = cache["tfidf"].transform(combined_text)
            X_final = sp.hstack((X_text, prices_array), format="csr")
            y_pred_prob = cache["junk_model"].predict(X_final)
            df["is_junk"] = y_pred_prob > 0.5
            print(
                f"[{source_name}] Junk detection: "
                f"{int(df['is_junk'].sum())}/{len(df)} flagged"
            )
        except Exception as exc:
            print(f"[{source_name}] Junk predict failed: {exc}")
            df = df.copy()
            df["is_junk"] = False
    else:
        print(
            f"[{source_name}] Junk model skipped: "
            f"{cache.get('junk_error', 'not loaded')}"
        )
        df = df.copy()
        df["is_junk"] = False

    return df
