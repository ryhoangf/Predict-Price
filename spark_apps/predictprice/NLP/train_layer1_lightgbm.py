from __future__ import annotations

import argparse
import os
import re
from pathlib import Path

import joblib
import lightgbm as lgb
import pandas as pd
import scipy.sparse as sp
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics import classification_report, confusion_matrix, roc_auc_score
from sklearn.model_selection import train_test_split


def clean_price(value) -> int:
    if pd.isna(value):
        return 0
    nums = re.sub(r"[^\d]", "", str(value))
    return int(nums) if nums else 0


def build_dataset(path: str) -> tuple[pd.Series, pd.Series, pd.Series]:
    df = pd.read_csv(path)
    if "is_junk" not in df.columns:
        raise ValueError("Dataset must contain is_junk column.")
    for col in ("name", "explanation", "price"):
        if col not in df.columns:
            df[col] = ""

    df = df.dropna(subset=["is_junk"]).copy()
    df["is_junk"] = df["is_junk"].astype(int)
    df["combined_text"] = df["name"].fillna("") + " " + df["explanation"].fillna("")
    df["price_num"] = df["price"].apply(clean_price)
    return df["combined_text"], df["price_num"], df["is_junk"]


def main() -> int:
    parser = argparse.ArgumentParser(description="Train TF-IDF + LightGBM junk detector.")
    parser.add_argument(
        "--dataset",
        default=str(Path(__file__).resolve().parent / "dataset" / "junk_labeling_task.csv"),
    )
    parser.add_argument(
        "--output-dir",
        default=str(Path(__file__).resolve().parent / "models"),
    )
    parser.add_argument("--threshold", type=float, default=0.5)
    parser.add_argument("--max-features", type=int, default=5000)
    args = parser.parse_args()

    texts, prices, y = build_dataset(args.dataset)
    print(f"Rows: {len(y)} | junk={int(y.sum())} | clean={int((y == 0).sum())}")

    tfidf = TfidfVectorizer(
        analyzer="char_wb",
        ngram_range=(2, 5),
        max_features=args.max_features,
        min_df=2,
    )
    X_text = tfidf.fit_transform(texts)
    X_price = prices.values.reshape(-1, 1)
    X = sp.hstack((X_text, X_price), format="csr")

    X_train, X_test, y_train, y_test = train_test_split(
        X, y.values, test_size=0.2, random_state=42, stratify=y.values
    )

    model = lgb.LGBMClassifier(
        n_estimators=250,
        learning_rate=0.05,
        max_depth=7,
        class_weight="balanced",
        random_state=42,
        n_jobs=-1,
    )
    model.fit(X_train, y_train)

    prob = model.predict_proba(X_test)[:, 1]
    pred = prob >= args.threshold
    print(classification_report(y_test, pred, target_names=["clean", "junk"]))
    print("Confusion matrix:")
    print(confusion_matrix(y_test, pred))
    print(f"ROC AUC: {roc_auc_score(y_test, prob):.4f}")

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    tfidf_path = out_dir / "tfidf_junk_v1.pkl"
    lgbm_path = out_dir / "lgbm_junk_v1.txt"
    joblib.dump(tfidf, tfidf_path)
    model.booster_.save_model(str(lgbm_path))
    print(f"Saved: {tfidf_path}")
    print(f"Saved: {lgbm_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
