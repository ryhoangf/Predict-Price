from __future__ import annotations

import argparse
import json
import os
import random
import re
import sys
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sqlalchemy import create_engine


APP_ROOT = Path(__file__).resolve().parents[1]
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

import config as cfg  # noqa: E402


FEATURE_COLUMNS = [
    "tfidf_sim",
    "same_brand",
    "same_series",
    "same_name",
    "same_storage",
    "same_ram",
]


def _present(value: Any) -> bool:
    if value is None:
        return False
    try:
        if pd.isna(value):
            return False
    except (TypeError, ValueError):
        pass
    return not (isinstance(value, str) and not value.strip())


def _norm(value: Any) -> str:
    if not _present(value):
        return ""
    return re.sub(r"\s+", " ", str(value)).strip().lower()


def _specs(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if not _present(raw):
        return {}
    try:
        return json.loads(str(raw))
    except Exception:
        return {}


def _storage(raw: Any) -> str:
    return _norm(_specs(raw).get("storage"))


def _ram(raw: Any) -> str:
    return _norm(_specs(raw).get("ram"))


def _product_text(row: pd.Series) -> str:
    specs = _specs(row.get("base_specs"))
    return _norm(
        " ".join(
            str(x)
            for x in [
                row.get("brand"),
                row.get("product_name"),
                row.get("model_series"),
                specs.get("storage"),
                specs.get("ram"),
            ]
            if _present(x)
        )
    )


def _listing_text(row: pd.Series) -> str:
    return _norm(
        " ".join(
            str(x)
            for x in [
                row.get("brand"),
                row.get("product_name"),
                row.get("model_series"),
                row.get("base_specs"),
                row.get("listing_name"),
                row.get("description"),
                row.get("color"),
            ]
            if _present(x)
        )
    )


def _mysql_uri() -> str:
    return (
        f"mysql+pymysql://{cfg.MYSQL_USER}:{cfg.MYSQL_PASSWORD}"
        f"@{cfg.MYSQL_HOST}:{cfg.MYSQL_PORT}/{cfg.MYSQL_DB}"
    )


def load_training_data(limit: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    engine = create_engine(_mysql_uri())
    with engine.connect() as conn:
        listings = pd.read_sql(
            """
            SELECT
                l.listing_id,
                l.product_id,
                l.source_url,
                l.color,
                l.description,
                p.name AS product_name,
                p.brand,
                p.model_series,
                p.base_specs
            FROM active_listings l
            JOIN products p ON p.product_id = l.product_id
            WHERE p.name IS NOT NULL
            ORDER BY l.last_updated DESC
            LIMIT %(limit)s
            """,
            conn,
            params={"limit": int(limit)},
        )
        products = pd.read_sql(
            """
            SELECT product_id, name AS product_name, brand, model_series, base_specs
            FROM products
            WHERE name IS NOT NULL
            """,
            conn,
        )
    return listings, products


def build_pairs(listings: pd.DataFrame, products: pd.DataFrame, negatives_per_positive: int):
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.metrics.pairwise import cosine_similarity

    listings = listings.copy().reset_index(drop=True)
    products = products.copy().reset_index(drop=True)
    listings["listing_text"] = listings.apply(_listing_text, axis=1)
    products["product_text"] = products.apply(_product_text, axis=1)

    tfidf = TfidfVectorizer(
        analyzer="char_wb",
        ngram_range=(2, 5),
        max_features=20000,
        min_df=1,
    )
    product_matrix = tfidf.fit_transform(products["product_text"].fillna(""))
    listing_matrix = tfidf.transform(listings["listing_text"].fillna(""))
    sim_matrix = cosine_similarity(listing_matrix, product_matrix)

    product_index = {
        pid: idx for idx, pid in enumerate(products["product_id"].astype(str).tolist())
    }
    by_brand: dict[str, list[int]] = {}
    for idx, row in products.iterrows():
        by_brand.setdefault(_norm(row.get("brand")), []).append(idx)

    rng = random.Random(42)
    rows = []
    y = []

    for li, listing in listings.iterrows():
        true_pid = str(listing.get("product_id"))
        true_idx = product_index.get(true_pid)
        if true_idx is None:
            continue
        rows.append(pair_features(listing, products.iloc[true_idx], float(sim_matrix[li, true_idx])))
        y.append(1)

        brand_pool = by_brand.get(_norm(listing.get("brand")), [])
        hard_candidates = [
            int(i)
            for i in np.argsort(sim_matrix[li])[-30:][::-1]
            if str(products.iloc[int(i)].get("product_id")) != true_pid
        ]
        pool = hard_candidates + [
            i for i in rng.sample(brand_pool, min(len(brand_pool), 30))
            if str(products.iloc[i].get("product_id")) != true_pid
        ]
        seen = set()
        for pi in pool:
            if pi in seen:
                continue
            seen.add(pi)
            rows.append(pair_features(listing, products.iloc[pi], float(sim_matrix[li, pi])))
            y.append(0)
            if len(seen) >= negatives_per_positive:
                break

    return pd.DataFrame(rows, columns=FEATURE_COLUMNS), np.array(y)


def pair_features(listing: pd.Series, product: pd.Series, sim: float) -> list[float]:
    return [
        float(sim),
        float(_norm(listing.get("brand")) == _norm(product.get("brand"))),
        float(_norm(listing.get("model_series")) == _norm(product.get("model_series"))),
        float(_norm(listing.get("product_name")) == _norm(product.get("product_name"))),
        float(_storage(listing.get("base_specs")) == _storage(product.get("base_specs"))),
        float(_ram(listing.get("base_specs")) == _ram(product.get("base_specs"))),
    ]


def main() -> int:
    parser = argparse.ArgumentParser(description="Train LightGBM product matching gate.")
    parser.add_argument("--limit", type=int, default=50000)
    parser.add_argument("--negatives-per-positive", type=int, default=3)
    parser.add_argument(
        "--output",
        default=str(Path(__file__).resolve().parent / "models" / "lgbm_product_match_v1.txt"),
    )
    args = parser.parse_args()

    import lightgbm as lgb
    from sklearn.metrics import classification_report, roc_auc_score
    from sklearn.model_selection import train_test_split

    listings, products = load_training_data(args.limit)
    if listings.empty or products.empty:
        raise SystemExit("No MySQL data available for product matcher training.")

    X, y = build_pairs(listings, products, args.negatives_per_positive)
    if X.empty:
        raise SystemExit("No training pairs generated.")

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )
    model = lgb.LGBMClassifier(
        n_estimators=250,
        learning_rate=0.05,
        max_depth=5,
        class_weight="balanced",
        random_state=42,
        n_jobs=-1,
    )
    model.fit(X_train, y_train)
    prob = model.predict_proba(X_test)[:, 1]
    pred = prob >= 0.5

    print(classification_report(y_test, pred, target_names=["not_match", "match"]))
    print(f"ROC AUC: {roc_auc_score(y_test, prob):.4f}")

    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    model.booster_.save_model(str(out))
    print(f"Saved product matcher: {out}")
    print(f"Feature columns: {FEATURE_COLUMNS}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
