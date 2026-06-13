"""Build a timestamp-preserving ML dataset from the NLP-enriched Mongo collection."""
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import pymongo

import config as cfg


FIELDS = [
    "link", "name", "price", "condition", "source", "platform",
    "brand", "model_line", "model_number", "variant", "capacity",
    "storage", "ram", "color", "battery_percentage", "battery_status",
    "battery_replaced", "has_box", "has_charger", "has_cable",
    "has_earphones", "accessories_complete", "is_sim_free",
    "network_restriction", "has_scratches", "screen_condition",
    "body_condition", "has_damage", "face_id_working",
    "touch_id_working", "fully_functional", "has_issues",
    "ingested_at", "nlp_at", "processed_at", "nlp_identity_version",
    "nlp_layer", "nlp_confidence", "is_junk", "junk_reason",
]


def _clean_text(series: pd.Series) -> pd.Series:
    out = series.astype("string").str.strip()
    return out.mask(out.isin(["", "None", "nan", "<NA>"]))


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def build_dataset(
    *,
    mongo_uri: str,
    database: str,
    collection: str,
    output: Path,
    manifest_output: Path,
    min_price: float = 5000,
    max_price: float = 300000,
) -> dict[str, Any]:
    query = {
        "nlp_done": True,
        "is_junk": {"$ne": True},
        "brand": {"$nin": [None, "", "Needs Review"]},
        "model_line": {"$nin": [None, ""]},
        "model_number": {"$nin": [None, ""]},
    }
    projection = {field: 1 for field in FIELDS}
    projection["_id"] = 0

    client = pymongo.MongoClient(
        mongo_uri,
        serverSelectionTimeoutMS=15000,
        connectTimeoutMS=15000,
    )
    try:
        cursor = client[database][collection].find(
            query,
            projection,
            batch_size=2000,
        )
        frame = pd.DataFrame.from_records(cursor)
    finally:
        client.close()

    initial_rows = len(frame)
    if frame.empty:
        raise RuntimeError("Mongo query returned no NLP-enriched training rows.")

    for field in FIELDS:
        if field not in frame.columns:
            frame[field] = None
    for field in ("brand", "model_line", "model_number", "variant", "storage", "ram"):
        frame[field] = _clean_text(frame[field])
    for field in ("ingested_at", "nlp_at", "processed_at"):
        frame[field] = pd.to_datetime(frame[field], errors="coerce", utc=True)

    frame["price"] = pd.to_numeric(frame["price"], errors="coerce")
    frame = frame[frame["price"].between(min_price, max_price)]
    frame = frame.dropna(subset=["brand", "model_line", "model_number", "ingested_at"])
    frame = frame.sort_values(["ingested_at", "link"])
    duplicate_links = int(frame.duplicated(subset=["link"], keep="last").sum())
    frame = frame.drop_duplicates(subset=["link"], keep="last").reset_index(drop=True)
    frame["ecosystem"] = frame["brand"].str.casefold().map(
        lambda value: "Apple" if value == "apple" else "Android"
    )
    frame["posted_at"] = frame["ingested_at"]

    output.parent.mkdir(parents=True, exist_ok=True)
    if output.suffix.lower() == ".parquet":
        frame.to_parquet(output, index=False)
    else:
        frame.to_csv(output, index=False, encoding="utf-8")

    identity = (
        frame["model_line"].fillna("") + " " +
        frame["model_number"].fillna("") + " " +
        frame["variant"].fillna("")
    ).str.replace(r"\s+", " ", regex=True).str.strip()
    manifest = {
        "schema_version": 1,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "source": {
            "database": database,
            "collection": collection,
            "query": query,
        },
        "output": str(output.resolve()),
        "sha256": _sha256(output),
        "rows": int(len(frame)),
        "initial_query_rows": int(initial_rows),
        "duplicate_links_removed": duplicate_links,
        "time_range": {
            "min_ingested_at": frame["ingested_at"].min().isoformat(),
            "max_ingested_at": frame["ingested_at"].max().isoformat(),
        },
        "identity": {
            "brands": int(frame["brand"].nunique()),
            "models": int(identity.nunique()),
            "missing_storage_pct": round(float(frame["storage"].isna().mean() * 100), 2),
            "nlp_versions": frame["nlp_identity_version"].value_counts(
                dropna=False
            ).head(20).to_dict(),
        },
    }
    manifest_output.parent.mkdir(parents=True, exist_ok=True)
    manifest_output.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    return manifest


def main() -> int:
    root = Path(__file__).resolve().parents[3]
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output",
        default=str(root / "book_data" / "training_data_temporal.parquet"),
    )
    parser.add_argument(
        "--manifest-output",
        default=str(root / "book_data" / "training_data_temporal.manifest.json"),
    )
    args = parser.parse_args()
    manifest = build_dataset(
        mongo_uri=cfg.MONGO_URI,
        database=cfg.DB_NAME,
        collection=cfg.COLLECTION_NAME,
        output=Path(args.output),
        manifest_output=Path(args.manifest_output),
    )
    print(json.dumps(manifest, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
