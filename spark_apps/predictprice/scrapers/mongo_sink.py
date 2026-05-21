"""Map parsed rows → MongoDB raw documents (NLP runs later on Spark)."""

from __future__ import annotations

from typing import Any

import pandas as pd

import config
import ingestion


def parsed_row_to_record(row: dict) -> dict[str, Any]:
    platform = row.get("platform") or ""
    source = config.PLATFORM_TO_SOURCE.get(platform, platform)
    return {
        "link": row.get("url") or "",
        "name": row.get("name"),
        "price": row.get("price"),
        "brand": row.get("brand"),
        "condition": row.get("item_condition"),
        "explanation": row.get("item_explanation"),
        "product_id": row.get("product_id"),
        "platform": platform,
        "category_id": row.get("category_id"),
        "source": source,
    }


def rows_to_dataframe(rows: list[dict]) -> pd.DataFrame:
    records = [parsed_row_to_record(r) for r in rows if r.get("url")]
    if not records:
        return pd.DataFrame()
    df = pd.DataFrame(records)
    if "link" not in df.columns:
        return pd.DataFrame()
    return df


def persist_rows(
    rows: list[dict],
    source_name: str,
    mongo_uri: str | None = None,
    *,
    run_nlp: bool = False,
) -> dict:
    """Insert raw scrape rows via ingestion.save_batch_to_datalake (no NLP)."""
    if not config.MONGO_ENABLED:
        return {"saved": 0, "stage": "mongo_disabled", "rows_in_batch": len(rows)}

    df = rows_to_dataframe(rows)
    if df.empty:
        return {"saved": 0, "stage": "empty_df", "rows_in_batch": len(rows)}

    uri = mongo_uri or config.WORKER_MONGO_URI
    print(
        f"[{source_name}] Mongo raw insert {len(df)} row(s) → "
        f"{ingestion.redact_mongo_uri(uri)} db={config.DB_NAME}"
    )
    return ingestion.save_batch_to_datalake(df, source_name, custom_mongo_uri=uri)


def ensure_indexes(mongo_uri: str | None = None) -> None:
    """Create Mongo indexes if missing (idempotent)."""
    if not config.MONGO_ENABLED:
        return
    uri = mongo_uri or config.WORKER_MONGO_URI
    with ingestion.get_mongo_connection(uri) as col:
        if col is None:
            return
        try:
            col.create_index("link", unique=True)
            col.create_index([("source", 1), ("ingested_at", -1)])
        except Exception as exc:
            print(f"[mongo_sink] index warning: {exc}")
