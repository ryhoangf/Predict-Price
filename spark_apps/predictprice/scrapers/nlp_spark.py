"""Spark NLP worker — read raw Mongo docs per source, enrich, write back."""

from __future__ import annotations

import pandas as pd

import config
import ingestion
from scrapers.nlp_pipeline import run_nlp_pipeline


def run_nlp_for_source(
    source_name: str,
    *,
    mongo_uri: str | None = None,
    fetch_limit: int | None = None,
) -> str:
    """
    Load pending raw docs for one source, run NLP + junk model, update Mongo.
    Returns status string for Spark driver.
    """
    if source_name not in config.SOURCE_TO_PLATFORM:
        return f"ERROR: {source_name} - Unknown source"

    uri = mongo_uri or config.WORKER_MONGO_URI
    docs = ingestion.fetch_pending_nlp_docs(
        source_name, uri, limit=fetch_limit
    )
    if not docs:
        return (
            f"OK: {source_name} - no pending NLP "
            f"(query status={ingestion.STATUS_RAW}, nlp_done!=true)"
        )

    df = pd.DataFrame(docs)
    if "_id" in df.columns:
        df = df.drop(columns=["_id"])

    print(f"[{source_name}] Spark NLP: {len(df)} doc(s) from Mongo")
    df = run_nlp_pipeline(df, source_name)

    stats = ingestion.apply_nlp_batch(source_name, df, uri)
    updated = int(stats.get("updated") or 0)
    stage = str(stats.get("stage") or "")

    if stage == "mongo_connection_failed":
        return f"ERROR: {source_name} - Mongo connection failed"

    if updated == 0:
        return (
            f"WARNING: {source_name} - NLP ran on {len(df)} row(s) "
            f"but updated=0 stage={stage}"
        )

    return (
        f"SUCCESS: {source_name} - nlp_rows={len(df)}, updated={updated}, "
        f"stage={stage} | mongo={ingestion.redact_mongo_uri(uri)}"
    )
