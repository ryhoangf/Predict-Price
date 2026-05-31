"""MySQL engine cho API (đọc price_history / products)."""
from __future__ import annotations

import functools
import json
from typing import Any, Optional

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

import config as cfg


@functools.lru_cache(maxsize=1)
def get_mysql_engine() -> Engine:
    uri = (
        f"mysql+pymysql://{cfg.MYSQL_USER}:{cfg.MYSQL_PASSWORD}"
        f"@{cfg.MYSQL_HOST}:{cfg.MYSQL_PORT}/{cfg.MYSQL_DB}"
    )
    return create_engine(uri, pool_pre_ping=True)


def fetch_product_row(engine: Engine, product_id: str) -> Optional[dict[str, Any]]:
    with engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT product_id, name, brand, model_series, base_specs
                FROM products
                WHERE product_id = :pid
                LIMIT 1
                """
            ),
            {"pid": product_id},
        ).mappings().first()
    return dict(row) if row else None


def fetch_price_history(engine: Engine, product_id: str, limit: int = 90) -> list[dict[str, Any]]:
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT record_date, avg_price, min_price, max_price, listing_count
                FROM price_history
                WHERE product_id = :pid
                ORDER BY record_date ASC
                LIMIT :lim
                """
            ),
            {"pid": product_id, "lim": limit},
        ).mappings().all()
    return [dict(r) for r in rows]


def fetch_latest_stored_forecast(engine: Engine, product_id: str) -> Optional[dict[str, Any]]:
    with engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT forecast_date, predicted_price, confidence_score, model_version
                FROM price_forecasts
                WHERE product_id = :pid
                ORDER BY forecast_date DESC, created_at DESC
                LIMIT 1
                """
            ),
            {"pid": product_id},
        ).mappings().first()
    return dict(row) if row else None


def fetch_listings_for_product(engine: Engine, product_id: str) -> pd.DataFrame:
    with engine.connect() as conn:
        return pd.read_sql(
            text(
                """
                SELECT
                    battery_percentage,
                    condition_rank,
                    platform,
                    has_box,
                    has_charger,
                    is_sim_free,
                    fully_functional,
                    has_scratches,
                    has_damage,
                    has_issues,
                    screen_condition,
                    body_condition,
                    price
                FROM active_listings
                WHERE product_id = :pid
                """
            ),
            conn,
            params={"pid": product_id},
        )


def parse_base_specs(raw: Any) -> dict[str, Any]:
    if not raw:
        return {}
    if isinstance(raw, dict):
        return raw
    try:
        return json.loads(raw)
    except (TypeError, json.JSONDecodeError):
        return {}


def specs_to_storage_ram(specs: dict[str, Any]) -> tuple[str, str]:
    storage = specs.get("storage")
    ram = specs.get("ram")
    s = "" if storage is None else str(storage).replace("GB", "").replace("gb", "").strip()
    r = "" if ram is None else str(ram).replace("GB", "").replace("gb", "").strip()
    return s or "64", r or "4"
