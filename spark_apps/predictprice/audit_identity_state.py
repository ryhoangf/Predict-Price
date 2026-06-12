from __future__ import annotations

from datetime import datetime, timezone

import pymongo
from sqlalchemy import create_engine, text

import config as cfg
from NLP.title_nlp import NLP_IDENTITY_VERSION


MYSQL_URI = (
    f"mysql+pymysql://{cfg.MYSQL_USER}:{cfg.MYSQL_PASSWORD}"
    f"@{cfg.MYSQL_HOST}:{cfg.MYSQL_PORT}/{cfg.MYSQL_DB}"
)


def main() -> int:
    mongo = pymongo.MongoClient(cfg.MONGO_URI)
    col = mongo[cfg.DB_NAME][cfg.COLLECTION_NAME]
    since = datetime(2026, 6, 8, tzinfo=timezone.utc)

    print("=== MongoDB ===")
    print("raw_items:", col.count_documents({}))
    print("ingested since 2026-06-08:", col.count_documents({"ingested_at": {"$gte": since}}))
    print(
        "current NLP version:",
        col.count_documents({"nlp_identity_version": NLP_IDENTITY_VERSION}),
    )
    print("missing current NLP version by source:")
    for row in col.aggregate(
        [
            {
                "$match": {
                    "is_junk": {"$ne": True},
                    "link": {"$exists": True, "$ne": None},
                    "nlp_identity_version": {"$ne": NLP_IDENTITY_VERSION},
                }
            },
            {"$group": {"_id": "$source", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
        ]
    ):
        print(f"  {row['_id']}: {row['count']}")
    print(
        "iPhone 13 title records:",
        col.count_documents({"name": {"$regex": r"iPhone\s*13", "$options": "i"}}),
    )
    print(
        "iPhone 13 Pro titles still parsed as base:",
        col.count_documents(
            {
                "name": {
                    "$regex": r"iPhone\s*13.*\bPro(?:\s*Max)?\b",
                    "$options": "i",
                },
                "model_number": "13",
            }
        ),
    )
    unresolved = col.find(
        {
            "name": {
                "$regex": r"iPhone\s*13.*\bPro(?:\s*Max)?\b",
                "$options": "i",
            },
            "model_number": "13",
        },
        {"_id": 0, "name": 1, "source": 1},
    ).limit(20)
    for row in unresolved:
        print(f"  unresolved sample: {row.get('source')} | {row.get('name')}")
    print("status counts:")
    for row in col.aggregate(
        [{"$group": {"_id": "$status", "count": {"$sum": 1}}}, {"$sort": {"count": -1}}]
    ):
        print(f"  {row['_id']}: {row['count']}")
    mongo.close()

    engine = create_engine(MYSQL_URI)
    with engine.connect() as conn:
        print("\n=== MySQL ===")
        for label, query in (
            (
                "active listings since 2026-06-08",
                "SELECT COUNT(*) FROM active_listings WHERE posted_at >= '2026-06-08'",
            ),
            (
                "price history since 2026-06-08",
                "SELECT COUNT(*) FROM price_history WHERE record_date >= '2026-06-08'",
            ),
            (
                "price forecasts since 2026-06-08",
                "SELECT COUNT(*) FROM price_forecasts WHERE forecast_date >= '2026-06-08'",
            ),
        ):
            print(f"{label}: {conn.execute(text(query)).scalar()}")

        print("iPhone 13 product buckets:")
        rows = conn.execute(
            text(
                """
                SELECT p.product_id, p.name, p.model_series,
                       JSON_UNQUOTE(JSON_EXTRACT(p.base_specs, '$.storage')) AS storage,
                       COUNT(l.listing_id) AS listings,
                       SUM(l.posted_at >= '2026-06-08') AS listings_since_2026_06_08,
                       (
                           SELECT COUNT(DISTINCT ph.record_date)
                           FROM price_history ph
                           WHERE ph.product_id = p.product_id
                       ) AS history_days,
                       (
                           SELECT MAX(ph.record_date)
                           FROM price_history ph
                           WHERE ph.product_id = p.product_id
                       ) AS latest_history_date,
                       (
                           SELECT MAX(pf.forecast_date)
                           FROM price_forecasts pf
                           WHERE pf.product_id = p.product_id
                       ) AS latest_forecast_date
                FROM products p
                LEFT JOIN active_listings l ON l.product_id = p.product_id
                WHERE p.brand = 'Apple' AND p.name LIKE 'iPhone 13%'
                GROUP BY p.product_id, p.name, p.model_series, p.base_specs
                HAVING listings > 0
                ORDER BY listings DESC
                """
            )
        ).mappings()
        for row in rows:
            print(dict(row))

        table_exists = conn.execute(
            text(
                """
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_schema = :db AND table_name = 'product_identity_repair_log'
                """
            ),
            {"db": cfg.MYSQL_DB},
        ).scalar()
        if table_exists:
            repaired = conn.execute(
                text(
                    """
                    SELECT COUNT(*) AS repairs,
                           SUM(l.posted_at IS NULL) AS null_posted_at,
                           SUM(l.last_updated IS NULL) AS null_last_updated
                    FROM product_identity_repair_log r
                    JOIN active_listings l ON l.listing_id = r.listing_id
                    WHERE r.created_at >= CURDATE()
                    """
                )
            ).mappings().one()
            print("repairs today:", dict(repaired))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
