from __future__ import annotations

import argparse

from sqlalchemy import bindparam, create_engine, text

import config as cfg


MYSQL_URI = (
    f"mysql+pymysql://{cfg.MYSQL_USER}:{cfg.MYSQL_PASSWORD}"
    f"@{cfg.MYSQL_HOST}:{cfg.MYSQL_PORT}/{cfg.MYSQL_DB}"
)


AMBIGUOUS_SQL = """
SELECT
    p.product_id,
    p.brand,
    p.name,
    p.base_specs,
    COUNT(l.listing_id) AS listings,
    ROUND(AVG(l.price), 0) AS avg_price,
    ROUND(STDDEV(l.price) / NULLIF(AVG(l.price), 0) * 100, 2) AS cv_pct,
    COUNT(DISTINCT DATE(l.last_updated)) AS updated_days
FROM products p
JOIN active_listings l ON l.product_id = p.product_id
WHERE LOWER(COALESCE(p.brand, '')) IN :brands
  AND (
      p.base_specs IS NULL
      OR JSON_EXTRACT(p.base_specs, '$.storage') IS NULL
      OR JSON_UNQUOTE(JSON_EXTRACT(p.base_specs, '$.storage')) IN ('', 'null', '0', '1', '4', '8', '12')
      OR p.name REGEXP '^[A-Za-z]+ [0-9]+GB$|^A [0-9]{2,4}$|iPhone 1[89]|Pixel 1[1-9]|Galaxy S2[7-9]'
  )
GROUP BY p.product_id
HAVING listings >= :min_listings
ORDER BY listings DESC, cv_pct DESC
LIMIT :limit
"""


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Plan top-brand reprocess/repair targets after identity quality changes."
    )
    parser.add_argument("--brands", default="Apple,Samsung,Google")
    parser.add_argument("--min-listings", type=int, default=50)
    parser.add_argument("--limit", type=int, default=80)
    args = parser.parse_args()

    brands = [b.strip().lower() for b in args.brands.split(",") if b.strip()]
    engine = create_engine(MYSQL_URI)
    with engine.connect() as conn:
        sql = text(AMBIGUOUS_SQL).bindparams(bindparam("brands", expanding=True))
        rows = conn.execute(
            sql,
            {
                "brands": tuple(brands),
                "min_listings": args.min_listings,
                "limit": args.limit,
            },
        ).mappings().all()

    print("===== Ambiguous top-brand products =====")
    for row in rows:
        print(dict(row))

    print("\n===== Suggested dry-run commands =====")
    print("python audit_mysql_identity.py")
    for brand in brands:
        print(f"python backtest_daily_price_snapshots.py --brand {brand.title()}")
    print("python repair_generic_product_buckets.py --bad-storage --dry-run --limit 200")
    print("python migrate_mysql_product_identity.py --dry-run --keyword iPhone")
    print("python migrate_mysql_product_identity.py --dry-run --keyword Galaxy")
    print("python migrate_mysql_product_identity.py --dry-run --keyword Pixel")
    print("\nUse --yes only after reviewing the dry-run plan.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
