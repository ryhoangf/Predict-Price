from __future__ import annotations

from sqlalchemy import create_engine, text

import config as cfg


MYSQL_URI = (
    f"mysql+pymysql://{cfg.MYSQL_USER}:{cfg.MYSQL_PASSWORD}"
    f"@{cfg.MYSQL_HOST}:{cfg.MYSQL_PORT}/{cfg.MYSQL_DB}"
)


QUERIES = {
    "cross_brand_iphone": """
        SELECT p.product_id, p.brand, p.name, p.base_specs, COUNT(*) c, MIN(l.source_url) sample_url
        FROM active_listings l
        JOIN products p ON p.product_id = l.product_id
        WHERE p.name LIKE '%iPhone%' AND COALESCE(p.brand, '') <> 'Apple'
        GROUP BY p.product_id, p.brand, p.name, p.base_specs
        ORDER BY c DESC
        LIMIT 40
    """,
    "duplicate_brand_names": """
        SELECT p.product_id, p.brand, p.name, p.base_specs, COUNT(*) c
        FROM active_listings l
        JOIN products p ON p.product_id = l.product_id
        WHERE p.name REGEXP '^(Apple iPhone|Google Pixel|Samsung Galaxy|Sony Xperia|Xiaomi Redmi|OPPO OPPO|Motorola Motorola|MOTOROLA)'
        GROUP BY p.product_id, p.brand, p.name, p.base_specs
        ORDER BY c DESC
        LIMIT 80
    """,
    "bad_storage_active": """
        SELECT p.product_id, p.brand, p.name, p.base_specs, COUNT(*) c
        FROM active_listings l
        JOIN products p ON p.product_id = l.product_id
        WHERE JSON_UNQUOTE(JSON_EXTRACT(p.base_specs, '$.storage')) IN ('0', '1TB', '125', '126', '258', '2998', '12', '4', '8')
        GROUP BY p.product_id, p.brand, p.name, p.base_specs
        ORDER BY c DESC
        LIMIT 80
    """,
    "small_suspicious_brands": """
        SELECT p.brand, COUNT(*) listings, COUNT(DISTINCT p.product_id) products
        FROM active_listings l
        JOIN products p ON p.product_id = l.product_id
        WHERE p.brand IS NULL
           OR CHAR_LENGTH(p.brand) <= 3
           OR p.brand IN ('Green', 'RED', 'Air', 'AP', 'TO', 'Soft', 'Super', 'IM', 'OP', 'GA', 'MO', 'SA')
        GROUP BY p.brand
        ORDER BY listings DESC
        LIMIT 80
    """,
}


def main() -> int:
    engine = create_engine(MYSQL_URI)
    with engine.connect() as conn:
        for name, query in QUERIES.items():
            print(f"\n===== {name} =====")
            rows = conn.execute(text(query)).mappings().all()
            if not rows:
                print("(empty)")
                continue
            for row in rows:
                print(dict(row))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
