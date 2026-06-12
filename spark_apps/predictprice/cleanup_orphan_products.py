"""Xóa products không còn listing và tên rõ ràng là rác phân loại."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from sqlalchemy import bindparam, create_engine, text

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import config as cfg

MYSQL_URI = (
    f"mysql+pymysql://{cfg.MYSQL_USER}:{cfg.MYSQL_PASSWORD}"
    f"@{cfg.MYSQL_HOST}:{cfg.MYSQL_PORT}/{cfg.MYSQL_DB}"
)

FAKE_NAME_PATTERNS = (
    "%iPhone 13 Ultra%",
    "%Xiaomi iPhone%",
    "%iPhone iPhone%",
    "%iPhone 13256%",
    "%iPhone 13 Pro Plus%",
    "%Green iPhone%",
    "%RED iPhone%",
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Delete orphan fake/misclassified products.")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--yes", action="store_true")
    args = parser.parse_args()

    if not args.dry_run and not args.yes:
        raise SystemExit("Refusing to write without --yes. Use --dry-run first.")

    engine = create_engine(MYSQL_URI)
    pattern_clause = " OR ".join(f"p.name LIKE :pat{i}" for i in range(len(FAKE_NAME_PATTERNS)))
    params = {f"pat{i}": pat for i, pat in enumerate(FAKE_NAME_PATTERNS)}

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                f"""
                SELECT p.product_id, p.name, p.brand, p.model_series,
                       COUNT(l.listing_id) AS listing_count
                FROM products p
                LEFT JOIN active_listings l ON l.product_id = p.product_id
                WHERE ({pattern_clause})
                GROUP BY p.product_id, p.name, p.brand, p.model_series
                HAVING listing_count = 0
                ORDER BY p.name
                """
            ),
            params,
        ).mappings().all()

    print(f"Orphan fake products to delete: {len(rows)}")
    for row in rows[:30]:
        print(f"  {row['brand']} / {row['name']} / {row['model_series']}")

    if args.dry_run or not rows:
        print("Dry-run or nothing to delete.")
        return 0

    ids = [row["product_id"] for row in rows]
    delete_sql = text("DELETE FROM products WHERE product_id IN :ids").bindparams(
        bindparam("ids", expanding=True)
    )
    with engine.begin() as conn:
        res = conn.execute(delete_sql, {"ids": ids})
    print(f"Deleted {int(res.rowcount or 0)} orphan product(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
