from __future__ import annotations

import argparse
import re
import sys
import uuid
from datetime import datetime
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
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _affected_product_ids(conn, repair_date: str) -> list[str]:
    rows = conn.execute(
        text(
            """
            SELECT r.old_product_id AS product_id
            FROM product_identity_repair_log r
            JOIN products p ON p.product_id = r.old_product_id
            WHERE DATE(created_at) = :repair_date
              AND COALESCE(p.category, '') <> 'Review'
              AND COALESCE(p.brand, '') <> 'Needs Review'
            UNION
            SELECT r.new_product_id AS product_id
            FROM product_identity_repair_log r
            JOIN products p ON p.product_id = r.new_product_id
            WHERE DATE(created_at) = :repair_date
              AND COALESCE(p.category, '') <> 'Review'
              AND COALESCE(p.brand, '') <> 'Needs Review'
            """
        ),
        {"repair_date": repair_date},
    ).scalars()
    return sorted({str(value) for value in rows if value})


def _history_rows(conn, product_ids: list[str]) -> list[dict]:
    query = text(
        """
        SELECT
            product_id,
            DATE(posted_at) AS record_date,
            AVG(price) AS avg_price,
            AVG(original_price) AS original_price,
            MIN(price) AS min_price,
            MAX(price) AS max_price,
            COUNT(*) AS listing_count
        FROM active_listings
        WHERE product_id IN :product_ids
          AND posted_at IS NOT NULL
          AND price IS NOT NULL
          AND price > 0
        GROUP BY product_id, DATE(posted_at)
        ORDER BY product_id, record_date
        """
    ).bindparams(bindparam("product_ids", expanding=True))
    return [dict(row) for row in conn.execute(query, {"product_ids": product_ids}).mappings()]


def _count_for_products(conn, table: str, product_ids: list[str]) -> int:
    query = text(f"SELECT COUNT(*) FROM {table} WHERE product_id IN :product_ids").bindparams(
        bindparam("product_ids", expanding=True)
    )
    return int(conn.execute(query, {"product_ids": product_ids}).scalar() or 0)


def _print_plan(conn, product_ids: list[str], rebuilt_rows: list[dict]) -> None:
    old_history = _count_for_products(conn, "price_history", product_ids)
    old_forecasts = _count_for_products(conn, "price_forecasts", product_ids)
    null_posted = conn.execute(
        text(
            """
            SELECT COUNT(*)
            FROM active_listings
            WHERE product_id IN :product_ids AND posted_at IS NULL
            """
        ).bindparams(bindparam("product_ids", expanding=True)),
        {"product_ids": product_ids},
    ).scalar()
    date_values = [row["record_date"] for row in rebuilt_rows if row.get("record_date")]

    print(f"Affected products: {len(product_ids)}")
    print(f"Existing price_history rows to replace: {old_history}")
    print(f"Rebuilt DATE(posted_at) history rows: {len(rebuilt_rows)}")
    print(f"Existing forecasts to archive/remove: {old_forecasts}")
    print(f"Listings skipped because posted_at is NULL: {int(null_posted or 0)}")
    if date_values:
        print(f"Rebuilt history date range: {min(date_values)} -> {max(date_values)}")
    print("Sample rebuilt rows:")
    for row in rebuilt_rows[:20]:
        print(row)


def _backup_table(conn, source_table: str, backup_table: str, product_ids: list[str]) -> int:
    conn.execute(text(f"CREATE TABLE {backup_table} LIKE {source_table}"))
    insert_sql = text(
        f"""
        INSERT INTO {backup_table}
        SELECT * FROM {source_table}
        WHERE product_id IN :product_ids
        """
    ).bindparams(bindparam("product_ids", expanding=True))
    return int(conn.execute(insert_sql, {"product_ids": product_ids}).rowcount or 0)


def _replace_history(conn, product_ids: list[str], rebuilt_rows: list[dict]) -> tuple[int, int]:
    delete_sql = text(
        "DELETE FROM price_history WHERE product_id IN :product_ids"
    ).bindparams(bindparam("product_ids", expanding=True))
    deleted = int(conn.execute(delete_sql, {"product_ids": product_ids}).rowcount or 0)

    records = [
        {
            "history_id": str(uuid.uuid4()),
            "product_id": row["product_id"],
            "record_date": row["record_date"],
            "avg_price": float(row["avg_price"]),
            "original_price": (
                float(row["original_price"]) if row.get("original_price") is not None else None
            ),
            "min_price": float(row["min_price"]),
            "max_price": float(row["max_price"]),
            "listing_count": int(row["listing_count"]),
        }
        for row in rebuilt_rows
    ]
    if records:
        conn.execute(
            text(
                """
                INSERT INTO price_history (
                    history_id, product_id, record_date, avg_price, original_price,
                    min_price, max_price, listing_count
                )
                VALUES (
                    :history_id, :product_id, :record_date, :avg_price, :original_price,
                    :min_price, :max_price, :listing_count
                )
                """
            ),
            records,
        )
    return deleted, len(records)


def _delete_forecasts(conn, product_ids: list[str]) -> int:
    query = text(
        "DELETE FROM price_forecasts WHERE product_id IN :product_ids"
    ).bindparams(bindparam("product_ids", expanding=True))
    return int(conn.execute(query, {"product_ids": product_ids}).rowcount or 0)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Rebuild history for repaired products from DATE(active_listings.posted_at)."
    )
    parser.add_argument("--repair-date", default=datetime.now().date().isoformat())
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--yes", action="store_true")
    parser.add_argument("--skip-forecast-regeneration", action="store_true")
    args = parser.parse_args()

    if not DATE_RE.match(args.repair_date):
        raise SystemExit("--repair-date must use YYYY-MM-DD.")
    if not args.dry_run and not args.yes:
        raise SystemExit("Writing requires --yes. Run --dry-run first.")

    engine = create_engine(MYSQL_URI)
    with engine.connect() as conn:
        product_ids = _affected_product_ids(conn, args.repair_date)
        if not product_ids:
            print(f"No repaired products found for {args.repair_date}.")
            return 0
        rebuilt_rows = _history_rows(conn, product_ids)
        _print_plan(conn, product_ids, rebuilt_rows)

    if args.dry_run:
        print("Dry-run only. No database changes made.")
        return 0

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    history_backup = f"price_history_identity_backup_{stamp}"
    forecast_backup = f"price_forecasts_identity_backup_{stamp}"

    with engine.begin() as conn:
        backed_history = _backup_table(conn, "price_history", history_backup, product_ids)
        backed_forecasts = _backup_table(conn, "price_forecasts", forecast_backup, product_ids)
        deleted_history, inserted_history = _replace_history(conn, product_ids, rebuilt_rows)
        deleted_forecasts = _delete_forecasts(conn, product_ids)

    print(f"History backup table: {history_backup} ({backed_history} rows)")
    print(f"Forecast backup table: {forecast_backup} ({backed_forecasts} rows)")
    print(f"History replaced: deleted={deleted_history}, inserted={inserted_history}")
    print(f"Forecasts archived and removed: {deleted_forecasts}")

    if not args.skip_forecast_regeneration:
        from etl import predict_product_prices

        predict_product_prices(engine)

    with engine.connect() as conn:
        final_history = _count_for_products(conn, "price_history", product_ids)
        final_forecasts = _count_for_products(conn, "price_forecasts", product_ids)
    print(f"Final affected history rows: {final_history}")
    print(f"Final affected forecast rows: {final_forecasts}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
