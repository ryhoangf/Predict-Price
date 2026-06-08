from __future__ import annotations

import argparse
import csv
from pathlib import Path

from sqlalchemy import create_engine, text

import config as cfg


MYSQL_URI = (
    f"mysql+pymysql://{cfg.MYSQL_USER}:{cfg.MYSQL_PASSWORD}"
    f"@{cfg.MYSQL_HOST}:{cfg.MYSQL_PORT}/{cfg.MYSQL_DB}"
)


SUMMARY_SQL = """
SELECT
    COALESCE(p.brand, 'NULL') AS brand,
    COUNT(*) AS joined_points,
    ROUND(AVG(ABS(pf.predicted_price - ph.avg_price)), 0) AS mae_vnd,
    ROUND(AVG(ABS(pf.predicted_price - ph.avg_price) / NULLIF(ph.avg_price, 0) * 100), 2) AS mape_pct,
    ROUND(AVG(pf.confidence_score), 2) AS stored_confidence_avg
FROM price_forecasts pf
JOIN price_history ph
    ON ph.product_id = pf.product_id
   AND ph.record_date = pf.forecast_date
JOIN products p ON p.product_id = pf.product_id
WHERE (:brand = '' OR LOWER(COALESCE(p.brand, '')) = LOWER(:brand))
GROUP BY COALESCE(p.brand, 'NULL')
HAVING joined_points >= :min_points
ORDER BY mape_pct DESC
"""


WORST_SQL = """
SELECT
    p.product_id,
    p.brand,
    p.name,
    p.base_specs,
    pf.forecast_date,
    ROUND(pf.predicted_price, 0) AS predicted_price,
    ROUND(ph.avg_price, 0) AS actual_price,
    ROUND(ABS(pf.predicted_price - ph.avg_price) / NULLIF(ph.avg_price, 0) * 100, 2) AS ape_pct,
    pf.model_version,
    pf.confidence_score
FROM price_forecasts pf
JOIN price_history ph
    ON ph.product_id = pf.product_id
   AND ph.record_date = pf.forecast_date
JOIN products p ON p.product_id = pf.product_id
WHERE (:brand = '' OR LOWER(COALESCE(p.brand, '')) = LOWER(:brand))
ORDER BY ape_pct DESC
LIMIT :limit
"""


def write_csv(path: Path, rows) -> None:
    rows = [dict(r) for r in rows]
    if not rows:
        return
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Backtest daily model price snapshots against same-day price_history."
    )
    parser.add_argument("--brand", default="", help="Optional exact brand filter, e.g. Apple")
    parser.add_argument("--min-points", type=int, default=20)
    parser.add_argument("--worst-limit", type=int, default=50)
    parser.add_argument("--csv-dir", default="", help="Optional directory for CSV outputs")
    args = parser.parse_args()

    engine = create_engine(MYSQL_URI)
    with engine.connect() as conn:
        summary = conn.execute(
            text(SUMMARY_SQL),
            {"brand": args.brand, "min_points": args.min_points},
        ).mappings().all()
        worst = conn.execute(
            text(WORST_SQL),
            {"brand": args.brand, "limit": args.worst_limit},
        ).mappings().all()

    print("===== Backtest summary =====")
    if not summary:
        print("(no rows)")
    for row in summary:
        print(dict(row))

    print("\n===== Worst predictions =====")
    for row in worst[: args.worst_limit]:
        print(dict(row))

    if args.csv_dir:
        out = Path(args.csv_dir)
        out.mkdir(parents=True, exist_ok=True)
        suffix = args.brand.lower() or "all"
        write_csv(out / f"backtest_summary_{suffix}.csv", summary)
        write_csv(out / f"backtest_worst_{suffix}.csv", worst)
        print(f"\nCSV written to {out}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
