from __future__ import annotations

import argparse
import json
import sys
import uuid
from pathlib import Path
from typing import Any

from sqlalchemy import bindparam, create_engine, text

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import config as cfg
from NLP.identity_quality import (
    compact_identity_text,
    identity_quality_reason,
    is_mixed_model_text,
    present,
)
from NLP.title_nlp import (
    PhoneInfoExtractor,
    build_base_specs,
    build_model_series,
    build_product_display_name,
    build_product_identity_key,
    product_identity_key_from_product_row,
    row_from_mongo_doc,
)


MYSQL_URI = (
    f"mysql+pymysql://{cfg.MYSQL_USER}:{cfg.MYSQL_PASSWORD}"
    f"@{cfg.MYSQL_HOST}:{cfg.MYSQL_PORT}/{cfg.MYSQL_DB}"
)

REVIEW_BRAND = "Needs Review"
MIXED_REVIEW_NAME = "Needs Review - Mixed Smartphone Bundle"
UNCLEAR_REVIEW_NAME = "Needs Review - Unclear Product Identity"
REVIEW_SPECS = json.dumps({"storage": None, "ram": None})
BAD_STORAGE_VALUES = ("0", "1", "4", "8", "12", "1TB")


def _json_dict(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if not present(raw):
        return {}
    try:
        return json.loads(str(raw))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}


def _canonical_from_text(text_value: str, extractor: PhoneInfoExtractor) -> dict[str, Any] | None:
    row = row_from_mongo_doc({"name": text_value}, extractor)
    name = build_product_display_name(row)
    key = build_product_identity_key(row)
    if not name or not key:
        return None

    canonical = {
        "product_identity_key": key,
        "name": name,
        "brand": row.get("brand"),
        "model_line": row.get("model_line"),
        "model_number": row.get("model_number"),
        "model_series": build_model_series(row),
        "category": "Smartphone",
        "base_specs": build_base_specs(row),
    }
    reason = identity_quality_reason(
        {
            **canonical,
            "name_raw": text_value,
            "description": "",
            "standard_name": name,
        }
    )
    if reason:
        canonical["quality_reason"] = reason
    return canonical


def _confidence(canonical: dict[str, Any] | None) -> float:
    if not canonical:
        return 0.0
    if canonical.get("quality_reason"):
        return 0.0
    specs = _json_dict(canonical.get("base_specs"))
    return 0.96 if specs.get("storage") else 0.90


def _source_text(listing: dict[str, Any]) -> str:
    description = compact_identity_text(listing.get("description"))
    if len(description) >= 8:
        return description
    return compact_identity_text(listing.get("product_name"))


def classify_listing(
    listing: dict[str, Any],
    extractor: PhoneInfoExtractor,
) -> tuple[str, dict[str, Any] | None, float]:
    source_text = _source_text(listing)
    if not source_text:
        return "quarantine_unclear_identity", None, 0.0

    canonical = _canonical_from_text(source_text, extractor)
    if is_mixed_model_text(source_text):
        return "quarantine_mixed_bundle", canonical, 0.0
    if not canonical:
        return "quarantine_unclear_identity", None, 0.0
    if canonical.get("quality_reason"):
        return f"quarantine_{canonical['quality_reason']}", canonical, 0.0

    old_brand = str(listing.get("product_brand") or "").strip().lower()
    new_brand = str(canonical.get("brand") or "").strip().lower()
    if old_brand and new_brand and old_brand != new_brand:
        # Cross-brand legacy products are exactly what we want to repair.
        return "migrate_cross_brand_repair", canonical, _confidence(canonical)

    return "migrate_generic_bucket_repair", canonical, _confidence(canonical)


def _ensure_tables(conn) -> None:
    conn.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS product_identity_review (
                review_id CHAR(36) NOT NULL PRIMARY KEY,
                listing_id CHAR(36) NOT NULL,
                source_url VARCHAR(1024) NOT NULL,
                original_product_id CHAR(36) NOT NULL,
                original_product_name VARCHAR(255),
                reason VARCHAR(100) NOT NULL,
                detected_product_name VARCHAR(255),
                detected_base_specs JSON,
                reviewed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY uq_product_identity_review_listing_reason (listing_id, reason)
            )
            """
        )
    )
    conn.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS product_identity_repair_log (
                repair_id CHAR(36) NOT NULL PRIMARY KEY,
                listing_id CHAR(36) NOT NULL,
                old_product_id CHAR(36) NOT NULL,
                new_product_id CHAR(36) NOT NULL,
                old_product_name VARCHAR(255),
                new_product_name VARCHAR(255),
                reason VARCHAR(100) NOT NULL,
                confidence DECIMAL(5,4),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                KEY idx_repair_log_listing (listing_id),
                KEY idx_repair_log_old_product (old_product_id),
                KEY idx_repair_log_new_product (new_product_id)
            )
            """
        )
    )


def _ensure_review_product(conn, name: str, series: str) -> str:
    row = conn.execute(
        text("SELECT product_id FROM products WHERE name = :name AND brand = :brand LIMIT 1"),
        {"name": name, "brand": REVIEW_BRAND},
    ).first()
    if row:
        return row[0]
    product_id = str(uuid.uuid4())
    conn.execute(
        text(
            """
            INSERT INTO products (product_id, name, brand, model_series, category, base_specs, created_at)
            VALUES (:product_id, :name, :brand, :model_series, 'Review', :base_specs, NOW())
            """
        ),
        {
            "product_id": product_id,
            "name": name,
            "brand": REVIEW_BRAND,
            "model_series": series,
            "base_specs": REVIEW_SPECS,
        },
    )
    return product_id


def _load_target_products(conn, source_ids: set[str]) -> dict[str, str]:
    rows = conn.execute(
        text(
            """
            SELECT product_id, name, brand, model_series, category, base_specs
            FROM products
            WHERE COALESCE(category, '') <> 'Review'
              AND COALESCE(brand, '') <> 'Needs Review'
            """
        )
    ).mappings()
    targets: dict[str, str] = {}
    for row in rows:
        product = dict(row)
        if product["product_id"] in source_ids:
            continue
        key = product_identity_key_from_product_row(product)
        if not key:
            continue
        targets.setdefault(key, product["product_id"])
    return targets


def _ensure_product(conn, canonical: dict[str, Any], targets: dict[str, str]) -> str:
    key = canonical["product_identity_key"]
    if key in targets:
        return targets[key]
    product_id = str(uuid.uuid4())
    conn.execute(
        text(
            """
            INSERT INTO products (product_id, name, brand, model_series, category, base_specs, created_at)
            VALUES (:product_id, :name, :brand, :model_series, :category, :base_specs, NOW())
            """
        ),
        {
            "product_id": product_id,
            "name": canonical["name"],
            "brand": canonical["brand"],
            "model_series": canonical["model_series"],
            "category": canonical["category"],
            "base_specs": canonical["base_specs"],
        },
    )
    targets[key] = product_id
    return product_id


def _insert_review(conn, listing: dict[str, Any], reason: str, canonical: dict[str, Any] | None) -> None:
    conn.execute(
        text(
            """
            INSERT IGNORE INTO product_identity_review (
                review_id, listing_id, source_url, original_product_id,
                original_product_name, reason, detected_product_name, detected_base_specs
            )
            VALUES (
                :review_id, :listing_id, :source_url, :original_product_id,
                :original_product_name, :reason, :detected_product_name, :detected_base_specs
            )
            """
        ),
        {
            "review_id": str(uuid.uuid4()),
            "listing_id": listing["listing_id"],
            "source_url": listing["source_url"],
            "original_product_id": listing["product_id"],
            "original_product_name": listing["product_name"],
            "reason": reason,
            "detected_product_name": canonical.get("name") if canonical else None,
            "detected_base_specs": canonical.get("base_specs") if canonical else None,
        },
    )


def _insert_log(
    conn,
    listing: dict[str, Any],
    target_product_id: str,
    target_product_name: str,
    reason: str,
    confidence: float,
) -> None:
    conn.execute(
        text(
            """
            INSERT INTO product_identity_repair_log (
                repair_id, listing_id, old_product_id, new_product_id,
                old_product_name, new_product_name, reason, confidence
            )
            VALUES (
                :repair_id, :listing_id, :old_product_id, :new_product_id,
                :old_product_name, :new_product_name, :reason, :confidence
            )
            """
        ),
        {
            "repair_id": str(uuid.uuid4()),
            "listing_id": listing["listing_id"],
            "old_product_id": listing["product_id"],
            "new_product_id": target_product_id,
            "old_product_name": listing["product_name"],
            "new_product_name": target_product_name,
            "reason": reason,
            "confidence": confidence,
        },
    )


def _select_listings(conn, args: argparse.Namespace) -> list[dict[str, Any]]:
    clauses = []
    params: dict[str, Any] = {}

    if args.product_id:
        product_ids = [p.strip() for p in args.product_id.split(",") if p.strip()]
        clauses.append("p.product_id IN :product_ids")
        params["product_ids"] = product_ids

    if args.keyword:
        clauses.append(
            """
            (
                p.name = :keyword
                OR CONCAT(COALESCE(p.brand, ''), ' ', p.name) = :keyword
                OR p.name LIKE :keyword_like
                OR p.model_series LIKE :keyword_like
            )
            """
        )
        params["keyword"] = args.keyword
        params["keyword_like"] = f"%{args.keyword}%"

    if args.bad_storage:
        clauses.append(
            """
            JSON_UNQUOTE(JSON_EXTRACT(p.base_specs, '$.storage')) IN :bad_storage_values
            """
        )
        params["bad_storage_values"] = list(BAD_STORAGE_VALUES)

    if not clauses:
        raise SystemExit("Provide --keyword, --product-id, or --bad-storage.")

    sql = text(
        f"""
        SELECT l.listing_id, l.product_id, l.source_url, l.description,
               l.price, l.posted_at, l.last_updated,
               p.name AS product_name, p.brand AS product_brand,
               p.model_series AS product_model_series, p.base_specs AS product_specs
        FROM active_listings l
        JOIN products p ON p.product_id = l.product_id
        WHERE {' OR '.join(clauses)}
          AND COALESCE(p.category, '') <> 'Review'
        ORDER BY p.name, l.last_updated, l.listing_id
        """
    )
    if "product_ids" in params:
        sql = sql.bindparams(bindparam("product_ids", expanding=True))
    if "bad_storage_values" in params:
        sql = sql.bindparams(bindparam("bad_storage_values", expanding=True))

    rows = conn.execute(sql, params).mappings().all()
    if args.limit:
        rows = rows[: args.limit]
    return [dict(row) for row in rows]


def _print_plan(actions: list[dict[str, Any]]) -> None:
    summary: dict[str, int] = {}
    by_target: dict[str, int] = {}
    for item in actions:
        summary[item["action"]] = summary.get(item["action"], 0) + 1
        canonical = item.get("canonical") or {}
        target_name = canonical.get("name") or item.get("review_name") or "NONE"
        by_target[target_name] = by_target.get(target_name, 0) + 1

    print("Planned listing actions:")
    for key, value in sorted(summary.items()):
        print(f"  {key}: {value}")

    print("\nTop target/review buckets:")
    for key, value in sorted(by_target.items(), key=lambda x: x[1], reverse=True)[:20]:
        print(f"  {key}: {value}")

    print("\nSamples:")
    for item in actions[:40]:
        listing = item["listing"]
        canonical = item.get("canonical") or {}
        print(
            f"{item['action']} | {listing.get('product_brand')} / {listing.get('product_name')} "
            f"-> {canonical.get('brand')} / {canonical.get('name')} | conf={item['confidence']:.2f} "
            f"| {listing.get('source_url')}"
        )


def main() -> int:
    parser = argparse.ArgumentParser(description="Listing-level repair for generic/cross-brand legacy product buckets.")
    parser.add_argument("--keyword")
    parser.add_argument("--product-id")
    parser.add_argument("--bad-storage", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--yes", action="store_true")
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args()

    if not args.dry_run and not args.yes:
        raise SystemExit("Refusing to write without --yes. Run --dry-run first.")

    engine = create_engine(MYSQL_URI)
    extractor = PhoneInfoExtractor()

    with engine.connect() as conn:
        listings = _select_listings(conn, args)

    actions: list[dict[str, Any]] = []
    for listing in listings:
        action, canonical, confidence = classify_listing(listing, extractor)
        review_name = None
        if action.startswith("quarantine_mixed"):
            review_name = MIXED_REVIEW_NAME
        elif action.startswith("quarantine"):
            review_name = UNCLEAR_REVIEW_NAME
        actions.append(
            {
                "listing": listing,
                "action": action,
                "canonical": canonical,
                "confidence": confidence,
                "review_name": review_name,
            }
        )

    print(f"Selected listings: {len(actions)}")
    _print_plan(actions)

    if args.dry_run:
        print("\nDry-run only. No MySQL changes made.")
        return 0

    moved = 0
    reviewed = 0
    skipped_same = 0
    with engine.begin() as conn:
        _ensure_tables(conn)
        source_ids = {str(item["listing"]["product_id"]) for item in actions}
        targets = _load_target_products(conn, source_ids)
        mixed_review_id = _ensure_review_product(conn, MIXED_REVIEW_NAME, "Mixed Smartphone Bundle")
        unclear_review_id = _ensure_review_product(conn, UNCLEAR_REVIEW_NAME, "Unclear Product Identity")

        for item in actions:
            listing = item["listing"]
            canonical = item["canonical"]
            action = item["action"]
            confidence = item["confidence"]

            if action.startswith("migrate") and canonical:
                target_product_id = _ensure_product(conn, canonical, targets)
                target_product_name = canonical["name"]
                reason = action
            else:
                target_product_id = mixed_review_id if action.startswith("quarantine_mixed") else unclear_review_id
                target_product_name = MIXED_REVIEW_NAME if action.startswith("quarantine_mixed") else UNCLEAR_REVIEW_NAME
                reason = action
                _insert_review(conn, listing, reason, canonical)
                reviewed += 1

            if target_product_id == listing["product_id"]:
                skipped_same += 1
                continue

            result = conn.execute(
                text(
                    """
                    UPDATE active_listings
                    SET product_id = :target_product_id,
                        last_updated = last_updated,
                        posted_at = posted_at
                    WHERE listing_id = :listing_id
                    """
                ),
                {
                    "target_product_id": target_product_id,
                    "listing_id": listing["listing_id"],
                },
            )
            if int(result.rowcount or 0):
                moved += 1
                _insert_log(conn, listing, target_product_id, target_product_name, reason, confidence)

    print(f"Done. moved_listings={moved}, review_rows={reviewed}, skipped_same_product={skipped_same}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
