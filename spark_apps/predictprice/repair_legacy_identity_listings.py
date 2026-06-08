from __future__ import annotations

import argparse
import json
import re
import sys
import uuid
from pathlib import Path
from typing import Any

from sqlalchemy import create_engine, text

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import config as cfg
from NLP.title_nlp import (
    PhoneInfoExtractor,
    build_base_specs,
    build_model_series,
    build_product_display_name,
    build_product_identity_key,
    row_from_mongo_doc,
)


MYSQL_URI = (
    f"mysql+pymysql://{cfg.MYSQL_USER}:{cfg.MYSQL_PASSWORD}"
    f"@{cfg.MYSQL_HOST}:{cfg.MYSQL_PORT}/{cfg.MYSQL_DB}"
)

LEGACY_PRODUCT_IDS = {
    "435ebe46-4eae-4630-86d8-571a00f0341d": "cross_brand_iphone4",
    "b04832b4-0e67-4f40-836d-c3e585618c1f": "mixed_iphone13_ultra",
    "e52e593f-f8bc-4a90-b163-2d2cfa957055": "mixed_xiaomi_iphone6",
}

QUARANTINE_PRODUCT_NAME = "Needs Review - Mixed Smartphone Bundle"
QUARANTINE_BRAND = "Needs Review"
QUARANTINE_SERIES = "Mixed Smartphone Bundle"
QUARANTINE_SPECS = json.dumps({"storage": None, "ram": None})


def _present(value: Any) -> bool:
    return value is not None and str(value).strip() != ""


def _normalize_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _identity_text(value: Any) -> str:
    text_value = _normalize_text(value)
    for marker in (" #", "＃", "ご覧いただきありがとうございます", "ご覧頂きありがとうございます"):
        idx = text_value.find(marker)
        if idx > 0:
            text_value = text_value[:idx]
    return text_value[:900].strip()


def _model_hits(text_value: str) -> set[str]:
    text_lc = text_value.lower()
    patterns = {
        "iphone": r"\biphone\s*(?:se\s*\d*|\d{1,2}\s*(?:pro\s*max|pro|plus|mini|s)?)\b",
        "galaxy": r"\bgalaxy\s+(?:s|a|note|z|fold|flip)?\s*\d{1,2}|galaxy\s+(?:note|fold|flip)",
        "pixel": r"\bpixel\s+\d{1,2}",
        "redmi": r"\bredmi\s+(?:note\s*)?\d{1,2}",
        "xperia": r"\bxperia\s+[a-z0-9]+",
        "aquos": r"\baquos\s+[a-z0-9]+",
        "oppo": r"\b(?:oppo|reno)\s*[a-z0-9]*",
        "motorola": r"\b(?:moto|motorola)\s+[a-z0-9]+",
    }
    hits = {name for name, pat in patterns.items() if re.search(pat, text_lc, re.IGNORECASE)}
    keyword_hints = {
        "iphone": ("iphone", "アイフォン"),
        "galaxy": ("galaxy",),
        "pixel": ("pixel",),
        "redmi": ("redmi",),
        "xperia": ("xperia",),
        "aquos": ("aquos",),
        "oppo": ("oppo", "reno"),
        "motorola": ("motorola", "moto"),
    }
    for name, words in keyword_hints.items():
        if any(word in text_lc for word in words):
            hits.add(name)
    return hits


def _is_mixed_bundle(text_value: str) -> bool:
    hits = _model_hits(text_value)
    return len(hits) >= 2


def _canonical_from_text(text_value: str, extractor: PhoneInfoExtractor) -> dict[str, Any] | None:
    row = row_from_mongo_doc({"name": text_value}, extractor)
    row["name_raw"] = None
    key = build_product_identity_key(row)
    name = build_product_display_name(row)
    if not key or not name:
        return None
    return {
        "product_identity_key": key,
        "name": name,
        "brand": row.get("brand"),
        "model_series": build_model_series(row),
        "category": "Smartphone",
        "base_specs": build_base_specs(row),
    }


def _load_product_targets(conn, extractor: PhoneInfoExtractor) -> dict[str, str]:
    rows = conn.execute(
        text("SELECT product_id, name, brand, base_specs FROM products")
    ).mappings()
    targets: dict[str, str] = {}
    for product in rows:
        canonical = _canonical_from_text(
            " ".join(
                str(x)
                for x in (product.get("brand"), product.get("name"))
                if _present(x)
            ),
            extractor,
        )
        if canonical:
            targets.setdefault(canonical["product_identity_key"], product["product_id"])
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
        {"product_id": product_id, **canonical},
    )
    targets[key] = product_id
    return product_id


def _ensure_review_table(conn) -> None:
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


def _ensure_quarantine_product(conn) -> str:
    row = conn.execute(
        text(
            """
            SELECT product_id
            FROM products
            WHERE name = :name AND brand = :brand
            LIMIT 1
            """
        ),
        {"name": QUARANTINE_PRODUCT_NAME, "brand": QUARANTINE_BRAND},
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
            "name": QUARANTINE_PRODUCT_NAME,
            "brand": QUARANTINE_BRAND,
            "model_series": QUARANTINE_SERIES,
            "base_specs": QUARANTINE_SPECS,
        },
    )
    return product_id


def _insert_review(
    conn,
    listing: dict[str, Any],
    reason: str,
    detected: dict[str, Any] | None,
) -> None:
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
            "detected_product_name": detected.get("name") if detected else None,
            "detected_base_specs": detected.get("base_specs") if detected else None,
        },
    )


def classify_listing(listing: dict[str, Any], extractor: PhoneInfoExtractor) -> tuple[str, dict[str, Any] | None]:
    source_text = listing.get("description") if _present(listing.get("description")) else listing.get("product_name")
    full_text = _identity_text(source_text)
    canonical = _canonical_from_text(full_text, extractor)

    if listing["product_id"] in {
        "b04832b4-0e67-4f40-836d-c3e585618c1f",
        "e52e593f-f8bc-4a90-b163-2d2cfa957055",
    }:
        return "quarantine_mixed_bundle", canonical

    if _is_mixed_bundle(full_text):
        return "quarantine_mixed_bundle", canonical

    if not canonical:
        return "quarantine_unclear_identity", None

    if canonical.get("brand") != "Apple":
        return "quarantine_non_apple_in_iphone_bucket", canonical

    name_lc = str(canonical.get("name") or "").lower()
    if not name_lc.startswith("iphone"):
        return "quarantine_unclear_identity", canonical

    if re.search(r"iphone\s+4\s+(?:plus|mini)\b", name_lc):
        return "quarantine_invalid_iphone_variant", canonical
    if re.search(r"iphone\s+(?:5|5s|5c)\s+(?:plus|mini|pro)\b", name_lc):
        return "quarantine_invalid_iphone_variant", canonical
    if re.search(r"iphone\s+(?:6|6s|7|8)\s+(?:mini|pro)\b", name_lc):
        return "quarantine_invalid_iphone_variant", canonical

    return "migrate", canonical


def main() -> int:
    parser = argparse.ArgumentParser(description="Repair legacy cross-brand active listings by listing-level NLP.")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--yes", action="store_true")
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args()

    if not args.dry_run and not args.yes:
        raise SystemExit("Refusing to write without --yes. Use --dry-run first.")

    engine = create_engine(MYSQL_URI)
    extractor = PhoneInfoExtractor()

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT l.listing_id, l.product_id, l.source_url, l.description,
                       p.name AS product_name, p.brand AS product_brand, p.base_specs AS product_specs
                FROM active_listings l
                JOIN products p ON p.product_id = l.product_id
                WHERE l.product_id IN :ids
                ORDER BY p.name, l.last_updated, l.listing_id
                """
            ),
            {"ids": tuple(LEGACY_PRODUCT_IDS.keys())},
        ).mappings().all()

    if args.limit:
        rows = rows[: args.limit]

    actions: list[dict[str, Any]] = []
    for row in rows:
        listing = dict(row)
        action, canonical = classify_listing(listing, extractor)
        actions.append({"listing": listing, "action": action, "canonical": canonical})

    summary: dict[str, int] = {}
    for item in actions:
        summary[item["action"]] = summary.get(item["action"], 0) + 1

    print("Planned listing actions:")
    for key, value in sorted(summary.items()):
        print(f"  {key}: {value}")
    print("\nSamples:")
    for item in actions[:30]:
        listing = item["listing"]
        canonical = item["canonical"] or {}
        print(
            f"{item['action']} | {listing['product_brand']} / {listing['product_name']} "
            f"-> {canonical.get('brand')} / {canonical.get('name')} | {listing['source_url']}"
        )

    if args.dry_run:
        print("\nDry-run only. No MySQL changes made.")
        return 0

    moved = 0
    reviewed = 0
    with engine.begin() as conn:
        _ensure_review_table(conn)
        targets = _load_product_targets(conn, extractor)
        quarantine_product_id = _ensure_quarantine_product(conn)

        for item in actions:
            listing = item["listing"]
            canonical = item["canonical"]
            if item["action"] == "migrate" and canonical:
                target_product_id = _ensure_product(conn, canonical, targets)
                reason = "legacy_identity_listing_migrated"
            else:
                target_product_id = quarantine_product_id
                reason = item["action"]
                _insert_review(conn, listing, reason, canonical)
                reviewed += 1

            if target_product_id == listing["product_id"]:
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
            moved += int(result.rowcount or 0)

    print(f"Done. moved_listings={moved}, review_rows={reviewed}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
