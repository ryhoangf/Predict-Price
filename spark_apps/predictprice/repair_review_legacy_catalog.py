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
from NLP.identity_quality import (
    identity_quality_reason,
    is_mixed_model_text,
    model_family_hits,
)
from NLP.product_matcher import product_candidate_priority
from NLP.title_nlp import (
    NLP_IDENTITY_VERSION,
    PhoneInfoExtractor,
    build_base_specs,
    build_model_series,
    build_product_display_name,
    build_product_identity_key,
)
from repair_generic_product_buckets import (
    MIXED_REVIEW_NAME,
    UNCLEAR_REVIEW_NAME,
    _ensure_product,
    _ensure_review_product,
    _ensure_tables,
    _insert_log,
    _insert_review,
    _load_raw_docs,
    _load_target_products,
)


MYSQL_URI = (
    f"mysql+pymysql://{cfg.MYSQL_USER}:{cfg.MYSQL_PASSWORD}"
    f"@{cfg.MYSQL_HOST}:{cfg.MYSQL_PORT}/{cfg.MYSQL_DB}"
)

LEGACY_SQL = """
    p.brand IS NULL
    OR CHAR_LENGTH(TRIM(p.brand)) <= 2
    OR (
        LOWER(p.brand) = 'motorola'
        AND LOWER(p.name) REGEXP '^motorola( (pro|plus|ultra))?( [0-9]+(gb| g))?$'
    )
    OR (
        LOWER(p.brand) = 'apple'
        AND (p.name LIKE '%SE1%' OR p.name REGEXP 'SE[0-9]{3,}')
    )
"""
_LEADING_INVENTORY_CODE_RE = re.compile(r"^\s*[A-Z]\d{1,4}\s+", re.IGNORECASE)
_RECOVERY_EXTRACTOR = PhoneInfoExtractor()


def _canonical_from_nlp_doc(doc: dict[str, Any] | None) -> dict[str, Any] | None:
    if not doc or doc.get("is_junk") is True:
        return None
    if doc.get("nlp_identity_version") != NLP_IDENTITY_VERSION:
        return None

    title = str(doc.get("name") or "").strip()
    quality_title = _LEADING_INVENTORY_CODE_RE.sub("", title)
    model_number = doc.get("model_number")
    if str(doc.get("model_line") or "").strip().lower() == "xperia":
        model_number = re.sub(
            r"^(\d{1,2})\s+[MO]$",
            r"\1",
            str(model_number or "").strip(),
            flags=re.IGNORECASE,
        )
    row = {
        "brand": doc.get("brand"),
        "model_line": doc.get("model_line"),
        "model_number": model_number,
        "variant": doc.get("variant"),
        "capacity": _RECOVERY_EXTRACTOR.extract_capacity(quality_title),
        "storage": None,
        "ram": _RECOVERY_EXTRACTOR.extract_ram(quality_title),
        "name_raw": None,
    }
    name = build_product_display_name(row)
    key = build_product_identity_key(row)
    if not title or not name or not key:
        return None
    if len(model_family_hits(quality_title)) >= 2 or is_mixed_model_text(quality_title):
        return None

    canonical = {
        "product_identity_key": key,
        "name": name,
        "brand": row.get("brand"),
        "model_line": row.get("model_line"),
        "model_number": row.get("model_number"),
        "variant": row.get("variant"),
        "model_series": build_model_series(row),
        "category": "Smartphone",
        "base_specs": build_base_specs(row),
    }
    if identity_quality_reason(
        {
            **canonical,
            "name_raw": quality_title,
            "description": str(doc.get("explanation") or ""),
            "standard_name": name,
        }
    ):
        return None
    return canonical


def _select_candidates(conn) -> list[dict[str, Any]]:
    rows = conn.execute(
        text(
            f"""
            SELECT l.listing_id, l.product_id, l.source_url, l.description,
                   l.price, l.posted_at, l.last_updated,
                   p.name AS product_name, p.brand AS product_brand,
                   p.model_series AS product_model_series,
                   p.base_specs AS product_specs, p.category AS product_category
            FROM active_listings l
            JOIN products p ON p.product_id = l.product_id
            WHERE p.category = 'Review' OR ({LEGACY_SQL})
            ORDER BY p.category = 'Review' DESC, p.name, l.listing_id
            """
        )
    ).mappings()
    return [dict(row) for row in rows]


def _candidate_actions(
    listings: list[dict[str, Any]],
    raw_docs: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    actions: list[dict[str, Any]] = []
    for listing in listings:
        raw_doc = raw_docs.get(str(listing.get("source_url") or ""))
        canonical = _canonical_from_nlp_doc(raw_doc)
        is_review = str(listing.get("product_category") or "") == "Review"
        if canonical:
            action = "recover_review_from_nlp_v3" if is_review else "repair_legacy_from_nlp_v3"
        elif is_review:
            action = "keep_review_unresolved"
        else:
            title = str((raw_doc or {}).get("name") or listing.get("product_name") or "")
            action = (
                "quarantine_legacy_mixed"
                if len(model_family_hits(title)) >= 2 or is_mixed_model_text(title)
                else "quarantine_legacy_unclear"
            )
        actions.append(
            {
                "listing": listing,
                "raw_doc": raw_doc,
                "canonical": canonical,
                "action": action,
            }
        )
    return actions


def _print_candidate_plan(actions: list[dict[str, Any]]) -> None:
    summary: dict[str, int] = {}
    targets: dict[str, int] = {}
    for item in actions:
        summary[item["action"]] = summary.get(item["action"], 0) + 1
        canonical = item.get("canonical") or {}
        if canonical:
            name = f"{canonical.get('brand')} / {canonical.get('name')}"
            targets[name] = targets.get(name, 0) + 1

    print("Candidate actions:")
    for action, count in sorted(summary.items()):
        print(f"  {action}: {count}")
    print("Top recovered targets:")
    for name, count in sorted(targets.items(), key=lambda x: x[1], reverse=True)[:20]:
        print(f"  {name}: {count}")
    print("Samples:")
    for item in actions[:40]:
        listing = item["listing"]
        canonical = item.get("canonical") or {}
        print(
            f"  {item['action']} | {listing.get('product_brand')} / "
            f"{listing.get('product_name')} -> {canonical.get('brand')} / "
            f"{canonical.get('name')} | {listing.get('source_url')}"
        )


def _duplicate_groups(conn) -> list[list[dict[str, Any]]]:
    rows = conn.execute(
        text(
            """
            SELECT p.product_id, p.name, p.brand, p.model_series, p.category,
                   p.base_specs, p.created_at, COUNT(l.listing_id) AS listing_count
            FROM products p
            JOIN active_listings l ON l.product_id = p.product_id
            WHERE COALESCE(p.category, '') <> 'Review'
              AND COALESCE(p.brand, '') <> 'Needs Review'
            GROUP BY p.product_id, p.name, p.brand, p.model_series,
                     p.category, p.base_specs, p.created_at
            ORDER BY p.name, p.product_id
            """
        )
    ).mappings()

    grouped: dict[tuple[str, str, str, str], list[dict[str, Any]]] = {}
    for row in rows:
        product = dict(row)
        specs = product.get("base_specs")
        if isinstance(specs, str):
            try:
                specs = json.loads(specs)
            except (TypeError, ValueError, json.JSONDecodeError):
                specs = {}
        specs = specs or {}
        signature = (
            str(product.get("brand") or "").strip().lower(),
            str(product.get("name") or "").strip().lower(),
            str(specs.get("storage") or "").strip().lower(),
            str(specs.get("ram") or "").strip().lower(),
        )
        grouped.setdefault(signature, []).append(product)
    return [products for products in grouped.values() if len(products) > 1]


def _target_priority(product: dict[str, Any]) -> tuple[int, int, int]:
    clean_name, listing_count = product_candidate_priority(product)
    created = int(product.get("created_at") is not None)
    return clean_name, listing_count, created


def _print_duplicate_plan(groups: list[list[dict[str, Any]]]) -> None:
    listing_count = sum(
        int(product.get("listing_count") or 0)
        for group in groups
        for product in group
    )
    print(
        f"Exact duplicate identities: groups={len(groups)}, "
        f"product_ids={sum(len(group) for group in groups)}, listings={listing_count}"
    )
    for group in groups[:30]:
        target = max(group, key=_target_priority)
        sources = [p for p in group if p["product_id"] != target["product_id"]]
        print(
            f"  {target.get('brand')} / {target.get('name')} / "
            f"{target.get('base_specs')} -> keep {target['product_id']}; "
            f"merge {len(sources)} product(s)"
        )


def _move_listing(
    conn,
    listing: dict[str, Any],
    target_id: str,
    target_name: str,
    reason: str,
    confidence: float,
) -> int:
    if target_id == listing["product_id"]:
        return 0
    result = conn.execute(
        text(
            """
            UPDATE active_listings
            SET product_id = :target_id,
                posted_at = posted_at,
                last_updated = last_updated
            WHERE listing_id = :listing_id
            """
        ),
        {"target_id": target_id, "listing_id": listing["listing_id"]},
    )
    if int(result.rowcount or 0):
        _insert_log(conn, listing, target_id, target_name, reason, confidence)
        return 1
    return 0


def _merge_forecasts(conn, source_id: str, target_id: str) -> tuple[int, int]:
    inserted = conn.execute(
        text(
            """
            INSERT IGNORE INTO price_forecasts (
                forecast_id, product_id, forecast_date, predicted_price,
                confidence_score, model_version, created_at
            )
            SELECT UUID(), :target_id, forecast_date, predicted_price,
                   confidence_score, model_version, created_at
            FROM price_forecasts
            WHERE product_id = :source_id
            """
        ),
        {"source_id": source_id, "target_id": target_id},
    )
    deleted = conn.execute(
        text("DELETE FROM price_forecasts WHERE product_id = :source_id"),
        {"source_id": source_id},
    )
    return int(inserted.rowcount or 0), int(deleted.rowcount or 0)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Recover confident Review listings, quarantine legacy buckets, and merge exact duplicates."
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--yes", action="store_true")
    args = parser.parse_args()
    if not args.dry_run and not args.yes:
        raise SystemExit("Refusing to write without --yes. Run --dry-run first.")

    engine = create_engine(MYSQL_URI)
    with engine.connect() as conn:
        listings = _select_candidates(conn)
    raw_docs = _load_raw_docs(listings)
    actions = _candidate_actions(listings, raw_docs)
    print(f"Mongo raw matched: {len(raw_docs)}/{len(listings)}")
    _print_candidate_plan(actions)
    with engine.connect() as conn:
        groups = _duplicate_groups(conn)
        _print_duplicate_plan(groups)

    if args.dry_run:
        print("Dry-run only. No database changes made.")
        return 0

    moved = 0
    recovered = 0
    quarantined = 0
    merged_products = 0
    forecasts_inserted = 0
    forecasts_deleted = 0
    with engine.begin() as conn:
        _ensure_tables(conn)
        targets = _load_target_products(
            conn,
            {str(item["listing"]["product_id"]) for item in actions},
        )
        mixed_review_id = _ensure_review_product(
            conn, MIXED_REVIEW_NAME, "Mixed Smartphone Bundle"
        )
        unclear_review_id = _ensure_review_product(
            conn, UNCLEAR_REVIEW_NAME, "Unclear Product Identity"
        )

        for item in actions:
            listing = item["listing"]
            canonical = item.get("canonical")
            action = item["action"]
            if action == "keep_review_unresolved":
                continue
            if canonical:
                target_id = _ensure_product(conn, canonical, targets)
                moved += _move_listing(
                    conn,
                    listing,
                    target_id,
                    canonical["name"],
                    action,
                    0.99,
                )
                recovered += 1
                continue

            target_id = (
                mixed_review_id if action == "quarantine_legacy_mixed" else unclear_review_id
            )
            target_name = (
                MIXED_REVIEW_NAME
                if action == "quarantine_legacy_mixed"
                else UNCLEAR_REVIEW_NAME
            )
            _insert_review(conn, listing, action, None)
            moved += _move_listing(
                conn,
                listing,
                target_id,
                target_name,
                action,
                0.0,
            )
            quarantined += 1

        for group in _duplicate_groups(conn):
            target = max(group, key=_target_priority)
            target_id = str(target["product_id"])
            for source in group:
                source_id = str(source["product_id"])
                if source_id == target_id:
                    continue
                source_listings = conn.execute(
                    text(
                        """
                        SELECT l.listing_id, l.product_id, l.source_url, l.description,
                               l.price, l.posted_at, l.last_updated,
                               p.name AS product_name, p.brand AS product_brand,
                               p.model_series AS product_model_series,
                               p.base_specs AS product_specs
                        FROM active_listings l
                        JOIN products p ON p.product_id = l.product_id
                        WHERE l.product_id = :source_id
                        """
                    ),
                    {"source_id": source_id},
                ).mappings()
                for listing_row in source_listings:
                    moved += _move_listing(
                        conn,
                        dict(listing_row),
                        target_id,
                        str(target["name"]),
                        "merge_duplicate_exact_identity",
                        1.0,
                    )
                inserted, deleted = _merge_forecasts(conn, source_id, target_id)
                forecasts_inserted += inserted
                forecasts_deleted += deleted
                merged_products += 1

    print(
        "Done. "
        f"moved_listings={moved}, recovered_candidates={recovered}, "
        f"quarantined_candidates={quarantined}, merged_products={merged_products}, "
        f"forecasts_inserted={forecasts_inserted}, forecasts_deleted={forecasts_deleted}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
