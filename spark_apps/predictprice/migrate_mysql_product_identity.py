from __future__ import annotations

import argparse
import json
import sys
import uuid
from pathlib import Path
from typing import Any

import pandas as pd
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
    product_identity_key_from_product_row,
    row_from_mongo_doc,
)


MYSQL_URI = (
    f"mysql+pymysql://{cfg.MYSQL_USER}:{cfg.MYSQL_PASSWORD}"
    f"@{cfg.MYSQL_HOST}:{cfg.MYSQL_PORT}/{cfg.MYSQL_DB}"
)


def _present(value: Any) -> bool:
    if value is None:
        return False
    try:
        if pd.isna(value):
            return False
    except (TypeError, ValueError):
        pass
    return not (isinstance(value, str) and not value.strip())


def _specs(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if not _present(raw):
        return {}
    try:
        return json.loads(str(raw))
    except Exception:
        return {}


def canonical_product_row(product: dict[str, Any], extractor: PhoneInfoExtractor) -> dict[str, Any] | None:
    specs = _specs(product.get("base_specs"))
    row = row_from_mongo_doc(
        {
            "name": product.get("name") or product.get("model_series") or "",
            "brand": product.get("brand"),
            "storage": specs.get("storage"),
            "ram": specs.get("ram"),
        },
        extractor,
    )
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
        "category": product.get("category") or "Smartphone",
        "base_specs": build_base_specs(row),
    }


def should_skip(product: dict[str, Any], canonical: dict[str, Any]) -> str | None:
    old_name = str(product.get("name") or "").lower()
    old_brand = str(product.get("brand") or "").lower()
    new_name = str(canonical.get("name") or "").lower()
    new_brand = str(canonical.get("brand") or "").lower()

    if "iphone 13 ultra" in old_name:
        return "skip_iphone_13_ultra_needs_review"
    if old_brand == "samsung" and new_brand == "apple":
        return "skip_cross_brand_samsung_to_apple"
    if "ultra" in old_name and "ultra" not in new_name:
        return "skip_ultra_would_be_dropped"
    return None


def ensure_target(conn, canonical: dict[str, Any], key_to_target: dict[str, dict[str, Any]]) -> dict[str, Any]:
    key = canonical["product_identity_key"]
    existing = key_to_target.get(key)
    if existing:
        return existing

    new_id = str(uuid.uuid4())
    conn.execute(
        text(
            """
            INSERT INTO products (product_id, name, brand, model_series, category, base_specs, created_at)
            VALUES (:product_id, :name, :brand, :model_series, :category, :base_specs, NOW())
            """
        ),
        {
            "product_id": new_id,
            "name": canonical["name"],
            "brand": canonical["brand"],
            "model_series": canonical["model_series"],
            "category": canonical["category"],
            "base_specs": canonical["base_specs"],
        },
    )
    target = {"product_id": new_id, **canonical}
    key_to_target[key] = target
    return target


def choose_targets(products_df: pd.DataFrame, extractor: PhoneInfoExtractor) -> dict[str, dict[str, Any]]:
    targets: dict[str, dict[str, Any]] = {}
    for _, row in products_df.iterrows():
        product = row.to_dict()
        key = product_identity_key_from_product_row(product)
        if not key:
            continue
        canonical = canonical_product_row(product, extractor)
        if not canonical:
            continue
        if should_skip(product, canonical):
            continue

        old_name = str(product.get("name") or "").strip()
        canon_name = str(canonical.get("name") or "").strip()
        old_brand = str(product.get("brand") or "").strip().lower()
        canon_brand = str(canonical.get("brand") or "").strip().lower()
        score = 0
        if old_name == canon_name:
            score += 4
        if old_brand == canon_brand:
            score += 4
        if product.get("created_at") is not None:
            score += 1

        current = targets.get(key)
        if current is None or score > current["_score"]:
            targets[key] = {"product_id": product["product_id"], **canonical, "_score": score}
    return targets


def needs_metadata_update(product: dict[str, Any], canonical: dict[str, Any]) -> bool:
    fields = ("name", "brand", "model_series", "category", "base_specs")
    for field in fields:
        old = product.get(field)
        new = canonical.get(field)
        if str(old or "").strip() != str(new or "").strip():
            return True
    return False


def main() -> int:
    parser = argparse.ArgumentParser(description="Remap legacy MySQL products to canonical NLP identities.")
    parser.add_argument("--keyword", default="", help="Only migrate products whose old or canonical name contains this text")
    parser.add_argument("--limit", type=int, default=0, help="Limit source products for testing")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--yes", action="store_true")
    args = parser.parse_args()

    if not args.dry_run and not args.yes:
        raise SystemExit("Refusing to write without --yes. Use --dry-run first.")

    engine = create_engine(MYSQL_URI)
    extractor = PhoneInfoExtractor()

    with engine.connect() as conn:
        products_df = pd.read_sql(
            """
            SELECT product_id, name, brand, model_series, category, base_specs, created_at
            FROM products
            ORDER BY created_at, name
            """,
            conn,
        )

    key_to_target = choose_targets(products_df, extractor)
    actions: list[dict[str, Any]] = []
    metadata_updates: list[dict[str, Any]] = []
    skips: list[dict[str, Any]] = []

    for _, row in products_df.iterrows():
        product = row.to_dict()
        source_id = product["product_id"]
        canonical = canonical_product_row(product, extractor)
        if not canonical:
            continue
        reason = should_skip(product, canonical)
        if reason:
            skips.append({"product_id": source_id, "name": product.get("name"), "reason": reason})
            continue

        if args.keyword:
            kw = args.keyword.lower()
            old_name = str(product.get("name") or "").lower()
            new_name = str(canonical.get("name") or "").lower()
            if kw not in old_name and kw not in new_name:
                continue

        target = key_to_target.get(canonical["product_identity_key"])
        if target is None:
            target = {"product_id": None, **canonical}

        if target.get("product_id") == source_id:
            if needs_metadata_update(product, canonical):
                metadata_updates.append(
                    {
                        "product_id": source_id,
                        "old_name": product.get("name"),
                        "old_brand": product.get("brand"),
                        "old_model_series": product.get("model_series"),
                        "old_category": product.get("category"),
                        "old_specs": product.get("base_specs"),
                        **canonical,
                    }
                )
            continue

        actions.append(
            {
                "source_product_id": source_id,
                "source_name": product.get("name"),
                "source_brand": product.get("brand"),
                "source_specs": product.get("base_specs"),
                "target_product_id": target.get("product_id"),
                "target_name": canonical["name"],
                "target_brand": canonical["brand"],
                "target_model_series": canonical["model_series"],
                "target_category": canonical["category"],
                "target_specs": canonical["base_specs"],
                "identity_key": canonical["product_identity_key"],
            }
        )
        if args.limit and len(actions) >= args.limit:
            break

    if not actions and not metadata_updates:
        print("No remap actions.")
        if skips:
            print(f"Skipped {len(skips)} product(s).")
            for s in skips[:20]:
                print("SKIP", s)
        return 0

    source_ids = [a["source_product_id"] for a in actions]
    counts = {}
    if source_ids:
        with engine.connect() as conn:
            counts = dict(
                conn.execute(
                    text(
                        """
                        SELECT product_id, COUNT(1) c
                        FROM active_listings
                        WHERE product_id IN :ids
                        GROUP BY product_id
                        """
                    ),
                    {"ids": tuple(source_ids)},
                ).fetchall()
            )

    print(f"Planned remap actions: {len(actions)}")
    print("source_listings_to_move:", sum(int(counts.get(a["source_product_id"], 0)) for a in actions))
    print("\n-- sample actions --")
    for a in actions[:50]:
        print(
            f"{counts.get(a['source_product_id'], 0):>5} | "
            f"{a['source_brand']} / {a['source_name']} / {a['source_specs']} -> "
            f"{a['target_brand']} / {a['target_name']} / {a['target_specs']}"
        )
    print(f"\nPlanned metadata updates: {len(metadata_updates)}")
    for u in metadata_updates[:50]:
        print(
            f"{u['old_brand']} / {u['old_name']} / {u['old_specs']} -> "
            f"{u['brand']} / {u['name']} / {u['base_specs']}"
        )
    if skips:
        print(f"\nSkipped {len(skips)} product(s); sample:")
        for s in skips[:20]:
            print("SKIP", s)

    if args.dry_run:
        print("\nDry-run only. No MySQL changes made.")
        return 0

    moved = 0
    created = 0
    updated_products = 0
    with engine.begin() as conn:
        for u in metadata_updates:
            res = conn.execute(
                text(
                    """
                    UPDATE products
                    SET name = :name,
                        brand = :brand,
                        model_series = :model_series,
                        category = :category,
                        base_specs = :base_specs
                    WHERE product_id = :product_id
                    """
                ),
                {
                    "product_id": u["product_id"],
                    "name": u["name"],
                    "brand": u["brand"],
                    "model_series": u["model_series"],
                    "category": u["category"],
                    "base_specs": u["base_specs"],
                },
            )
            updated_products += int(res.rowcount or 0)

        for a in actions:
            target_id = a["target_product_id"]
            if target_id is None:
                target = ensure_target(
                    conn,
                    {
                        "product_identity_key": a["identity_key"],
                        "name": a["target_name"],
                        "brand": a["target_brand"],
                        "model_series": a["target_model_series"],
                        "category": a["target_category"],
                        "base_specs": a["target_specs"],
                    },
                    key_to_target,
                )
                target_id = target["product_id"]
                created += 1

            res = conn.execute(
                text(
                    """
                    UPDATE active_listings
                    SET product_id = :target_product_id,
                        last_updated = last_updated,
                        posted_at = posted_at
                    WHERE product_id = :source_product_id
                    """
                ),
                {
                    "target_product_id": target_id,
                    "source_product_id": a["source_product_id"],
                },
            )
            moved += int(res.rowcount or 0)

    print(f"Done. moved_listings={moved}, created_products={created}, updated_products={updated_products}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
