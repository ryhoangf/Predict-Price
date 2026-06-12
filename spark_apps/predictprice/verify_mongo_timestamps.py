from __future__ import annotations

from datetime import date, datetime

import pymongo

import config as cfg


BACKUP_DB = "ivaluate_identity_verify"
BATCH_SIZE = 1000


def _is_date_field(key: str, value: object) -> bool:
    key_lower = key.lower()
    return (
        isinstance(value, (date, datetime))
        or key_lower == "date"
        or key_lower.endswith("_at")
        or key_lower.endswith("_date")
        or "timestamp" in key_lower
    )


def main() -> int:
    client = pymongo.MongoClient(cfg.MONGO_URI)
    current = client[cfg.DB_NAME][cfg.COLLECTION_NAME]
    backup = client[BACKUP_DB][cfg.COLLECTION_NAME]

    checked = 0
    missing = 0
    mismatches: list[tuple[object, str, object, object]] = []
    batch: list[dict] = []

    def compare_batch(rows: list[dict]) -> None:
        nonlocal checked, missing
        old_by_id = {
            row["_id"]: row
            for row in backup.find({"_id": {"$in": [item["_id"] for item in rows]}})
        }
        for new in rows:
            old = old_by_id.get(new["_id"])
            if old is None:
                missing += 1
                continue
            checked += 1
            keys = set(old) | set(new)
            for key in keys:
                old_value = old.get(key)
                new_value = new.get(key)
                if not (_is_date_field(key, old_value) or _is_date_field(key, new_value)):
                    continue
                if old_value != new_value:
                    mismatches.append((new["_id"], key, old_value, new_value))

    for doc in current.find({}):
        batch.append(doc)
        if len(batch) >= BATCH_SIZE:
            compare_batch(batch)
            batch = []
    if batch:
        compare_batch(batch)

    print(f"documents_checked={checked}")
    print(f"documents_missing_from_backup={missing}")
    print(f"timestamp_mismatches={len(mismatches)}")
    for mismatch in mismatches[:20]:
        print(mismatch)
    client.close()
    return 1 if mismatches or missing else 0


if __name__ == "__main__":
    raise SystemExit(main())
