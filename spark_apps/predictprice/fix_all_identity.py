"""
Repair NLP and product identities while preserving source and ETL timestamps.

Flow:
  1. Back up MongoDB raw_items and MySQL identity tables.
  2. Re-run NLP for documents not carrying the current NLP identity version.
  3. Restore loaded_mysql status from URLs already present in MySQL.
  4. Repair the audited iPhone 13 family from Mongo raw titles. A description
     is trusted only when it contains an explicit model-name label.
  5. Delete known fake products only when they have no listings.

The workflow never runs mysql-clear or rewrites posted_at/last_updated.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent


def _run(cmd: list[str]) -> int:
    print(f"\n>>> {' '.join(cmd)}", flush=True)
    return subprocess.call(cmd, cwd=str(ROOT))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--yes", action="store_true")
    parser.add_argument("--skip-mongo", action="store_true")
    parser.add_argument("--skip-mysql", action="store_true")
    parser.add_argument("--batch-size", type=int, default=500)
    args = parser.parse_args()

    if not args.dry_run and not args.yes:
        raise SystemExit("Writing requires --yes. Run --dry-run first.")

    py = sys.executable

    backup_cmd = [py, "backup_identity_data.py"]
    if args.dry_run:
        backup_cmd.append("--dry-run")
    if _run(backup_cmd) != 0:
        return 1

    if not args.skip_mongo:
        renlp_cmd = [
            py,
            "reprocess_nlp_data.py",
            "--mongo-renlp-all",
            "--preserve-dates",
            "--batch-size",
            str(args.batch_size),
        ]
        if args.dry_run:
            renlp_cmd.append("--dry-run")
        if _run(renlp_cmd) != 0:
            return 1

        sync_cmd = [
            py,
            "reprocess_nlp_data.py",
            "--mongo-sync-mysql",
            "--preserve-dates",
            "--batch-size",
            str(args.batch_size),
        ]
        if args.dry_run:
            sync_cmd.append("--dry-run")
        if _run(sync_cmd) != 0:
            return 1

    if not args.skip_mysql:
        repair_cmd = [
            py,
            "repair_generic_product_buckets.py",
            "--keyword",
            "iPhone 13",
        ]
        repair_cmd.append("--dry-run" if args.dry_run else "--yes")
        if _run(repair_cmd) != 0:
            return 1

        cleanup_cmd = [py, "cleanup_orphan_products.py"]
        cleanup_cmd.append("--dry-run" if args.dry_run else "--yes")
        if _run(cleanup_cmd) != 0:
            return 1

    print(
        "\nCompleted without rewriting Mongo date fields or "
        "MySQL posted_at/last_updated."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
