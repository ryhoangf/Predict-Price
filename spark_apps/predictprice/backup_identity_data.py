from __future__ import annotations

import argparse
import os
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import config as cfg


MYSQL_TABLES = (
    "products",
    "active_listings",
    "price_history",
    "price_forecasts",
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Back up identity-related MongoDB and MySQL data.")
    parser.add_argument("--output-dir", default="/root/backups/ivaluate_identity")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_dir = Path(args.output_dir)
    mongo_path = output_dir / f"mongo_raw_items_{stamp}.archive.gz"
    mysql_path = output_dir / f"mysql_identity_{stamp}.sql"

    mongodump = shutil.which("mongodump")
    mysqldump = shutil.which("mysqldump")
    if not mongodump:
        raise SystemExit("mongodump is required before identity repair.")
    if not mysqldump:
        raise SystemExit("mysqldump is required before identity repair.")

    print(f"Backup directory: {output_dir}")
    print(f"Mongo archive: {mongo_path}")
    print(f"MySQL dump: {mysql_path}")
    if args.dry_run:
        return 0

    output_dir.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        [
            mongodump,
            f"--uri={cfg.MONGO_URI}",
            f"--db={cfg.DB_NAME}",
            f"--collection={cfg.COLLECTION_NAME}",
            f"--archive={mongo_path}",
            "--gzip",
        ],
        check=True,
    )

    mysql_env = os.environ.copy()
    mysql_env["MYSQL_PWD"] = cfg.MYSQL_PASSWORD
    with mysql_path.open("wb") as stream:
        subprocess.run(
            [
                mysqldump,
                f"--host={cfg.MYSQL_HOST}",
                f"--port={cfg.MYSQL_PORT}",
                f"--user={cfg.MYSQL_USER}",
                "--single-transaction",
                "--quick",
                "--skip-lock-tables",
                cfg.MYSQL_DB,
                *MYSQL_TABLES,
            ],
            env=mysql_env,
            stdout=stream,
            check=True,
        )

    for path in (mongo_path, mysql_path):
        if not path.exists() or path.stat().st_size == 0:
            raise RuntimeError(f"Backup was not created: {path}")
        print(f"Created {path} ({path.stat().st_size:,} bytes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
