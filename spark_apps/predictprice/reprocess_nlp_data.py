"""
Re-process Mongo + MySQL sau khi sửa NLP (vd. thêm regex OPPO Reno).

Luồng khuyến nghị:
  1. python reprocess_nlp_data.py --dry-run          # xem số lượng + mẫu tên mới
  2. python reprocess_nlp_data.py --mongo-renlp      # chạy lại NLP → ghi Mongo
  3. python reprocess_nlp_data.py --mongo-etl-ready  # processed=false cho etl.py
  4. python reprocess_nlp_data.py --mongo-sync-mysql  # URL đã có listing → loaded_mysql
  5. python merge_products_by_brand.py --brands OPPO,Huawei --yes  # (tuỳ chọn)
  6. python etl.py                                  # chỉ tin URL mới còn processed=false

Hoặc một lệnh (trừ etl): python reprocess_nlp_data.py --all --yes
  rồi: python etl.py

Lưu ý: etl.py bỏ qua URL đã có trong active_listings — phải --mysql-clear
       hoặc xóa tay listings trước khi chạy lại ETL.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd
import pymongo
from sqlalchemy import create_engine, text

_ROOT = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import config as cfg
from NLP.title_nlp import NLP_IDENTITY_VERSION
from scrapers.nlp_pipeline import run_nlp_pipeline

# Trùng etl.extract query (sau khi đã sửa NLP trên Mongo)
ETL_MONGO_QUERY = {
    "status": "extracted_layer2",
    "processed": False,
    "is_junk": {"$ne": True},
    "$or": [
        {"nlp_done": True},
        {"nlp_done": {"$exists": False}},
    ],
}

RENLP_QUERY = {
    "status": {"$in": ["extracted_layer2", "loaded_mysql"]},
    "is_junk": {"$ne": True},
    "link": {"$exists": True, "$ne": None},
}

RENLP_LOADED_QUERY = {
    "status": "loaded_mysql",
    "is_junk": {"$ne": True},
    "link": {"$exists": True, "$ne": None},
}

RENLP_ALL_QUERY = {
    "is_junk": {"$ne": True},
    "link": {"$exists": True, "$ne": None},
    "nlp_identity_version": {"$ne": NLP_IDENTITY_VERSION},
}

BATCH = 500
MYSQL_URI = (
    f"mysql+pymysql://{cfg.MYSQL_USER}:{cfg.MYSQL_PASSWORD}"
    f"@{cfg.MYSQL_HOST}:{cfg.MYSQL_PORT}/{cfg.MYSQL_DB}"
)


def _collection(uri: str | None = None):
    client = pymongo.MongoClient(uri or cfg.MONGO_URI)
    return client, client[cfg.DB_NAME][cfg.COLLECTION_NAME]


def count_mongo(col, query: dict) -> int:
    return col.count_documents(query)


def preview_standard_names(col, limit: int = 8) -> None:
    """Chạy NLP mới trên vài doc, in standard_name (logic etl.transform)."""
    from etl import transform

    cursor = col.find(RENLP_QUERY, {"_id": 0}).limit(limit * 3)
    docs = list(cursor)
    if not docs:
        print("Không có document nào khớp RENLP_QUERY.")
        return

    df = pd.DataFrame(docs)
    if "name" not in df.columns:
        print("Collection không có cột 'name'.")
        return

    df = run_nlp_pipeline(df, source_name="preview")
    df_t = transform(df.rename(columns={"link": "link"}))
    if df_t.empty:
        print("Sau transform không còn dòng (thiếu brand/giá/platform...).")
        return

    cols = ["name_raw", "standard_name", "model_series", "brand"]
    cols = [c for c in cols if c in df_t.columns]
    print("\n--- Mẫu standard_name (NLP mới + etl.transform) ---")
    print(df_t[cols].head(limit).to_string(index=False))


def mongo_renlp(
    dry_run: bool,
    batch_size: int,
    query: dict | None = None,
    *,
    preserve_dates: bool = False,
) -> int:
    """Chạy lại Layer-2 NLP trên Mongo (không cần Spark)."""
    client, col = _collection()
    renlp_query = query or RENLP_QUERY
    total = count_mongo(col, renlp_query)
    print(f"Mongo: {total} doc(s) sẽ re-NLP (status loaded_mysql | extracted_layer2, not junk).")

    if dry_run or total == 0:
        client.close()
        return total

    processed = 0
    cursor = col.find(renlp_query, batch_size=batch_size)

    batch_docs: list[dict] = []
    for doc in cursor:
        batch_docs.append(doc)
        if len(batch_docs) < batch_size:
            continue

        processed += _renlp_batch(col, batch_docs, preserve_dates=preserve_dates)
        batch_docs = []

    if batch_docs:
        processed += _renlp_batch(col, batch_docs, preserve_dates=preserve_dates)

    client.close()
    print(f"Mongo re-NLP: đã cập nhật ~{processed} doc(s) (theo bulk_write modified_count).")
    return processed


def _renlp_batch(col, docs: list[dict], *, preserve_dates: bool = False) -> int:
    df = pd.DataFrame(docs)
    if "_id" in df.columns:
        df = df.drop(columns=["_id"])

    # Mỗi source: filter link+source trong ingestion.apply_nlp_batch
    from ingestion import apply_nlp_batch

    updated = 0
    if "source" not in df.columns:
        df["source"] = "unknown"

    for source_name, grp in df.groupby("source", dropna=False):
        src = str(source_name) if pd.notna(source_name) else "unknown"
        sub = run_nlp_pipeline(grp.copy(), src)
        stats = apply_nlp_batch(
            src,
            sub,
            cfg.MONGO_URI,
            preserve_timestamps=preserve_dates,
        )
        updated += int(stats.get("updated") or 0)
        print(f"  source={src}: rows={len(sub)} updated={stats.get('updated')} stage={stats.get('stage')}")

    return updated


def mongo_prepare_etl(dry_run: bool) -> int:
    """Đặt processed=false để etl.py đọc được (reset_mongo_status cũ thiếu bước này)."""
    client, col = _collection()
    query = {"status": {"$in": ["extracted_layer2", "loaded_mysql", "dropped_etl"]}}
    n = count_mongo(col, query)

    print(f"Mongo: {n} doc(s) → status=extracted_layer2, processed=false, nlp_done=true")

    if dry_run or n == 0:
        client.close()
        return n

    result = col.update_many(
        query,
        {
            "$set": {
                "status": "extracted_layer2",
                "processed": False,
                "nlp_done": True,
            },
            "$unset": {"processed_at": ""},
        },
    )
    client.close()
    print(f"Mongo ETL-ready: modified={result.modified_count}")
    return result.modified_count


def mongo_sync_already_in_mysql(
    dry_run: bool,
    batch_size: int = 5000,
    *,
    preserve_dates: bool = False,
) -> int:
    """
    Doc đã có active_listings (theo source_url) → processed=true, status=loaded_mysql.
    Giải quyết hàng chục nghìn processed=false sau --mongo-etl-ready.
    """
    engine = create_engine(MYSQL_URI)
    with engine.connect() as conn:
        urls = [
            str(u).strip()
            for u in conn.execute(text("SELECT DISTINCT source_url FROM active_listings")).scalars()
            if u and str(u).strip()
        ]
    print(f"MySQL: {len(urls)} distinct source_url trong active_listings.")

    if not urls:
        return 0

    client, col = _collection()
    pending_before = count_mongo(col, ETL_MONGO_QUERY)
    updated_total = 0

    if dry_run:
        sample = urls[: min(5000, len(urls))]
        would = col.count_documents(
            {"link": {"$in": sample}, "processed": {"$ne": True}}
        )
        client.close()
        print(
            f"(dry-run) Ước lượng từ sample {len(sample)} URL: ~{would} doc pending sẽ được đánh loaded_mysql."
        )
        print(f"  ETL pending hiện tại: {pending_before}")
        return would

    from datetime import datetime

    sync_set = {"processed": True, "status": "loaded_mysql"}
    if not preserve_dates:
        sync_set["processed_at"] = datetime.now()

    for i in range(0, len(urls), batch_size):
        chunk = urls[i : i + batch_size]
        res = col.update_many(
            {"link": {"$in": chunk}},
            {"$set": sync_set},
        )
        updated_total += res.modified_count
        print(f"  batch {i // batch_size + 1}: modified={res.modified_count}")

    pending_after = count_mongo(col, ETL_MONGO_QUERY)
    client.close()
    print(f"Mongo sync: modified_total={updated_total}")
    print(f"  ETL pending: {pending_before} -> {pending_after}")
    return updated_total


def mysql_clear(dry_run: bool) -> None:
    """Xóa catalog MySQL (CASCADE từ products). Listings phải xóa trước products."""
    engine = create_engine(MYSQL_URI)
    tables = [
        ("price_forecasts", "SELECT COUNT(*) FROM price_forecasts"),
        ("price_history", "SELECT COUNT(*) FROM price_history"),
        ("active_listings", "SELECT COUNT(*) FROM active_listings"),
        ("products", "SELECT COUNT(*) FROM products"),
    ]

    with engine.connect() as conn:
        counts = {}
        for table, q in tables:
            try:
                counts[table] = conn.execute(text(q)).scalar()
            except Exception as e:
                counts[table] = f"ERR: {e}"

    print("\n--- MySQL hiện tại ---")
    for t, c in counts.items():
        print(f"  {t}: {c}")

    if dry_run:
        print("\n(dry-run) Không xóa. Chạy lại không có --dry-run để xóa.")
        return

    with engine.begin() as conn:
        conn.execute(text("SET FOREIGN_KEY_CHECKS = 0"))
        for table in ["price_forecasts", "price_history", "active_listings", "products"]:
            try:
                conn.execute(text(f"DELETE FROM {table}"))
            except Exception as e:
                print(f"  WARN delete {table}: {e}")
        conn.execute(text("SET FOREIGN_KEY_CHECKS = 1"))

    print("\nMySQL: đã xóa products + listings + history + forecasts.")
    print("Chạy tiếp: python etl.py")


def print_status_summary() -> None:
    client, col = _collection()
    print("\n--- Mongo status ---")
    for s in ["extracted_raw", "extracted_layer2", "loaded_mysql", "dropped_etl"]:
        print(f"  {s}: {col.count_documents({'status': s})}")
    print(f"  ETL pending (processed=false, layer2): {count_mongo(col, ETL_MONGO_QUERY)}")
    client.close()

    try:
        engine = create_engine(MYSQL_URI)
        with engine.connect() as conn:
            print("\n--- MySQL ---")
            for t in ["products", "active_listings", "price_history", "price_forecasts"]:
                n = conn.execute(text(f"SELECT COUNT(*) FROM {t}")).scalar()
                print(f"  {t}: {n}")
    except Exception as e:
        print(f"\nMySQL: không kết nối được — {e}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dry-run", action="store_true", help="Chỉ đếm / xem mẫu, không ghi DB")
    parser.add_argument("--yes", action="store_true", help="Bỏ xác nhận khi xóa MySQL")
    parser.add_argument("--status", action="store_true", help="In thống kê Mongo + MySQL")
    parser.add_argument("--mongo-renlp", action="store_true", help="Chạy lại NLP pipeline → Mongo")
    parser.add_argument("--mongo-renlp-loaded-only", action="store_true", help="Re-run NLP only for loaded_mysql, not junk")
    parser.add_argument("--mongo-etl-ready", action="store_true", help="processed=false cho etl.py")
    parser.add_argument(
        "--mongo-sync-mysql",
        action="store_true",
        help="URL đã có trong active_listings → loaded_mysql + processed=true",
    )
    parser.add_argument("--mysql-clear", action="store_true", help="Xóa bảng catalog MySQL")
    parser.add_argument("--all", action="store_true", help="mongo-renlp + mongo-etl-ready + mysql-clear")
    parser.add_argument("--preview", type=int, default=0, metavar="N", help="In N dòng standard_name mẫu")
    parser.add_argument("--batch-size", type=int, default=BATCH)
    parser.add_argument(
        "--preserve-dates",
        action="store_true",
        help="Giữ ingested_at, nlp_at, processed_at, status khi re-NLP/sync Mongo",
    )
    parser.add_argument(
        "--mongo-renlp-all",
        action="store_true",
        help="Re-NLP toàn bộ raw_items không phải junk (không chỉ loaded_mysql/layer2)",
    )
    args = parser.parse_args()

    if args.status:
        print_status_summary()
        return

    if not any(
        [
            args.mongo_renlp,
            args.mongo_renlp_all,
            args.mongo_renlp_loaded_only,
            args.mongo_etl_ready,
            args.mongo_sync_mysql,
            args.mysql_clear,
            args.all,
            args.preview,
        ]
    ):
        print_status_summary()
        print("\nGợi ý: --mongo-sync-mysql | merge OPPO/Huawei | python etl.py")
        return

    if args.preview:
        _, col = _collection()
        preview_standard_names(col, limit=args.preview)
        return

    dry = args.dry_run

    if args.all:
        args.mongo_renlp = True
        args.mongo_etl_ready = True
        args.mysql_clear = True

    preserve = args.preserve_dates

    if args.mongo_renlp:
        mongo_renlp(dry, args.batch_size, preserve_dates=preserve)

    if args.mongo_renlp_all:
        mongo_renlp(dry, args.batch_size, query=RENLP_ALL_QUERY, preserve_dates=preserve)

    if args.mongo_renlp_loaded_only:
        mongo_renlp(dry, args.batch_size, query=RENLP_LOADED_QUERY, preserve_dates=preserve)

    if args.mongo_etl_ready:
        mongo_prepare_etl(dry)

    if args.mongo_sync_mysql:
        mongo_sync_already_in_mysql(dry, args.batch_size, preserve_dates=preserve)

    if args.mysql_clear:
        if not dry and not args.yes:
            ans = input("Xóa TOÀN BỘ products/listings/history/forecasts trong MySQL? [y/N] ")
            if ans.strip().lower() != "y":
                print("Đã hủy --mysql-clear.")
                return
        mysql_clear(dry)

    print_status_summary()
    if not dry and (args.mongo_renlp or args.mongo_etl_ready or args.mysql_clear or args.all):
        print("\n▶ Bước cuối: cd spark_apps/predictprice && python etl.py")


if __name__ == "__main__":
    main()
