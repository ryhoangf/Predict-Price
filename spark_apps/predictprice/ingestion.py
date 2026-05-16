import pymongo
import pandas as pd
import time
from datetime import datetime, timezone
from urllib.parse import urlparse, urlunparse

import config as cfg
from contextlib import contextmanager


def redact_mongo_uri(uri: str | None) -> str:
    """URI để log (ẩn password)."""
    if not uri:
        return "(empty)"
    try:
        p = urlparse(uri)
        host = p.hostname or ""
        port = p.port or 27017
        if p.username:
            netloc = f"{p.username}:***@{host}:{port}"
        else:
            netloc = f"{host}:{port}"
        return urlunparse((p.scheme, netloc, p.path or "/", p.params, p.query, p.fragment))
    except Exception:
        return "(unparseable uri)"


@contextmanager
def get_mongo_connection(custom_uri=None):
    """
    Context manager để đảm bảo MongoDB connection được đóng
    """
    client = None
    uri_to_use = custom_uri if custom_uri else cfg.MONGO_URI
    try:
        client = pymongo.MongoClient(
            uri_to_use,
            serverSelectionTimeoutMS=10000,
            connectTimeoutMS=10000,
            # Insert batch nhiều field text lớn có thể > 60s trên mạng chậm / disk chậm
            socketTimeoutMS=300_000,
            maxPoolSize=50,
            maxIdleTimeMS=30000,
            retryWrites=True,
        )
        client.admin.command("ping")
        print(
            f"[mongo] ping OK | URI={redact_mongo_uri(uri_to_use)} | "
            f"db={cfg.DB_NAME} | coll={cfg.COLLECTION_NAME}"
        )

        db = client[cfg.DB_NAME]
        col = db[cfg.COLLECTION_NAME]
        yield col

    except pymongo.errors.ConnectionFailure as e:
        print(
            f"[mongo] ConnectionFailure | URI={redact_mongo_uri(uri_to_use)} | "
            f"{type(e).__name__}: {e}"
        )
        yield None
    except Exception as e:
        print(
            f"[mongo] Lỗi kết nối / ping | URI={redact_mongo_uri(uri_to_use)} | "
            f"{type(e).__name__}: {e}"
        )
        yield None
    finally:
        if client:
            client.close()


def save_batch_to_datalake(df, source_name, custom_mongo_uri=None):
    """
    Hàm nhận DataFrame và lưu vào MongoDB - CÓ KIỂM TRA DUPLICATE.
    Trả dict {saved, stage, ...} để worker/driver phân biệt scrape vs insert.
    """
    uri_log = redact_mongo_uri(custom_mongo_uri or cfg.MONGO_URI)
    out: dict = {
        "saved": 0,
        "stage": "init",
        "rows_in_batch": 0,
        "after_dedup": 0,
        "mongo_uri": uri_log,
    }

    if df is None or df.empty:
        print(f"[{source_name}] Không có dữ liệu để lưu.")
        out["stage"] = "empty_df"
        return out

    rows_received = len(df)
    out["rows_in_batch"] = rows_received
    print(
        f"[{source_name}] DEBUG ingest: rows_in={rows_received} | "
        f"target={uri_log} | db={cfg.DB_NAME} | coll={cfg.COLLECTION_NAME}"
    )

    with get_mongo_connection(custom_mongo_uri) as col:
        if col is None:
            print(f"[{source_name}] Không thể kết nối MongoDB (xem [mongo] phía trên).")
            out["stage"] = "mongo_connection_failed"
            return out

        if "link" not in df.columns:
            print(f"[{source_name}] Thiếu cột 'link' — không thể lưu.")
            out["stage"] = "missing_link_column"
            return out

        df = df.copy()
        _lnk = df["link"]
        _ok = _lnk.notna()
        df.loc[_ok, "link"] = _lnk[_ok].astype(str).str.strip()
        bad = df["link"].isna() | (df["link"].astype(str).str.len() == 0)
        if bad.any():
            n_bad = int(bad.sum())
            df = df.loc[~bad].copy()
            print(f"[{source_name}] Bỏ {n_bad} dòng không có link hợp lệ (trống/NaN).")
        if df.empty:
            print(f"[{source_name}] Không còn dòng có link — không insert.")
            out["stage"] = "no_valid_links"
            return out

        before_intra = len(df)
        df = df.drop_duplicates(subset=["link"], keep="first").reset_index(drop=True)
        intra_dropped = before_intra - len(df)
        if intra_dropped > 0:
            print(
                f"[{source_name}] Bỏ {intra_dropped} dòng trùng link trong cùng batch "
                f"(giữ bản đầu, tránh BulkWrite 11000)."
            )

        print(f"[{source_name}] Kiểm tra duplicate Mongo với {len(df)} items...")

        try:
            batch_links = (
                df["link"]
                .dropna()
                .astype(str)
                .loc[lambda s: s.str.strip() != ""]
                .unique()
                .tolist()
            )
        except Exception as e:
            print(f"[{source_name}] DEBUG không lấy được danh sách link | {type(e).__name__}: {e}")
            out["stage"] = "link_column_invalid"
            return out

        existing_links: set = set()
        # distinct + $in: một round-trip/chunk, server làm việc trên index — nhanh hơn find + iterate cursor.
        in_chunk = 500
        t_dedup_start = time.perf_counter()
        try:
            for ci in range(0, len(batch_links), in_chunk):
                chunk = [x for x in batch_links[ci : ci + in_chunk] if isinstance(x, str)]
                if not chunk:
                    continue
                matched = col.distinct("link", {"link": {"$in": chunk}})
                if matched:
                    existing_links.update(matched)
            dedup_s = time.perf_counter() - t_dedup_start
            print(
                f"[{source_name}] Batch có {len(batch_links)} URL duy nhất; "
                f"{len(existing_links)} đã tồn tại trong Mongo (sẽ bỏ qua). "
                f"[timing] dedup={dedup_s:.2f}s"
            )
        except pymongo.errors.ExecutionTimeout:
            print(
                f"[{source_name}] Timeout khi query duplicate ($in). "
                f"Không insert để tránh cả batch lỗi 11000 vì thiếu lọc trùng."
            )
            out["stage"] = "dedup_query_timeout"
            return out
        except Exception as e:
            err_msg = f"{type(e).__name__}: {e}"
            print(f"[{source_name}] DEBUG duplicate query | {err_msg}")
            out["stage"] = "dedup_query_failed"
            out["dedup_error"] = err_msg[:800]
            return out

        if existing_links:
            before_mongo = len(df)
            df = df[~df["link"].isin(existing_links)].copy()
            duplicates_removed = before_mongo - len(df)

            if duplicates_removed > 0:
                print(
                    f"[{source_name}] Loại bỏ {duplicates_removed} URL trùng "
                    f"(đã tồn tại trong Mongo)."
                )

            if df.empty:
                print(f"[{source_name}] Tất cả dữ liệu đều đã tồn tại — không insert.")
                out["stage"] = "all_duplicates"
                out["after_dedup"] = 0
                return out

        df["source"] = source_name
        df["ingested_at"] = datetime.now(timezone.utc)
        df["status"] = "extracted_layer2"
        df["processed"] = False
        if "is_junk" not in df.columns:
            df["is_junk"] = False

        for _col in ("explanation", "condition"):
            if _col in df.columns:
                df[_col] = df[_col].apply(
                    lambda x: None
                    if x is None or (isinstance(x, float) and pd.isna(x))
                    else (None if isinstance(x, str) and not str(x).strip() else x)
                )

        df = df.where(pd.notna(df), None)
        n_to_insert = len(df)
        out["after_dedup"] = n_to_insert
        print(f"[{source_name}] DEBUG ingest: rows_to_insert={n_to_insert}")

        # insert_many theo batch; to_dict("records") từng batch — giảm peak RAM vs convert cả DataFrame một lần.
        BATCH_SIZE = 1000
        total_saved = 0
        t_ins_start = time.perf_counter()
        t_build_total = 0.0

        for i in range(0, n_to_insert, BATCH_SIZE):
            t_b = time.perf_counter()
            batch = df.iloc[i : i + BATCH_SIZE].to_dict("records")
            t_build_total += time.perf_counter() - t_b
            try:
                result = col.insert_many(batch, ordered=False)
                total_saved += len(result.inserted_ids)
                print(
                    f"[{source_name}] Batch {i//BATCH_SIZE + 1}: "
                    f"Đã lưu {len(result.inserted_ids)} bản ghi."
                )

            except pymongo.errors.BulkWriteError as bwe:
                saved_count = bwe.details.get("nInserted", 0)
                total_saved += saved_count
                errors = bwe.details.get("writeErrors", [])
                duplicate_errors = sum(1 for err in errors if err.get("code") == 11000)
                print(
                    f"[{source_name}] DEBUG BulkWriteError batch {i//BATCH_SIZE + 1}: "
                    f"nInserted={saved_count}, writeErrors={len(errors)}, dup_11000={duplicate_errors}"
                )
                if errors and len(errors) != duplicate_errors:
                    print(f"[{source_name}] DEBUG mẫu writeError: {errors[0]}")

            except Exception as e:
                print(
                    f"[{source_name}] Lỗi batch {i//BATCH_SIZE + 1}: "
                    f"{type(e).__name__}: {e}"
                )

        ins_s = time.perf_counter() - t_ins_start
        out["saved"] = total_saved
        if total_saved > 0:
            out["stage"] = "inserted"
        elif out["after_dedup"] > 0:
            out["stage"] = "insert_failed_or_all_dup_batches"
        else:
            out["stage"] = "no_rows_to_insert"

        print(
            f"[{source_name}] Tổng lưu {total_saved}/{rows_received} bản ghi "
            f"(sau dedup: {out['after_dedup']}) | stage={out['stage']} "
            f"| [timing] build_records={t_build_total:.2f}s insert={ins_s:.2f}s"
        )
        return out


def create_indexes():
    """
    Tạo index cho MongoDB
    """
    with get_mongo_connection() as col:
        if col is None:
            print("Không thể kết nối MongoDB để tạo index.")
            return

        try:
            col.create_index("status", background=True)
            print("Đã tạo index cho field 'status'")

            col.create_index("is_junk", background=True)
            print("Đã tạo index cho field 'is_junk'")

            col.create_index("link", unique=True, background=True)
            print("Đã tạo unique index cho field 'link'")

            col.create_index("processed", background=True)
            print("Đã tạo index cho field 'processed'")

            col.create_index("ingested_at", background=True)
            print("Đã tạo index cho field 'ingested_at'")

            col.create_index([("source", 1), ("ingested_at", -1)], background=True)
            print("Đã tạo compound index cho 'source' + 'ingested_at'")

            print("\nDanh sách indexes hiện tại:")
            for idx in col.list_indexes():
                print(f"   - {idx['name']}: {idx.get('key', {})}")

        except Exception as e:
            print(f"Lỗi khi tạo index: {e}")


if __name__ == "__main__":
    print("Tạo indexes cho MongoDB collection...")
    create_indexes()
