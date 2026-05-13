import pymongo
import pandas as pd
from datetime import datetime, timezone
import config as cfg
from contextlib import contextmanager

@contextmanager
def get_mongo_connection(custom_uri=None):
    """
    Context manager để đảm bảo MongoDB connection được đóng
    """
    client = None
    try:
        uri_to_use = custom_uri if custom_uri else cfg.MONGO_URI
        
        # Cấu hình connection với timeouts
        client = pymongo.MongoClient(
            uri_to_use,
            serverSelectionTimeoutMS=10000,  # 10s timeout khi chọn server
            connectTimeoutMS=10000,           # 10s timeout khi connect
            socketTimeoutMS=60000,            # 60s timeout cho socket operations
            maxPoolSize=50,                   # Giới hạn pool size
            maxIdleTimeMS=30000,              # Đóng connection idle sau 30s
            retryWrites=True                  # Retry khi write fail
        )
        
        db = client[cfg.DB_NAME]
        col = db[cfg.COLLECTION_NAME]
        yield col
        
    except pymongo.errors.ConnectionFailure as e:
        print(f"MongoDB connection failed: {e}")
        yield None
    except Exception as e:
        print(f"Lỗi kết nối MongoDB: {e}")
        yield None
    finally:
        if client:
            client.close()

def save_batch_to_datalake(df, source_name, custom_mongo_uri=None):
    """
    Hàm nhận DataFrame và lưu vào MongoDB - CÓ KIỂM TRA DUPLICATE
    """
    if df is None or df.empty:
        print(f"[{source_name}] Không có dữ liệu để lưu.")
        return

    initial_count = len(df)
    
    # Sử dụng context manager để tự động đóng connection
    with get_mongo_connection(custom_mongo_uri) as col:
        if col is None:
            print(f"[{source_name}] Không thể kết nối MongoDB.")
            return

        # BƯỚC 1: Lấy danh sách URLs đã tồn tại (với timeout)
        print(f"[{source_name}] Kiểm tra duplicate với {initial_count} items...")
        
        existing_links = set()
        try:
            # Sử dụng projection và limit để tăng tốc
            existing_docs = col.find(
                {},
                {"link": 1, "_id": 0}
            ).max_time_ms(30000)  # Timeout 30s cho query này
            
            existing_links = {doc.get("link") for doc in existing_docs if doc.get("link")}
            print(f"[{source_name}] Tìm thấy {len(existing_links)} URLs đã tồn tại.")
        except pymongo.errors.ExecutionTimeout:
            print(f"[{source_name}] Timeout khi query URLs. Bỏ qua kiểm tra duplicate.")
        except Exception as e:
            print(f"[{source_name}] Cảnh báo: Không thể kiểm tra duplicate - {e}")
        
        # BƯỚC 2: Lọc bỏ URLs trùng
        if existing_links:
            df = df[~df['link'].isin(existing_links)].copy()
            duplicates_removed = initial_count - len(df)
            
            if duplicates_removed > 0:
                print(f"[{source_name}] Loại bỏ {duplicates_removed} URLs trùng lặp.")
            
            if df.empty:
                print(f"[{source_name}] Tất cả dữ liệu đều đã tồn tại.")
                return
        
        # BƯỚC 3: Thêm Metadata
        df["source"] = source_name
        df["ingested_at"] = datetime.now(timezone.utc)
        df["status"] = "extracted_layer2"
        df["processed"] = False
        if 'is_junk' not in df.columns:
            df["is_junk"] = False

        for _col in ("explanation", "condition"):
            if _col in df.columns:
                df[_col] = df[_col].apply(
                    lambda x: None
                    if x is None or (isinstance(x, float) and pd.isna(x))
                    else (None if isinstance(x, str) and not str(x).strip() else x)
                )

        df = df.where(pd.notna(df), None)
        records = df.to_dict("records")

        # BƯỚC 4: Lưu vào MongoDB (chia nhỏ batches để tránh timeout)
        BATCH_SIZE = 1000  # Lưu mỗi lần 1000 records
        total_saved = 0
        
        for i in range(0, len(records), BATCH_SIZE):
            batch = records[i:i+BATCH_SIZE]
            try:
                result = col.insert_many(batch, ordered=False)
                total_saved += len(result.inserted_ids)
                print(f"[{source_name}] Batch {i//BATCH_SIZE + 1}: Đã lưu {len(result.inserted_ids)} bản ghi.")
                
            except pymongo.errors.BulkWriteError as bwe:
                saved_count = bwe.details.get('nInserted', 0)
                total_saved += saved_count
                errors = bwe.details.get('writeErrors', [])
                
                duplicate_errors = sum(1 for e in errors if e.get('code') == 11000)
                if duplicate_errors > 0:
                    print(f"[{source_name}] Batch {i//BATCH_SIZE + 1}: Bỏ qua {duplicate_errors} bản ghi trùng lặp.")
                    
            except Exception as e:
                print(f"[{source_name}] Lỗi batch {i//BATCH_SIZE + 1}: {e}")
        
        print(f"[{source_name}] Tổng cộng đã lưu {total_saved}/{initial_count} bản ghi vào Data Lake.")

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
    # Chạy file này trực tiếp để tạo indexes
    print("Tạo indexes cho MongoDB collection...")
    create_indexes()