import os
import sys
import pandas as pd
import pymongo
import re
import joblib
import lightgbm as lgb
import scipy.sparse as sp
from pymongo import UpdateOne
import config as cfg

def clean_price(p):
    """Làm sạch giá để đưa vào mô hình"""
    if pd.isna(p): return 0
    nums = re.sub(r"[^\d]", "", str(p))
    return int(nums) if nums else 0

def main():
    print("="*60)
    print(" 🧹 DỌN DẸP LỊCH SỬ (BACKFILL): CẬP NHẬT IS_JUNK CHO MONGODB")
    print("="*60)

    # 1. Load Models
    print("Đang nạp mô hình TF-IDF và LightGBM...")
    try:
        tfidf_path = 'NLP/models/tfidf_junk_v1.pkl'
        lgbm_path = 'NLP/models/lgbm_junk_v1.txt'
        
        tfidf = joblib.load(tfidf_path)
        junk_model = lgb.Booster(model_file=lgbm_path)
        print("✅ Đã nạp mô hình thành công!")
    except Exception as e:
        print(f"❌ Lỗi khi nạp mô hình: {e}")
        return

    # 2. Kết nối MongoDB
    print("\nĐang kết nối MongoDB...")
    try:
        client = pymongo.MongoClient(cfg.MONGO_URI)
        col = client[cfg.DB_NAME][cfg.COLLECTION_NAME]
    except Exception as e:
        print(f"❌ Lỗi kết nối MongoDB: {e}")
        return

    # Lấy tổng số bản ghi cần xử lý
    # Chỉ lấy những bản ghi chưa được gán nhãn is_junk hoặc đang là False
    # (Để an toàn, chúng ta có thể quét toàn bộ database bằng query rỗng {})
    total_docs = col.count_documents({})
    print(f"Tổng số bản ghi trong Database: {total_docs}")

    if total_docs == 0:
        print("Không có dữ liệu để xử lý.")
        return

    # 3. Kéo data và Xử lý theo từng Batch (Tránh tràn RAM)
    BATCH_SIZE = 5000
    cursor = col.find({}, {"_id": 1, "name": 1, "price": 1, "explanation": 1, "original_explanation": 1})
    
    batch_data = []
    processed_count = 0
    total_junks_found = 0

    print("\nBắt đầu chạy AI phân loại và cập nhật MongoDB...")
    
    for doc in cursor:
        batch_data.append(doc)
        
        if len(batch_data) >= BATCH_SIZE:
            total_junks_found += process_batch(batch_data, tfidf, junk_model, col)
            processed_count += len(batch_data)
            print(f" ⏳ Đã xử lý: {processed_count}/{total_docs} (Phát hiện {total_junks_found} rác)")
            batch_data = [] # Reset batch

    # Xử lý nốt phần data còn dư (nếu có)
    if len(batch_data) > 0:
        total_junks_found += process_batch(batch_data, tfidf, junk_model, col)
        processed_count += len(batch_data)
        print(f" ⏳ Đã xử lý: {processed_count}/{total_docs} (Phát hiện {total_junks_found} rác)")

    print("\n" + "="*60)
    print(f" 🎉 HOÀN TẤT DỌN DẸP!")
    print(f" Đã quét {processed_count} sản phẩm.")
    print(f" Bắt được {total_junks_found} tin rác.")
    print("="*60)

def process_batch(batch_data, tfidf, junk_model, col):
    """Hàm xử lý và cập nhật database cho 1 batch"""
    df = pd.DataFrame(batch_data)
    
    # Gom name và explanation
    df['name_clean'] = df['name'].fillna('')
    
    # Ưu tiên explanation, nếu không có thì lấy original_explanation
    if 'explanation' not in df.columns:
        df['explanation'] = ''
    if 'original_explanation' not in df.columns:
        df['original_explanation'] = ''
        
    df['expl_clean'] = df['explanation'].fillna(df['original_explanation']).fillna('')
    combined_text = df['name_clean'] + " " + df['expl_clean']
    
    # Xử lý giá
    if 'price' not in df.columns:
        df['price'] = 0
    prices_array = df['price'].apply(clean_price).values.reshape(-1, 1)

    # Transform TF-IDF
    X_text = tfidf.transform(combined_text)
    X_final = sp.hstack((X_text, prices_array), format='csr')

    # Predict
    y_pred_prob = junk_model.predict(X_final)
    df['is_junk'] = y_pred_prob > 0.5  # Trả về True/False

    # Cập nhật hàng loạt (Bulk Update) vào MongoDB để tăng tốc độ
    bulk_operations = []
    for _, row in df.iterrows():
        bulk_operations.append(
            UpdateOne(
                {"_id": row['_id']},
                {"$set": {"is_junk": bool(row['is_junk'])}}
            )
        )
    
    # Thực thi lệnh update vào MongoDB
    if bulk_operations:
        col.bulk_write(bulk_operations)
        
    # Trả về số lượng rác tìm được trong batch này
    return int(df['is_junk'].sum())

if __name__ == "__main__":
    main()