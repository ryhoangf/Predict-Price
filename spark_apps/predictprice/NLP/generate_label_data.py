import os, sys
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
    
import pandas as pd
import pymongo
import config as cfg
import re

def clean_price(price_raw):
    """Hàm phụ trợ để lấy con số từ chuỗi giá (VD: '5,000 YEN' -> 5000)"""
    if pd.isna(price_raw): return 0
    nums = re.sub(r"[^\d]", "", str(price_raw))
    return int(nums) if nums else 0

def suggest_junk(row):
    """
    Hàm Heuristics: Tự động gợi ý nhãn
    1: Rác (Junk - Phụ kiện, ốp lưng, hộp rỗng, mô hình...)
    0: Máy thật
    """
    raw_name = row.get('name')
    raw_expl = row.get('explanation') if pd.notna(row.get('explanation')) else row.get('original_explanation', '')
    
    #Không có tên -> Rác
    if pd.isna(raw_name) or str(raw_name).strip() == '':
        return 1
        
    name = str(raw_name).lower()
    expl = str(raw_expl).lower()
    price = clean_price(row.get('price'))

    #Lọc theo giá: Dưới 3000 Yên  => phụ kiện/xác máy
    if price > 0 and price < 3000:
        return 1

    #Name: Bắt các từ ốp lưng, hộp rỗng, bao da, phụ kiện
    name_junk_keywords = r'(ケース|空箱|箱のみ|フィルム|ガラス|カバー|モックアップ|部品|パーツ)'
    if re.search(name_junk_keywords, name, re.IGNORECASE):
        #Nếu tên có chữ ốp lưng nhưng kèm chữ "Máy", "GB -> Pass
        safeguard_keywords = r'(本体|gb|tb|simフリー|付き|おまけ|セット|付属)'
        if not re.search(safeguard_keywords, name, re.IGNORECASE):
            return 1

    #Explanation: CHỈ bắt những từ "rác" tuyệt đối
    expl_junk_keywords = r'(モックアップ|部品取り|パーツ取り|箱のみ)'
    if re.search(expl_junk_keywords, expl, re.IGNORECASE):
        return 1

    return 0

def create_labeling_file(limit_records=3000):
    print("="*60)
    print(" 🛠️ TẠO DATASET ĐỂ GÁN NHÃN (JUNK DETECTION)")
    print("="*60)
    
    # 1. Kết nối MongoDB
    try:
        client = pymongo.MongoClient(cfg.MONGO_URI)
        col = client[cfg.DB_NAME][cfg.COLLECTION_NAME]
    except Exception as e:
        print(f"Lỗi kết nối MongoDB: {e}")
        return

    print(f"Đang kéo {limit_records} bản ghi từ MongoDB...")
    cursor = col.find({}, {
        "_id": 0, "link": 1, "name": 1, "price": 1, "source": 1, 
        "explanation": 1, "original_explanation": 1
    }).limit(limit_records)
    
    df = pd.DataFrame(list(cursor))
    
    if df.empty:
        print("Không có dữ liệu trong MongoDB!")
        return

    if 'explanation' not in df.columns and 'original_explanation' in df.columns:
        df['explanation'] = df['original_explanation']
    elif 'explanation' not in df.columns:
        df['explanation'] = ''

    print("Đang chạy Heuristics để tự động gợi ý nhãn...")
    df['is_junk'] = df.apply(suggest_junk, axis=1)

    cols = ['is_junk', 'name', 'price', 'explanation', 'source', 'link']
    df = df[cols]

    junk_count = df['is_junk'].sum()
    print(f"Phân tích sơ bộ: {junk_count} tin rác / {len(df) - junk_count} tin thật")

    #Xuất ra file CSV
    output_dir = 'dataset'
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, 'junk_labeling_task.csv')
    
    df.to_csv(output_file, index=False, encoding='utf-8-sig')
    
    print("\nHOÀN TẤT!")
    print(f"File đã được lưu tại: {output_file}")

if __name__ == "__main__":
    create_labeling_file(limit_records=3000)