import os
import json
import uuid
import re
import pandas as pd
import pymongo
from sqlalchemy import text, create_engine
from datetime import datetime, timedelta
import config as cfg
# from NLP.title_nlp import PhoneInfoExtractor
# from NLP.item_explanation import ItemExplanationExtractor

# --- CONFIG ---
MONGO_URI = cfg.MONGO_URI
DB_NAME = cfg.DB_NAME
COLLECTION_NAME = cfg.COLLECTION_NAME
MYSQL_URI = f"mysql+pymysql://{cfg.MYSQL_USER}:{cfg.MYSQL_PASSWORD}@{cfg.MYSQL_HOST}:{cfg.MYSQL_PORT}/{cfg.MYSQL_DB}"

BATCH_SIZE = 500
YEN_TO_VND_RATE = 175 

# Threshold giá hợp lý cho điện thoại
MIN_PRICE_YEN = 5000  # ~875,000 VND
MAX_PRICE_YEN = 300000  # ~52,500,000 VND

# --- LOAD NLP MODEL ---
# print("Loading NLP Model...")
# extractor = PhoneInfoExtractor()
# print("NLP Model Loaded.")

# --- LOAD ML PRICE PREDICTOR ---
print("Loading ML Price Predictor...")
price_predictor = None  # Initialize as None

try:
    import sys
    
    # Add parent directory to sys.path
    parent_dir = os.path.dirname(os.path.abspath(__file__))
    if parent_dir not in sys.path:
        sys.path.insert(0, parent_dir)
    
    # Import from ml_models package
    from ml_models.smart_price_predictor import SmartPricePredictor
    
    MODEL_PATH = os.path.join(parent_dir, 'models', 'smart_price_predictor.pkl')
    
    if os.path.exists(MODEL_PATH):
        price_predictor = SmartPricePredictor()
        price_predictor.load(MODEL_PATH)
        print("✓ ML Price Predictor Loaded Successfully!")
    else:
        print(f"⚠️ Warning: Model not found at {MODEL_PATH}")
        print("   Price prediction will be skipped.")
except Exception as e:
    print(f"⚠️ Warning: Could not load ML model - {e}")
    print("   Price prediction will be skipped.")
    price_predictor = None

# # --- LOAD EXPLANATION EXTRACTOR ---
# print("Loading Explanation Extractor...")
# explanation_extractor = ItemExplanationExtractor()
# print("Explanation Extractor Loaded.")

# --- 1. EXTRACT ---
def extract() -> pd.DataFrame:
    """
    Extract data từ MongoDB - CHỈ lấy data mới chưa xử lý
    """
    print("\n" + "="*60)
    print("STEP 1: EXTRACT FROM MONGODB")
    print("="*60)
    print("Connecting to MongoDB...")
    
    try:
        client = pymongo.MongoClient(MONGO_URI)
        col = client[DB_NAME][COLLECTION_NAME]
        
        # Tính ngày 7 ngày trước
        seven_days_ago = datetime.now() - timedelta(days=7)
        
        query = {
            "$or": [
                {"status": "extracted_layer2", "is_junk": {"$ne": True}},
                {"processed": False, "is_junk": {"$ne": True}}
            ]
        }
        
        data = list(col.find(query, {"_id": 0}))
        
        if not data:
            print("No new data found to process.")
            return pd.DataFrame()
            
        df = pd.DataFrame(data)
        print(f"Extracted {len(df)} NEW documents from MongoDB.")
        
        # KIỂM TRA DUPLICATE TRONG MONGODB
        print("\n--- Duplicate Check in MongoDB ---")
        initial_count = len(df)
        
        # Kiểm tra duplicate theo 'link'
        if 'link' in df.columns:
            df = df.drop_duplicates(subset=["link"], keep="first")
            mongo_dups = initial_count - len(df)
            if mongo_dups > 0:
                print(f"Removed {mongo_dups} duplicate URLs from MongoDB data.")
        
        # Kiểm tra records không có link
        if 'link' in df.columns:
            no_link = df['link'].isna().sum()
            if no_link > 0:
                df = df.dropna(subset=['link'])
                print(f"Removed {no_link} records without URLs.")
        
        print(f"\nMongoDB Extract Summary:")
        print(f"   Total extracted: {initial_count}")
        print(f"   After dedup:     {len(df)}")
        print(f"   Loss:            {initial_count - len(df)} ({(initial_count - len(df))/initial_count*100:.1f}%)")
        
        return df
        
    except Exception as e:
        print(f"❌ Error extracting data: {e}")
        return pd.DataFrame()

def check_duplicates_in_mysql(df: pd.DataFrame, engine) -> pd.DataFrame:
    """
    Kiểm tra xem URLs đã tồn tại trong MySQL chưa
    """
    if df.empty or 'source_url' not in df.columns:
        return df
    
    print("\n--- Duplicate Check in MySQL ---")
    initial_count = len(df)
    
    try:
        # Lấy tất cả URLs hiện có trong active_listings
        with engine.connect() as conn:
            existing_urls_df = pd.read_sql(
                "SELECT DISTINCT source_url FROM active_listings",
                conn
            )
        
        existing_urls = set(existing_urls_df['source_url'].tolist())
        print(f"Found {len(existing_urls)} existing URLs in MySQL database.")
        
        if existing_urls:
            # Lọc bỏ URLs đã tồn tại
            df = df[~df['source_url'].isin(existing_urls)].copy()
            mysql_dups = initial_count - len(df)
            
            if mysql_dups > 0:
                print(f"Filtered out {mysql_dups} URLs already in MySQL.")
            else:
                print(f"No duplicates found with MySQL database.")
        
        print(f"\nMySQL Duplicate Check Summary:")
        print(f"   Before check: {initial_count}")
        print(f"   After filter: {len(df)}")
        print(f"   Duplicates:   {initial_count - len(df)}")
        
        return df
        
    except Exception as e:
        print(f"Warning: Could not check MySQL duplicates - {e}")
        print(f"   Continuing with all {len(df)} records...")
        return df

def update_mongo_status(source_urls):
    """Đánh dấu data đã vào MySQL thành công"""
    if not source_urls: return
    try:
        client = pymongo.MongoClient(MONGO_URI)
        col = client[DB_NAME][COLLECTION_NAME]
        
        result = col.update_many(
            {"link": {"$in": source_urls}},
            {
                "$set": {
                    "status": "loaded_mysql",
                    "processed": True,
                    "processed_at": datetime.now()
                }
            }
        )
        print(f"Marked {result.modified_count} docs as 'loaded_mysql' in MongoDB.")
    except Exception as e:
        print(f"Warning: Could not update Mongo status: {e}")

# --- 2. TRANSFORM ---

def normalize_platform(url):
    if not isinstance(url, str): return None
    url_lower = url.lower()
    
    if 'mercari' in url_lower: return 'Mercari'
    if 'rakuma' in url_lower or 'rakuten' in url_lower: return 'Rakuma'
    if 'yahoo' in url_lower or 'jdirectitems' in url_lower: return 'YahooAuction'
    
    return None

def clean_price_yen(val):
    """
    Làm sạch giá Yên - Cải thiện xử lý edge cases
    """
    if pd.isna(val): 
        return None
    
    if isinstance(val, (int, float)): 
        return float(val) if val > 0 else None
    
    s = str(val)
    
    # Loại bỏ ký tự không phải số
    nums = re.sub(r"[^\d]", "", s)
    
    if not nums:
        return None
    
    price = float(nums)
    
    # Xử lý trường hợp số quá lớn (có thể là ghép nhiều số)
    # VD: "10000500" từ "10,000円 + 500円"
    if price > MAX_PRICE_YEN * 10:  # Quá 3,000,000 yên
        # Thử cắt số cuối (có thể là phí ship)
        price_str = str(int(price))
        if len(price_str) > 6:
            # Lấy 6 số đầu
            price = float(price_str[:6])
    
    return price if price > 0 else None

def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform data: Đổi tên cột, lọc giá, và lắp ráp (Stitching) các feature NLP
    đã được Spark Worker trích xuất từ trước.
    """
    if df.empty: 
        return df
    
    df = df.copy()
    initial_count = len(df)
    
    print("\n" + "="*60)
    print("STEP 2: TRANSFORM & VALIDATE (Stitching NLP Data)")
    print("="*60)
    print(f"Initial records: {initial_count}")

    # Step 1: Rename columns (Chuẩn hóa tên cột để mapping với MySQL)
    rename_map = {
        "link": "source_url",
        "name": "name_raw",
        "price": "price_raw",
        "condition": "condition_rank",
        "explanation": "description"
    }
    df = df.rename(columns=rename_map)

    # Đảm bảo các cột tối thiểu tồn tại để code không bị crash
    for col in ["source_url", "name_raw", "price_raw"]:
        if col not in df.columns: 
            df[col] = None

    # Step 2: Normalize Platform
    df["platform"] = df["source_url"].apply(normalize_platform)
    before_platform = len(df)
    df = df.dropna(subset=["platform"])
    print(f"After platform filter: {len(df)} ({before_platform - len(df)} dropped)")

    # Step 3: Clean Price & Lọc khoảng giá hợp lý
    df["price_yen"] = df["price_raw"].apply(clean_price_yen)
    
    before_price = len(df)
    df = df.dropna(subset=["price_yen"])
    df = df[(df["price_yen"] >= MIN_PRICE_YEN) & (df["price_yen"] <= MAX_PRICE_YEN)]
    print(f"After price filter ({MIN_PRICE_YEN}-{MAX_PRICE_YEN}¥): {len(df)} ({before_price - len(df)} dropped)")
    
    df["price_vnd"] = df["price_yen"] * YEN_TO_VND_RATE

    # =========================================================================
    # STEP 4: LOGIC LẮP RÁP (STITCHING) TỪ DỮ LIỆU ĐÃ CÓ TRONG MONGODB
    # Không chạy lại NLP (extractor/explanation_extractor) ở đây nữa!
    # =========================================================================
    print("\nBuilding standard identities from extracted features...")
    
    # 4.1 Lọc Rác Cơ Bản: Máy phải có Brand (do Spark Worker bắt được) thì mới cho vào kho
    before_brand = len(df)
    df = df.dropna(subset=["brand"])
    print(f"After removing items without Brand (Junk Filter): {len(df)} ({before_brand - len(df)} dropped)")

    # 4.2 Lắp ráp tên chuẩn (standard_name)
    def build_std_name(row):
        # Lấy dung lượng từ Title (capacity) hoặc Description (storage)
        mem = row.get('capacity') if pd.notna(row.get('capacity')) else row.get('storage')
        
        parts = [
            row.get('brand'), 
            row.get('model_line'), 
            row.get('model_number'), 
            row.get('variant'), 
            mem
        ]
        # Gom các phần tử có giá trị lại với nhau
        name = " ".join([str(p) for p in parts if pd.notna(p) and p]).strip()
        
        # Nếu tên lắp ráp quá ngắn, lấy một phần của tên raw làm fallback
        if len(name) < 5 and pd.notna(row.get('name_raw')):
            return row['name_raw'][:100]
        return name
        
    df['standard_name'] = df.apply(build_std_name, axis=1)
    
    # 4.3 Lắp ráp Model Series
    def build_series(row):
        parts = [row.get('model_line'), row.get('model_number')]
        return " ".join([str(p) for p in parts if pd.notna(p) and p]).strip()
        
    df['model_series'] = df.apply(build_series, axis=1)
    df['category'] = 'Smartphone'

    # 4.4 Lắp ráp Base Specs (Dưới dạng JSON)
    def build_base_specs(row):
        # Ưu tiên lấy 'storage' từ explanation, nếu không có thì lấy 'capacity' từ title
        storage_val = row.get('storage') if pd.notna(row.get('storage')) else row.get('capacity')
        
        storage = str(storage_val).replace('GB', '').replace('gb', '').strip() if pd.notna(storage_val) else None
        ram = str(row.get('ram')).replace('GB', '').replace('gb', '').strip() if pd.notna(row.get('ram')) else None
        
        return json.dumps({"storage": storage, "ram": ram})
        
    df['base_specs'] = df.apply(build_base_specs, axis=1)

    # Thống kê nhanh Base Specs
    specs_with_storage = df['base_specs'].apply(lambda x: '"storage": null' not in x).sum()
    specs_with_ram = df['base_specs'].apply(lambda x: '"ram": null' not in x).sum()
    print(f"✓ base_specs built:")
    print(f"  With storage: {specs_with_storage}/{len(df)} ({specs_with_storage/len(df)*100:.1f}%)")
    print(f"  With RAM:     {specs_with_ram}/{len(df)} ({specs_with_ram/len(df)*100:.1f}%)")

    # Step 5: Validate tính toàn vẹn của Identity
    before_validation = len(df)
    df = df.dropna(subset=["standard_name", "source_url"])
    print(f"After NLP validation (Null Identity): {len(df)} ({before_validation - len(df)} dropped)")

    # Step 6: Handle Color (Cột color đã được Spark trích xuất)
    if "color" not in df.columns:
        df["color"] = None

    # Step 7: Remove duplicates (Phòng ngừa URL trùng lặp)
    before_dup = len(df)
    df = df.drop_duplicates(subset=["source_url"], keep="first")
    print(f"After URL deduplication: {len(df)} ({before_dup - len(df)} dropped)")

    print(f"\nTRANSFORM SUMMARY:")
    print(f"  Input:  {initial_count}")
    print(f"  Output: {len(df)}")
    print(f"  Loss:   {initial_count - len(df)} ({(initial_count - len(df))/initial_count*100:.1f}%)")
    
    return df

# --- 3. LOAD ---

def get_engine():
    return create_engine(MYSQL_URI)

def sync_products_master(df: pd.DataFrame, engine):
    """
    Sync products master table
    """
    unique_products = df[
        ['standard_name', 'brand', 'model_series', 'category', 'base_specs']
    ].drop_duplicates(subset=['standard_name'])

    if unique_products.empty: 
        return {}

    print(f"\n--- Syncing Products Master ---")
    print(f"Processing {len(unique_products)} unique models...")

    with engine.connect() as conn:
        existing_db = pd.read_sql("SELECT product_id, name FROM products", conn)
    
    product_map = dict(zip(existing_db['name'], existing_db['product_id']))
    new_records = []

    for _, row in unique_products.iterrows():
        p_name = row['standard_name']
        if p_name not in product_map:
            new_id = str(uuid.uuid4())
            new_records.append({
                'product_id': new_id, 'name': p_name, 'brand': row['brand'],
                'model_series': row['model_series'], 'category': row['category'],
                'base_specs': row['base_specs']
            })
            product_map[p_name] = new_id
    
    if new_records:
        print(f"✓ Registering {len(new_records)} NEW products...")
        with engine.begin() as conn:
            stmt = text("""
                INSERT INTO products (product_id, name, brand, model_series, category, base_specs, created_at)
                VALUES (:product_id, :name, :brand, :model_series, :category, :base_specs, NOW())
            """)
            for i in range(0, len(new_records), BATCH_SIZE):
                conn.execute(stmt, new_records[i:i+BATCH_SIZE])
    else:
        print(f"All products already exist in database.")
    
    return product_map

def _nan_to_none(val):
    """Chuyển float NaN → None để tránh lỗi 'nan can not be used with MySQL'"""
    if val is None:
        return None
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    return val


def load_listings_and_history(df: pd.DataFrame, product_map, engine):
    """
    Load listings và tính toán Price History ĐÚNG theo product_id + date
    """
    df['product_id'] = df['standard_name'].map(product_map)
    valid_df = df.dropna(subset=['product_id'])
    
    print(f"\n" + "="*60)
    print("STEP 3: LOAD TO MYSQL")
    print("="*60)
    print(f"Loading {len(valid_df)} listings...")

    listing_records = []

    for _, row in valid_df.iterrows():
        l_id = str(uuid.uuid4())
        
        listing_records.append({
            'listing_id': l_id,
            'product_id': row['product_id'],
            'source_url': row['source_url'],
            'platform': row['platform'],
            'price': row['price_vnd'],
            'original_price': row['price_yen'],
            'currency': 'VND',
            'condition_rank': _nan_to_none(row.get('condition_rank')),
            'color': _nan_to_none(row.get('color')),
            'description': _nan_to_none(row.get('description')) or '',
            
            # Battery
            'battery_health': int(row['battery_percentage']) if pd.notna(row.get('battery_percentage')) else None,
            'battery_percentage': int(row['battery_percentage']) if pd.notna(row.get('battery_percentage')) else None,
            'battery_status': _nan_to_none(row.get('battery_status')),
            'battery_replaced': bool(row.get('battery_replaced', False)),
            
            # Accessories
            'has_box': bool(row.get('has_box', False)),
            'has_charger': bool(row.get('has_charger', False)),
            'has_cable': bool(row.get('has_cable', False)),
            'has_earphones': bool(row.get('has_earphones', False)),
            
            # SIM
            'is_sim_free': bool(row.get('is_sim_free', False)),
            'network_restriction': _nan_to_none(row.get('network_restriction')),
            
            # Physical Condition
            'screen_condition': _nan_to_none(row.get('screen_condition')),
            'body_condition': _nan_to_none(row.get('body_condition')),
            'has_scratches': bool(row.get('has_scratches', False)),
            'has_damage': bool(row.get('has_damage', False)),
            
            # Functional
            'fully_functional': bool(row.get('fully_functional', True)),
            'has_issues': bool(row.get('has_issues', False)),
            'posted_at': row.get('ingested_at') if pd.notna(row.get('ingested_at')) else None,

        })

    if not listing_records:
        print("Warning: No listings to load.")
        return

    # LOAD LISTINGS - mỗi batch là 1 transaction độc lập
    # Tránh lock timeout do 1 transaction khổng lồ giữ lock quá lâu
    stmt_listing = text("""
            INSERT INTO active_listings 
            (listing_id, product_id, source_url, platform, price, original_price, currency, 
             condition_rank, color, description,
             battery_health, battery_percentage, battery_status, battery_replaced,
             has_box, has_charger, has_cable, has_earphones,
             is_sim_free, network_restriction,
             screen_condition, body_condition, has_scratches, has_damage,
             fully_functional, has_issues,
             posted_at, last_updated)
            VALUES 
            (:listing_id, :product_id, :source_url, :platform, :price, :original_price, :currency,
             :condition_rank, :color, :description,
             :battery_health, :battery_percentage, :battery_status, :battery_replaced,
             :has_box, :has_charger, :has_cable, :has_earphones,
             :is_sim_free, :network_restriction,
             :screen_condition, :body_condition, :has_scratches, :has_damage,
             :fully_functional, :has_issues,
             :posted_at, NOW())
            ON DUPLICATE KEY UPDATE 
                price = VALUES(price),
                original_price = VALUES(original_price),
                condition_rank = VALUES(condition_rank),
                color = VALUES(color),
                description = VALUES(description),
                battery_health = VALUES(battery_health),
                battery_percentage = VALUES(battery_percentage),
                battery_status = VALUES(battery_status),
                has_box = VALUES(has_box),
                has_charger = VALUES(has_charger),
                is_sim_free = VALUES(is_sim_free),
                screen_condition = VALUES(screen_condition),
                body_condition = VALUES(body_condition),
                has_scratches = VALUES(has_scratches),
                fully_functional = VALUES(fully_functional),
                last_updated = NOW()
    """)

    total_batches = (len(listing_records) - 1) // BATCH_SIZE + 1
    failed_batches = 0
    succeeded_count = 0

    for i in range(0, len(listing_records), BATCH_SIZE):
        batch_num = i // BATCH_SIZE + 1
        batch = listing_records[i:i + BATCH_SIZE]
        try:
            # Mỗi batch = 1 transaction riêng: commit/rollback độc lập, không giữ lock lâu
            with engine.begin() as conn:
                conn.execute(stmt_listing, batch)
            succeeded_count += len(batch)
            print(f"   Saved batch {batch_num}/{total_batches} ({len(batch)} rows)")
        except Exception as e:
            failed_batches += 1
            print(f"   [WARN] Batch {batch_num}/{total_batches} failed: {type(e).__name__}: {e}")

    print(f"Loaded {succeeded_count:,} listings. ({failed_batches} batches failed)")

    # AGGREGATE & LOAD PRICE HISTORY
    print("\n--- Aggregating Price History ---")
    
    # Group by product_id để tính toán thống kê
    history_aggregated = valid_df.groupby('product_id').agg({
        'price_vnd': ['mean', 'min', 'max', 'count'],
        'price_yen': 'mean'
    }).reset_index()
    
    history_aggregated.columns = ['product_id', 'avg_price', 'min_price', 'max_price', 'listing_count', 'avg_price_yen']
    
    print(f"Aggregated price history for {len(history_aggregated)} products.")
    
    history_records = []
    for _, row in history_aggregated.iterrows():
        history_records.append({
            'history_id': str(uuid.uuid4()),
            'product_id': row['product_id'],
            'avg_price': float(row['avg_price']),
            'original_price': float(row['avg_price_yen']),
            'min_price': float(row['min_price']),
            'max_price': float(row['max_price']),
            'listing_count': int(row['listing_count'])
        })
    
    # INSERT với UPSERT logic
    with engine.begin() as conn:
        stmt_history = text("""
            INSERT INTO price_history
            (history_id, product_id, record_date, avg_price, original_price, min_price, max_price, listing_count)
            VALUES (:history_id, :product_id, CURDATE(), :avg_price, :original_price, :min_price, :max_price, :listing_count)
            ON DUPLICATE KEY UPDATE 
                avg_price = (avg_price * listing_count + VALUES(avg_price) * VALUES(listing_count)) / (listing_count + VALUES(listing_count)),
                original_price = (original_price * listing_count + VALUES(original_price) * VALUES(listing_count)) / (listing_count + VALUES(listing_count)),
                min_price = LEAST(min_price, VALUES(min_price)),
                max_price = GREATEST(max_price, VALUES(max_price)),
                listing_count = listing_count + VALUES(listing_count)
        """)
        
        try:
            conn.execute(stmt_history, history_records)
            print(f"Updated price history for {len(history_records)} products.")
        except Exception as e:
            print(f"Error updating price history: {e}")
            print("Hint: Make sure price_history table has UNIQUE constraint on (product_id, record_date)")

    print("All data loaded successfully.")

# etl.py - PREDICT CHO PRODUCTS, KHÔNG PHẢI LISTINGS

# Sau khi load listings xong, AGGREGATE và PREDICT cho products
def predict_product_prices(engine):
    """
    Predict giá cho PRODUCTS dựa trên aggregate features từ listings
    """
    print("\n" + "="*60)
    print("STEP 4: ML PRICE PREDICTION FOR PRODUCTS")
    print("="*60)
    
    if price_predictor is None:
        print("⚠️ ML Predictor not available. Skipping.")
        return
    
    try:
        # Get model version
        model_version = "smart_v1.0"
        if hasattr(price_predictor, 'train_stats_') and price_predictor.train_stats_:
            test_r2 = price_predictor.train_stats_.get('test_r2', 0)
            model_version = f"smart_v1.0_r2_{test_r2:.3f}"
        
        # Load products cần predict
        with engine.connect() as conn:
            products_df = pd.read_sql("""
                SELECT 
                    p.product_id,
                    p.name,
                    p.brand,
                    p.model_series,
                    p.base_specs
                FROM products p
                WHERE NOT EXISTS (
                    SELECT 1 FROM price_forecasts pf
                    WHERE pf.product_id = p.product_id
                    AND pf.forecast_date = CURDATE()
                )
            """, conn)
        products_df['model_line'] = products_df['model_series'].fillna('')
        products_df['model_number'] = ''
        products_df['variant'] = ''
        
        if products_df.empty:
            print("✓ All products have today's forecast already.")
            return
        
        print(f"Predicting prices for {len(products_df)} products...")
        
        # ===== BƯỚC MỚI: AGGREGATE FEATURES TỪ LISTINGS =====
        print("\nAggregating features from active listings...")
        
        with engine.connect() as conn:
            listings_df = pd.read_sql("""
                SELECT 
                    al.product_id,
                    al.condition_rank AS cond_rank,
                    al.platform,
                    al.battery_percentage,
                    al.battery_status,
                    al.has_box,
                    al.has_charger,
                    al.is_sim_free,
                    al.fully_functional,
                    al.has_scratches,
                    al.has_damage,
                    al.has_issues,
                    al.screen_condition,
                    al.body_condition
                FROM active_listings al
                WHERE al.product_id IN %(product_ids)s
            """, conn, params={'product_ids': tuple(products_df['product_id'].tolist())})

        print(f"Loaded {len(listings_df)} listings with features")

        # Aggregate features per product (KHÔNG CẦN extract nữa - đã có sẵn trong MySQL)
        if not listings_df.empty:
            print("Aggregating features per product...")
            
            # Numeric: MEAN
            numeric_agg = listings_df.groupby('product_id').agg({
                'battery_percentage': 'mean',
            }).reset_index()
            
            # Boolean: MODE
            bool_cols = ['has_box', 'has_charger', 'is_sim_free', 'fully_functional',
                        'has_scratches', 'has_damage', 'has_issues']
            bool_agg = listings_df.groupby('product_id')[bool_cols].agg(
                lambda x: x.mode()[0] if len(x.mode()) > 0 else False
            ).reset_index()
            
            # Categorical: MODE
            cat_agg = listings_df.groupby('product_id').agg({
                'screen_condition': lambda x: x.mode()[0] if len(x.mode()) > 0 else 'clean',
                'body_condition': lambda x: x.mode()[0] if len(x.mode()) > 0 else 'good',
                'cond_rank': lambda x: x.mode()[0] if len(x.mode()) > 0 else 'B',
                'platform': lambda x: x.mode()[0] if len(x.mode()) > 0 else 'Mercari'
            }).reset_index()
            
            # Merge
            agg_features = numeric_agg.merge(bool_agg, on='product_id', how='left')
            agg_features = agg_features.merge(cat_agg, on='product_id', how='left')
            products_df = products_df.merge(agg_features, on='product_id', how='left')
            
            print(f"✓ Aggregated features for {len(agg_features)} products")
        
        # Fill missing với defaults (dùng .get() style để tránh KeyError khi listings_df rỗng)
        products_df['battery_percentage'] = products_df.get('battery_percentage', 80.0)
        if 'battery_percentage' not in products_df.columns:
            products_df['battery_percentage'] = 80.0
        products_df['battery_percentage'] = products_df['battery_percentage'].fillna(80.0)
        products_df['condition'] = products_df.get('cond_rank', 'B')
        if 'cond_rank' not in products_df.columns:
            products_df['condition'] = 'B'
        else:
            products_df['condition'] = products_df['cond_rank'].fillna('B')
        products_df['platform'] = products_df['platform'].fillna('Mercari') if 'platform' in products_df.columns else 'Mercari'
        products_df['screen_condition'] = products_df['screen_condition'].fillna('clean') if 'screen_condition' in products_df.columns else 'clean'
        products_df['body_condition'] = products_df['body_condition'].fillna('good') if 'body_condition' in products_df.columns else 'good'
        
        bool_defaults = {
            'has_box': True, 'has_charger': True, 'is_sim_free': True,
            'fully_functional': True, 'has_scratches': False,
            'has_damage': False, 'has_issues': False
        }
        for col, default in bool_defaults.items():
            if col not in products_df.columns:
                products_df[col] = default
            else:
                products_df[col] = products_df[col].fillna(default)
        
        # Parse specs
        products_df['specs_dict'] = products_df['base_specs'].apply(
            lambda x: json.loads(x) if x else {}
        )
        products_df['storage'] = products_df['specs_dict'].apply(lambda x: x.get('storage'))
        products_df['ram'] = products_df['specs_dict'].apply(lambda x: x.get('ram'))
        
        # ===== PREDICT VỚI REAL FEATURES =====
        print("\nPredicting with aggregated features...")
        predictions_yen = price_predictor.predict(products_df)
        predictions_vnd = predictions_yen * YEN_TO_VND_RATE
        
        # Calculate confidence
        if hasattr(price_predictor, 'train_stats_') and price_predictor.train_stats_:
            base_confidence = price_predictor.train_stats_.get('test_r2', 0.5) * 100
        else:
            base_confidence = 50.0
        
        # Insert into price_forecasts
        forecast_records = []
        for idx, row in products_df.iterrows():
            forecast_records.append({
                'forecast_id': str(uuid.uuid4()),
                'product_id': row['product_id'],
                'forecast_date': datetime.now().date(),
                'predicted_price': float(predictions_vnd[idx]),
                'confidence_score': float(base_confidence),
                'model_version': model_version
            })
        
        # Batch insert
        with engine.begin() as conn:
            stmt = text("""
                INSERT INTO price_forecasts 
                (forecast_id, product_id, forecast_date, predicted_price, confidence_score, model_version, created_at)
                VALUES (:forecast_id, :product_id, :forecast_date, :predicted_price, :confidence_score, :model_version, NOW())
            """)
            
            for i in range(0, len(forecast_records), BATCH_SIZE):
                batch = forecast_records[i:i+BATCH_SIZE]
                conn.execute(stmt, batch)
                print(f"   Saved batch {i//BATCH_SIZE + 1}/{(len(forecast_records)-1)//BATCH_SIZE + 1}")
        
        print(f"✓ Saved {len(forecast_records)} price forecasts")
        
        # Show sample
        print(f"\n📊 Sample Predictions (Model: {model_version}, Confidence: {base_confidence:.1f}%):")
        sample = products_df.head(3)
        for idx, row in sample.iterrows():
            pred_yen = predictions_yen[idx]
            pred_vnd = predictions_vnd[idx]
            battery = row.get('battery_percentage', 'N/A')
            has_box = '✓' if row.get('has_box') else '✗'
            print(f"   {row['name'][:40]:<40} | Battery: {battery:>5}% | Box: {has_box} → ¥{pred_yen:>8,.0f}")
        
    except Exception as e:
        print(f"❌ Error predicting product prices: {e}")
        import traceback
        traceback.print_exc()

def mark_dropped_in_mongo(urls):
    """
    Đánh dấu các record bị loại (rác, lỗi giá, không có brand, trùng lặp...) là đã xử lý
    để lần sau hàm extract() không kéo lên lại làm tốn thời gian.
    """
    if not urls: 
        return
    try:
        print(f"Marking {len(urls)} dropped records as 'dropped_etl' in Mongo...")
        client = pymongo.MongoClient(MONGO_URI)
        col = client[DB_NAME][COLLECTION_NAME]
        col.update_many(
            {"link": {"$in": urls}},
            {"$set": {"processed": True, "processed_at": datetime.now(), "status": "dropped_etl"}}
        )
    except Exception as e:
        print(f"Warning: Could not mark dropped documents in Mongo: {e}")


def main():
    print("\n" + "="*60)
    print("  ETL PIPELINE - WITH ML PREDICTION")
    print("="*60)
    
    engine = get_engine()
    
    # STEP 1: Extract từ MongoDB
    df_raw = extract()
    if df_raw.empty:
        print("\nNo new data. Checking if products need price updates...")
        # Vẫn chạy prediction cho products cũ
        predict_product_prices(engine)
        return
    
    # Lưu lại toàn bộ URL gốc từ MongoDB để đối chiếu ở bước cuối
    all_incoming_urls = df_raw['link'].tolist() if 'link' in df_raw.columns else []
    
    # STEP 2: Transform
    df_clean = transform(df_raw)
    
    if df_clean.empty:
        print("\nNo valid data after transformation.")
        # Toàn bộ data bị loại -> Đánh dấu dropped hết
        mark_dropped_in_mongo(all_incoming_urls)
        predict_product_prices(engine)
        return
    
    # STEP 2.5: Check MySQL duplicates
    df_clean = check_duplicates_in_mysql(df_clean, engine)
    
    if df_clean.empty:
        print("\nAll data already exists in MySQL.")
        # Bị trùng hết -> Đánh dấu dropped hết
        mark_dropped_in_mongo(all_incoming_urls)
        predict_product_prices(engine)
        return
    
    # Lấy danh sách các URL "SỐNG SÓT" qua tất cả các màng lọc
    successful_urls = df_clean['source_url'].tolist()
    
    # STEP 3: Load listings
    product_map_uuid = sync_products_master(df_clean, engine)
    load_listings_and_history(df_clean, product_map_uuid, engine)
    
    # STEP 4: Predict prices for PRODUCTS (not listings!)
    predict_product_prices(engine)
    
    # STEP 5: Update MongoDB status
    print("\n--- Updating MongoDB Status ---")
    # 5.1: Đánh dấu 'loaded_mysql' cho các tin hợp lệ
    update_mongo_status(successful_urls)
    
    # 5.2: Đánh dấu 'dropped_etl' cho các tin rác bị rơi rụng giữa đường
    dropped_urls = list(set(all_incoming_urls) - set(successful_urls))
    if dropped_urls:
        mark_dropped_in_mongo(dropped_urls)
    
    print("\n" + "="*60)
    print("  ETL PIPELINE COMPLETED SUCCESSFULLY")
    print("="*60)

if __name__ == "__main__":
    main()