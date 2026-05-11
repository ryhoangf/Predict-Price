import sys
import os

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
sys.path.insert(0, project_root)

predictprice_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, predictprice_path)

import pandas as pd
import numpy as np
import pymongo

from ml_models.smart_price_predictor import SmartPricePredictor, EnsemblePricePredictor

# Import from parent package
from NLP.item_explanation import ItemExplanationExtractor
from spark_apps.predictprice.NLP.title_nlp import PhoneInfoExtractor
import config as cfg

def load_from_mongodb():
    """
    Load raw data từ MongoDB
    """
    print("\n" + "="*80)
    print("LOAD DATA FROM MONGODB")
    print("="*80)
    
    print(f"Connecting to MongoDB: {cfg.MONGO_URI}")
    print(f"Database: {cfg.DB_NAME}, Collection: {cfg.COLLECTION_NAME}")
    
    try:
        client = pymongo.MongoClient(
            cfg.MONGO_URI,
            serverSelectionTimeoutMS=10000,
            connectTimeoutMS=10000
        )
        
        db = client[cfg.DB_NAME]
        col = db[cfg.COLLECTION_NAME]
        
        # Count total documents
        total_count = col.count_documents({})
        print(f"✓ Total documents in MongoDB: {total_count}")
        
        # Load all data
        print("Loading all documents...")
        cursor = col.find({})
        data = list(cursor)
        
        client.close()
        
        if not data:
            raise Exception("No data found in MongoDB!")
        
        # Convert to DataFrame
        df = pd.DataFrame(data)
        print(f"✓ Loaded {len(df)} records from MongoDB")
        
        # Check required columns
        required_cols = ['name', 'price', 'explanation']
        missing_cols = [col for col in required_cols if col not in df.columns]
        
        if missing_cols:
            print(f"\n⚠️ Missing columns: {missing_cols}")
            print(f"Available columns: {df.columns.tolist()}")
            raise Exception(f"Required columns missing: {missing_cols}")
        
        # Show sample
        print(f"\n📋 Sample data:")
        print(df[['name', 'price', 'source']].head(3))
        
        return df
        
    except pymongo.errors.ConnectionFailure as e:
        raise Exception(f"MongoDB connection failed: {e}")
    except Exception as e:
        raise Exception(f"Error loading from MongoDB: {e}")

def load_and_prepare_data(use_mongodb=True, csv_path=None):
    """
    Bước 1: Load raw data và extract features
    """
    print("\n" + "="*80)
    print("BƯỚC 1: LOAD & EXTRACT DATA")
    print("="*80)
    
    # Load raw data
    if use_mongodb:
        df = load_from_mongodb()
    else:
        if csv_path is None:
            csv_path = os.path.join(project_root, 'book_data', 'all_items.csv')
        print(f"Loading data from {csv_path}...")
        df = pd.read_csv(csv_path)
        print(f"✓ Loaded {len(df)} records")
    
    # Ensure 'condition' column exists
    if 'condition' not in df.columns:
        # Try to map from other columns
        if 'item_condition' in df.columns:
            df['condition'] = df['item_condition']
        elif 'grade' in df.columns:
            df['condition'] = df['grade']
        else:
            # Default condition
            print("⚠️ No condition column found, setting default to 'B'")
            df['condition'] = 'B'
    
    # Extract từ Title
    print("\nExtracting features from TITLE...")
    title_extractor = PhoneInfoExtractor()
    
    title_features = []
    for idx, row in df.iterrows():
        info = title_extractor.extract_all_info(row['name'])
        title_features.append(info)
        
        if (idx + 1) % 500 == 0:
            print(f"  Processed {idx + 1}/{len(df)} titles...")
    
    df_title = pd.DataFrame(title_features)
    print(f"✓ Extracted title features: {df_title.columns.tolist()[:10]}...")
    
    # Extract từ Explanation
    print("\nExtracting features from EXPLANATION...")
    explanation_extractor = ItemExplanationExtractor()
    
    # Fill NA explanations
    df['explanation'] = df['explanation'].fillna('')
    
    df_explanation = explanation_extractor.process_dataframe(
        df.copy(), 
        explanation_column='explanation'
    )
    
    print(f"✓ Extracted explanation features")
    
    # Merge all features
    print("\nMerging features...")
    df_final = df[['name', 'price', 'condition', 'source']].copy()
    
    # Add title features
    df_final['brand'] = df_title['brand']
    df_final['model_line'] = df_title['model_line']
    df_final['model_number'] = df_title['model_number']
    df_final['variant'] = df_title['variant']
    df_final['capacity'] = df_title['capacity']
    df_final['color'] = df_title['color']
    
    def determine_ecosystem(brand):
        if pd.isna(brand):
            return 'Android'  # Tạm coi các hãng không rõ là Android
        if str(brand).strip().lower() == 'apple':
            return 'Apple'
        return 'Android'
        
    df_final['ecosystem'] = df_final['brand'].apply(determine_ecosystem)
    
    # Add explanation features
    explanation_cols = [
        'battery_percentage', 'battery_status', 'battery_replaced',
        'storage', 'ram',
        'has_box', 'has_charger', 'has_cable', 'has_earphones', 
        'accessories_complete',
        'is_sim_free', 'network_restriction',
        'has_scratches', 'screen_condition', 'body_condition', 
        'camera_condition', 'has_damage',
        'face_id_working', 'touch_id_working', 
        'fully_functional', 'has_issues'
    ]
    
    for col in explanation_cols:
        if col in df_explanation.columns:
            df_final[col] = df_explanation[col]
    
    # Add platform from source
    df_final['platform'] = df_final['source']
    
    print(f"✓ Final dataset: {len(df_final)} records × {len(df_final.columns)} features")
    return df_final

def clean_and_filter_data(df):
    """
    Bước 2: Clean và filter data
    """
    print("\n" + "="*80)
    print("BƯỚC 2: CLEAN & FILTER DATA")
    print("="*80)
    
    initial_count = len(df)
    print(f"Initial records: {initial_count}")
    
    # DEBUG: Check raw price format
    print(f"\n🔍 RAW PRICE SAMPLES:")
    print(df['price'].head(10).tolist())
    
    # 0. Clean price string (remove ¥, 円, YEN, commas, spaces)
    print("\n0️⃣ Cleaning price strings...")
    df['price'] = df['price'].astype(str)
    df['price'] = df['price'].str.replace('¥', '', regex=False)
    df['price'] = df['price'].str.replace('円', '', regex=False)
    df['price'] = df['price'].str.replace('YEN', '', regex=False)
    df['price'] = df['price'].str.replace('yen', '', regex=False)
    df['price'] = df['price'].str.replace(',', '', regex=False)
    df['price'] = df['price'].str.replace(' ', '', regex=False)
    df['price'] = df['price'].str.strip()
    
    print(f"After cleaning: {df['price'].head(10).tolist()}")
    
    # Convert to numeric
    df['price'] = pd.to_numeric(df['price'], errors='coerce')
    
    # DEBUG: Check data after price conversion
    print(f"\n🔍 DEBUG - After price conversion:")
    print(df[['name', 'price', 'brand']].head(5))
    print(f"Valid prices: {df['price'].notna().sum()} / {len(df)}")
    print(f"Price stats: min={df['price'].min():.0f}, max={df['price'].max():.0f}, mean={df['price'].mean():.0f}")
    
    # 1. Remove missing price
    before = len(df)
    df = df.dropna(subset=['price'])
    removed = before - len(df)
    print(f"\n1️⃣ Removed {removed} records with missing/invalid price (remaining: {len(df)})")
    
    if len(df) == 0:
        print("\n❌ CRITICAL: All prices are invalid!")
        return df
    
    # 2. Filter price range (5,000 - 300,000 yên)
    before = len(df)
    print(f"\n🔍 Price range: {df['price'].min():.0f} - {df['price'].max():.0f}")
    df = df[(df['price'] >= 5000) & (df['price'] <= 300000)]
    removed = before - len(df)
    print(f"2️⃣ Removed {removed} records outside 5K-300K¥ range (remaining: {len(df)})")
    
    if len(df) == 0:
        print("\n❌ CRITICAL: All prices outside range!")
        return df
    
    # 3. Check brand
    before = len(df)
    print(f"\n🔍 Brand analysis:")
    print(f"  Null brands: {df['brand'].isna().sum()} / {len(df)}")
    print(f"  Top brands: {df['brand'].value_counts().head()}")
    
    # If too many missing brands, don't filter them out yet
    missing_brand_pct = df['brand'].isna().sum() / len(df) * 100
    if missing_brand_pct > 50:
        print(f"⚠️ WARNING: {missing_brand_pct:.1f}% brands are missing!")
        print("   Keeping records without brand for now...")
        # Set default brand for missing ones
        df['brand'] = df['brand'].fillna('Unknown')
    else:
        df = df.dropna(subset=['brand'])
        removed = before - len(df)
        print(f"3️⃣ Removed {removed} records with missing brand (remaining: {len(df)})")
    
    if len(df) == 0:
        print("\n❌ CRITICAL: All brands are missing!")
        return df
    
    # 4. Remove duplicates
    before = len(df)
    df = df.drop_duplicates(subset=['name', 'price', 'source'])
    removed = before - len(df)
    print(f"4️⃣ Removed {removed} duplicate records (remaining: {len(df)})")
    
    print(f"\n✅ Final clean dataset: {len(df)} records ({len(df)/initial_count*100:.1f}%)")
    
    return df

def analyze_data_quality(df):
    """
    Bước 3: Analyze data quality
    """
    print("\n" + "="*80)
    print("BƯỚC 3: DATA QUALITY ANALYSIS")
    print("="*80)
    
    total = len(df)
    
    print("\n📊 FEATURE COVERAGE:")
    print(f"{'Feature':<30} {'Count':<10} {'Coverage':<10}")
    print("-" * 50)
    
    key_features = [
        'brand', 'model_line', 'storage', 'ram',
        'battery_percentage', 'condition',
        'has_box', 'screen_condition', 'is_sim_free'
    ]
    
    for feature in key_features:
        if feature in df.columns:
            if df[feature].dtype == 'bool':
                count = df[feature].sum()
            else:
                count = df[feature].notna().sum()
            coverage = count / total * 100
            print(f"{feature:<30} {count:<10} {coverage:>6.1f}%")
    
    print("\n📈 PRICE STATISTICS:")
    print(f"  Mean:   ¥{df['price'].mean():>10,.0f}")
    print(f"  Median: ¥{df['price'].median():>10,.0f}")
    print(f"  Min:    ¥{df['price'].min():>10,.0f}")
    print(f"  Max:    ¥{df['price'].max():>10,.0f}")
    print(f"  Std:    ¥{df['price'].std():>10,.0f}")
    
    print("\n📱 TOP BRANDS:")
    print(df['brand'].value_counts().head(10))
    
    print("\n🏪 PLATFORM DISTRIBUTION:")
    print(df['platform'].value_counts())
    
    print("\n⭐ CONDITION DISTRIBUTION:")
    print(df['condition'].value_counts())

def train_model(df):
    """
    Bước 4: Train model
    """
    print("\n" + "="*80)
    print("BƯỚC 4: TRAIN ENSEMBLE MODEL")
    print("="*80)
    
    # Initialize model
    predictor = EnsemblePricePredictor()
    
    # Train
    predictor.train(df, target_col='price', test_size=0.2)
    
    # Save
    model_path = os.path.join(predictprice_path, 'models', 'ensemble_price_predictor.pkl')
    predictor.save(model_path)
    
    return predictor

def evaluate_model(predictor, df):
    """
    Bước 5: Evaluate model với examples
    """
    print("\n" + "="*80)
    print("BƯỚC 5: MODEL EVALUATION")
    print("="*80)
    
    # Random sample
    sample = df.sample(min(10, len(df)), random_state=42)
    
    # Predict
    predictions = predictor.predict(sample)
    
    # Show results
    print("\n📋 SAMPLE PREDICTIONS:\n")
    
    for idx, (_, row) in enumerate(sample.iterrows()):
        actual = row['price']
        predicted = predictions[idx]
        error = abs(actual - predicted) / actual * 100
        
        print(f"\n[{idx+1}] {row['name'][:60]}")
        print(f"    Brand: {row['brand']} | Model: {row['model_line']}")
        print(f"    Storage: {row.get('storage', 'N/A')} | Condition: {row['condition']}")
        print(f"    Battery: {row.get('battery_percentage', 'N/A')}%")
        print(f"    Actual:    ¥{actual:>10,.0f}")
        print(f"    Predicted: ¥{predicted:>10,.0f}")
        print(f"    Error:     {error:>9.1f}%")
        
        if error < 10:
            print(f"    Rating: ✅ Excellent")
        elif error < 20:
            print(f"    Rating: ✓ Good")
        else:
            print(f"    Rating: ⚠️ Fair")

def save_processed_data(df, output_path=None):
    """
    Bước 6: Save processed data for future use
    """
    print("\n" + "="*80)
    print("BƯỚC 6: SAVE PROCESSED DATA")
    print("="*80)
    
    if output_path is None:
        output_path = os.path.join(project_root, 'book_data', 'training_data.csv')
    
    # Ensure directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"✓ Saved to {output_path}")

def main():
    """
    Main training pipeline
    """
    print("\n" + "="*80)
    print("  🤖 SMART PRICE PREDICTOR TRAINING PIPELINE")
    print("="*80)
    
    try:
        # Bước 1: Load & Extract (từ MongoDB)
        df = load_and_prepare_data(use_mongodb=True)
        
        # Bước 2: Clean & Filter
        df_clean = clean_and_filter_data(df)
        
        # Bước 3: Analyze
        analyze_data_quality(df_clean)
        
        # Bước 4: Train
        predictor = train_model(df_clean)
        
        # Bước 5: Evaluate
        evaluate_model(predictor, df_clean)
        
        # Bước 6: Save
        save_processed_data(df_clean)
        
        print("\n" + "="*80)
        print("  ✅ TRAINING COMPLETED SUCCESSFULLY!")
        print("="*80)
        print("\n📁 Output files:")
        print(f"  - {os.path.join(predictprice_path, 'models', 'ensemble_price_predictor.pkl')} (trained model)")
        print(f"  - {os.path.join(project_root, 'book_data', 'training_data.csv')} (processed data)")
        print("\n🚀 Next steps:")
        print("  1. Test model: python test_ensemble_predictor.py")
        print("  2. Integrate to ETL: Update etl.py")
        print("  3. Deploy API: Update web_app.py")
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()