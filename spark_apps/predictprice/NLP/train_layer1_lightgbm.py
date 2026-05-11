import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics import classification_report, confusion_matrix
import lightgbm as lgb
import scipy.sparse as sp
import joblib
import os

def main():
    print("HUẤN LUYỆN LAYER 1: JUNK DETECTION (LightGBM)")

    #Đọc dữ liệu đã gán nhãn
    data_path = 'dataset/junk_labeling_task.csv'
    if not os.path.exists(data_path):
        print(f"Không tìm thấy file {data_path}!")
        return

    print("Đang đọc dữ liệu...")
    df = pd.read_csv(data_path)
    
    #Xóa các dòng bị null nhãn
    df = df.dropna(subset=['is_junk'])
    df['is_junk'] = df['is_junk'].astype(int)

    print(f"Tổng số bản ghi: {len(df)}")
    print(f" - Số lượng Máy thật (0): {len(df[df['is_junk'] == 0])}")
    print(f" - Số lượng Rác (1): {len(df[df['is_junk'] == 1])}")

    #Xử lý Feature
    print("\nĐang trích xuất đặc trưng (Feature Engineering)...")
    
    # Gom Name và Explanation lại thành 1 cục text
    df['name'] = df['name'].fillna('')
    df['explanation'] = df['explanation'].fillna('')
    df['combined_text'] = df['name'] + " " + df['explanation']
    
    df['price'] = pd.to_numeric(df['price'], errors='coerce').fillna(0)

    #TF-IDF Vectorizer
    tfidf = TfidfVectorizer(analyzer='char_wb', ngram_range=(2, 5), max_features=5000)
    
    #Fit và Transform text
    X_text = tfidf.fit_transform(df['combined_text'])
    
    # Ghép cột Price vào ma trận Text (Dùng scipy.sparse để tiết kiệm RAM)
    X_price = df[['price']].values
    X_final = sp.hstack((X_text, X_price), format='csr')
    
    y = df['is_junk'].values

    #hia tập Train / Test
    X_train, X_test, y_train, y_test = train_test_split(X_final, y, test_size=0.2, random_state=42, stratify=y)
    print(f"Chia tập dữ liệu: {X_train.shape[0]} dòng Train, {X_test.shape[0]} dòng Test.")

    #Khởi tạo và Huấn luyện LightGBM
    print("\nĐang huấn luyện mô hình LightGBM...")
    
    model = lgb.LGBMClassifier(
        n_estimators=200,
        learning_rate=0.1,
        max_depth=7,
        class_weight='balanced',
        random_state=42,
        n_jobs=-1
    )
    
    model.fit(X_train, y_train)

    #Đánh giá mô hình (Chấm điểm thi)
    print("\nKẾT QUẢ ĐÁNH GIÁ TRÊN TẬP TEST:")
    y_pred = model.predict(X_test)
    
    print("\nClassification Report:")
    print(classification_report(y_test, y_pred, target_names=["Máy thật (0)", "Rác (1)"]))
    
    print("Confusion Matrix (Ma trận nhầm lẫn):")
    cm = confusion_matrix(y_test, y_pred)
    print(f"Đoán đúng máy thật : {cm[0][0]}")
    print(f"Đoán NHẦM máy thật thành rác: {cm[0][1]}")
    print(f"Đoán NHẦM rác thành máy thật: {cm[1][0]}")
    print(f"Đoán đúng rác : {cm[1][1]}")

    #Xuất Model (Export)
    print("\nĐang xuất mô hình ra file...")
    os.makedirs('models', exist_ok=True)
    
    #Lưu TF-IDF
    tfidf_path = 'models/tfidf_junk_v1.pkl'
    joblib.dump(tfidf, tfidf_path)
    
    #Lưu LightGBM
    lgbm_path = 'models/lgbm_junk_v1.txt'
    model.booster_.save_model(lgbm_path)
    
    print("Đã lưu 2 file:")
    print(f"  1. {tfidf_path} (Trí nhớ từ vựng)")
    print(f"  2. {lgbm_path} (Não bộ AI)")

if __name__ == "__main__":
    main()