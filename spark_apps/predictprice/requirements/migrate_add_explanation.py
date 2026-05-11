import sqlite3
import os

def migrate():
    # Thay đổi đường dẫn phù hợp với Windows
    # Ví dụ nếu database ở thư mục gốc project:
    db_path = r'c:/project code/Predict Price/spark_apps/predictprice/priceprediction.db'
    
    # Hoặc tìm tự động:
    # db_path = os.path.join(os.path.dirname(__file__), '..', 'priceprediction.db')
    # db_path = os.path.abspath(db_path)
    
    print(f"Attempting to connect to: {db_path}")
    
    if not os.path.exists(db_path):
        print(f"⚠️ Database file not found!")
        print(f"Please check if priceprediction.db exists at: {db_path}")
        return
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Kiểm tra xem cột explanation đã tồn tại chưa
    cursor.execute("PRAGMA table_info(product_catalog_items)")
    columns = [column[1] for column in cursor.fetchall()]
    
    print(f"Current columns: {columns}")
    
    if 'explanation' not in columns:
        print("Adding 'explanation' column...")
        cursor.execute('''
            ALTER TABLE product_catalog_items 
            ADD COLUMN explanation TEXT;
        ''')
        conn.commit()
        print("✅ Column 'explanation' added successfully!")
    else:
        print("⚠️ Column 'explanation' already exists!")
    
    conn.close()

if __name__ == "__main__":
    migrate()