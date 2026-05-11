import sqlite3
from sqlalchemy import create_engine

DB_CONN = "sqlite:////opt/spark/apps/predictprice/priceprediction.db"

# Tạo kết nối cơ sở dữ liệu SQLite bằng sqlite3
def get_connection():
    conn = sqlite3.connect('/opt/spark/apps/predictprice/priceprediction.db')
    return conn

# Tạo bảng trong SQLite
def create_table():
    conn = get_connection()
    cursor = conn.cursor()

    # Tạo bảng product_catalog_items
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS product_catalog_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT NOT NULL UNIQUE,
            source TEXT NOT NULL CHECK(source IN ('mercari', 'yahooauction', 'rakuma')),
            name TEXT NOT NULL,
            price_yen NUMERIC(12,2),
            price_vnd NUMERIC(14,0),
            condition TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    ''')

    # Tạo bảng product_catalog_prices_history
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS product_catalog_prices_history (
            history_id INTEGER PRIMARY KEY AUTOINCREMENT,
            item_id INTEGER,
            record_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            price_yen NUMERIC(12,2),
            price_vnd NUMERIC(14,0),
            FOREIGN KEY(item_id) REFERENCES product_catalog_items(id) ON DELETE CASCADE
        );
    ''')

    # # Tạo Trigger tính giá VND từ YEN
    # cursor.execute('''
    #     CREATE TRIGGER IF NOT EXISTS trg_compute_price_vnd
    #     BEFORE INSERT ON product_catalog_items
    #     FOR EACH ROW
    #     BEGIN
    #         UPDATE product_catalog_items
    #         SET price_vnd = ROUND(NEW.price_yen * 180)
    #         WHERE id = NEW.id;
    #     END;
    # ''')

    # Tạo Trigger trg_set_updated_at
    cursor.execute('''
        CREATE TRIGGER IF NOT EXISTS trg_set_updated_at
        BEFORE UPDATE ON product_catalog_items
        FOR EACH ROW
        BEGIN
            UPDATE product_catalog_items
            SET updated_at = CURRENT_TIMESTAMP
            WHERE id = NEW.id;
        END;
    ''')

    # Tạo Trigger trg_log_price_change
    cursor.execute('''
        CREATE TRIGGER IF NOT EXISTS trg_log_price_change
        AFTER UPDATE ON product_catalog_items
        FOR EACH ROW
        BEGIN
            INSERT INTO product_catalog_prices_history (item_id, record_time, price_yen, price_vnd)
            VALUES (OLD.id, CURRENT_TIMESTAMP, NEW.price_yen, NEW.price_vnd);
        END;
    ''')

    conn.commit()
    conn.close()

# Lấy engine cho SQLAlchemy (sử dụng khi cần kết nối qua SQLAlchemy)
def get_engine():
    engine = create_engine(DB_CONN, pool_size=5)
    return engine
