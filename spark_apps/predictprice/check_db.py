import sqlite3
import os
import sys

sys.path.append('/opt/spark/apps/predictprice')

DB_PATH_IN_CONTAINER = "/opt/spark/apps/predictprice/priceprediction.db"

print(f"Checking database at: {DB_PATH_IN_CONTAINER}")

if not os.path.exists(DB_PATH_IN_CONTAINER):
    print(f"Error: Database file NOT found at {DB_PATH_IN_CONTAINER}")
    sys.exit(1)

try:
    # Kết nối đến database
    conn = sqlite3.connect(DB_PATH_IN_CONTAINER)
    cursor = conn.cursor()
    print("Successfully connected to the database.")

    # Liệt kê các bảng
    print("\nTables in the database:")
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()

    if tables:
        table_names = [t[0] for t in tables]
        for table_name in table_names:
            print(f"- {table_name}")

        if 'product_catalog_items' in table_names:
             cursor.execute("SELECT COUNT(*) FROM product_catalog_items;")
             count = cursor.fetchone()[0]
             print(f"\nNumber of rows in product_catalog_items: {count}")
        else:
             print("\nTable 'product_catalog_items' not found in the database.")

    else:
        print("\nNo tables found in the database.")

    conn.close()
    print("\nDatabase connection closed.")

except sqlite3.Error as e:
    print(f"\nDatabase error during connection or query: {e}")
    sys.exit(1)
except Exception as e:
    print(f"\nAn unexpected error occurred: {e}")
    sys.exit(1)

sys.exit(0)