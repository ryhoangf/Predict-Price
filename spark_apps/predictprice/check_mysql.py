"""Quick check MySQL table counts."""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sqlalchemy import create_engine, text
from dotenv import load_dotenv
load_dotenv()
import os

HOST = os.getenv("MYSQL_HOST", "localhost")
PORT = os.getenv("MYSQL_PORT", "3000")
USER = os.getenv("MYSQL_USER", "root")
PASS = os.getenv("MYSQL_PASSWORD", "")
DB   = os.getenv("MYSQL_DB", "ivaluate")
DB_URL = f"mysql+pymysql://{USER}:{PASS}@{HOST}:{PORT}/{DB}"

engine = create_engine(DB_URL)
with engine.connect() as conn:
    for table in ["products", "active_listings", "price_history", "price_forecasts"]:
        try:
            count = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
            print(f"  {table}: {count} rows")
        except Exception as e:
            print(f"  {table}: ERROR - {e}")
