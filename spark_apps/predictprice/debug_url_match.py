import pymongo, os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from dotenv import load_dotenv
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
import config as cfg

load_dotenv()
c = pymongo.MongoClient(cfg.MONGO_URI)
col = c[cfg.DB_NAME][cfg.COLLECTION_NAME]

docs = list(col.find({"status": "loaded_mysql"}, {"link": 1}).limit(5))
mongo_links = [d['link'] for d in docs]

engine = create_engine(f"mysql+pymysql://{cfg.MYSQL_USER}:{cfg.MYSQL_PASSWORD}@{cfg.MYSQL_HOST}:{cfg.MYSQL_PORT}/{cfg.MYSQL_DB}")
with engine.connect() as conn:
    for link in mongo_links:
        result = conn.execute(text("SELECT source_url, nlp_layer, bert_audited_at FROM active_listings WHERE source_url = :url"), {'url': link})
        row = result.fetchone()
        print(f"Mongo link: {link[:80]}")
        print(f"MySQL row:  {row}")
        print()

c.close()