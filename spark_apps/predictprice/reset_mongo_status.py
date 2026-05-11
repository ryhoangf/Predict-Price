"""Reset docs sai status 'loaded_mysql' về 'extracted_layer2' để ETL chạy lại."""
import pymongo
import sys, os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
try:
    import config as cfg
    MONGO_URI = cfg.MONGO_URI
    DB_NAME = cfg.DB_NAME
    COLLECTION_NAME = cfg.COLLECTION_NAME
except Exception:
    MONGO_URI = "mongodb://localhost:27017"
    DB_NAME = "ivaluate_datalake"
    COLLECTION_NAME = "raw_items"

client = pymongo.MongoClient(MONGO_URI)
col = client[DB_NAME][COLLECTION_NAME]

result = col.update_many(
    {"status": {"$in": ["loaded_mysql", "dropped_etl"]}},
    {"$set": {"status": "extracted_layer2"}}
)
print(f"Reset {result.modified_count} docs: loaded_mysql -> extracted_layer2")

for s in ["extracted_layer2", "loaded_mysql", "dropped_etl"]:
    print(f"  {s}: {col.count_documents({'status': s})}")

client.close()
print("Done.")
