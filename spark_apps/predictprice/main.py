# import sys
# sys.path.append('/opt/spark/apps/predictprice/scrapers')

# import pandas as pd
# from pyspark import SparkContext, SparkConf
# from scrapers.mercari_scraping import scrape_mercari
# from scrapers.rakuma_scraping import scrape_rakuma
# from scrapers.yahooauction_scraping import scrape_yahooauction

# import etl

# def main():
#     conf = SparkConf().setAppName("Data Scraping").setMaster("spark://spark-master:7077")
#     sc = SparkContext(conf=conf)

#     sources = ['mercari', 'rakuma', 'yahooauction']

#     def scrape_and_add_source(source_name):
#         df = pd.DataFrame()
#         if source_name == 'mercari':
#             df = scrape_mercari(end_page=1)
#             df["source"] = "mercari"
#         elif source_name == 'rakuma':
#             df = scrape_rakuma(end_page=1)
#             df["source"] = "rakuma"
#         elif source_name == 'yahooauction':
#             df = scrape_yahooauction(end_page=1)
#             df["source"] = "yahooauction"

#         return df

#     #phân phối danh sách các nguồn cho các worker
#     #số lượng phân vùng (numSlices) có thể đặt bằng số nguồn để mỗi nguồn là một tác vụ
#     all_dfs_rdd = sc.parallelize(sources, numSlices=len(sources)).map(scrape_and_add_source)

#     #collect các dtframe từ các worker về driver
#     all_dfs_list = all_dfs_rdd.collect()

#     #ghép dtframe
#     df_all = pd.concat(all_dfs_list, ignore_index=True)

#     csv_path = "/opt/spark/data/all_items.csv" 
#     df_all.to_csv(csv_path, index=False)
#     print(f"Saved CSV with {len(df_all)} rows → {csv_path}")

#     print("Starting ETL")
#     etl.main()
#     print("All done.")
#     sc.stop()

# if __name__ == "__main__":
#     main()



# # import os
# # import pandas as pd

# # from scrapers.mercari_scraping      import scrape_mercari
# # from scrapers.rakuma_scraping      import scrape_rakuma
# # from scrapers.yahooauction_scraping import scrape_yahooauction

# # import etl

# # def main():
# #     df1 = scrape_mercari(end_page=1)
# #     df2 = scrape_rakuma(end_page=1)
# #     df3 = scrape_yahooauction(end_page=1)

# #     df1["source"] = "mercari"
# #     df2["source"] = "rakuma"
# #     df3["source"] = "yahooauction"

# #     df_all = pd.concat([df1, df2, df3], ignore_index=True)

# #     # Sửa đường dẫn - khớp với etl.py
# #     csv_path = "/opt/spark/data/all_items.csv"
# #     # Tạo thư mục nếu chưa tồn tại
# #     os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    
# #     df_all.to_csv(csv_path, index=False)
# #     print(f"Saved CSV with {len(df_all)} rows → {csv_path}")

# #     print("Starting ETL")
# #     etl.main()
# #     print("All done.")

# # if __name__ == "__main__":
# #     main()

# import sys
# import os

# # Đường dẫn cần thiết để import modules trong môi trường Spark
# sys.path.append('/opt/spark/apps/predictprice')

# from pyspark import SparkContext, SparkConf

# # QUAN TRỌNG: Định nghĩa hàm xử lý bên trong để Spark Worker có thể serialize
# def process_source_on_worker(source_name):
#     """
#     Hàm này sẽ chạy hoàn toàn trên Worker Node
#     """
#     import pandas as pd
#     # Import cục bộ để tránh lỗi ModuleNotFoundError trên Worker
#     from scrapers.mercari_scraping import scrape_mercari
#     from scrapers.rakuma_scraping import scrape_rakuma
#     from scrapers.yahooauction_scraping import scrape_yahooauction
#     import ingestion
#     import config as cfg

#     # Cấu hình URI cho Worker (Vì Worker chạy trong Docker network)
#     # Worker nhìn thấy service mongo qua tên "da-mongo"
#     WORKER_MONGO_URI = "mongodb://da-mongo:27017/"
    
#     print(f"--- [Worker] Bắt đầu xử lý nguồn: {source_name} ---")
    
#     df = pd.DataFrame()
#     try:
#         # 1. Thực hiện Scrape
#         if source_name == 'mercari':
#             df = scrape_mercari(end_page=cfg.MAX_PAGES_MERCARI)
#         elif source_name == 'rakuma':
#             df = scrape_rakuma(end_page=cfg.MAX_PAGES_RAKUMA)
#         elif source_name == 'yahooauction':
#             df = scrape_yahooauction(end_page=cfg.MAX_PAGES_YAHOO)
        
#         # 2. Ghi thẳng vào MongoDB từ Worker (Distributed Write)
#         # Không return DataFrame về Driver để tránh nghẽn mạng/RAM
#         if not df.empty:
#             ingestion.save_batch_to_datalake(df, source_name, custom_mongo_uri=WORKER_MONGO_URI)
#             return f"SUCCESS: {source_name} - {len(df)} items saved."
#         else:
#             return f"WARNING: {source_name} - No items found."

#     except Exception as e:
#         return f"ERROR: {source_name} - {str(e)}"

# def main():
#     # Cấu hình Spark
#     conf = SparkConf().setAppName("Distributed_Scraper_To_DataLake")
#     sc = SparkContext(conf=conf)
    
#     # Thiết lập mức log
#     sc.setLogLevel("WARN")

#     sources = ['mercari', 'rakuma', 'yahooauction']
#     print(f"🚀 Bắt đầu phân phối {len(sources)} nguồn dữ liệu cho Spark Cluster...")

#     # 1. Phân tán danh sách nguồn thành RDD (Resilient Distributed Dataset)
#     # numSlices=len(sources) để đảm bảo mỗi nguồn có thể vào 1 worker khác nhau
#     sources_rdd = sc.parallelize(sources, numSlices=len(sources))

#     # 2. Ánh xạ (Map): Gửi hàm process_source_on_worker xuống các Worker
#     # collect() ở đây chỉ nhận về các chuỗi trạng thái (String), không phải dữ liệu lớn
#     results = sources_rdd.map(process_source_on_worker).collect()

#     print("-" * 50)
#     print("📊 KẾT QUẢ TỪ CÁC WORKER:")
#     for res in results:
#         print(res)
#     print("-" * 50)
    
#     sc.stop()

# if __name__ == "__main__":
#     main()

# import sys
# import os
# sys.path.append('/opt/spark/apps/predictprice')

# from pyspark import SparkContext, SparkConf

# def process_source_on_worker(source_name):
#     import pandas as pd
#     from scrapers.mercari_scraping import scrape_mercari
#     from scrapers.rakuma_scraping import scrape_rakuma
#     from scrapers.yahooauction_scraping import scrape_yahooauction
#     import ingestion
#     import config as cfg
    
#     print(f"--- [Worker] Bắt đầu xử lý nguồn: {source_name} ---")
    
#     df = pd.DataFrame()
#     try:
#         # 1. Thực hiện Scrape
#         # if source_name == 'mercari':
#         #     df = scrape_mercari(end_page=cfg.MAX_PAGES_MERCARI)
#         # elif source_name == 'rakuma':
#         #     df = scrape_rakuma(end_page=cfg.MAX_PAGES_RAKUMA)
#         # elif source_name == 'yahooauction':
#         df = scrape_yahooauction(end_page=cfg.MAX_PAGES_YAHOO)

#         # 2. Ghi thẳng vào MongoDB từ Worker
#         if not df.empty:
#             # Gọi hàm save_batch_to_datalake
#             ingestion.save_batch_to_datalake(df, source_name, custom_mongo_uri=cfg.WORKER_MONGO_URI)
#             return f"SUCCESS: {source_name} - {len(df)} items saved."
#         else:
#             return f"WARNING: {source_name} - No items found."

#     except Exception as e:
#         import traceback
#         traceback_str = traceback.format_exc()
#         print(f"ERROR DETAILS [{source_name}]: {traceback_str}")
#         return f"ERROR: {source_name} - {str(e)}"

# def main():
#     # Cấu hình Spark
#     conf = SparkConf().setAppName("Distributed_Scraper_To_DataLake")
#     sc = SparkContext(conf=conf)
#     sc.setLogLevel("WARN")

#     sources = ['mercari', 'rakuma', 'yahooauction']
#     print(f"Bắt đầu phân phối {len(sources)} nguồn dữ liệu cho Spark Cluster...")

#     sources_rdd = sc.parallelize(sources, numSlices=len(sources))
#     results = sources_rdd.map(process_source_on_worker).collect()

#     print("KẾT QUẢ TỪ CÁC WORKER:")
#     for res in results:
#         print(res)
    
#     sc.stop()

# if __name__ == "__main__":
#     main()



import sys
import os
sys.path.append('/opt/spark/apps/predictprice')

from pyspark import SparkContext, SparkConf

def process_source_on_worker(source_name):
    """
    Hàm này chạy trên Worker Node: lister (max_pages) → detail → NLP → Mongo.
    """
    import sys
    if '/opt/spark/apps/predictprice' not in sys.path:
        sys.path.append('/opt/spark/apps/predictprice')
    import config as cfg
    from scrapers.orchestrator import run_pipeline_source

    print(f"--- [Worker] Bắt đầu pipeline: {source_name} ---")
    print(
        f"[{source_name}] max_pages={cfg.max_pages_for_source(source_name)} "
        f"mongo={cfg.MONGO_ENABLED} csv={cfg.CSV_ENABLED} "
        f"keys={len(cfg.PROXY_XOAY_KEYS)}"
    )
    try:
        return run_pipeline_source(
            source_name,
            mongo_uri=cfg.WORKER_MONGO_URI,
            warm_waf=cfg.buyee_settings.BUYEE_WAF_WARMUP,
        )
    except Exception as e:
        import traceback
        traceback_str = traceback.format_exc()
        print(f"ERROR DETAILS [{source_name}]:\n{traceback_str}")
        return f"ERROR: {source_name} - {str(e)}"

def main():
    """
    Main function: Phân phối công việc scraping cho Spark Cluster
    """
    # Cấu hình Spark với timeouts hợp lý
    conf = SparkConf().setAppName("Distributed_Scraper_To_DataLake")
    
    # Tăng timeout cho task dài (scraping)
    conf.set("spark.network.timeout", "1800s")           # 30 phút network timeout
    conf.set("spark.executor.heartbeatInterval", "180s") # Heartbeat mỗi 2 phút
    conf.set("spark.task.maxFailures", "3")              # Retry tối đa 3 lần
    conf.set("spark.speculation", "false")               # Tắt speculative execution (vì scraping không idempotent)
    conf.set("spark.rpc.askTimeout", "600s")             # 10 phút RPC timeout
    conf.set("spark.rpc.lookupTimeout", "600s")          # 10 phút lookup timeout
    
    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")

    sources = ['mercari', 'rakuma', 'yahooauction']
    
    print("=" * 60)
    print(f"Bắt đầu phân phối {len(sources)} nguồn dữ liệu cho Spark Cluster")
    print("=" * 60)

    sources_rdd = sc.parallelize(sources, numSlices=len(sources))
    
    try:
        results = sources_rdd.map(process_source_on_worker).collect()

        print("\n" + "=" * 60)
        print("KẾT QUẢ TỪ CÁC WORKER:")
        print("=" * 60)
        for res in results:
            print(f"  {res}")
    except Exception as e:
        print(f"\nLỗi khi chạy Spark job: {e}")
    finally:
        sc.stop()
        print("\nHoàn tất quá trình scraping!")

if __name__ == "__main__":
    main()