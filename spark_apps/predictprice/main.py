"""
Spark entry — NLP layer only (post-ingest).

Scrape + raw ingest: python -m scrapers.run → Mongo status=extracted_raw.
This job: 3 Spark tasks (mercari / rakuma / yahooauction) run NLP + junk → extracted_layer2.
Downstream: etl.py picks processed=false + extracted_layer2.
"""

import sys
import os

sys.path.append("/opt/spark/apps/predictprice")

from pyspark import SparkContext, SparkConf


def process_nlp_source(source_name: str) -> str:
    """Runs on Spark executor: NLP for one source from Mongo."""
    if "/opt/spark/apps/predictprice" not in sys.path:
        sys.path.append("/opt/spark/apps/predictprice")
    import config as cfg
    from scrapers.nlp_spark import run_nlp_for_source

    print(f"--- [Spark NLP] source={source_name} ---")
    try:
        return run_nlp_for_source(
            source_name,
            mongo_uri=cfg.WORKER_MONGO_URI,
        )
    except Exception as e:
        import traceback

        print(f"ERROR DETAILS [{source_name}]:\n{traceback.format_exc()}")
        return f"ERROR: {source_name} - {str(e)}"


def main():
    conf = SparkConf().setAppName("Distributed_NLP_PostIngest")
    
    # CẤU HÌNH ÉP TÀI NGUYÊN CHUẨN K3S (SỬA ĐOẠN NÀY)
    conf.set("spark.executor.memory", "600m")         # Cấp 600M RAM cho mỗi Executor JVM
    conf.set("spark.driver.memory", "512m")           # Cấp 512M RAM cho Driver chạy tại Job Pod
    conf.set("spark.executor.memoryOverhead", "128m") # Chừa 128M Off-heap cho Python worker bên ngoài JVM
    conf.set("spark.driver.memoryOverhead", "128m")
    conf.set("spark.executor.cores", "1")             # Mỗi Executor chỉ ăn đúng 1 Core
    conf.set("spark.cores.max", "3")                  # Cả Job ăn tối đa 3 Cores (tương ứng 3 nguồn song song)

    conf.set("spark.network.timeout", "1800s")
    conf.set("spark.executor.heartbeatInterval", "180s")
    conf.set("spark.task.maxFailures", "3")
    conf.set("spark.speculation", "false")
    conf.set("spark.rpc.askTimeout", "600s")
    conf.set("spark.rpc.lookupTimeout", "600s")

    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")

    sources = ["mercari", "rakuma", "yahooauction"]

    print("=" * 60)
    print(f"Spark NLP — {len(sources)} nguồn song song (đọc Mongo raw → NLP → layer2)")
    print("=" * 60)

    sources_rdd = sc.parallelize(sources, numSlices=len(sources))

    try:
        results = sources_rdd.map(process_nlp_source).collect()

        print("\n" + "=" * 60)
        print("KẾT QUẢ NLP TỪ CÁC WORKER:")
        print("=" * 60)
        for res in results:
            print(f"  {res}")
    except Exception as e:
        print(f"\nLỗi khi chạy Spark NLP job: {e}")
    finally:
        sc.stop()
        print("\nHoàn tất Spark NLP!")


if __name__ == "__main__":
    main()