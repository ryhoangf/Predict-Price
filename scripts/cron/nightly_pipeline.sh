#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="/root/Predict-Price"
cd "$REPO_ROOT"
LOG_DIR="${REPO_ROOT}/logs"
STAMP="$(date +%Y%m%d_%H%M%S)"

echo "[${STAMP}] ========== K8s Nightly pipeline start =========="

# Bước 1: Chạy trực tiếp script cào dữ liệu bằng Python
echo "Step 1/3: Scraping..."
# TODO: Thay bằng lệnh python chạy code cào dữ liệu gốc. VD: python3 scrapers/main.py

# Bước 2: Bắn Job thẳng lên Spark Master qua IP nội bộ K3s
echo "Step 2/3: Spark NLP..."
# TODO: Thay bằng lệnh spark-submit thực tế. 
# QUAN TRỌNG: Đổi cờ --master thành spark://spark-master:7077

# Bước 3: Chạy ETL
echo "Step 3/3: Morning ETL..."
# TODO: Thay bằng lệnh python chạy ETL gốc. VD: python3 spark_apps/predictprice/etl.py

echo "[$(date +%Y%m%d_%H%M%S)] ========== K8s Nightly pipeline finished OK =========="