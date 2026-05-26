#!/usr/bin/env bash
set -euo pipefail

# Định nghĩa các thư mục cốt lõi
REPO_ROOT="/root/Predict-Price"
cd "$REPO_ROOT"
LOG_DIR="${REPO_ROOT}/logs"
STAMP="$(date +%Y%m%d_%H%M%S)"

# Import các hàm helper (để dùng lại hàm pack_scrapers_zip đã tối ưu ở bước trước)
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/_common.sh"

SCRAPE_LOG="${LOG_DIR}/cron-scrape-${STAMP}.log"
LOG_NLP="${LOG_DIR}/cron-nlp-${STAMP}.log"

cron_log "========== K8s Nightly pipeline start (REAL RUN) =========="

# ---- BƯỚC 1: CÀO DỮ LIỆU (Tương ứng với 'make scrape') ----
cron_log "Step 1/3: make scrape → ${SCRAPE_LOG}"
cd "${REPO_ROOT}/spark_apps/predictprice"

# Chạy trực tiếp module python cào dữ liệu, không qua docker exec nữa
if ! python3 -m scrapers.run --session new >>"$SCRAPE_LOG" 2>&1; then
    cron_log "ERROR: python scrapers.run failed (see ${SCRAPE_LOG})"
    exit 1
fi
cron_log "Scrape finished OK"


# ---- BƯỚC 2: CHẠY SPARK NLP (Tương ứng với 'make submitmain') ----
cron_log "Step 2/3: Preparing zips and submitting Spark NLP Job..."
cd "$REPO_ROOT"

# Gọi hàm nén file scrapers.zip trực tiếp bằng Python
pack_scrapers_zip || exit 1

# Khởi chạy spark-submit trực tiếp từ trong Pod, bắn job lên cụm Spark Master nội bộ của K3s
# Chú ý: Ta sửa lại toàn bộ đường dẫn file sang thư mục K3s mount (/root/Predict-Price/...)
if ! spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --py-files /root/Predict-Price/spark_apps/predictprice/scrapers.zip,/root/Predict-Price/spark_apps/predictprice/config.py,/root/Predict-Price/spark_apps/predictprice/ingestion.py \
    /root/Predict-Price/spark_apps/predictprice/main.py >>"$LOG_NLP" 2>&1; then
    cron_log "ERROR: spark-submit main.py failed (see ${LOG_NLP})"
    exit 1
fi
cron_log "Spark NLP finished OK"


# ---- BƯỚC 3: CHẠY MORNING ETL (Mongo -> MySQL) ----
cron_log "Step 3/3: morning ETL"
bash "$SCRIPT_DIR/morning_etl.sh"

cron_log "========== K8s Nightly pipeline finished OK =========="