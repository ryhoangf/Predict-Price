#!/usr/bin/env bash
set -euo pipefail

# ==============================================================================
# HỆ THỐNG PIPELINE iValuate - K3S NATIVE MODE
# Đã vá lỗi DNS K8s, tối ưu RAM (1.5G Worker) và xử lý đa luồng NLP
# ==============================================================================

# Định nghĩa các thư mục cốt lõi
REPO_ROOT="/root/Predict-Price"
cd "$REPO_ROOT"
LOG_DIR="${REPO_ROOT}/logs"
STAMP="$(date +%Y%m%d_%H%M%S)"

# Import các hàm helper (để dùng lại hàm pack_scrapers_zip)
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/_common.sh"

SCRAPE_LOG="${LOG_DIR}/cron-scrape-${STAMP}.log"
LOG_NLP="${LOG_DIR}/cron-nlp-${STAMP}.log"

cron_log "========== K8s Nightly pipeline start (REAL RUN) =========="

# ---- BƯỚC 1: CÀO DỮ LIỆU (Tương ứng với 'make scrape') ----
cron_log "Step 1/3: make scrape → ${SCRAPE_LOG}"
cd "${REPO_ROOT}/spark_apps/predictprice"

# Chạy trực tiếp module python cào dữ liệu
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

# [QUAN TRỌNG]: Lấy IP thật của Pod Job hiện tại để phá vỡ "Hố đen DNS" của K3s
export POD_IP=$(hostname -i | awk '{print $1}')
export SPARK_LOCAL_IP=$POD_IP
cron_log "Resolved Job Pod IP: $POD_IP (Spark Driver Host)"

# Khởi chạy spark-submit
# Lưu ý: RAM và Core đã được set chuẩn (1000m/512m/1 Core) ngay trong main.py
if ! spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --conf spark.driver.host=$POD_IP \
    --conf spark.driver.bindAddress=$POD_IP \
    --py-files /root/Predict-Price/spark_apps/predictprice/scrapers.zip,/root/Predict-Price/spark_apps/predictprice/config.py,/root/Predict-Price/spark_apps/predictprice/ingestion.py \
    /root/Predict-Price/spark_apps/predictprice/main.py >>"$LOG_NLP" 2>&1; then
    cron_log "ERROR: spark-submit main.py failed (see ${LOG_NLP})"
    exit 1
fi
cron_log "Spark NLP finished OK"


# ---- BƯỚC 3: CHẠY MORNING ETL (Mongo -> MySQL/Vitess) ----
cron_log "Step 3/3: morning ETL"
bash "$SCRIPT_DIR/morning_etl.sh"

cron_log "========== K8s Nightly pipeline finished OK =========="