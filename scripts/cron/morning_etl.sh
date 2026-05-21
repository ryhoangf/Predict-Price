#!/usr/bin/env bash
# ETL: Mongo (extracted_layer2) → MySQL
# Gọi riêng hoặc từ nightly_pipeline.sh (sau scrape + NLP).
#
# Crontab riêng (nếu không dùng nightly_pipeline):
#   0 10 * * * REPO_ROOT=/root/Predict-Price bash /root/Predict-Price/scripts/cron/morning_etl.sh >> /root/Predict-Price/logs/cron-etl.log 2>&1
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=_common.sh
source "$SCRIPT_DIR/_common.sh"

REPO_ROOT="${REPO_ROOT:-$(cron_repo_root)}"
ETL_DIR="${REPO_ROOT}/spark_apps/predictprice"
LOG_DIR="$(cron_log_dir)"
STAMP="$(date +%Y%m%d_%H%M%S)"
ETL_LOG="${LOG_DIR}/cron-etl-${STAMP}.log"

export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

if docker ps --format '{{.Names}}' 2>/dev/null | grep -qx 'da-spark-master'; then
  cron_log "Running etl.py in da-spark-master → ${ETL_LOG}"
  if docker exec da-spark-master bash -c \
    'cd /opt/spark/apps/predictprice && python etl.py' >>"$ETL_LOG" 2>&1; then
    cron_log "ETL finished OK (docker)"
    exit 0
  fi
  cron_log "WARN: docker etl failed — trying host python (see ${ETL_LOG})"
fi

require_python || exit 1
cd "$ETL_DIR"
cron_log "Running etl.py on host in ${ETL_DIR} → ${ETL_LOG}"
run_python etl.py >>"$ETL_LOG" 2>&1
cron_log "ETL finished OK (host)"
