#!/usr/bin/env bash
# Pipeline đêm: scrape → (10h) Spark NLP → (2h) ETL MySQL
#
# Crontab ví dụ (22:00 mỗi ngày, ETL ~14:00 hôm sau):
#   0 22 * * * REPO_ROOT=/root/Predict-Price bash /root/Predict-Price/scripts/cron/nightly_pipeline.sh >> /root/Predict-Price/logs/cron-pipeline.log 2>&1
#
# Test nhanh:
#   CRON_SLEEP_NLP_SEC=60 CRON_SLEEP_ETL_SEC=30 bash scripts/cron/nightly_pipeline.sh
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=_common.sh
source "$SCRIPT_DIR/_common.sh"

REPO_ROOT="${REPO_ROOT:-$(cron_repo_root)}"
cd "$REPO_ROOT"

export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

require_make_docker || exit 1

LOG_DIR="$(cron_log_dir)"
STAMP="$(date +%Y%m%d_%H%M%S)"
SCRAPE_LOG="${LOG_DIR}/cron-scrape-${STAMP}.log"
NLP_LOG="${LOG_DIR}/cron-nlp-${STAMP}.log"

cron_log "========== Nightly pipeline start (repo=${REPO_ROOT}) =========="

ensure_docker_stack || exit 1

cron_log "Step 1/3: make scrape → ${SCRAPE_LOG}"
if ! make scrape >>"$SCRAPE_LOG" 2>&1; then
  cron_log "ERROR: make scrape failed (see ${SCRAPE_LOG})"
  exit 1
fi
cron_log "Scrape finished OK"

sleep_hours "${CRON_SLEEP_NLP_SEC}" "wait before Spark NLP"

pack_scrapers_zip || exit 1

cron_log "Step 2/3: make submitmain → ${NLP_LOG}"
if ! make submitmain >>"$NLP_LOG" 2>&1; then
  cron_log "ERROR: make submitmain failed (see ${NLP_LOG})"
  exit 1
fi
cron_log "Spark NLP finished OK"

sleep_hours "${CRON_SLEEP_ETL_SEC}" "wait before ETL"

cron_log "Step 3/3: morning ETL"
bash "$SCRIPT_DIR/morning_etl.sh"

cron_log "========== Nightly pipeline finished OK =========="
