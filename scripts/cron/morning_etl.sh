#!/usr/bin/env bash
# ETL: Mongo (extracted_layer2) → MySQL (Chuẩn Kubernetes-Native)

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

# Chạy thẳng etl.py bằng Python của Pod, không cần mượn tay docker exec
require_python || exit 1
cd "$ETL_DIR"
cron_log "K3s-Native: Running etl.py directly inside Pod → ${ETL_LOG}"
run_python etl.py >>"$ETL_LOG" 2>&1
cron_log "ETL finished OK (K3s-Native)"