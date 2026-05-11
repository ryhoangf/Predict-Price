#!/usr/bin/env bash
# Sáng: cd Predict-Price/spark_apps/predictprice và chạy python etl.py (trên host, không docker exec)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=_common.sh
source "$SCRIPT_DIR/_common.sh"

REPO_ROOT="${REPO_ROOT:-$(cd "$SCRIPT_DIR/../.." && pwd)}"
ETL_DIR="${REPO_ROOT}/spark_apps/predictprice"

cd "$ETL_DIR"

export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

require_python || exit 1

cron_log "Running etl.py in ${ETL_DIR}"
run_python etl.py
cron_log "ETL finished OK."
