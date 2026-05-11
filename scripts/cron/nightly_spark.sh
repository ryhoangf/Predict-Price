#!/usr/bin/env bash
# Tối: make down → 60s → make run-scaled (nền: compose up không có -d sẽ treo) → 60s → make submitmain
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=_common.sh
source "$SCRIPT_DIR/_common.sh"

REPO_ROOT="${REPO_ROOT:-$(cd "$SCRIPT_DIR/../.." && pwd)}"
cd "$REPO_ROOT"

export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

require_make_docker || exit 1

LOG_DIR="${REPO_ROOT}/logs"
mkdir -p "$LOG_DIR"
UP_LOG="${LOG_DIR}/cron-spark-up.log"

cron_log "make down"
make down
sleep 60

cron_log "make run-scaled (chạy nền; xem ${UP_LOG})"
nohup make run-scaled >>"$UP_LOG" 2>&1 &
sleep 60

cron_log "make submitmain"
make submitmain

cron_log "Nightly make chain finished OK."
