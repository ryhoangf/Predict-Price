#!/usr/bin/env bash
# Helpers — source only, do not execute.

cron_log() { echo "[$(date -Iseconds)] $*"; }

# Đã loại bỏ require_make_docker vì không cần dùng lệnh docker nữa

require_python() {
  if command -v python3 >/dev/null 2>&1; then
    return 0
  fi
  if command -v python >/dev/null 2>&1; then
    return 0
  fi
  cron_log "ERROR: need python3 or python on PATH"
  return 1
}

run_python() {
  if command -v python3 >/dev/null 2>&1; then
    python3 "$@"
  else
    python "$@"
  fi
}

cron_repo_root() {
  local script_dir
  script_dir="$(cd "$(dirname "${BASH_SOURCE[1]:-$0}")" && pwd)"
  echo "${REPO_ROOT:-$(cd "$script_dir/../.." && pwd)}"
}

cron_log_dir() {
  local root dir
  root="$(cron_repo_root)"
  dir="${root}/logs"
  mkdir -p "$dir"
  echo "$dir"
}

# Đã loại bỏ ensure_docker_stack vì K3s luôn giữ Spark Master sống 24/7

pack_scrapers_zip() {
  local root packer
  root="$(cron_repo_root)"
  packer="${root}/spark_apps/predictprice/pack_zips.py"
  if [[ -f "$packer" ]]; then
    cron_log "K3s-Native: Running pack_zips.py directly..."
    run_python "$packer" # Chạy trực tiếp bằng python, không qua lệnh 'make' của docker
  elif [[ -f "${root}/spark_apps/predictprice/scrapers.zip" ]]; then
    cron_log "WARN: pack_zips.py missing — using existing scrapers.zip"
  else
    cron_log "ERROR: no pack_zips.py and no scrapers.zip"
    return 1
  fi
}

# Cấu hình thời gian sleep
: "${CRON_SLEEP_NLP_SEC:=0}"
: "${CRON_SLEEP_ETL_SEC:=0}"

sleep_hours() {
  local sec="$1"
  local label="$2"
  if [[ "$sec" -le 0 ]]; then
    return 0
  fi
  cron_log "Sleep ${sec}s (${label})..."
  sleep "$sec"
}