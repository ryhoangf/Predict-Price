#!/usr/bin/env bash
# Helpers — source only, do not execute.

cron_log() { echo "[$(date -Iseconds)] $*"; }

require_make_docker() {
  local c
  for c in make docker; do
    command -v "$c" >/dev/null 2>&1 || {
      cron_log "ERROR: missing required command: $c"
      return 1
    }
  done
  return 0
}

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

ensure_docker_stack() {
  if docker ps --format '{{.Names}}' 2>/dev/null | grep -qx 'da-spark-master'; then
    cron_log "Docker stack OK (da-spark-master running)"
    return 0
  fi
  cron_log "da-spark-master not running — starting with make run-d"
  make run-d
  local i
  for i in $(seq 1 30); do
    if docker ps --format '{{.Names}}' 2>/dev/null | grep -qx 'da-spark-master'; then
      cron_log "Stack ready after ${i}x5s"
      sleep 5
      return 0
    fi
    sleep 5
  done
  cron_log "ERROR: da-spark-master did not start in time"
  return 1
}

pack_scrapers_zip() {
  local root packer
  root="$(cron_repo_root)"
  packer="${root}/spark_apps/predictprice/pack_zips.py"
  if [[ -f "$packer" ]]; then
    cron_log "make pack-zips"
    make pack-zips
  elif [[ -f "${root}/spark_apps/predictprice/scrapers.zip" ]]; then
    cron_log "WARN: pack_zips.py missing — using existing scrapers.zip"
  else
    cron_log "ERROR: no pack_zips.py and no scrapers.zip"
    return 1
  fi
}

# Default: no delay between steps. Override via env if needed, e.g.:
#   CRON_SLEEP_NLP_SEC=36000 CRON_SLEEP_ETL_SEC=7200
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
