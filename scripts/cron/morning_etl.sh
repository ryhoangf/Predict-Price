#!/usr/bin/env bash
# 08:00 — chạy ETL trong container Spark master (cùng image/deps với stack).
set -euo pipefail

REPO_ROOT="${REPO_ROOT:-$(cd "$(dirname "$0")/../.." && pwd)}"
cd "$REPO_ROOT"

export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

log() { echo "[$(date -Iseconds)] $*"; }

if ! docker ps --format '{{.Names}}' | grep -qx 'da-spark-master'; then
  log "da-spark-master not running; starting stack detached (1 worker, no volume wipe)..."
  docker compose up -d --scale spark-worker=1
  log "Waiting for master..."
  ok=0
  for i in $(seq 1 120); do
    if curl -sf "http://127.0.0.1:9090" >/dev/null 2>&1; then
      ok=1
      break
    fi
    sleep 5
  done
  if [[ "$ok" -ne 1 ]]; then
    log "ERROR: Spark master did not become ready."
    exit 1
  fi
fi

log "Running etl.py..."
docker exec da-spark-master python /opt/spark/apps/predictprice/etl.py
log "ETL finished OK."
