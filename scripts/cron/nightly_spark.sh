#!/usr/bin/env bash
# 22:45 — khởi động Spark (scaled), chờ master sẵn sàng, chạy main pipeline.
set -euo pipefail

REPO_ROOT="${REPO_ROOT:-$(cd "$(dirname "$0")/../.." && pwd)}"
cd "$REPO_ROOT"

export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

log() { echo "[$(date -Iseconds)] $*"; }

log "Starting nightly Spark (detached, 3 workers)..."
make down
docker compose up -d --scale spark-worker=3

log "Waiting for Spark master UI (host :9090 → container :8080)..."
ok=0
for i in $(seq 1 120); do
  if curl -sf "http://127.0.0.1:9090" >/dev/null 2>&1; then
    ok=1
    break
  fi
  sleep 5
done
if [[ "$ok" -ne 1 ]]; then
  log "ERROR: Spark master did not become ready in time."
  exit 1
fi

log "Running submitmain..."
make submitmain
log "Nightly Spark job finished OK."
