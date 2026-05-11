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
