#!/usr/bin/env bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

stop_pgpool() {
  local pidfile=$1
  local label=$2
  if [ -f "$pidfile" ]; then
    local pid
    pid=$(cat "$pidfile" 2>/dev/null || echo "")
    if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
      echo "Stopping $label (pid $pid)…"
      kill "$pid" && sleep 1
    else
      echo "$label not running (stale pid file)"
    fi
    rm -f "$pidfile"
  else
    echo "$label not running (no pid file)"
  fi
}

stop_pgpool "$SCRIPT_DIR/pgpool_cluster_shard1/pgpool.pid" "cluster_shard1 pgpool"
stop_pgpool "$SCRIPT_DIR/pgpool_cluster_shard2/pgpool.pid" "cluster_shard2 pgpool"
echo "Done."
