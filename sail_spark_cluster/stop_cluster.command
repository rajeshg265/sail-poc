#!/usr/bin/env bash
# Stop sail_spark_cluster — does NOT touch the sail_spark stack.

LOG_DIR="$HOME/.frtb_cluster_logs"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "Stopping sail_spark_cluster…"

stop_pid() {
  local pidfile="$1"
  local label="$2"
  if [ -f "$pidfile" ]; then
    local pid
    pid=$(cat "$pidfile" 2>/dev/null || echo "")
    if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
      echo "  Stopping $label (pid $pid)…"
      kill "$pid" 2>/dev/null
      sleep 1
      kill -9 "$pid" 2>/dev/null || true
    else
      echo "  $label not running"
    fi
    rm -f "$pidfile"
  else
    echo "  $label: no pid file"
  fi
}

stop_pid "$LOG_DIR/ui_server_cluster.pid"          "UI server"
stop_pid "$LOG_DIR/bootstrap_spark_cluster.pid"    "Spark JVM bridge"
stop_pid "$LOG_DIR/bootstrap_cluster.pid"          "Sail bootstrap"

# Also kill any stray processes on cluster ports
for port in 8096 8099 50055 7073; do
  pid=$(lsof -ti tcp:$port 2>/dev/null || true)
  if [ -n "$pid" ]; then
    echo "  Killing stray process on port $port (pid $pid)"
    kill "$pid" 2>/dev/null || true
  fi
done

bash "$SCRIPT_DIR/infra/stop_pgpool_cluster.sh"

echo "Done."
