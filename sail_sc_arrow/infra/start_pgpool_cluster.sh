#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

PID1="$SCRIPT_DIR/pgpool_cluster_shard1/pgpool.pid"
PID2="$SCRIPT_DIR/pgpool_cluster_shard2/pgpool.pid"

stop_if_running() {
  local pidfile=$1
  if [ -f "$pidfile" ]; then
    local pid
    pid=$(cat "$pidfile" 2>/dev/null || echo "")
    if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
      echo "Stopping existing pgpool (pid $pid)…"
      kill "$pid" && sleep 1
    fi
    rm -f "$pidfile"
  fi
}

stop_if_running "$PID1"
stop_if_running "$PID2"

> "$SCRIPT_DIR/pgpool_cluster_shard1/pgpool.log"
> "$SCRIPT_DIR/pgpool_cluster_shard2/pgpool.log"

echo "Starting pgpool for cluster_shard1 (port 6440 -> cluster_shard1:5450)…"
pgpool -f "$SCRIPT_DIR/pgpool_cluster_shard1/pgpool.conf" \
       -a "$SCRIPT_DIR/pgpool_cluster_shard1/pool_hba.conf" \
       -n \
       2>>"$SCRIPT_DIR/pgpool_cluster_shard1/pgpool.log" &

echo "Starting pgpool for cluster_shard2 (port 6441 -> cluster_shard2:5451)…"
pgpool -f "$SCRIPT_DIR/pgpool_cluster_shard2/pgpool.conf" \
       -a "$SCRIPT_DIR/pgpool_cluster_shard2/pool_hba.conf" \
       -n \
       2>>"$SCRIPT_DIR/pgpool_cluster_shard2/pgpool.log" &

sleep 4

for shard in pgpool_cluster_shard1 pgpool_cluster_shard2; do
  log="$SCRIPT_DIR/$shard/pgpool.log"
  if grep -qiE "^[0-9]{4}.*FATAL" "$log" 2>/dev/null; then
    echo "=== $shard startup errors ==="
    cat "$log"
    exit 1
  fi
done

echo ""
echo "pgpool instances running:"
[ -f "$PID1" ] && echo "  cluster_shard1: localhost:6440 (pid $(cat $PID1))" || echo "  cluster_shard1: WARNING – pid file not found"
[ -f "$PID2" ] && echo "  cluster_shard2: localhost:6441 (pid $(cat $PID2))" || echo "  cluster_shard2: WARNING – pid file not found"

echo ""
echo "Verifying connectivity…"
psql -h localhost -p 6440 -U rajeshganta -d cluster_shard1 \
     -c "SELECT COUNT(*) AS shard1_rows FROM public.frtb_sa_capital_output;" 2>&1 | tail -4
psql -h localhost -p 6441 -U rajeshganta -d cluster_shard2 \
     -c "SELECT COUNT(*) AS shard2_rows FROM public.frtb_sa_capital_output;" 2>&1 | tail -4
echo "Done."
