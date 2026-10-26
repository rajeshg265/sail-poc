#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

PID1="$SCRIPT_DIR/pgpool_shard1/pgpool.pid"
PID2="$SCRIPT_DIR/pgpool_shard2/pgpool.pid"

stop_if_running() {
  local pidfile=$1
  if [ -f "$pidfile" ]; then
    local pid
    pid=$(cat "$pidfile" 2>/dev/null || echo "")
    if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
      echo "Stopping existing pgpool (pid $pid)…"
      kill "$pid" && sleep 1
    fi
    # pgpool writes its own pid file; delete stale one so fresh start succeeds
    rm -f "$pidfile"
  fi
}

stop_if_running "$PID1"
stop_if_running "$PID2"

# Clear old logs
> "$SCRIPT_DIR/pgpool_shard1/pgpool.log"
> "$SCRIPT_DIR/pgpool_shard2/pgpool.log"

echo "Starting pgpool for shard1 (port 6432 -> frtb_shard1)…"
# pgpool will write pid_file_name itself; do NOT write to it here
pgpool -f "$SCRIPT_DIR/pgpool_shard1/pgpool.conf" \
       -a "$SCRIPT_DIR/pgpool_shard1/pool_hba.conf" \
       -n \
       2>>"$SCRIPT_DIR/pgpool_shard1/pgpool.log" &

echo "Starting pgpool for shard2 (port 6433 -> frtb_shard2)…"
pgpool -f "$SCRIPT_DIR/pgpool_shard2/pgpool.conf" \
       -a "$SCRIPT_DIR/pgpool_shard2/pool_hba.conf" \
       -n \
       2>>"$SCRIPT_DIR/pgpool_shard2/pgpool.log" &

# Give pgpool time to write its pid file and start listening
sleep 4

# Abort if pgpool logged any FATAL
for shard in pgpool_shard1 pgpool_shard2; do
  log="$SCRIPT_DIR/$shard/pgpool.log"
  if grep -qiE "^[0-9]{4}.*FATAL" "$log" 2>/dev/null; then
    echo "=== $shard startup errors ==="
    cat "$log"
    exit 1
  fi
done

echo ""
echo "pgpool instances running:"
[ -f "$PID1" ] && echo "  shard1: localhost:6432 (pid $(cat $PID1))" || echo "  shard1: WARNING – pid file not found"
[ -f "$PID2" ] && echo "  shard2: localhost:6433 (pid $(cat $PID2))" || echo "  shard2: WARNING – pid file not found"

echo ""
echo "Verifying connectivity…"
psql -h localhost -p 6432 -U rajeshganta -d frtb_shard1 \
     -c "SELECT COUNT(*) AS shard1_rows FROM public.frtb_sa_capital_output;" 2>&1 | tail -4
psql -h localhost -p 6433 -U rajeshganta -d frtb_shard2 \
     -c "SELECT COUNT(*) AS shard2_rows FROM public.frtb_sa_capital_output;" 2>&1 | tail -4
echo "Done."
