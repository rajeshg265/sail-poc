#!/bin/bash
# FRTB Sail POC — one-click start
# Double-click this file in Finder to launch everything.

cd "$(dirname "$0")"

echo "=== FRTB Sail POC ==="

# ── Ensure TimescaleDB containers are running ────────────────────────────────
echo "Checking TimescaleDB containers..."
TS_CONTAINERS=(timescale-frtb-shard1 timescale-frtb-shard2 timescale-frtb-shard3
               timescale-equity timescale-credit timescale-macro)
for c in "${TS_CONTAINERS[@]}"; do
  STATUS=$(docker inspect -f '{{.State.Status}}' "$c" 2>/dev/null)
  if [ "$STATUS" = "running" ]; then
    echo "  $c: already running"
  elif [ "$STATUS" = "exited" ]; then
    echo "  $c: restarting..."
    docker start "$c" > /dev/null
  else
    echo "  $c: not found — run infra/setup_timescale.py first"
  fi
done
echo "TimescaleDB containers ready."

# ── Start native pgpool (one per FRTB shard) ────────────────────────────────
echo "Starting pgpool instances..."
pkill -f pgpool 2>/dev/null; sleep 2
# Clean up any shared memory segments left by previous pgpool (e.g. from kill -9)
ipcs -m | awk 'NR>3 && /'"$(whoami)"'/ {print $2}' | xargs -I{} ipcrm -m {} 2>/dev/null
for shard in 1 2 3; do
  pgpool -f "$(dirname "$0")/infra/pgpool_shard${shard}/pgpool.conf" \
         -F "$(dirname "$0")/infra/pgpool_shard${shard}/pool_hba.conf" \
         -n 2>/tmp/pgpool_shard${shard}.log &
done
for domain in equity credit macro; do
  pgpool -f "$(dirname "$0")/infra/pgpool_${domain}/pgpool.conf" \
         -F "$(dirname "$0")/infra/pgpool_${domain}/pool_hba.conf" \
         -n 2>/tmp/pgpool_${domain}.log &
done
sleep 4
echo "pgpool instances ready (6 total: shard1/2/3 + equity/credit/macro)."

# ── Start ShardingSphere proxy ────────────────────────────────────────────────
echo "Starting ShardingSphere proxy..."
SS_DIR="$(dirname "$0")/infra/apache-shardingsphere-5.5.0-shardingsphere-proxy-bin"
"$SS_DIR/bin/stop.sh" 2>/dev/null; sleep 2
"$SS_DIR/bin/start.sh"
echo "Waiting for ShardingSphere on :3307..."
for i in $(seq 1 20); do
  if nc -z 127.0.0.1 3307 2>/dev/null; then
    echo "ShardingSphere ready after ${i}s"
    break
  fi
  sleep 1
done

echo "Starting Sail servers..."

# Kill any stale processes
pkill -f "bootstrap.py"      2>/dev/null
pkill -f "bootstrap_spark.py" 2>/dev/null
pkill -f "ui_server.py"      2>/dev/null
sleep 1

LOG_DIR="$HOME/.frtb_sail_logs"
mkdir -p "$LOG_DIR"

# ── Dev Sail server (1 DataFusion worker, local mode, ports :50051/:32010/:8080-8082) ──
python3 sail/bootstrap.py > "$LOG_DIR/bootstrap.log" 2>&1 &
BOOT_PID=$!
echo "Dev bootstrap PID: $BOOT_PID (SparkConnect :50051, HTTP bridges :8080-8082)"

# Wait for dev HTTP workers (port 8080)
echo "Waiting for dev workers..."
for i in $(seq 1 30); do
  if curl -sf http://127.0.0.1:8080/health > /dev/null 2>&1; then
    echo "Dev workers ready after ${i}s"
    break
  fi
  sleep 1
done

# ── Production Sail server (3 DataFusion workers, local-cluster, ports :50053/:32011/:8083-8085) ──
SAIL_WORKER_COUNT=3 \
  SAIL_SPARK_PORT=50053 \
  SAIL_FLIGHT_PORT=32011 \
  SAIL_WORKER_PORTS=8083,8084,8085 \
  SAIL_ARROW_DIR=/tmp/sail_flightsql_prod \
  SAIL_META_PORT=7071 \
  python3 sail/bootstrap.py > "$LOG_DIR/bootstrap_prod.log" 2>&1 &
PROD_PID=$!
echo "Production bootstrap PID: $PROD_PID (SparkConnect :50053, 3 DataFusion workers, HTTP bridges :8083-8085)"

# Wait for production HTTP workers (port 8083)
echo "Waiting for production workers..."
for i in $(seq 1 60); do
  if curl -sf http://127.0.0.1:8083/health > /dev/null 2>&1; then
    echo "Production workers ready after ${i}s"
    break
  fi
  sleep 1
done

# ── Spark JVM bridge (real PySpark, port :8095) ──────────────────────────────
echo "Starting Spark JVM bridge..."
python3 sail/bootstrap_spark.py > "$LOG_DIR/bootstrap_spark.log" 2>&1 &
SPARK_PID=$!
echo "Spark bridge PID: $SPARK_PID (Flask bridge :8095)"

# Wait for Spark bridge (JVM startup takes ~30s)
echo "Waiting for Spark JVM bridge..."
for i in $(seq 1 60); do
  if curl -sf http://127.0.0.1:8095/health > /dev/null 2>&1; then
    echo "Spark JVM bridge ready after ${i}s"
    break
  fi
  sleep 1
done

# Start QueryService HTTP server (UI routes all queries through it)
# SAIL_ENGINE controls the Sail query path:
#   sc_arrow    — SparkConnect gRPC + Arrow IPC (zero-copy, default)
#   flightsql   — Arrow FlightSQL via ADBC
#   sparkconnect— SparkConnect JDBC
#   http        — HTTP bridge (Flask workers)
# Override at launch: SAIL_ENGINE=flightsql bash start_sail.command
SAIL_ENGINE="${SAIL_ENGINE:-sc_arrow}"
echo "Starting QueryService HTTP server (engine: $SAIL_ENGINE)..."
pkill -f "QueryService" 2>/dev/null; sleep 1
cd "$(dirname "$0")/java" && SAIL_ENGINE="$SAIL_ENGINE" ./build_and_run.sh --mode server > "$LOG_DIR/queryservice.log" 2>&1 &
QS_PID=$!
cd "$(dirname "$0")"
echo "QueryService PID: $QS_PID"
for i in $(seq 1 15); do
  if curl -sf http://127.0.0.1:8090/health > /dev/null 2>&1; then
    echo "QueryService ready after ${i}s"
    break
  fi
  sleep 1
done

# Start UI server (inherits SAIL_ENGINE so initial chip matches QueryService)
echo "Starting UI server..."
SAIL_ENGINE="$SAIL_ENGINE" python3 sail/ui_server.py > "$LOG_DIR/ui_server.log" 2>&1 &
UI_PID=$!
echo "ui_server PID: $UI_PID"

sleep 2

# Open browser
open http://localhost:5050

echo ""
echo "===================================="
echo "FRTB Sail POC is running"
echo "  UI:             http://localhost:5050"
echo "  QueryService:   http://127.0.0.1:8090  (engine: $SAIL_ENGINE)"
echo "  Dev  SC+FS:     SparkConnect :50051 / FlightSQL :32010 (1 DataFusion worker)"
echo "  Prod SC+FS:     SparkConnect :50053 / FlightSQL :32011 (3 DataFusion workers)"
echo "  Dev  HTTP:      :8080 :8081 :8082  (HTTP bridge workers)"
echo "  Prod HTTP:      :8083 :8084 :8085  (HTTP bridge workers)"
echo "  Spark JVM:      http://127.0.0.1:8095  (PySpark local[3] bridge)"
echo "  Engine switch:  click the ENGINE chip in the UI (no restart needed)"
echo "  Logs:           $LOG_DIR/"
echo "===================================="
echo ""
echo "Press Ctrl-C or close this window to stop all servers."

# Keep terminal open and forward signals to children
trap "echo 'Stopping...'; kill $BOOT_PID $PROD_PID $SPARK_PID $QS_PID $UI_PID 2>/dev/null; exit 0" INT TERM

wait $BOOT_PID
