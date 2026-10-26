#!/usr/bin/env bash
# Start sail_sc_arrow — SC Arrow (local-cluster, 3 workers) + Spark JVM + UI
# All ports are unique — does NOT touch any other sail_* app.
#
#   SparkConnect gRPC : 50075
#   HTTP bridge       : 8097
#   Metadata          : 7075
#   Java QueryService : 8092  (SAIL_ENGINE=sc_arrow)
#   Spark JVM bridge  : 8100
#   UI                : 5053
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="$HOME/.frtb_sc_arrow_logs"
mkdir -p "$LOG_DIR"

echo "============================================="
echo "  sail_sc_arrow — Starting up"
echo "============================================="
echo ""

# ── Kill any leftover processes from a previous run ───────────────────────────
for PIDFILE in bootstrap_sc_arrow.pid bootstrap_spark_sc_arrow.pid java_sc_arrow.pid ui_sc_arrow.pid; do
  PPATH="$LOG_DIR/$PIDFILE"
  if [ -f "$PPATH" ]; then
    OLD_PID=$(cat "$PPATH" 2>/dev/null)
    [ -n "$OLD_PID" ] && kill "$OLD_PID" 2>/dev/null && echo "  Stopped old PID $OLD_PID ($PIDFILE)"
    rm -f "$PPATH"
  fi
done
for PORT in 50075 8097 7075 8092 8100 5053; do
  PIDS=$(lsof -ti :$PORT 2>/dev/null) && [ -n "$PIDS" ] && kill $PIDS 2>/dev/null && echo "  Freed port $PORT"
done
sleep 1

# ── pgpool (:6440/:6441) ──────────────────────────────────────────────────────
echo "▶ Starting pgpool (cluster_shard1:6440, cluster_shard2:6441)…"
bash "$SCRIPT_DIR/infra/start_pgpool_cluster.sh"
echo ""

# ── Sail bootstrap (local, threaded) ──────────────────────────────────────────
echo "▶ Starting Sail sc_arrow bootstrap (SparkConnect :50075, local mode)…"
SAIL_MODE=local \
SAIL_SPARK_PORT=50075 \
SAIL_BRIDGE_PORT=8097 \
SAIL_META_PORT=7075 \
  python3 "$SCRIPT_DIR/sail/bootstrap.py" \
  > "$LOG_DIR/bootstrap_sc_arrow.log" 2>&1 &
echo $! > "$LOG_DIR/bootstrap_sc_arrow.pid"
echo "  Sail bootstrap PID: $(cat $LOG_DIR/bootstrap_sc_arrow.pid)"

echo -n "  Waiting for metadata ready (:7075, data loaded)"
for i in $(seq 1 150); do
  READY=$(curl -sf http://127.0.0.1:7075/session 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('ready',''))" 2>/dev/null)
  if [ "$READY" = "True" ]; then
    echo " ready"
    break
  fi
  echo -n "."
  sleep 2
  [ "$i" -eq 150 ] && echo " TIMEOUT" && tail -20 "$LOG_DIR/bootstrap_sc_arrow.log" && exit 1
done

# ── Java QueryService (sc_arrow, :8092) ───────────────────────────────────────
echo ""
echo "▶ Building and starting Java QueryService (sc_arrow, :8092)…"
SAIL_ENGINE=sc_arrow \
SAIL_META_URL=http://127.0.0.1:7075 \
  bash "$SCRIPT_DIR/java/build_and_run.sh" --mode server --port 8092 \
  > "$LOG_DIR/java_sc_arrow.log" 2>&1 &
echo $! > "$LOG_DIR/java_sc_arrow.pid"
echo "  Java QueryService PID: $(cat $LOG_DIR/java_sc_arrow.pid)"

echo -n "  Waiting for Java QueryService (:8092)"
for i in $(seq 1 60); do
  if curl -sf http://127.0.0.1:8092/health > /dev/null 2>&1; then
    echo " ready"
    break
  fi
  echo -n "."
  sleep 2
  [ "$i" -eq 60 ] && echo " TIMEOUT" && tail -20 "$LOG_DIR/java_sc_arrow.log" && exit 1
done

echo ""
echo "▶ Starting Spark JVM bridge (local[3] — no standalone master, :8100)…"
SPARK_BRIDGE_PORT=8100 \
  python3 "$SCRIPT_DIR/sail/bootstrap_spark.py" \
  > "$LOG_DIR/bootstrap_spark_sc_arrow.log" 2>&1 &
echo $! > "$LOG_DIR/bootstrap_spark_sc_arrow.pid"
echo "  Spark JVM PID: $(cat $LOG_DIR/bootstrap_spark_sc_arrow.pid)"

echo -n "  Waiting for Spark JVM bridge (:8100)"
for i in $(seq 1 90); do
  if curl -sf http://127.0.0.1:8100/health > /dev/null 2>&1; then
    echo " ready"
    break
  fi
  echo -n "."
  sleep 3
  [ "$i" -eq 90 ] && echo " TIMEOUT" && tail -20 "$LOG_DIR/bootstrap_spark_sc_arrow.log" && exit 1
done

# ── UI server (:5053) ─────────────────────────────────────────────────────────
echo ""
echo "▶ Starting UI server on http://localhost:5053/ …"
python3 "$SCRIPT_DIR/sail/ui_server.py" \
  > "$LOG_DIR/ui_sc_arrow.log" 2>&1 &
echo $! > "$LOG_DIR/ui_sc_arrow.pid"

sleep 2

echo ""
echo "============================================="
echo "  sail_sc_arrow is UP"
echo "  UI:                   http://localhost:5053/"
echo "  SC Arrow (sc_arrow):  :8092  (Sail local-cluster, 3 DataFusion workers)"
echo "  Spark JVM:            :8100  (local[3])"
echo "  Metadata:             :7075"
echo "  SparkConnect gRPC:    :50075"
echo "  Logs:  $LOG_DIR/"
echo "============================================="
open http://localhost:5053/
