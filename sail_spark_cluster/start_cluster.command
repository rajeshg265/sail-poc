#!/usr/bin/env bash
# Start sail_spark_cluster — SC Arrow (3 workers) + Spark JVM (local[3])
# Does NOT touch the sail_spark stack (ports 50051/8080-8095/5050).
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="$HOME/.frtb_cluster_logs"
mkdir -p "$LOG_DIR"

echo "============================================="
echo "  sail_spark_cluster — Starting up"
echo "============================================="
echo ""

# ── Kill any leftover processes from a previous run ───────────────────────────
for PIDFILE in bootstrap_cluster.pid bootstrap_spark_cluster.pid ui_server_cluster.pid kubectl_proxy.pid; do
  PPATH="$LOG_DIR/$PIDFILE"
  if [ -f "$PPATH" ]; then
    OLD_PID=$(cat "$PPATH" 2>/dev/null)
    [ -n "$OLD_PID" ] && kill "$OLD_PID" 2>/dev/null && echo "  Stopped old PID $OLD_PID ($PIDFILE)"
    rm -f "$PPATH"
  fi
done
# Also free cluster ports if still bound
for PORT in 50055 8096 7073; do
  PIDS=$(lsof -ti :$PORT 2>/dev/null) && [ -n "$PIDS" ] && kill $PIDS 2>/dev/null && echo "  Freed port $PORT"
done
sleep 1

# ── pgpool ────────────────────────────────────────────────────────────────────
echo "▶ Starting pgpool (cluster_shard1:6440, cluster_shard2:6441)…"
bash "$SCRIPT_DIR/infra/start_pgpool_cluster.sh"
echo ""

# ── kubectl proxy (HTTP facade for K8s API — avoids rustls panic in pysail) ───
echo "▶ Starting kubectl proxy on :7079…"
kubectl proxy --port=7079 > "$LOG_DIR/kubectl_proxy.log" 2>&1 &
echo $! > "$LOG_DIR/kubectl_proxy.pid"
sleep 1
curl -sf http://127.0.0.1:7079/api > /dev/null && echo "  kubectl proxy ready" || echo "  WARN: kubectl proxy not responding"
echo ""

# ── Sail bootstrap (sc_arrow, 3 workers) ──────────────────────────────────────
echo "▶ Starting Sail cluster bootstrap (SparkConnect :50055, ${SAIL_WORKER_COUNT:-3} workers managed by Sail)…"
SAIL_WORKER_COUNT=${SAIL_WORKER_COUNT:-3} \
SAIL_SPARK_PORT=50055 \
SAIL_BRIDGE_PORT=8096 \
SAIL_META_PORT=7073 \
  python3 "$SCRIPT_DIR/sail/bootstrap.py" \
  > "$LOG_DIR/bootstrap_cluster.log" 2>&1 &
echo $! > "$LOG_DIR/bootstrap_cluster.pid"
echo "  Sail bootstrap PID: $(cat $LOG_DIR/bootstrap_cluster.pid)"

# Wait for SC Arrow HTTP bridge to be ready
echo -n "  Waiting for SC Arrow bridge (:8096)"
for i in $(seq 1 150); do
  if curl -sf http://127.0.0.1:8096/health > /dev/null 2>&1; then
    echo " ready"
    break
  fi
  echo -n "."
  sleep 2
  [ "$i" -eq 150 ] && echo " TIMEOUT" && tail -20 "$LOG_DIR/bootstrap_cluster.log" && exit 1
done

# ── Spark JVM bridge (local[3]) ───────────────────────────────────────────────
echo ""
echo "▶ Starting Spark JVM bridge (local[3], port :8099)…"
SPARK_BRIDGE_PORT=8099 \
  python3 "$SCRIPT_DIR/sail/bootstrap_spark.py" \
  > "$LOG_DIR/bootstrap_spark_cluster.log" 2>&1 &
echo $! > "$LOG_DIR/bootstrap_spark_cluster.pid"
echo "  Spark JVM PID: $(cat $LOG_DIR/bootstrap_spark_cluster.pid)"

echo -n "  Waiting for Spark JVM bridge"
for i in $(seq 1 90); do
  if curl -sf http://127.0.0.1:8099/health > /dev/null 2>&1; then
    echo " ready"
    break
  fi
  echo -n "."
  sleep 3
  [ "$i" -eq 90 ] && echo " TIMEOUT" && tail -20 "$LOG_DIR/bootstrap_spark_cluster.log" && exit 1
done

# ── UI server ─────────────────────────────────────────────────────────────────
echo ""
echo "▶ Starting UI server on http://localhost:5051/ …"
python3 "$SCRIPT_DIR/sail/ui_server.py" \
  > "$LOG_DIR/ui_server_cluster.log" 2>&1 &
echo $! > "$LOG_DIR/ui_server_cluster.pid"

sleep 2

echo ""
echo "============================================="
echo "  sail_spark_cluster is UP"
echo "  UI:          http://localhost:5051/"
echo "  SC Arrow:    :8096 (Sail local-cluster, 3 DataFusion workers)"
echo "  Spark JVM:   :8099 (local[3])"
echo "  Logs:        $LOG_DIR/"
echo "============================================="
open http://localhost:5051/
