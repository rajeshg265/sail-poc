#!/usr/bin/env bash
set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"

echo "=== sail_k8s_cluster startup ==="

# ── Checks ─────────────────────────────────────────────────────────────────────
echo "[1/5] Checking Docker Desktop + Kubernetes..."
docker version --format '  Docker: {{.Client.Version}}' 2>/dev/null || {
  echo "ERROR: Docker not running. Start Docker Desktop first."; exit 1; }
kubectl cluster-info --request-timeout=5s 2>/dev/null | head -1 || {
  echo "ERROR: Kubernetes not available."; exit 1; }
echo "  K8s context: $(kubectl config current-context)"

# ── Build worker image ─────────────────────────────────────────────────────────
echo "[2/5] Building sail-worker-k8s:0.7.2..."
docker build -t sail-worker-k8s:0.7.2 -f Dockerfile.worker . --quiet
echo "  Image built."

# ── Apply K8s RBAC ─────────────────────────────────────────────────────────────
echo "[3/5] Applying K8s RBAC..."
kubectl apply -f k8s/rbac.yaml
echo "  RBAC applied."

# ── Start Sail K8s-cluster bridge (background) ─────────────────────────────────
echo "[4/5] Starting Sail K8s-cluster bridge on :8102..."
python3 bootstrap.py > /tmp/sail_k8s_cluster.log 2>&1 &
SAIL_PID=$!
echo "  Sail PID: $SAIL_PID  (log: /tmp/sail_k8s_cluster.log)"

# Wait for Sail bridge
echo "  Waiting for Sail bridge on :8102 (up to 120s)..."
for i in $(seq 1 120); do
  if curl -sf http://127.0.0.1:8102/health >/dev/null 2>&1; then
    echo "  Sail K8s bridge ready."
    break
  fi
  if [ $i -eq 120 ]; then
    echo "ERROR: Sail bridge did not start in 120s. Check /tmp/sail_k8s_cluster.log"
    exit 1
  fi
  sleep 1
done

# ── Start Spark bridge (background) ────────────────────────────────────────────
echo "[4b] Starting Spark JVM bridge on :8103..."
python3 bootstrap_spark.py > /tmp/spark_k8s_cluster.log 2>&1 &
SPARK_PID=$!
echo "  Spark PID: $SPARK_PID  (log: /tmp/spark_k8s_cluster.log)"

# Wait for Spark bridge
echo "  Waiting for Spark bridge on :8103 (up to 120s)..."
for i in $(seq 1 120); do
  if curl -sf http://127.0.0.1:8103/health >/dev/null 2>&1; then
    echo "  Spark bridge ready."
    break
  fi
  if [ $i -eq 120 ]; then
    echo "ERROR: Spark bridge did not start. Check /tmp/spark_k8s_cluster.log"
    exit 1
  fi
  sleep 1
done

# ── Start UI ───────────────────────────────────────────────────────────────────
echo "[5/5] Starting UI on http://localhost:5054/ ..."
echo ""
echo "=== All services up ==="
echo "  Sail K8s bridge:  http://127.0.0.1:8102/health"
echo "  Spark JVM bridge: http://127.0.0.1:8103/health"
echo "  Benchmark UI:     http://localhost:5054/"
echo ""
python3 ui_server.py
