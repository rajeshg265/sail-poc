#!/usr/bin/env bash
set -euo pipefail
echo "=== Stopping sail_k8s_cluster ==="
lsof -ti :8102,:8103,:5054 2>/dev/null | xargs kill -9 2>/dev/null || true
echo "  Ports 8102, 8103, 5054 cleared."
kubectl delete pods -l app.kubernetes.io/managed-by=sail --ignore-not-found 2>/dev/null || true
kubectl delete pods -l sail-role=worker --ignore-not-found 2>/dev/null || true
kubectl get pods --no-headers 2>/dev/null | grep "sail-k8s-worker" | awk '{print $1}' | xargs kubectl delete pod --ignore-not-found 2>/dev/null || true
kubectl delete rolebinding sail-driver-rolebinding --ignore-not-found
kubectl delete role sail-driver-role --ignore-not-found
kubectl delete serviceaccount sail-driver --ignore-not-found
echo "  K8s resources removed."
echo "Done."
