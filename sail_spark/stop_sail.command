#!/bin/bash
# FRTB Sail POC — one-click stop

cd "$(dirname "$0")"

echo "=== Stopping FRTB Sail POC ==="

pkill -f "ui_server.py"       2>/dev/null && echo "  ui_server stopped"       || echo "  ui_server not running"
pkill -f "QueryService"       2>/dev/null && echo "  QueryService stopped"    || echo "  QueryService not running"
pkill -f "bootstrap_spark.py" 2>/dev/null && echo "  Spark JVM bridge stopped" || echo "  Spark JVM bridge not running"
pkill -f "bootstrap.py"       2>/dev/null && echo "  bootstrap stopped"       || echo "  bootstrap not running"

SS_DIR="$(dirname "$0")/infra/apache-shardingsphere-5.5.0-shardingsphere-proxy-bin"
"$SS_DIR/bin/stop.sh" 2>/dev/null && echo "  ShardingSphere stopped" || echo "  ShardingSphere not running"

# SIGTERM so pgpool cleans up its shared memory segments
pkill -f pgpool 2>/dev/null && echo "  pgpool stopped"        || echo "  pgpool not running"
sleep 2

echo ""
echo "Docker containers left running (start faster next time)."
echo "=== Stopped ==="
