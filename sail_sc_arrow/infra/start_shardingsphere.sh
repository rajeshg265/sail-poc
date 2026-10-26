#!/usr/bin/env bash
# Starts ShardingSphere Proxy 5.5.0 on port 3307.
# Requires Java 17+. Connects to PostgreSQL shard1/shard2 on localhost:5432.
# On Linux prod: update database-frtb.yaml to use pgpool ports 6432/6433.
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SS_HOME="$SCRIPT_DIR/apache-shardingsphere-5.5.0-shardingsphere-proxy-bin"
PID_FILE="$SCRIPT_DIR/shardingsphere.pid"
LOG="$SCRIPT_DIR/shardingsphere.log"

if [ -f "$PID_FILE" ]; then
    OLD_PID=$(cat "$PID_FILE")
    if kill -0 "$OLD_PID" 2>/dev/null; then
        echo "ShardingSphere already running (PID $OLD_PID)"
        exit 0
    fi
    rm -f "$PID_FILE"
fi

echo "Starting ShardingSphere Proxy on port 3307…"

nohup "$SS_HOME/bin/start.sh" 3307 >> "$LOG" 2>&1 &
SS_PID=$!
echo $SS_PID > "$PID_FILE"

echo "Waiting for proxy to be ready…"
for i in $(seq 1 30); do
    if nc -z localhost 3307 2>/dev/null; then
        echo "ShardingSphere Proxy ready (PID $SS_PID)"
        exit 0
    fi
    sleep 1
done

echo "ERROR: ShardingSphere Proxy did not start in 30s. Check $LOG"
exit 1
