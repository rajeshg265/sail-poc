#!/usr/bin/env bash
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SS_HOME="$SCRIPT_DIR/apache-shardingsphere-5.5.0-shardingsphere-proxy-bin"
PID_FILE="$SCRIPT_DIR/shardingsphere.pid"

if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    if kill -0 "$PID" 2>/dev/null; then
        kill "$PID" && echo "Stopped ShardingSphere Proxy (PID $PID)"
    fi
    rm -f "$PID_FILE"
else
    "$SS_HOME/bin/stop.sh" 2>/dev/null || true
    echo "ShardingSphere Proxy stopped"
fi
