#!/usr/bin/env bash
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

for shard in pgpool_shard1 pgpool_shard2; do
  pidfile="$SCRIPT_DIR/$shard/pgpool.pid"
  if [ -f "$pidfile" ]; then
    pid=$(cat "$pidfile")
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid"
      echo "Stopped $shard (pid $pid)"
    fi
    rm -f "$pidfile"
  fi
done
