#!/usr/bin/env bash
# Stop sail_sc_arrow
LOG_DIR="$HOME/.frtb_sc_arrow_logs"

for PIDFILE in bootstrap_sc_arrow.pid bootstrap_spark_sc_arrow.pid java_sc_arrow.pid ui_sc_arrow.pid; do
  PPATH="$LOG_DIR/$PIDFILE"
  if [ -f "$PPATH" ]; then
    OLD_PID=$(cat "$PPATH" 2>/dev/null)
    [ -n "$OLD_PID" ] && kill "$OLD_PID" 2>/dev/null && echo "Stopped $PIDFILE (PID $OLD_PID)"
    rm -f "$PPATH"
  fi
done

for PORT in 50075 8097 7075 8092 8100 5053; do
  PIDS=$(lsof -ti :$PORT 2>/dev/null) && [ -n "$PIDS" ] && kill $PIDS 2>/dev/null && echo "Freed port $PORT"
done

echo "sail_sc_arrow stopped."
