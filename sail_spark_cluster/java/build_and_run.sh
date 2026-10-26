#!/usr/bin/env bash
# Compiles QueryService and runs it in SAIL, SHARDING, or SERVER mode.
#
# Usage:
#   ./build_and_run.sh                          # server mode (default) — HTTP on :8090
#   ./build_and_run.sh --mode server            # HTTP server on :8090 (for Flask UI)
#   ./build_and_run.sh --mode server --port 9090
#   ./build_and_run.sh --mode sail              # CLI demo — direct Sail queries
#   ./build_and_run.sh --mode sharding          # CLI demo — direct ShardingSphere queries
#
# SERVER mode is the normal startup path for the Flask UI (ui_server.py).
# SHARDING mode requires ShardingSphere Proxy running on :3307.
# Start it first: ../infra/start_shardingsphere.sh
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC="$SCRIPT_DIR/src/main/java/com/frtb/queryservice/QueryService.java"
LIB="$SCRIPT_DIR/lib"
OUT="$SCRIPT_DIR/target/classes"

# Classpath for compilation: Arrow JAR (unused at runtime but needed to compile against)
# and PostgreSQL JDBC from ShardingSphere Proxy lib
SS_LIB="$SCRIPT_DIR/../infra/apache-shardingsphere-5.5.0-shardingsphere-proxy-bin/lib"
PG_JAR="$SS_LIB/postgresql-42.7.2.jar"

COMPILE_CP="$PG_JAR"
[ -f "$LIB/flight-sql-jdbc-all.jar" ] && COMPILE_CP="$COMPILE_CP:$LIB/flight-sql-jdbc-all.jar"

# SparkConnect + Arrow jars needed at compile time (sc_arrow mode)
PYSPARK_DIR_COMPILE=$(python3 -c "import pyspark; import os; print(os.path.dirname(pyspark.__file__))" 2>/dev/null || true)
if [ -n "$PYSPARK_DIR_COMPILE" ] && [ -d "$PYSPARK_DIR_COMPILE/jars" ]; then
    COMPILE_CP="$COMPILE_CP:$PYSPARK_DIR_COMPILE/jars/*"
    SC_CLIENT_JAR_C="$PYSPARK_DIR_COMPILE/jars/connect-repl/spark-connect-client-jdbc_2.13-4.2.0.jar"
    [ -f "$SC_CLIENT_JAR_C" ] && COMPILE_CP="$COMPILE_CP:$SC_CLIENT_JAR_C"
fi

mkdir -p "$OUT"

echo "Compiling…"
javac -cp "$COMPILE_CP" -d "$OUT" "$SRC"

FLIGHT_JAR=""
[ -f "$LIB/flight-sql-jdbc-all.jar" ] && FLIGHT_JAR=":$LIB/flight-sql-jdbc-all.jar"

# SparkConnect JDBC jars (all pyspark jars via wildcard — easiest to include all
# rather than chasing transitive deps one by one)
PYSPARK_DIR=$(python3 -c "import pyspark; import os; print(os.path.dirname(pyspark.__file__))" 2>/dev/null || true)
SC_JARS=""
if [ -n "$PYSPARK_DIR" ] && [ -d "$PYSPARK_DIR/jars" ]; then
    SC_JARS=":$PYSPARK_DIR/jars/*"
    SC_CLIENT_JAR="$PYSPARK_DIR/jars/connect-repl/spark-connect-client-jdbc_2.13-4.2.0.jar"
    [ -f "$SC_CLIENT_JAR" ] && SC_JARS="$SC_JARS:$SC_CLIENT_JAR"
fi

echo "Running QueryService $*…"
java --add-opens=java.base/java.nio=ALL-UNNAMED \
     --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
     --add-opens=java.base/sun.util.calendar=ALL-UNNAMED \
     -cp "$OUT:$PG_JAR$FLIGHT_JAR$SC_JARS" \
     com.frtb.queryservice.QueryService "$@"
