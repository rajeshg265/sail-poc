#!/usr/bin/env python3
"""
Spark JVM bootstrap for sail_parquet benchmark.

Starts Spark local[4] (4 threads), registers Parquet tables, exposes
HTTP query bridge on :8100.

Run AFTER generate_data.py has completed.
"""

import os
import json
import re
import logging
import datetime
from collections import deque
from pathlib import Path
from flask import Flask, request, jsonify

logging.basicConfig(level=logging.INFO, format="%(asctime)s [SPARK] %(message)s")
log = logging.getLogger("spark.parquet")

_log_buffer = deque(maxlen=300)

class _BufHandler(logging.Handler):
    def emit(self, record):
        _log_buffer.append({
            "ts":     datetime.datetime.utcnow().isoformat(timespec="milliseconds"),
            "engine": "spark",
            "msg":    record.getMessage(),
        })

_bh = _BufHandler()
log.addHandler(_bh)

DATA_DIR = str(Path(__file__).parent / "data")

HTTP_BRIDGE_PORT = 8100

os.environ["SPARK_LOCAL_IP"]   = "127.0.0.1"
os.environ["PYSPARK_PYTHON"]   = os.sys.executable

import pyarrow.parquet as pq
from pathlib import Path
from pyspark.sql import SparkSession

_FILE_ROWS: dict = {}
for _f in Path(os.path.join(os.path.dirname(__file__), "data")).glob("*.parquet"):
    try:
        _FILE_ROWS[_f.name] = pq.read_metadata(str(_f)).num_rows
    except Exception:
        pass

print("Starting Spark JVM session ...", flush=True)
spark = (
    SparkSession.builder
    .master("local[4]")
    .appName("FRTB_Parquet_Spark")
    .config("spark.driver.host",               "127.0.0.1")
    .config("spark.driver.bindAddress",        "127.0.0.1")
    .config("spark.sql.shuffle.partitions",    "4")
    .config("spark.sql.adaptive.enabled",      "true")
    .config("spark.sql.parquet.filterPushdown","true")
    .config("spark.ui.enabled",                "false")
    .config("spark.driver.memory",             "2g")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ── Register tables from Parquet ───────────────────────────────────────────────

def _register_tables():
    print("Registering Parquet tables ...", flush=True)

    spark.read.parquet(f"{DATA_DIR}/parquet1.parquet") \
         .createOrReplaceTempView("frtb_sa_capital_output")

    spark.read.parquet(f"{DATA_DIR}/entity_hierarchy.parquet") \
         .createOrReplaceTempView("entity_hierarchy")

    spark.read.parquet(f"{DATA_DIR}/chorushierarchy.parquet") \
         .createOrReplaceTempView("chorushierarchy")

    spark.read.parquet(f"{DATA_DIR}/cobdate.parquet") \
         .createOrReplaceTempView("cobdate")

    row_count = spark.sql("SELECT COUNT(*) FROM frtb_sa_capital_output").collect()[0][0]
    print(f"  frtb_sa_capital_output  {row_count:,} rows (parquet1 only)")
    print("  entity_hierarchy, chorushierarchy, cobdate registered")
    print("Tables registered.", flush=True)

_register_tables()

# ── HTTP bridge ────────────────────────────────────────────────────────────────

app = Flask(__name__)

def _log_data_source(sql: str) -> None:
    """Run EXPLAIN ANALYZE and log actual rows scanned, files, and columns."""
    try:
        rows = spark.sql(f"EXPLAIN ANALYZE {sql}").collect()
        plan = "\n".join(r[0] for r in rows)

        has_parquet = "FileScan parquet" in plan
        has_memory  = "ExistingRDD" in plan or "InMemoryTableScan" in plan

        if has_parquet:
            files = list(dict.fromkeys(re.findall(r'[\w/\-\.]+\.parquet', plan)))
            col_groups = re.findall(r'FileScan parquet \[([^\]]+)\]', plan)
            cols = list(dict.fromkeys(
                c.split('#')[0] for group in col_groups for c in group.split(',')
            ))
            file_info = [os.path.basename(f) for f in files]
            # Spark EXPLAIN ANALYZE shows "number of output rows: N" per operator
            scan_rows = re.findall(r'FileScan.*?number of output rows: (\d+)', plan, re.DOTALL)
            scanned = sum(int(r) for r in scan_rows) if scan_rows else 0
            rows_str = f"{scanned:,}" if scanned else "unknown"
            log.info("READ FROM: Parquet  files: %s  rows_scanned: %s  columns: %s", file_info, rows_str, cols)
        if has_memory:
            rdd_match = re.search(r'ExistingRDD\[([^\]]+)\]', plan)
            mem_cols = [c.split('#')[0].strip() for c in rdd_match.group(1).split(',')] if rdd_match else []
            log.info("READ FROM: Memory (in-memory DataFrame)  columns: %s", mem_cols)
        if not has_parquet and not has_memory:
            log.info("READ FROM: unknown  plan_nodes=%s", re.findall(r'\w+\(', plan)[:6])
    except Exception as e:
        log.warning("Could not inspect plan: %s", e)


@app.route("/query", methods=["POST"])
def query():
    import time
    sql = (request.get_json() or {}).get("sql", "").strip()
    if not sql:
        return jsonify({"error": "missing sql"}), 400
    try:
        t0 = time.time()
        rows = [row.asDict() for row in spark.sql(sql).collect()]
        elapsed_ms = (time.time() - t0) * 1000
        log.info("RESULT: %.0f ms — %d rows returned", elapsed_ms, len(rows))
        _log_data_source(sql)  # runs after timing — EXPLAIN ANALYZE is a second execution
        return jsonify(rows)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "engine": "spark", "master": "local[4]"})

@app.route("/logs", methods=["GET"])
def logs():
    since = request.args.get("since", "")
    entries = [e for e in _log_buffer if not since or e["ts"] > since]
    return jsonify(entries)

if __name__ == "__main__":
    import logging
    logging.getLogger("werkzeug").setLevel(logging.WARNING)
    print(f"Spark HTTP bridge: http://localhost:{HTTP_BRIDGE_PORT}/", flush=True)
    app.run(host="0.0.0.0", port=HTTP_BRIDGE_PORT, threaded=True)
