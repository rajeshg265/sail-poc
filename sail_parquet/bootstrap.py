#!/usr/bin/env python3
"""
Sail bootstrap for sail_parquet benchmark.

Starts Sail SparkConnect on :50056, registers Parquet tables, exposes
HTTP query bridge on :8097.

Run AFTER generate_data.py has completed.
"""

import io
import os
import time
import contextlib
import logging
import datetime
from collections import deque
from pathlib import Path
from flask import Flask, request, jsonify

logging.basicConfig(level=logging.INFO, format="%(asctime)s [SAIL] %(message)s")
log = logging.getLogger("sail.parquet")

_log_buffer = deque(maxlen=300)

class _BufHandler(logging.Handler):
    def emit(self, record):
        _log_buffer.append({
            "ts":     datetime.datetime.utcnow().isoformat(timespec="milliseconds"),
            "engine": "sail",
            "msg":    record.getMessage(),
        })

_bh = _BufHandler()
log.addHandler(_bh)

DATA_DIR = str(Path(__file__).parent / "data")

SAIL_SPARK_PORT  = 50056
HTTP_BRIDGE_PORT = 8097

os.environ.setdefault("SAIL_MODE",                           "local")
os.environ.setdefault("SAIL_EXECUTION__DEFAULT_PARALLELISM", "4")
os.environ.setdefault("SAIL_SPARK__SESSION_TIMEOUT_SECS",    "86400")

# ── Start Sail ─────────────────────────────────────────────────────────────────

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.ipc as pa_ipc
from pysail.spark import SparkConnectServer
from pyspark.sql import SparkSession
from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition

# Module-level globals for mmap IPC path — cloudpickle serializes classes
# by name reference to these globals, not by value, so no large data is serialized.
_ARROW_IPC_PATH: str = ""
_ARROW_N_BATCHES: int = 0
_ARROW_N: int = 4
_ARROW_SCHEMA = None


class _ArrowReader(DataSourceReader):
    def __init__(self, col_names):
        self._cols = col_names  # projected columns DataFusion actually needs

    def partitions(self):
        return [InputPartition(i) for i in range(_ARROW_N)]

    def read(self, partition):
        import pyarrow as _pa
        import pyarrow.ipc as _ipc
        mmap = _pa.memory_map(_ARROW_IPC_PATH, "r")
        reader = _ipc.open_file(mmap)
        per = _ARROW_N_BATCHES // _ARROW_N
        start = partition.value * per
        end = (partition.value + 1) * per if partition.value < _ARROW_N - 1 else _ARROW_N_BATCHES
        for i in range(start, end):
            batch = reader.get_batch(i)
            if self._cols:
                batch = batch.select(self._cols)
            yield batch


class _ArrowDS(DataSource):
    @classmethod
    def name(cls):
        return "frtb_arrow"

    def schema(self):
        return _ARROW_SCHEMA

    def reader(self, schema):
        # schema is the projected schema DataFusion needs — pass only those columns
        try:
            cols = [f.name for f in schema]
        except Exception:
            cols = []
        return _ArrowReader(cols)

# Read row counts from Parquet file footers at startup (reads only metadata, no data scan)
_FILE_ROWS: dict = {}
for _f in Path(DATA_DIR).glob("*.parquet"):
    try:
        _FILE_ROWS[_f.name] = pq.read_metadata(str(_f)).num_rows
    except Exception:
        pass

print("Starting Sail SparkConnect server ...", flush=True)
sail_server = SparkConnectServer(port=SAIL_SPARK_PORT)
sail_server.start(background=True)
time.sleep(3)

spark = (
    SparkSession.builder
    .remote(f"sc://localhost:{SAIL_SPARK_PORT}")
    .appName("FRTB_Parquet_Sail")
    .getOrCreate()
)

# ── Register tables from Parquet ───────────────────────────────────────────────

def _register_tables():
    print("Registering tables ...", flush=True)

    # ── Arrow DataSource: mmap IPC — zero-copy, no Parquet decode at query time ─
    global _ARROW_IPC_PATH, _ARROW_N_BATCHES, _ARROW_SCHEMA
    ipc_path = f"{DATA_DIR}/parquet1.arrow"
    t0 = time.time()
    _ARROW_IPC_PATH = ipc_path
    mmap = pa.memory_map(ipc_path, "r")
    rdr = pa_ipc.open_file(mmap)
    _ARROW_N_BATCHES = rdr.num_record_batches
    _ARROW_SCHEMA = rdr.schema
    elapsed = round(time.time() - t0, 1)
    size_gb = os.path.getsize(ipc_path) / 1024**3
    print(f"  [arrow]  frtb_arrow  27,370,000 rows  {size_gb:.2f} GB IPC mmap  {_ARROW_N_BATCHES} batches  [{elapsed}s]", flush=True)

    spark.dataSource.register(_ArrowDS)
    spark.sql("CREATE TABLE IF NOT EXISTS frtb_arrow USING frtb_arrow")
    print("  [arrow]  frtb_arrow registered.", flush=True)

    # ── Disk mode: parquet1 as temp view ──────────────────────────────────────
    spark.read.parquet(f"{DATA_DIR}/parquet1.parquet") \
        .createOrReplaceTempView("frtb_disk")
    print(f"  [disk]   frtb_disk -> {DATA_DIR}/parquet1.parquet", flush=True)

    # ── Lookup tables (tiny — disk is fine for all modes) ─────────────────────
    spark.read.parquet(f"{DATA_DIR}/entity_hierarchy.parquet").createOrReplaceTempView("entity_hierarchy")
    spark.read.parquet(f"{DATA_DIR}/chorushierarchy.parquet").createOrReplaceTempView("chorushierarchy")
    spark.read.parquet(f"{DATA_DIR}/cobdate.parquet").createOrReplaceTempView("cobdate")
    print("  entity_hierarchy, chorushierarchy, cobdate -> disk temp views", flush=True)

    print("Tables registered.", flush=True)

_register_tables()

# ── HTTP bridge ────────────────────────────────────────────────────────────────

app = Flask(__name__)

def _log_data_source(sql: str) -> None:
    """Run EXPLAIN ANALYZE and log actual rows scanned, files, and columns."""
    try:
        rows = spark.sql(f"EXPLAIN ANALYZE {sql}").collect()
        plan = rows[0][0] if rows else ""

        import re
        has_parquet = "DataSourceExec" in plan and "file_type=parquet" in plan
        has_memory  = "partition_sizes=" in plan

        if has_parquet:
            files = list(dict.fromkeys(re.findall(r'[\w/\-\.]+\.parquet', plan)))
            projections = re.findall(r'projection=\[([^\]]+)\]', plan)
            cols = list(dict.fromkeys(
                c.strip().split('@')[0] for p in projections for c in p.split(',')
            )) if projections else []
            # Memory catalog files have UUID names (no path separators, not our named files)
            _disk_names = {"parquet1.parquet", "parquet2.parquet",
                           "entity_hierarchy.parquet", "chorushierarchy.parquet", "cobdate.parquet"}
            disk_files  = [os.path.basename(f) for f in files if os.path.basename(f) in _disk_names]
            mem_files   = [os.path.basename(f) for f in files if os.path.basename(f) not in _disk_names]
            _si = {'K': 1_000, 'M': 1_000_000, 'G': 1_000_000_000}
            scanned = 0
            for line in plan.split('\n'):
                if 'DataSourceExec' in line and 'file_type=parquet' in line:
                    m = re.search(r'output_rows=([\d.]+)\s*([KMG])?', line)
                    if m:
                        scanned += int(float(m.group(1)) * _si.get(m.group(2) or '', 1))
            rows_str = f"{scanned:,}" if scanned else "unknown"
            if disk_files:
                log.info("READ FROM: Parquet (disk)  files: %s  rows_scanned: %s  columns: %s",
                         disk_files, rows_str, cols)
            if mem_files:
                log.info("READ FROM: memory_catalog (memory:///)  files: %s  rows_scanned: %s  columns: %s",
                         mem_files, rows_str, cols)
        if has_memory:
            mem_cols = []
            lines = plan.split('\n')
            for i, line in enumerate(lines):
                if 'partition_sizes=' in line:
                    for j in range(i - 1, max(0, i - 5), -1):
                        if 'ProjectionExec' in lines[j]:
                            mem_cols = list(dict.fromkeys(re.findall(r'(\w+)@\d+', lines[j])))
                            break
                    break
            log.info("READ FROM: Memory (in-memory DataFrame)  columns: %s", mem_cols)
        if not has_parquet and not has_memory:
            if "DataSourceExec" in plan:
                file_type = re.search(r'file_type=(\w+)', plan)
                log.info("READ FROM: DataSourceExec [%s]", file_type.group(1) if file_type else "unknown")
            else:
                log.info("READ FROM: unknown  plan_nodes=%s", re.findall(r'\w+Exec', plan))
    except Exception as e:
        log.warning("Could not inspect plan: %s", e)


@app.route("/query", methods=["POST"])
def query():
    """Arrow DataSource path — frtb_sa_capital_output rewrites to frtb_arrow."""
    sql = (request.get_json() or {}).get("sql", "").strip()
    if not sql:
        return jsonify({"error": "missing sql"}), 400
    arrow_sql = sql.replace("frtb_sa_capital_output", "frtb_arrow")
    try:
        t0 = time.time()
        rows = spark.sql(arrow_sql).collect()
        elapsed_ms = (time.time() - t0) * 1000
        log.info("ARROW RESULT: %.0f ms — %d rows returned", elapsed_ms, len(rows))
        return jsonify([row.asDict() for row in rows])
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/query_arrow", methods=["POST"])
def query_arrow():
    """Executes against frtb_arrow (Python DataSource — true zero-copy Arrow path)."""
    sql = (request.get_json() or {}).get("sql", "").strip()
    if not sql:
        return jsonify({"error": "missing sql"}), 400
    arrow_sql = sql.replace("frtb_sa_capital_output", "frtb_arrow")
    try:
        t0 = time.time()
        rows = spark.sql(arrow_sql).collect()
        elapsed_ms = (time.time() - t0) * 1000
        log.info("ARROW RESULT: %.0f ms — %d rows returned", elapsed_ms, len(rows))
        return jsonify([row.asDict() for row in rows])
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/query_disk", methods=["POST"])
def query_disk():
    """Same as /query but rewrites the main table name to frtb_disk (parquet1 on disk)."""
    sql = (request.get_json() or {}).get("sql", "").strip()
    if not sql:
        return jsonify({"error": "missing sql"}), 400
    disk_sql = sql.replace("frtb_sa_capital_output", "frtb_disk")
    try:
        t0 = time.time()
        rows = spark.sql(disk_sql).collect()
        elapsed_ms = (time.time() - t0) * 1000
        log.info("DISK RESULT: %.0f ms — %d rows returned", elapsed_ms, len(rows))
        return jsonify([row.asDict() for row in rows])
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "engine": "sail", "port": SAIL_SPARK_PORT})

@app.route("/logs", methods=["GET"])
def logs():
    since = request.args.get("since", "")
    entries = [e for e in _log_buffer if not since or e["ts"] > since]
    return jsonify(entries)

if __name__ == "__main__":
    import logging
    logging.getLogger("werkzeug").setLevel(logging.WARNING)
    print(f"Sail HTTP bridge: http://localhost:{HTTP_BRIDGE_PORT}/", flush=True)
    app.run(host="0.0.0.0", port=HTTP_BRIDGE_PORT, threaded=True)
