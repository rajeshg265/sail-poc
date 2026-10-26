#!/usr/bin/env python3
"""
Sail bootstrap — starts Spark Connect (50051) + HTTP query bridge (8080)
in the same process, registers all datasets from shard_registry.yaml,
and creates UNION ALL logical views for cross-shard queries.

Architecture:
    PostgreSQL frtb_shard1  <-- (pgpool :6432 on cloud/Linux)
    PostgreSQL frtb_shard2  <-- (pgpool :6433 on cloud/Linux)
                                    |
                              Sail (this process)
                             /              \
                SparkConnect :50051    HTTP bridge :8080
                                            |
                                      Java QueryService
                                   (POST /query → SQL → Arrow/JSON)

Metadata-driven: shard_registry.yaml defines all datasets and shards.
Adding a new dataset = add entry to registry + restart. No other changes.

Two execution engines are supported (SAIL_ENGINE env var):
  http       (default) — Java QueryService / UI server → HTTP bridge :8080 → SparkConnect
  flightsql             — Java QueryService / UI server → FlightSqlClient → FlightSQL :32010

FlightSQL session bridge: at startup, each PostgreSQL shard table is read
directly via connectorX (PG → Arrow IPC, bypassing SparkConnect entirely) into
per-shard Arrow files under /tmp/sail_flightsql/<view>/<shard>.arrow.  Each view
is registered once as a DataFusion EXTERNAL TABLE (STORED AS ARROW LOCATION dir/).
ui_server.py refreshes the Arrow files from pgpool before every FlightSQL query
so data is always live (DataFusion re-scans the directory at each query).
"""
import json
import os
import sys
import threading
import time
import signal
import logging
import decimal
import datetime
from pathlib import Path

import yaml
import pyarrow as pa
import pyarrow.ipc as pa_ipc
import adbc_driver_flightsql.dbapi as flightsql_dbapi
import connectorx as cx
from pysail.spark import SparkConnectServer
from pysail.flight import FlightSqlServer
from pyspark.sql import SparkSession
from flask import Flask, request, jsonify

sys.path.insert(0, str(Path(__file__).parent))
from csm import get_secret

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("sail.bootstrap")

# Ports are configurable so a second instance (production server) can run alongside dev.
# Dev defaults:    SAIL_SPARK_PORT=50051  SAIL_FLIGHT_PORT=32010  SAIL_WORKER_PORTS=8080,8081,8082
# Prod invocation: SAIL_SPARK_PORT=50053  SAIL_FLIGHT_PORT=32011  SAIL_WORKER_PORTS=8083,8084,8085
SPARK_PORT    = int(os.environ.get("SAIL_SPARK_PORT",  "50075"))
FLIGHT_PORT   = int(os.environ.get("SAIL_FLIGHT_PORT", "32010"))
WORKER_PORTS  = [int(p) for p in os.environ.get("SAIL_WORKER_PORTS", "8080,8081,8082").split(",")]

# Number of real Sail DataFusion cluster workers.
# 1 → local mode (single-process, threaded).  ≥2 → local-cluster mode (RPC between driver + workers).
# Dev default = 1 (local). Production server is started with SAIL_WORKER_COUNT=3 (local-cluster).
SAIL_WORKER_COUNT = int(os.environ.get("SAIL_WORKER_COUNT", "3"))

# Execution engine: "http" (default) uses the HTTP bridge + SparkConnect session.
# "flightsql" uses Arrow FlightSQL + Arrow-backed views registered in the
# FlightSQL DataFusion session (true zero-copy Arrow end-to-end).
SAIL_ENGINE = os.environ.get("SAIL_ENGINE", "http").lower()
FLIGHTSQL_ARROW_DIR = Path(os.environ.get("SAIL_ARROW_DIR", "/tmp/sail_flightsql"))

# Metadata service — exposes session discovery endpoint for QueryService.
# SAIL_ENGINE=sparkconnect: QueryService polls GET /session until ready=true,
# then attaches to the Spark Connect session via jdbc:sc://host:port/;session_id=<id>.
SAIL_META_PORT = int(os.environ.get("SAIL_META_PORT", "7075"))

_session_state: dict = {
    "session_id": None,
    "sc_host":    "127.0.0.1",
    "sc_port":    SPARK_PORT,
    "ready":      False,
}


def _start_metadata_server() -> None:
    """Start a minimal HTTP server on SAIL_META_PORT serving GET /session."""
    from http.server import BaseHTTPRequestHandler, HTTPServer

    class _Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path != "/session":
                self.send_response(404); self.end_headers(); return
            body = json.dumps(_session_state).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass  # suppress per-request access logs

    srv = HTTPServer(("127.0.0.1", SAIL_META_PORT), _Handler)
    t = threading.Thread(target=srv.serve_forever, daemon=True, name="sail-meta")
    t.start()
    log.info("Sail metadata server listening on http://127.0.0.1:%d/session", SAIL_META_PORT)


def _configure_sail_mode() -> None:
    os.environ["SAIL_MODE"] = "local"
    log.info("Sail mode: local (threaded, all CPU cores)")

PG_DRIVER = "org.postgresql.Driver"

REGISTRY_PATH = Path(__file__).parent / "shard_registry.yaml"

_spark: SparkSession   = None   # module-level for the Flask handlers
_registry: dict        = {}     # loaded once in main(), used by session recovery
_shard_creds: dict     = {}     # same


# ── Helpers ───────────────────────────────────────────────────────────────────

def _jdbc_url(host: str, port: int, db: str) -> str:
    return f"jdbc:postgresql://{host}:{port}/{db}"


def _load_table(spark: SparkSession, host: str, port: int, db: str,
                user: str, password: str, table: str, view_name: str,
                num_partitions: int = 1,
                partition_column: str | None = None,
                partition_lower_bound: int | None = None,
                partition_upper_bound: int | None = None) -> None:
    url = _jdbc_url(host, port, db)
    reader = (
        spark.read
        .format("jdbc")
        .option("url",               url)
        .option("dbtable",           table)
        .option("user",              user)
        .option("password",          password)
        .option("driver",            PG_DRIVER)
        .option("numPartitions",     str(num_partitions))
        .option("pushDownPredicate", "true")
    )
    if num_partitions > 1 and partition_column:
        reader = (
            reader
            .option("partitionColumn", partition_column)
            .option("lowerBound",      str(partition_lower_bound))
            .option("upperBound",      str(partition_upper_bound))
        )
    reader.load().createOrReplaceTempView(view_name)
    log.info("  %-40s registered (lazy, %d partition(s))", view_name, num_partitions)


def _json_default(obj):
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    if isinstance(obj, (datetime.date, datetime.datetime)):
        return obj.isoformat()
    raise TypeError(f"Not serializable: {type(obj)}")


# ── Registry-driven dataset registration ──────────────────────────────────────

def _register_datasets(spark: SparkSession, registry: dict,
                        shard_creds: dict) -> None:
    """
    Read shard_registry.yaml and register all datasets + broadcast tables
    as Spark temp views.

    - Multi-shard datasets (e.g. frtb_sa_capital_output across shard1/2/3):
      Each shard loaded as a per-shard temp view, then combined as a UNION ALL
      logical view. Range WHERE clauses derived from key_range are embedded in
      the UNION ALL so DataFusion can prune irrelevant shard branches at
      query time when a cobdate filter is present.

    - Single-shard domain datasets (equity, credit, macro):
      Loaded directly as the logical view — no UNION ALL wrapper needed.
    """
    shards = registry.get("shards", {})

    # ── Sharded / domain datasets ─────────────────────────────────────────────
    for ds in registry.get("datasets", []):
        ds_name        = ds["name"]
        table          = ds["table"]
        ds_shards      = ds.get("shards", list(shards.keys()))
        num_parts      = ds.get("num_partitions", 1)
        part_col       = ds.get("partition_column")
        part_lo        = ds.get("partition_lower_bound", 0)
        part_hi        = ds.get("partition_upper_bound", 100)

        log.info("Registering dataset '%s' across shards: %s (numPartitions=%d)", ds_name, ds_shards, num_parts)

        if len(ds_shards) == 1:
            # Single domain shard — register directly, no UNION ALL needed
            shard_id  = ds_shards[0]
            shard_cfg = shards[shard_id]
            user, pw  = shard_creds[shard_id]
            _load_table(
                spark,
                shard_cfg["host"], shard_cfg["port"], shard_cfg["database"],
                user, pw,
                table, ds_name,
                num_partitions=num_parts, partition_column=part_col,
                partition_lower_bound=part_lo, partition_upper_bound=part_hi,
            )
            log.info("Domain view '%s' ready (single shard: %s)", ds_name, shard_id)
            continue

        # Multi-shard: load each shard directly by table name, then UNION ALL.
        # Data is already correctly partitioned per shard so no date filter needed.
        union_parts = []
        for shard_id in ds_shards:
            shard_cfg = shards[shard_id]
            user, pw  = shard_creds[shard_id]
            view_name = f"{ds_name}_{shard_id}_view"
            _load_table(
                spark,
                shard_cfg["host"], shard_cfg["port"], shard_cfg["database"],
                user, pw,
                table, view_name,
                num_partitions=num_parts, partition_column=part_col,
                partition_lower_bound=part_lo, partition_upper_bound=part_hi,
            )
            union_parts.append(f"SELECT * FROM {view_name}")

        union_sql = "\nUNION ALL\n".join(union_parts)
        spark.sql(f"CREATE OR REPLACE TEMP VIEW {ds_name} AS {union_sql}")
        log.info("Unified view '%s' ready (table scan, pushdown enabled)", ds_name)

    # ── Broadcast tables ──────────────────────────────────────────────────────
    for bt in registry.get("broadcast_tables", []):
        bt_name    = bt["name"]
        table      = bt["table"]
        source_id  = bt.get("source_shard", list(shards.keys())[0])
        shard_cfg  = shards[source_id]
        user, pw   = shard_creds[source_id]

        log.info("Registering broadcast table '%s' from %s", bt_name, source_id)
        _load_table(
            spark,
            shard_cfg["host"], shard_cfg["port"], shard_cfg["database"],
            user, pw,
            table, bt_name,
        )
        log.info("Broadcast view '%s' ready", bt_name)


# ── HTTP bridge ───────────────────────────────────────────────────────────────

def _start_http_bridge(spark: SparkSession) -> None:
    app      = Flask(__name__)
    log_flask = logging.getLogger("werkzeug")
    log_flask.setLevel(logging.WARNING)

    # Keep-alive heartbeat: pySail evicts idle SparkConnect sessions quickly.
    # Use _spark global so the thread stays current after session recovery.
    def _keepalive():
        while True:
            time.sleep(20)
            try:
                _spark.sql("SELECT 1").collect()
            except Exception as exc:
                log.warning("Keep-alive failed (session evicted?): %s", exc)

    threading.Thread(target=_keepalive, daemon=True, name="spark-keepalive").start()
    log.info("SparkConnect keep-alive thread started (20 s interval)")

    @app.route("/query", methods=["POST"])
    def query():
        payload = request.get_json(force=True, silent=True) or {}
        sql = payload.get("sql") or (request.data.decode() if request.data else "")
        if not sql:
            return jsonify({"error": "missing 'sql' field"}), 400
        try:
            rows = spark.sql(sql).collect()
            data = [row.asDict() for row in rows]
            return app.response_class(
                response=json.dumps(data, default=_json_default),
                status=200,
                mimetype="application/json",
            )
        except Exception as exc:
            return jsonify({"error": str(exc)}), 500

    @app.route("/health", methods=["GET"])
    def health():
        return jsonify({"status": "ok"})

    for port in WORKER_PORTS:
        t = threading.Thread(
            target=lambda p=port: app.run(host="127.0.0.1", port=p, use_reloader=False),
            daemon=True,
        )
        t.start()
        log.info("HTTP worker started on port %d", port)


def _start_flightsql_keepalive(conn) -> None:
    """Keep the FlightSQL 'flight-default' session alive so registered EXTERNAL
    TABLEs survive idle periods.  Sail evicts the session when the last connection
    closes; this thread holds a persistent connection and pings every 60 s."""
    def _keepalive():
        _conn = conn
        while True:
            time.sleep(60)
            try:
                cur = _conn.cursor()
                cur.execute("SELECT 1")
                cur.fetchall()
                cur.close()
            except Exception as exc:
                log.warning("FlightSQL keep-alive failed (session evicted?): %s", exc)

    threading.Thread(target=_keepalive, daemon=True, name="flightsql-keepalive").start()
    log.info("FlightSQL keep-alive thread started (60 s interval)")


def _start_flightsql_refresh(registry: dict, shard_creds: dict, interval: int = 60) -> None:
    """Refresh Arrow IPC files from pgpool on a fixed interval.

    Keeps FlightSQL views live without per-query data movement in the UI layer.
    DataFusion re-scans the Arrow IPC directory on every query, so this thread
    is the only place that touches the files after startup — ui_server.py never
    writes them.
    """
    shards = registry.get("shards", {})

    def _refresh():
        while True:
            time.sleep(interval)
            try:
                for ds in registry.get("datasets", []):
                    ds_name   = ds["name"]
                    table     = ds["table"]
                    ds_shards = ds.get("shards", list(shards.keys()))
                    view_dir  = FLIGHTSQL_ARROW_DIR / ds_name
                    view_dir.mkdir(parents=True, exist_ok=True)
                    for shard_id in ds_shards:
                        shard_cfg = shards[shard_id]
                        user, pw  = shard_creds[shard_id]
                        pg_url    = (
                            f"postgresql://{user}:{pw}"
                            f"@{shard_cfg['host']}:{shard_cfg['port']}/{shard_cfg['database']}"
                        )
                        arrow_path = view_dir / f"{shard_id}.arrow"
                        try:
                            n = _write_arrow_shard(pg_url, table, arrow_path)
                            log.debug("FlightSQL refresh: %s/%s → %d rows", ds_name, shard_id, n)
                        except Exception as exc:
                            log.warning("FlightSQL refresh failed %s/%s: %s", ds_name, shard_id, exc)

                for bt in registry.get("broadcast_tables", []):
                    source_id = bt.get("source_shard", list(shards.keys())[0])
                    shard_cfg = shards[source_id]
                    user, pw  = shard_creds[source_id]
                    pg_url    = (
                        f"postgresql://{user}:{pw}"
                        f"@{shard_cfg['host']}:{shard_cfg['port']}/{shard_cfg['database']}"
                    )
                    view_dir   = FLIGHTSQL_ARROW_DIR / bt["name"]
                    view_dir.mkdir(parents=True, exist_ok=True)
                    arrow_path = view_dir / f"{source_id}.arrow"
                    try:
                        n = _write_arrow_shard(pg_url, bt["table"], arrow_path)
                        log.debug("FlightSQL refresh: %s/%s → %d rows", bt["name"], source_id, n)
                    except Exception as exc:
                        log.warning("FlightSQL refresh failed %s/%s: %s", bt["name"], source_id, exc)

                log.info("FlightSQL Arrow IPC views refreshed from pgpool")
            except Exception as exc:
                log.warning("FlightSQL background refresh error: %s", exc)

    threading.Thread(target=_refresh, daemon=True, name="flightsql-refresh").start()
    log.info("FlightSQL data refresh thread started (%ds interval)", interval)


# ── FlightSQL session bridge ──────────────────────────────────────────────────

def _write_arrow_shard(pg_url: str, table: str, arrow_path: Path) -> int:
    """Read one PostgreSQL table via connectorX and write as Arrow IPC file.
    Returns row count. Uses atomic tmp→rename to avoid partial reads."""
    arrow_table = cx.read_sql(pg_url, f"SELECT * FROM {table}", return_type="arrow")
    tmp_path = arrow_path.parent / (arrow_path.name + ".tmp")
    with pa.OSFile(str(tmp_path), "wb") as f:
        writer = pa_ipc.RecordBatchFileWriter(f, arrow_table.schema)
        writer.write_table(arrow_table)
        writer.close()
    tmp_path.rename(arrow_path)
    return arrow_table.num_rows


def _register_flightsql_views(registry: dict, shard_creds: dict) -> None:
    """
    Register live PostgreSQL tables in the FlightSQL DataFusion session.

    Each shard is read directly via connectorX (PostgreSQL → Arrow IPC, bypassing
    SparkConnect entirely) and written to /tmp/sail_flightsql/<view>/<shard>.arrow.
    Each view is registered as a DataFusion EXTERNAL TABLE (STORED AS ARROW) pointing
    to its directory — DataFusion unions the shard files and re-scans on every query,
    so background refresh only needs to overwrite Arrow files (no DDL re-execution).
    """
    FLIGHTSQL_ARROW_DIR.mkdir(parents=True, exist_ok=True)

    shards = registry.get("shards", {})
    conn   = flightsql_dbapi.connect(f"grpc://127.0.0.1:{FLIGHT_PORT}")
    cur    = conn.cursor()
    registered = 0

    # ── Multi-shard + single-shard datasets ─────────────────────────────────
    for ds in registry.get("datasets", []):
        ds_name   = ds["name"]
        table     = ds["table"]
        ds_shards = ds.get("shards", list(shards.keys()))

        view_dir = FLIGHTSQL_ARROW_DIR / ds_name
        view_dir.mkdir(parents=True, exist_ok=True)
        any_written = False

        for shard_id in ds_shards:
            shard_cfg = shards[shard_id]
            user, pw  = shard_creds[shard_id]
            pg_url    = (
                f"postgresql://{user}:{pw}"
                f"@{shard_cfg['host']}:{shard_cfg['port']}/{shard_cfg['database']}"
            )
            arrow_path = view_dir / f"{shard_id}.arrow"
            try:
                n = _write_arrow_shard(pg_url, table, arrow_path)
                log.info("  FlightSQL: %s / %s → %d rows", ds_name, shard_id, n)
                any_written = True
            except Exception as exc:
                log.warning("  FlightSQL: %s / %s read failed: %s", ds_name, shard_id, exc)

        if any_written:
            try:
                cur.execute(f"DROP TABLE IF EXISTS {ds_name}")
                cur.execute(
                    f"CREATE EXTERNAL TABLE {ds_name} "
                    f"STORED AS ARROW LOCATION '{view_dir}/'"
                )
                registered += 1
                log.info("  FlightSQL: '%s' registered (STORED AS ARROW)", ds_name)
            except Exception as exc:
                log.warning("  FlightSQL: registration of '%s' failed: %s", ds_name, exc)

    # ── Broadcast tables ─────────────────────────────────────────────────────
    for bt in registry.get("broadcast_tables", []):
        bt_name   = bt["name"]
        table     = bt["table"]
        source_id = bt.get("source_shard", list(shards.keys())[0])
        shard_cfg = shards[source_id]
        user, pw  = shard_creds[source_id]
        pg_url    = (
            f"postgresql://{user}:{pw}"
            f"@{shard_cfg['host']}:{shard_cfg['port']}/{shard_cfg['database']}"
        )
        view_dir   = FLIGHTSQL_ARROW_DIR / bt_name
        view_dir.mkdir(parents=True, exist_ok=True)
        arrow_path = view_dir / f"{source_id}.arrow"

        try:
            n = _write_arrow_shard(pg_url, table, arrow_path)
            cur.execute(f"DROP TABLE IF EXISTS {bt_name}")
            cur.execute(
                f"CREATE EXTERNAL TABLE {bt_name} "
                f"STORED AS ARROW LOCATION '{view_dir}/'"
            )
            registered += 1
            log.info("  FlightSQL: '%s' → %d rows registered", bt_name, n)
        except Exception as exc:
            log.warning("  FlightSQL: '%s' failed: %s", bt_name, exc)

    cur.close()
    log.info(
        "FlightSQL bridge complete: %d view(s) on grpc://127.0.0.1:%d",
        registered, FLIGHT_PORT,
    )
    return conn   # caller must keep this alive to prevent session eviction


# ── Main ──────────────────────────────────────────────────────────────────────

def _recover_session(sc_ip: str, sc_port: int) -> None:
    """Create a fresh SparkConnect session and re-register all datasets.
    Called by the heartbeat when the current session has been dropped by pySail.
    """
    global _spark
    log.info("Session recovery: creating new SparkConnect session on %s:%d…", sc_ip, sc_port)
    _session_state["ready"] = False
    # Stop the dead session to clear pySpark's internal singleton cache;
    # without this, getOrCreate() may return the same dead session object.
    try:
        _spark.stop()
    except Exception:
        pass
    try:
        # .create() always creates a new session (Spark 4.x Connect API),
        # bypassing getOrCreate()'s cached-session check.
        new_spark = (
            SparkSession.builder
            .remote(f"sc://{sc_ip}:{sc_port}")
            .create()
        )
        from pysail.spark.datasource.jdbc import JdbcDataSource
        new_spark.dataSource.register(JdbcDataSource)
        _register_datasets(new_spark, _registry, _shard_creds)
        _spark = new_spark
        _session_state["session_id"] = new_spark._client._session_id
        _session_state["ready"]      = True
        log.info("Session recovery succeeded: new session_id=%s", _session_state["session_id"])
    except Exception as e:
        log.error("Session recovery failed: %s", e)
        # leave ready=False so Java keeps retrying


def main() -> None:
    # ── 1. Load registry ────────────────────────────────────────────────────
    log.info("Loading shard registry from %s", REGISTRY_PATH)
    with open(REGISTRY_PATH) as f:
        registry = yaml.safe_load(f)

    cred_keys = registry.get("credentials", {})
    shards    = registry.get("shards", {})

    # Resolve credentials per shard
    shard_creds = {}
    for shard_id, shard_cfg in shards.items():
        user_key = cred_keys.get(f"{shard_id}_user_secret", f"FRTB_{shard_id.upper()}_DB_USER")
        pass_key = cred_keys.get(f"{shard_id}_pass_secret", f"FRTB_{shard_id.upper()}_DB_PASSWORD")
        user = get_secret(user_key) or "rajeshganta"
        pw   = get_secret(pass_key) or ""
        shard_creds[shard_id] = (user, pw)
        log.info("Credentials resolved for shard '%s' (user=%s)", shard_id, user)

    # Persist to module globals so session recovery can re-register datasets
    global _registry, _shard_creds
    _registry    = registry
    _shard_creds = shard_creds

    # ── 2. Configure Sail execution mode ────────────────────────────────────
    _configure_sail_mode()

    # ── 2b. Start metadata server (ready=False until datasets registered) ────
    _start_metadata_server()

    # ── 3. Start Sail servers ────────────────────────────────────────────────
    log.info("Starting Spark Connect server on port %d…", SPARK_PORT)
    spark_server = SparkConnectServer(ip="127.0.0.1", port=SPARK_PORT)
    spark_server.start(background=True)

    log.info("Starting Arrow Flight SQL server on port %d…", FLIGHT_PORT)
    flight_server = FlightSqlServer(ip="127.0.0.1", port=FLIGHT_PORT)
    flight_server.start(background=True)

    time.sleep(2)

    sc_ip, sc_port = spark_server.listening_address
    fl_ip, fl_port = flight_server.listening_address
    log.info("Spark Connect listening on %s:%d", sc_ip, sc_port)
    log.info("Flight SQL     listening on %s:%d", fl_ip, fl_port)

    # ── 4. Create Spark session ──────────────────────────────────────────────
    spark = (
        SparkSession.builder
        .remote(f"sc://{sc_ip}:{sc_port}")
        .getOrCreate()
    )
    log.info("Spark session created")

    from pysail.spark.datasource.jdbc import JdbcDataSource
    spark.dataSource.register(JdbcDataSource)
    log.info("JdbcDataSource registered")

    global _spark
    _spark = spark

    # Publish session id so QueryService can attach via jdbc:sc://
    _session_state["session_id"] = spark._client._session_id
    _session_state["sc_host"]    = sc_ip
    _session_state["sc_port"]    = sc_port
    log.info("Session id: %s (metadata server: http://127.0.0.1:%d/session)",
             _session_state["session_id"], SAIL_META_PORT)

    # ── 5. Register all datasets from registry ───────────────────────────────
    log.info("Registering datasets from shard registry…")
    _register_datasets(spark, registry, shard_creds)

    # Datasets registered — SparkConnect session is ready for JDBC attachment
    _session_state["ready"] = True
    log.info("Session ready (metadata: http://127.0.0.1:%d/session)", SAIL_META_PORT)

    # ── 7. Stay running ──────────────────────────────────────────────────────
    log.info("")
    log.info("=" * 62)
    log.info("Sail servers running. Press Ctrl-C to stop.")
    log.info("  Sail mode     : %s (%d DataFusion worker%s)",
             os.environ.get("SAIL_MODE", "local"), SAIL_WORKER_COUNT,
             "s" if SAIL_WORKER_COUNT != 1 else "")
    log.info("  Engine        : %s", SAIL_ENGINE)

    log.info("  Spark Connect : sc://127.0.0.1:%d", sc_port)
    log.info("  Flight SQL    : grpc://127.0.0.1:%d", fl_port)
    for p in WORKER_PORTS:
        log.info("  HTTP worker   : http://127.0.0.1:%d/query", p)
    log.info("  Registry      : %s", REGISTRY_PATH)
    log.info("=" * 62)

    def _shutdown(sig, frame):
        log.info("Shutting down…")
        spark_server.stop()
        flight_server.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT,  _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    _heartbeat_tick = 0
    while True:
        time.sleep(10)
        if not spark_server.running or not flight_server.running:
            log.error("A server stopped unexpectedly. Exiting.")
            sys.exit(1)
        _heartbeat_tick += 1
        if _heartbeat_tick % 3 == 0 and _session_state["ready"]:
            try:
                _spark.sql("SELECT 1 AS n").collect()
            except Exception as _hb_err:
                log.warning("Session heartbeat failed (%s) — recovering…", _hb_err)
                _recover_session(sc_ip, sc_port)


if __name__ == "__main__":
    main()
