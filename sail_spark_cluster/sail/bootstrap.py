#!/usr/bin/env python3
"""
sail_spark_cluster — Sail bootstrap (SC Arrow, local-cluster mode).

Architecture:
    pgpool :6440/:6441 → PostgreSQL shards
    PySpark → SparkConnect :50055 → Sail server (manages SAIL_WORKER_COUNT
              DataFusion workers internally via Arrow Flight gRPC)
    Single HTTP bridge :8096 → ui_server.py

Sail handles all worker coordination internally; no HTTP fanout needed.
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

import shutil
import socket
import subprocess

import yaml
import pyarrow as pa
import pyarrow.ipc as pa_ipc
import connectorx as cx
from pysail.spark import SparkConnectServer
from pyspark.sql import SparkSession
from flask import Flask, request, jsonify

sys.path.insert(0, str(Path(__file__).parent))
from csm import get_secret

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("sail.cluster.bootstrap")

# Cluster-specific ports — no overlap with sail_spark (50051/8080-8085/7070)
SPARK_PORT    = int(os.environ.get("SAIL_SPARK_PORT",  "50055"))
BRIDGE_PORT   = int(os.environ.get("SAIL_BRIDGE_PORT", "8096"))
SAIL_META_PORT = int(os.environ.get("SAIL_META_PORT",  "7073"))

# Sail manages these workers internally via Arrow Flight — we just tell it how many
SAIL_WORKER_COUNT = int(os.environ.get("SAIL_WORKER_COUNT", "3"))

PG_DRIVER    = "org.postgresql.Driver"
REGISTRY_PATH = Path(__file__).parent / "shard_registry.yaml"

_spark: SparkSession = None
_registry: dict      = {}
_shard_creds: dict   = {}

_session_state: dict = {
    "session_id": None,
    "sc_host":    "127.0.0.1",
    "sc_port":    SPARK_PORT,
    "ready":      False,
}


def _start_metadata_server() -> None:
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
            pass

    srv = HTTPServer(("127.0.0.1", SAIL_META_PORT), _Handler)
    threading.Thread(target=srv.serve_forever, daemon=True, name="sail-meta").start()
    log.info("Metadata server on http://127.0.0.1:%d/session", SAIL_META_PORT)


SAIL_CLUSTER_MODE = os.environ.get("SAIL_CLUSTER_MODE", "local-cluster")
SAIL_DRIVER_FLIGHT_PORT = int(os.environ.get("SAIL_DRIVER_FLIGHT_PORT", "50057"))



def _configure_sail_mode() -> None:
    os.environ["SAIL_MODE"] = SAIL_CLUSTER_MODE
    os.environ.setdefault("SAIL_CLUSTER__WORKER_INITIAL_COUNT", "3")
    os.environ.setdefault("SAIL_CLUSTER__WORKER_MAX_COUNT",     "3")
    os.environ.setdefault("SAIL_EXECUTION__DEFAULT_PARALLELISM", "3")
    os.environ.setdefault("SAIL_CLUSTER__WORKER_TASK_SLOTS", "2")
    os.environ.setdefault("SAIL_CLUSTER__SHUFFLE_BACKEND__FLIGHT__COMPRESSION", "none")
    os.environ.setdefault("SAIL_SPARK__SESSION_TIMEOUT_SECS", "86400")
    log.info(
        "Sail mode: %s (workers=3, parallelism=3, task_slots=1)",
        SAIL_CLUSTER_MODE,
    )


_sail_proc: "subprocess.Popen | None" = None


def _find_sail_binary() -> str:
    sail = shutil.which("sail")
    if sail:
        return sail
    for candidate in [
        "/Library/Frameworks/Python.framework/Versions/3.13/bin/sail",
        "/Library/Frameworks/Python.framework/Versions/3.12/bin/sail",
        "/usr/local/bin/sail",
        os.path.expanduser("~/.local/bin/sail"),
    ]:
        if os.path.exists(candidate):
            return candidate
    raise FileNotFoundError("sail binary not found; ensure pysail is installed")


def _wait_for_port(host: str, port: int, timeout: int = 40) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1):
                return True
        except OSError:
            time.sleep(0.5)
    return False


def _start_sail_subprocess() -> "subprocess.Popen":
    global _sail_proc
    sail = _find_sail_binary()
    log.info("Starting sail subprocess: %s spark server --ip 127.0.0.1 --port %d", sail, SPARK_PORT)
    sail_log = open("/tmp/sail_server.log", "w")
    proc = subprocess.Popen(
        [sail, "spark", "server", "--ip", "127.0.0.1", "--port", str(SPARK_PORT)],
        stdout=sail_log,
        stderr=sail_log,
    )
    _sail_proc = proc
    log.info("sail subprocess output → /tmp/sail_server.log")

    log.info("Waiting for sail spark server on port %d…", SPARK_PORT)
    if not _wait_for_port("127.0.0.1", SPARK_PORT, timeout=40):
        proc.terminate()
        raise RuntimeError(f"sail spark server did not start on port {SPARK_PORT} within 40s")
    log.info("sail spark server ready on :%d", SPARK_PORT)
    return proc


def _cx_uri(host: str, port: int, db: str, user: str, pw: str) -> str:
    import urllib.parse
    return f"postgresql://{user}:{urllib.parse.quote(pw, safe='')}@{host}:{port}/{db}"


_CX_BATCH_ROWS = 100_000  # ≈28MB per batch, well under gRPC 128MB limit


def _fetch_table_cx(host, port, db, user, pw, table) -> pa.Table:
    """Read a table from PostgreSQL into an Arrow table (no Spark involved)."""
    uri = _cx_uri(host, port, db, user, pw)
    t = cx.read_sql(uri, f"SELECT * FROM {table}", return_type="arrow")
    return t.combine_chunks()


def _register_arrow_table(spark, arrow_table: pa.Table, view_name: str) -> int:
    """Push an Arrow table to Sail as batched temp view. Call after SparkSession is ready."""
    nrows = len(arrow_table)
    nbatches = max(1, (nrows + _CX_BATCH_ROWS - 1) // _CX_BATCH_ROWS)

    if nbatches == 1:
        spark.createDataFrame(arrow_table.to_pandas()).createOrReplaceTempView(view_name)
    else:
        batch_views = []
        for i in range(nbatches):
            batch = arrow_table.slice(i * _CX_BATCH_ROWS, _CX_BATCH_ROWS).to_pandas()
            bv = f"_cx_{view_name}_{i}"
            spark.createDataFrame(batch).createOrReplaceTempView(bv)
            batch_views.append(f"SELECT * FROM {bv}")
        spark.sql(
            f"CREATE OR REPLACE TEMP VIEW {view_name} AS "
            + "\nUNION ALL\n".join(batch_views)
        )

    log.info("  %-45s %d rows (%d batch(es))", view_name, nrows, nbatches)
    return nrows


def _prefetch_all_cx(registry, shard_creds) -> "dict[str, pa.Table | list[tuple[str, pa.Table]]]":
    """
    Phase 1 (before Spark session): read all tables from PostgreSQL into Arrow tables.
    Returns a map of final_view_name → Arrow table (single shard)
                         or list of (shard_view_name, Arrow table) (multi-shard union).
    """
    shards = registry.get("shards", {})
    result = {}

    for ds in registry.get("datasets", []):
        ds_name   = ds["name"]
        table     = ds["table"]
        ds_shards = ds.get("shards", list(shards.keys()))
        log.info("Fetching dataset '%s' from shards: %s", ds_name, ds_shards)

        if len(ds_shards) == 1:
            shard_id  = ds_shards[0]
            shard_cfg = shards[shard_id]
            user, pw  = shard_creds[shard_id]
            result[ds_name] = _fetch_table_cx(shard_cfg["host"], shard_cfg["port"],
                                               shard_cfg["database"], user, pw, table)
        else:
            parts = []
            for shard_id in ds_shards:
                shard_cfg = shards[shard_id]
                user, pw  = shard_creds[shard_id]
                vn = f"{ds_name}_{shard_id}_view"
                log.info("  fetching shard %s…", shard_id)
                parts.append((vn, _fetch_table_cx(shard_cfg["host"], shard_cfg["port"],
                                                   shard_cfg["database"], user, pw, table)))
            result[ds_name] = parts  # sentinel: list means multi-shard union

    for bt in registry.get("broadcast_tables", []):
        bt_name   = bt["name"]
        table     = bt["table"]
        source_id = bt.get("source_shard", list(shards.keys())[0])
        shard_cfg = shards[source_id]
        user, pw  = shard_creds[source_id]
        log.info("Fetching broadcast table '%s'", bt_name)
        result[bt_name] = _fetch_table_cx(shard_cfg["host"], shard_cfg["port"],
                                           shard_cfg["database"], user, pw, bt_name)
    return result


def _register_prefetched(spark, prefetched: dict) -> None:
    """Phase 2 (after Spark session + workers alive): push prefetched tables to Sail."""
    for view_name, data in prefetched.items():
        if isinstance(data, list):
            # multi-shard: list of (shard_view_name, Arrow table)
            union_parts = []
            for shard_vn, arrow_table in data:
                _register_arrow_table(spark, arrow_table, shard_vn)
                union_parts.append(f"SELECT * FROM {shard_vn}")
            spark.sql(
                f"CREATE OR REPLACE TEMP VIEW {view_name} AS "
                + "\nUNION ALL\n".join(union_parts)
            )
            log.info("  Unified view '%s' ready", view_name)
        else:
            _register_arrow_table(spark, data, view_name)


def _jdbc_url(host: str, port: int, db: str) -> str:
    # In K8s mode, the sail server process runs on the host and calls schema() locally,
    # while worker pods call read() from inside K8s. We need an IP that's reachable from
    # both: the host's LAN IP (pgpool listens on 0.0.0.0) works from both sides.
    # 'localhost' works only from the host; 'host.docker.internal' only resolves in containers.
    if SAIL_CLUSTER_MODE in ("kubernetes-cluster", "k8s-cluster", "kube-cluster"):
        if host in ("localhost", "127.0.0.1", "::1"):
            host = _k8s_pgpool_host()
    return f"jdbc:postgresql://{host}:{port}/{db}"


def _k8s_pgpool_host() -> str:
    """Return the host IP reachable from both the host process and K8s pods.

    Set SAIL_K8S_PGPOOL_HOST env var to override.
    On Docker Desktop macOS, the host's LAN/Wi-Fi IP (192.168.x.x) works
    because pgpool listens on 0.0.0.0 and K8s pods can reach the host IP.
    """
    override = os.environ.get("SAIL_K8S_PGPOOL_HOST")
    if override:
        return override
    import socket
    # Pick the first non-loopback 192.168.x.x or 10.x.x.x address that is
    # NOT a VPN-typical /32 point-to-point link.
    try:
        addrs = socket.getaddrinfo(socket.gethostname(), None, socket.AF_INET)
        candidates = sorted(
            {a[4][0] for a in addrs if a[4][0] not in ("127.0.0.1",)},
        )
        # Prefer 192.168.x.x LAN addresses (Docker Desktop bridge network)
        for ip in candidates:
            if ip.startswith("192.168."):
                return ip
        for ip in candidates:
            if ip.startswith("10.") and not ip.startswith("10.2."):
                return ip
        if candidates:
            return candidates[0]
    except Exception:
        pass
    return "localhost"


def _load_table(spark, host, port, db, user, password, table, view_name,
                date_col=None, date_from=None, date_to=None,
                num_partitions=1, partition_column=None,
                partition_lower_bound=None, partition_upper_bound=None) -> int:
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
    df = reader.load()
    # K8s mode: skip cache() — it sends Persist to sail which starts a worker pod.
    # The worker registers with "was not pending", eventually stops, and the driver shuts
    # down, killing the session before view registration completes.
    if SAIL_CLUSTER_MODE not in ("kubernetes-cluster", "k8s-cluster", "kube-cluster"):
        df.cache()
    df.createOrReplaceTempView(view_name)
    if SAIL_CLUSTER_MODE not in ("kubernetes-cluster", "k8s-cluster", "kube-cluster"):
        count = spark.sql(f"SELECT COUNT(*) AS n FROM {view_name}").collect()[0]["n"]
        log.info("  %-45s %d rows", view_name, count)
        return count
    log.info("  %-45s registered (lazy, no pre-count)", view_name)
    return 0


def _json_default(obj):
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    if isinstance(obj, (datetime.date, datetime.datetime)):
        return obj.isoformat()
    raise TypeError(f"Not serializable: {type(obj)}")


def _register_datasets(spark, registry, shard_creds) -> None:
    shards = registry.get("shards", {})

    for ds in registry.get("datasets", []):
        ds_name   = ds["name"]
        table     = ds["table"]
        shard_key = ds.get("shard_key")
        ds_shards = ds.get("shards", list(shards.keys()))
        num_parts  = ds.get("num_partitions", 1)
        part_col   = ds.get("partition_column")
        part_lo    = ds.get("partition_lower_bound", 0)
        part_hi    = ds.get("partition_upper_bound", 100)
        log.info("Registering dataset '%s' across shards: %s (numPartitions=%d)", ds_name, ds_shards, num_parts)

        if len(ds_shards) == 1:
            shard_id  = ds_shards[0]
            shard_cfg = shards[shard_id]
            user, pw  = shard_creds[shard_id]
            _load_table(spark, shard_cfg["host"], shard_cfg["port"],
                        shard_cfg["database"], user, pw, table, ds_name,
                        num_partitions=num_parts, partition_column=part_col,
                        partition_lower_bound=part_lo, partition_upper_bound=part_hi)
            continue

        union_parts = []
        for shard_id in ds_shards:
            shard_cfg = shards[shard_id]
            user, pw  = shard_creds[shard_id]
            view_name = f"{ds_name}_{shard_id}_view"
            _load_table(spark, shard_cfg["host"], shard_cfg["port"],
                        shard_cfg["database"], user, pw, table, view_name,
                        num_partitions=num_parts, partition_column=part_col,
                        partition_lower_bound=part_lo, partition_upper_bound=part_hi)
            union_parts.append(f"SELECT * FROM {view_name}")

        union_sql = "\nUNION ALL\n".join(union_parts)
        spark.sql(f"CREATE OR REPLACE TEMP VIEW {ds_name} AS {union_sql}")
        if SAIL_CLUSTER_MODE not in ("kubernetes-cluster", "k8s-cluster", "kube-cluster"):
            try:
                total = spark.sql(f"SELECT COUNT(*) AS n FROM {ds_name}").collect()[0]["n"]
                log.info("Unified view '%s' ready — %d rows total", ds_name, total)
            except Exception as e:
                log.warning("Unified view '%s' ready (row count skipped — %s)", ds_name, e)
        else:
            log.info("Unified view '%s' registered (lazy DDL)", ds_name)

    for bt in registry.get("broadcast_tables", []):
        bt_name   = bt["name"]
        table     = bt["table"]
        source_id = bt.get("source_shard", list(shards.keys())[0])
        shard_cfg = shards[source_id]
        user, pw  = shard_creds[source_id]
        log.info("Registering broadcast table '%s' from %s", bt_name, source_id)
        _load_table(spark, shard_cfg["host"], shard_cfg["port"],
                    shard_cfg["database"], user, pw, table, bt_name)


def _start_http_bridge(spark) -> None:
    """Single HTTP bridge — Sail handles multi-worker coordination internally."""
    app = Flask(__name__)
    logging.getLogger("werkzeug").setLevel(logging.WARNING)

    def _keepalive():
        consecutive_failures = 0
        while True:
            time.sleep(20)
            try:
                _spark.sql("SELECT 1").collect()
                consecutive_failures = 0
            except Exception as exc:
                consecutive_failures += 1
                log.warning("Keep-alive failed (%d consecutive): %s", consecutive_failures, exc)
                if consecutive_failures >= 3:
                    log.error("Session appears dead — attempting recovery")
                    _recover_session(_session_state["sc_host"], _session_state["sc_port"])
                    consecutive_failures = 0

    threading.Thread(target=_keepalive, daemon=True, name="spark-keepalive").start()

    @app.route("/query", methods=["POST"])
    def query():
        payload = request.get_json(force=True, silent=True) or {}
        sql = payload.get("sql") or (request.data.decode() if request.data else "")
        if not sql:
            return jsonify({"error": "missing 'sql' field"}), 400
        try:
            rows = _spark.sql(sql).collect()
            return app.response_class(
                response=json.dumps([row.asDict() for row in rows], default=_json_default),
                status=200,
                mimetype="application/json",
            )
        except Exception as exc:
            return jsonify({"error": str(exc)}), 500

    @app.route("/health", methods=["GET"])
    def health():
        return jsonify({"status": "ok", "engine": "sc_arrow", "workers": SAIL_WORKER_COUNT})

    log.info("SC Arrow HTTP bridge on http://127.0.0.1:%d (Sail manages %d workers internally)",
             BRIDGE_PORT, SAIL_WORKER_COUNT)
    app.run(host="127.0.0.1", port=BRIDGE_PORT, use_reloader=False, threaded=True)


def _recover_session(sc_ip, sc_port) -> None:
    global _spark
    log.info("Session recovery: creating new SparkConnect session on %s:%d…", sc_ip, sc_port)
    _session_state["ready"] = False
    try:
        _spark.stop()
    except Exception:
        pass
    try:
        new_spark = SparkSession.builder.remote(f"sc://{sc_ip}:{sc_port}").create()
        if SAIL_CLUSTER_MODE in ("kubernetes-cluster", "k8s-cluster", "kube-cluster"):
            from pysail.spark.datasource.jdbc import JdbcDataSource
            new_spark.dataSource.register(JdbcDataSource)
            _register_datasets(new_spark, _registry, _shard_creds)
        else:
            from pysail.spark.datasource.jdbc import JdbcDataSource
            new_spark.dataSource.register(JdbcDataSource)
            _register_datasets(new_spark, _registry, _shard_creds)
        _spark = new_spark
        _session_state["session_id"] = new_spark._client._session_id
        _session_state["ready"]      = True
        log.info("Session recovery succeeded")
    except Exception as e:
        log.error("Session recovery failed: %s", e)


def main() -> None:
    log.info("Loading shard registry from %s", REGISTRY_PATH)
    with open(REGISTRY_PATH) as f:
        registry = yaml.safe_load(f)

    shards    = registry.get("shards", {})
    cred_keys = registry.get("credentials", {})
    shard_creds = {}
    for shard_id in shards:
        user_key = cred_keys.get(f"{shard_id}_user_secret", f"FRTB_{shard_id.upper()}_DB_USER")
        pass_key = cred_keys.get(f"{shard_id}_pass_secret", f"FRTB_{shard_id.upper()}_DB_PASSWORD")
        shard_creds[shard_id] = (get_secret(user_key) or "rajeshganta",
                                  get_secret(pass_key) or "")
        log.info("Credentials resolved for shard '%s'", shard_id)

    global _registry, _shard_creds
    _registry    = registry
    _shard_creds = shard_creds

    _configure_sail_mode()
    _start_metadata_server()

    global _spark

    if SAIL_CLUSTER_MODE in ("kubernetes-cluster", "k8s-cluster", "kube-cluster"):
        # K8s mode: the Python SparkConnectServer extension never calls
        # CryptoProvider::install_default(), causing a rustls panic when
        # kube-client initialises TLS for pod spawning.  The native 'sail'
        # binary calls install_default() in its Rust main(), so we run it as
        # a subprocess and connect PySpark to it as a client.
        #
        # Two-phase data loading: fetch all data from PostgreSQL FIRST (before
        # SparkSession.getOrCreate() which triggers K8s worker pod startup).
        # This prevents worker registration timeouts caused by the slow I/O
        # phase (connectorX reads ~20s/shard) overlapping with the 60s worker
        # idle window.
        sail_proc = _start_sail_subprocess()

        spark = SparkSession.builder.remote(f"sc://127.0.0.1:{SPARK_PORT}").getOrCreate()
        log.info("Spark Connect client connected to sail subprocess")

        from pysail.spark.datasource.jdbc import JdbcDataSource
        spark.dataSource.register(JdbcDataSource)

        _spark = spark
        _session_state["session_id"] = spark._client._session_id
        _session_state["sc_host"]    = "127.0.0.1"
        _session_state["sc_port"]    = SPARK_PORT

        log.info("Registering datasets via JDBC (driver on host, workers via Arrow Flight)…")
        _register_datasets(spark, registry, shard_creds)
        _session_state["ready"] = True
        log.info("Session ready")

        def _shutdown(sig, frame):
            log.info("Shutting down…")
            sail_proc.terminate()
            sys.exit(0)

        signal.signal(signal.SIGTERM, _shutdown)
        signal.signal(signal.SIGINT,  _shutdown)

        _start_http_bridge(spark)
    else:
        log.info("Starting Spark Connect server on port %d…", SPARK_PORT)
        spark_server = SparkConnectServer(ip="127.0.0.1", port=SPARK_PORT)
        spark_server.start(background=True)
        time.sleep(2)

        sc_ip, sc_port = spark_server.listening_address
        log.info("Spark Connect listening on %s:%d", sc_ip, sc_port)

        spark = SparkSession.builder.remote(f"sc://{sc_ip}:{sc_port}").getOrCreate()
        log.info("Spark session created")

        from pysail.spark.datasource.jdbc import JdbcDataSource
        spark.dataSource.register(JdbcDataSource)

        _spark = spark
        _session_state["session_id"] = spark._client._session_id
        _session_state["sc_host"]    = sc_ip
        _session_state["sc_port"]    = sc_port

        log.info("Registering datasets from shard registry…")
        _register_datasets(spark, registry, shard_creds)
        _session_state["ready"] = True
        log.info("Session ready")

        def _shutdown(sig, frame):
            log.info("Shutting down…")
            sys.exit(0)

        signal.signal(signal.SIGTERM, _shutdown)
        signal.signal(signal.SIGINT,  _shutdown)

        _start_http_bridge(spark)

    log.info("Cluster Sail stack ready — %d workers, HTTP bridge on :%d", SAIL_WORKER_COUNT, BRIDGE_PORT)

    while True:
        time.sleep(60)


if __name__ == "__main__":
    main()
