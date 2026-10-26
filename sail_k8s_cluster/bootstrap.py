#!/usr/bin/env python3
"""
sail_k8s_cluster — Sail K8s-cluster mode benchmark driver.

Architecture: sail server runs INSIDE K8s (pod network), so driver→worker
Arrow Flight stays within the cluster. macOS reaches the server via
kubectl port-forward (K8s API tunnel, not direct pod IP).

TimescaleDB via pgpool: shard1 :6440, shard2 :6441 on macOS host.
JDBC URLs use the LAN IP so worker pods can reach pgpool.

Ports (no overlap with sail_spark_cluster):
  SparkConnect:  50060  (port-forwarded from K8s pod)
  Sail bridge:   8102
  Sail meta:     7075
"""

import decimal
import datetime
import json
import logging
import os
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path

import yaml
from flask import Flask, request, jsonify
from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).parent))
from csm import get_secret

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("sail.k8s.bootstrap")

SPARK_PORT   = int(os.environ.get("SAIL_SPARK_PORT",  "50060"))
BRIDGE_PORT  = int(os.environ.get("SAIL_BRIDGE_PORT", "8102"))
META_PORT    = int(os.environ.get("SAIL_META_PORT",   "7075"))
WORKER_COUNT = 3
PG_DRIVER    = "org.postgresql.Driver"
REGISTRY_PATH    = Path(__file__).parent / "shard_registry.yaml"
DRIVER_YAML      = Path(__file__).parent / "k8s" / "sail-driver-deployment.yaml"
RBAC_YAML        = Path(__file__).parent / "k8s" / "rbac.yaml"

_spark = None
_spark_lock = threading.Lock()
_registry_cache: dict | None = None
_shard_creds_cache: dict | None = None
_port_forward_proc = None
_session_state = {"ready": False, "sc_host": "127.0.0.1", "sc_port": SPARK_PORT}


def _host_lan_ip() -> str:
    override = os.environ.get("SAIL_K8S_PGPOOL_HOST")
    if override:
        return override
    try:
        addrs = socket.getaddrinfo(socket.gethostname(), None, socket.AF_INET)
        candidates = sorted({a[4][0] for a in addrs if a[4][0] != "127.0.0.1"})
        for ip in candidates:
            if ip.startswith("192.168.1."):
                return ip
        for ip in candidates:
            if ip.startswith("192.168."):
                return ip
        if candidates:
            return candidates[0]
    except Exception:
        pass
    return "192.168.1.142"


def _jdbc_url(host: str, port: int, db: str) -> str:
    if host in ("localhost", "127.0.0.1", "::1"):
        host = os.environ.get("SAIL_K8S_PGPOOL_HOST", "host.docker.internal")
    return f"jdbc:postgresql://{host}:{port}/{db}"


def _wait_port(host: str, port: int, timeout: int = 60) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1):
                return True
        except OSError:
            time.sleep(0.5)
    return False


def _cleanup_stale_resources() -> None:
    try:
        pod_list = subprocess.check_output(
            ["kubectl", "get", "pods", "--no-headers", "--namespace", "default"],
            text=True, stderr=subprocess.DEVNULL, timeout=10
        )
        stale = [line.split()[0] for line in pod_list.splitlines()
                 if line and "sail-k8s-worker" in line.split()[0].lower()]
        if stale:
            log.info("Deleting %d stale worker pods (background)...", len(stale))
            subprocess.Popen(
                ["kubectl", "delete", "pod", "--ignore-not-found",
                 "--namespace", "default"] + stale,
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
            )
        else:
            log.info("No stale worker pods.")
    except Exception as e:
        log.warning("Pod cleanup non-fatal: %s", e)


def _driver_pod_is_ready() -> bool:
    """Return True if sail-driver pod is already Running and Ready."""
    try:
        out = subprocess.check_output(
            ["kubectl", "get", "pods", "-l", "app=sail-driver", "--no-headers",
             "-n", "default", "-o", "custom-columns=STATUS:.status.phase,READY:.status.containerStatuses[0].ready"],
            text=True, stderr=subprocess.DEVNULL, timeout=10
        )
        for line in out.strip().splitlines():
            parts = line.split()
            if len(parts) >= 2 and parts[0] == "Running" and parts[1] == "true":
                return True
    except Exception:
        pass
    return False


def _deploy_k8s_driver() -> None:
    """Apply RBAC and sail-driver deployment. Reuse existing pod if already Running."""
    subprocess.run(["kubectl", "apply", "-f", str(RBAC_YAML)],
                   check=True, capture_output=True)

    if _driver_pod_is_ready():
        log.info("sail-driver pod already Running — reusing existing pod.")
        return

    log.info("Deploying sail-driver in K8s...")
    subprocess.run(["kubectl", "apply", "-f", str(DRIVER_YAML)],
                   check=True, capture_output=True)

    log.info("Waiting for sail-driver pod to be ready (up to 120s)...")
    subprocess.run(
        ["kubectl", "rollout", "status", "deployment/sail-driver",
         "-n", "default", "--timeout=120s"],
        check=True
    )
    log.info("sail-driver pod is ready.")


def _start_port_forward() -> subprocess.Popen:
    """Forward K8s sail-driver service :50060 to localhost:50060."""
    global _port_forward_proc
    # Kill any existing port-forward
    try:
        pids = subprocess.check_output(
            ["lsof", "-ti", f":{SPARK_PORT}"], text=True
        ).split()
        for pid in pids:
            try:
                os.kill(int(pid), signal.SIGKILL)
            except ProcessLookupError:
                pass
    except subprocess.CalledProcessError:
        pass
    time.sleep(0.5)

    log.info("Starting kubectl port-forward svc/sail-driver-svc %d:50060...", SPARK_PORT)
    pf_log = open("/tmp/sail_k8s_portforward.log", "w")
    proc = subprocess.Popen(
        ["kubectl", "port-forward", "svc/sail-driver-svc",
         f"{SPARK_PORT}:50060", "-n", "default"],
        stdout=pf_log, stderr=pf_log
    )
    _port_forward_proc = proc

    if not _wait_port("127.0.0.1", SPARK_PORT, timeout=30):
        proc.terminate()
        raise RuntimeError(f"kubectl port-forward did not expose :{SPARK_PORT}")
    log.info("port-forward ready — sail gRPC on localhost:%d", SPARK_PORT)
    return proc


def _register_datasets(spark, registry: dict, shard_creds: dict) -> None:
    shards = registry.get("shards", {})

    for ds in registry.get("datasets", []):
        ds_name    = ds["name"]
        table      = ds["table"]
        shard_key  = ds.get("shard_key")
        ds_shards  = ds.get("shards", list(shards.keys()))
        num_parts  = ds.get("num_partitions", 1)
        part_col   = ds.get("partition_column")
        part_lo    = ds.get("partition_lower_bound", 0)
        part_hi    = ds.get("partition_upper_bound", 100)
        log.info("Registering '%s' across shards %s (numPartitions=%d)", ds_name, ds_shards, num_parts)

        union_parts = []
        for shard_id in ds_shards:
            sc = shards[shard_id]
            user, pw = shard_creds[shard_id]
            view = f"{ds_name}_{shard_id}_view"
            kr = sc.get("key_range", {})
            # Embed shard date filter in the SQL subquery so PostgreSQL applies it
            # as a native date comparison — avoids pysail 0.7.2 epoch-integer pushdown bug
            if kr and shard_key:
                dbtable = (f"(SELECT * FROM {table}"
                           f" WHERE {shard_key} >= DATE '{kr['from']}'"
                           f" AND {shard_key} <= DATE '{kr['to']}') AS shard_data")
            else:
                dbtable = table
            reader = (
                spark.read.format("jdbc")
                .option("url",               _jdbc_url(sc["host"], sc["port"], sc["database"]))
                .option("dbtable",           dbtable)
                .option("user",              user)
                .option("password",          pw)
                .option("driver",            PG_DRIVER)
                .option("fetchsize",         "50000")
                .option("numPartitions",     str(num_parts))
                .option("pushDownPredicate", "true")
            )
            if num_parts > 1 and part_col:
                reader = (reader
                    .option("partitionColumn", part_col)
                    .option("lowerBound",      str(part_lo))
                    .option("upperBound",      str(part_hi)))
            reader.load().createOrReplaceTempView(view)
            union_parts.append(f"SELECT * FROM {view}")

        if len(union_parts) == 1:
            spark.sql(f"CREATE OR REPLACE TEMP VIEW {ds_name} AS {union_parts[0]}")
        else:
            spark.sql(f"CREATE OR REPLACE TEMP VIEW {ds_name} AS " + "\nUNION ALL\n".join(union_parts))
        log.info("  View '%s' registered (lazy)", ds_name)

    for bt in registry.get("broadcast_tables", []):
        sc = shards[bt.get("source_shard", list(shards.keys())[0])]
        user, pw = shard_creds[bt.get("source_shard", list(shards.keys())[0])]
        (spark.read.format("jdbc")
            .option("url",      _jdbc_url(sc["host"], sc["port"], sc["database"]))
            .option("dbtable",  bt["table"])
            .option("user",     user)
            .option("password", pw)
            .option("driver",   PG_DRIVER)
            .load()
            .createOrReplaceTempView(bt["name"]))
        log.info("  Broadcast '%s' registered", bt["name"])


def _json_default(obj):
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    if isinstance(obj, (datetime.date, datetime.datetime)):
        return obj.isoformat()
    raise TypeError(type(obj))


def _start_meta_server() -> None:
    from http.server import BaseHTTPRequestHandler, HTTPServer

    class H(BaseHTTPRequestHandler):
        def do_GET(self):
            body = json.dumps(_session_state).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        def log_message(self, *a): pass

    threading.Thread(target=HTTPServer(("127.0.0.1", META_PORT), H).serve_forever,
                     daemon=True, name="meta").start()
    log.info("Meta server on :%d", META_PORT)


def _reconnect_spark() -> None:
    """Reconnect SparkConnect session and re-register all views after a worker failure."""
    global _spark
    log.warning("Reconnecting SparkConnect session after failure...")
    try:
        _spark.stop()
    except Exception:
        pass
    time.sleep(8)  # wait for K8s to reschedule worker pod
    deadline = time.time() + 120
    while time.time() < deadline:
        try:
            new_spark = SparkSession.builder.remote(f"sc://127.0.0.1:{SPARK_PORT}").getOrCreate()
            from pysail.spark.datasource.jdbc import JdbcDataSource
            new_spark.dataSource.register(JdbcDataSource)
            new_spark.sql("SELECT 1").collect()
            _spark = new_spark
            if _registry_cache and _shard_creds_cache:
                _register_datasets(_spark, _registry_cache, _shard_creds_cache)
            log.warning("Reconnect succeeded — views re-registered")
            return
        except Exception as e:
            log.warning("Reconnect attempt failed: %s (retrying...)", e)
            time.sleep(5)
    log.error("Failed to reconnect SparkConnect session within 120s")


def _is_connection_error(e: Exception) -> bool:
    msg = str(e).lower()
    return any(x in msg for x in ("grpc", "broken pipe", "connection", "unavailable",
                                   "cancelled", "reset", "eof", "closed", "session"))


def _start_bridge(spark) -> None:
    app = Flask(__name__)
    logging.getLogger("werkzeug").setLevel(logging.WARNING)

    def _keepalive():
        while True:
            time.sleep(30)
            try:
                _spark.sql("SELECT 1").collect()
            except Exception as e:
                log.warning("keepalive: %s", e)
                if _is_connection_error(e):
                    with _spark_lock:
                        _reconnect_spark()

    threading.Thread(target=_keepalive, daemon=True, name="keepalive").start()

    @app.route("/query", methods=["POST"])
    def query():
        body = request.get_json(force=True, silent=True) or {}
        sql  = (body.get("sql") or "").strip()
        if not sql:
            return jsonify({"error": "missing sql"}), 400
        with _spark_lock:
            try:
                rows = _spark.sql(sql).collect()
                return app.response_class(
                    response=json.dumps([r.asDict() for r in rows], default=_json_default),
                    status=200, mimetype="application/json")
            except Exception as e:
                if _is_connection_error(e):
                    log.warning("/query connection error — reconnecting: %s", e)
                    _reconnect_spark()
                    try:
                        rows = _spark.sql(sql).collect()
                        return app.response_class(
                            response=json.dumps([r.asDict() for r in rows], default=_json_default),
                            status=200, mimetype="application/json")
                    except Exception as e2:
                        return jsonify({"error": str(e2)}), 500
                return jsonify({"error": str(e)}), 500

    @app.route("/health")
    def health():
        return jsonify({"status": "ok", "engine": "sail-k8s", "workers": WORKER_COUNT})

    log.info("Sail K8s bridge on :%d", BRIDGE_PORT)
    app.run(host="0.0.0.0", port=BRIDGE_PORT, use_reloader=False, threaded=True)


def main():
    with open(REGISTRY_PATH) as f:
        registry = yaml.safe_load(f)

    cred_keys = registry.get("credentials", {})
    shard_creds = {}
    for sid in registry.get("shards", {}):
        uk = cred_keys.get(f"{sid}_user_secret", f"FRTB_{sid.upper()}_DB_USER")
        pk = cred_keys.get(f"{sid}_pass_secret", f"FRTB_{sid.upper()}_DB_PASSWORD")
        shard_creds[sid] = (get_secret(uk) or "rajeshganta", get_secret(pk) or "")

    _cleanup_stale_resources()
    _start_meta_server()

    # Deploy sail driver inside K8s — driver↔worker Arrow Flight stays within pod network
    _deploy_k8s_driver()

    # Tunnel the sail gRPC port to localhost via kubectl port-forward
    pf_proc = _start_port_forward()

    spark = SparkSession.builder.remote(f"sc://127.0.0.1:{SPARK_PORT}").getOrCreate()
    log.info("SparkConnect client connected to K8s sail driver")

    from pysail.spark.datasource.jdbc import JdbcDataSource
    spark.dataSource.register(JdbcDataSource)
    log.info("JdbcDataSource registered")

    # Wait for cluster — workers will spin up on first query dispatch
    log.info("Waiting for K8s cluster to be ready (SELECT 1)...")
    deadline = time.time() + 300
    while time.time() < deadline:
        try:
            spark.sql("SELECT 1").collect()
            log.info("K8s cluster ready — sail driver responding")
            break
        except Exception as e:
            log.info("Not ready yet: %s  (retrying...)", e)
            time.sleep(5)
    else:
        raise RuntimeError("K8s sail cluster did not become ready within 300s")

    global _spark
    _spark = spark
    try:
        _session_state.update({"session_id": spark._client._session_id, "sc_port": SPARK_PORT})
    except Exception:
        pass

    global _registry_cache, _shard_creds_cache
    _registry_cache = registry
    _shard_creds_cache = shard_creds
    _register_datasets(spark, registry, shard_creds)
    _session_state["ready"] = True
    log.info("All views registered — K8s cluster ready for benchmark")

    def _shutdown(sig, frame):
        log.info("Shutting down")
        if pf_proc:
            pf_proc.terminate()
        # Leave the driver pod running (stop.sh handles cleanup)
        sys.exit(0)

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT,  _shutdown)

    _start_bridge(spark)


if __name__ == "__main__":
    main()
