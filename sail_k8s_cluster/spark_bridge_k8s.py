#!/usr/bin/env python3
"""
sail_k8s_cluster — Spark K8s bridge (proper K8s cluster mode).

Architecture: Spark driver runs in a K8s pod. Executor pods are spawned
separately via the K8s API (same path as Sail K8s workers). Executor pods
reach pgpool via host.docker.internal (Docker Desktop VM bridge), identical
to how Sail worker pods access pgpool.

  Driver pod  →  K8s API  →  3 executor pods
  Executor pods →  host.docker.internal:6440/6441  →  pgpool  →  TimescaleDB

HTTP bridge on :8104 — benchmark runner hits /query with SQL, gets JSON rows.
"""

import decimal
import datetime
import json
import logging
import os
import threading

from flask import Flask, request, jsonify
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("spark.k8s.bridge")

BRIDGE_PORT  = int(os.environ.get("SPARK_BRIDGE_PORT", "8104"))
K8S_IMAGE    = os.environ.get("SPARK_K8S_IMAGE",
                               "host.docker.internal:5000/spark-k8s:v5")
DRIVER_HOST  = os.environ.get("MY_POD_IP") or os.environ.get("SPARK_DRIVER_HOST", "127.0.0.1")
PG_JAR       = "/app/lib/postgresql-42.7.2.jar"

# Shard definitions — host.docker.internal resolves to the Mac host from K8s pods
SHARDS = {
    "cluster_shard1": {
        "host": "host.docker.internal", "port": 6440, "db": "cluster_shard1",
        "date_from": "2024-01-01", "date_to": "2025-12-31",
    },
    "cluster_shard2": {
        "host": "host.docker.internal", "port": 6441, "db": "cluster_shard2",
        "date_from": "2026-01-01", "date_to": "2027-12-31",
    },
}

BROADCAST_TABLES = ["cobdate", "entity_hierarchy", "chorushierarchy", "vw_pillar_node_mapping"]

# Credentials: read from env vars (set in K8s deployment spec)
def _creds(shard_id):
    user_env = f"FRTB_{shard_id.upper()}_DB_USER"
    pass_env = f"FRTB_{shard_id.upper()}_DB_PASSWORD"
    return (os.environ.get(user_env, "rajeshganta"),
            os.environ.get(pass_env, "frtb_local"))


def _jdbc_url(host, port, db):
    return f"jdbc:postgresql://{host}:{port}/{db}"


def _register_views(spark):
    shard_views = []
    for shard_id, sc in SHARDS.items():
        user, pw = _creds(shard_id)
        url  = _jdbc_url(sc["host"], sc["port"], sc["db"])
        view = f"frtb_sa_capital_output_{shard_id}_view"
        # Embed date filter in subquery — avoids date pushdown issues
        dbtable = (
            f"(SELECT * FROM public.frtb_sa_capital_output"
            f" WHERE cobdate >= DATE '{sc['date_from']}'"
            f" AND cobdate <= DATE '{sc['date_to']}') AS shard_data"
        )
        (spark.read.format("jdbc")
            .option("url",           url)
            .option("dbtable",       dbtable)
            .option("user",          user)
            .option("password",      pw)
            .option("driver",        "org.postgresql.Driver")
            .option("fetchsize",     "50000")
            .option("numPartitions", "1")
            .load()
            .createOrReplaceTempView(view))
        shard_views.append(f"SELECT * FROM {view}")
        log.info("  Shard view '%s' registered", view)

    spark.sql(
        "CREATE OR REPLACE TEMP VIEW frtb_sa_capital_output AS "
        + "\nUNION ALL\n".join(shard_views)
    )
    log.info("  Union view 'frtb_sa_capital_output' registered")

    # Broadcast / dimension tables — all from shard1
    sc1   = SHARDS["cluster_shard1"]
    user1, pw1 = _creds("cluster_shard1")
    url1  = _jdbc_url(sc1["host"], sc1["port"], sc1["db"])
    for bt in BROADCAST_TABLES:
        (spark.read.format("jdbc")
            .option("url",     url1)
            .option("dbtable", f"public.{bt}")
            .option("user",    user1)
            .option("password", pw1)
            .option("driver",  "org.postgresql.Driver")
            .load()
            .createOrReplaceTempView(bt))
        log.info("  Broadcast table '%s' registered", bt)


def _ser(obj):
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    if isinstance(obj, (datetime.date, datetime.datetime)):
        return obj.isoformat()
    raise TypeError(type(obj))


def main():
    log.info("Building SparkSession (master=k8s://kubernetes.default.svc:443)…")
    spark = (
        SparkSession.builder
        .master("k8s://https://kubernetes.default.svc:443")
        .appName("FRTBSparkK8s")
        # Executor pods: use same image so JARs are at the same path
        .config("spark.kubernetes.container.image",                    K8S_IMAGE)
        .config("spark.kubernetes.container.image.pullPolicy",         "IfNotPresent")
        .config("spark.kubernetes.namespace",                          "default")
        .config("spark.kubernetes.authenticate.serviceAccountName",    "sail-driver")
        .config("spark.kubernetes.executor.podNamePrefix",             "spark-k8s-exec")
        # Driver networking — executors reach driver via K8s Service DNS
        .config("spark.driver.host",          DRIVER_HOST)
        .config("spark.driver.bindAddress",   "0.0.0.0")
        .config("spark.driver.port",          "7077")
        .config("spark.blockManager.port",    "7078")
        # Cluster sizing — 3 executor pods, matching Sail's 3 worker pods
        .config("spark.executor.instances",   "3")
        .config("spark.executor.memory",      "1g")
        .config("spark.executor.cores",       "2")
        .config("spark.driver.memory",        "2g")
        # Query config
        .config("spark.sql.shuffle.partitions", "3")
        .config("spark.sql.adaptive.enabled",   "true")
        .config("spark.ui.enabled",             "false")
        .config("spark.jars",                   PG_JAR)
        .getOrCreate()
    )
    log.info("Spark %s started — registering views…", spark.version)
    _register_views(spark)
    log.info("All views registered — bridge on :%d", BRIDGE_PORT)

    app = Flask(__name__)
    logging.getLogger("werkzeug").setLevel(logging.WARNING)

    _lock = threading.Lock()

    @app.route("/health")
    def health():
        return jsonify({"status": "ok", "engine": "spark-k8s"})

    @app.route("/query", methods=["POST"])
    def query():
        body = request.get_json(force=True, silent=True) or {}
        sql  = (body.get("sql") or "").strip()
        if not sql:
            return jsonify({"error": "missing sql"}), 400
        with _lock:
            try:
                rows = spark.sql(sql).collect()
                return app.response_class(
                    response=json.dumps([r.asDict() for r in rows], default=_ser),
                    status=200,
                    mimetype="application/json",
                )
            except Exception as e:
                return jsonify({"error": str(e)}), 500

    app.run(host="0.0.0.0", port=BRIDGE_PORT, use_reloader=False, threaded=True)


if __name__ == "__main__":
    main()
