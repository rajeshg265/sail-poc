#!/usr/bin/env python3
"""
bootstrap_spark.py — Real Java/JVM Spark bridge for POC comparison.

Starts a classic PySpark session (JVM, local[4]) with the same JDBC views
as bootstrap.py, then serves queries via a Flask HTTP bridge on :8095.

Purpose: demonstrate that Sail (DataFusion/Rust) is faster than production
Apache Spark JVM for interactive OLAP queries on this dataset.
"""
import datetime
import decimal
import json
import logging
import os
import threading
import time

import sys

import yaml
from flask import Flask, jsonify, request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from csm import get_secret

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

BRIDGE_PORT   = int(os.environ.get("SPARK_BRIDGE_PORT", "8095"))
REGISTRY_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "shard_registry.yaml")

PG_DRIVER = "org.postgresql.Driver"


# ── Credentials ──────────────────────────────────────────────────────────────

def _build_shard_creds(registry):
    """Resolve credentials from the registry's `credentials` section, same as bootstrap.py."""
    cred_keys = registry.get("credentials", {})
    shard_creds = {}
    for shard_id in registry.get("shards", {}):
        user_key = cred_keys.get(f"{shard_id}_user_secret", f"FRTB_{shard_id.upper()}_DB_USER")
        pass_key = cred_keys.get(f"{shard_id}_pass_secret", f"FRTB_{shard_id.upper()}_DB_PASSWORD")
        user = get_secret(user_key) or "rajeshganta"
        pw   = get_secret(pass_key) or ""
        shard_creds[shard_id] = (user, pw)
    return shard_creds


# ── JDBC URL ──────────────────────────────────────────────────────────────────

def _jdbc_url(host, port, db):
    return f"jdbc:postgresql://{host}:{port}/{db}"


# ── View registration ─────────────────────────────────────────────────────────

def _register_datasets(spark, registry, shard_creds):
    """Register the same JDBC views as pySail bootstrap, using real PySpark JDBC."""
    shards = registry.get("shards", {})

    for ds in registry.get("datasets", []):
        ds_name   = ds["name"]
        table     = ds["table"]
        shard_key = ds.get("shard_key")
        ds_shards = ds.get("shards", list(shards.keys()))

        log.info("Registering '%s' across %s", ds_name, ds_shards)

        if len(ds_shards) == 1:
            shard_id  = ds_shards[0]
            shard_cfg = shards[shard_id]
            user, pw  = shard_creds[shard_id]
            url = _jdbc_url(shard_cfg["host"], shard_cfg["port"], shard_cfg["database"])
            df = (
                spark.read.format("jdbc")
                .option("url",               url)
                .option("dbtable",           table)
                .option("user",              user)
                .option("password",          pw)
                .option("driver",            PG_DRIVER)
                .option("pushDownPredicate", "true")
                .load()
            )
            df.createOrReplaceTempView(ds_name)
            log.info("  Domain view '%s' ready", ds_name)
            continue

        # Multi-shard: UNION ALL with key-range pruning
        union_parts = []
        for shard_id in ds_shards:
            shard_cfg = shards[shard_id]
            user, pw  = shard_creds[shard_id]
            view_name = f"{ds_name}_{shard_id}_view"
            url = _jdbc_url(shard_cfg["host"], shard_cfg["port"], shard_cfg["database"])
            df = (
                spark.read.format("jdbc")
                .option("url",               url)
                .option("dbtable",           table)
                .option("user",              user)
                .option("password",          pw)
                .option("driver",            PG_DRIVER)
                .option("pushDownPredicate", "true")
                .load()
            )
            df.createOrReplaceTempView(view_name)
            key_range = shard_cfg.get("key_range", {})
            if key_range and shard_key:
                range_filter = (
                    f" WHERE {shard_key} >= DATE '{key_range['from']}'"
                    f" AND {shard_key} <= DATE '{key_range['to']}'"
                )
            else:
                range_filter = ""
            union_parts.append(f"SELECT * FROM {view_name}{range_filter}")

        union_sql = "\nUNION ALL\n".join(union_parts)
        spark.sql(f"CREATE OR REPLACE TEMP VIEW {ds_name} AS {union_sql}")
        total = spark.sql(f"SELECT COUNT(*) AS n FROM {ds_name}").collect()[0]["n"]
        log.info("  Unified view '%s' ready — %d rows total", ds_name, total)

    for bt in registry.get("broadcast_tables", []):
        bt_name   = bt["name"]
        table     = bt["table"]
        source_id = bt.get("source_shard", list(shards.keys())[0])
        shard_cfg = shards[source_id]
        user, pw  = shard_creds[source_id]
        url = _jdbc_url(shard_cfg["host"], shard_cfg["port"], shard_cfg["database"])
        df = (
            spark.read.format("jdbc")
            .option("url",      url)
            .option("dbtable",  table)
            .option("user",     user)
            .option("password", pw)
            .option("driver",   PG_DRIVER)
            .load()
        )
        df.createOrReplaceTempView(bt_name)
        log.info("  Broadcast view '%s' ready", bt_name)


# ── Serialisation ─────────────────────────────────────────────────────────────

def _ser(obj):
    if isinstance(obj, decimal.Decimal): return float(obj)
    if isinstance(obj, (datetime.date, datetime.datetime)): return obj.isoformat()
    raise TypeError(type(obj))


def _rows_to_json(rows):
    out = []
    for row in rows:
        d = row.asDict()
        for k, v in d.items():
            if isinstance(v, decimal.Decimal): d[k] = float(v)
            elif isinstance(v, datetime.datetime): d[k] = v.isoformat()
            elif isinstance(v, datetime.date):     d[k] = v.isoformat()
        out.append(d)
    return out


# ── Flask bridge ──────────────────────────────────────────────────────────────

_spark = None

def _start_bridge():
    app = Flask(__name__)
    logging.getLogger("werkzeug").setLevel(logging.WARNING)

    @app.route("/health")
    def health():
        return jsonify({"status": "ok", "engine": "spark-jvm"})

    @app.route("/query", methods=["POST"])
    def query():
        body = request.get_json() or {}
        sql  = (body.get("sql") or "").strip()
        if not sql:
            return jsonify({"error": "missing sql"}), 400
        try:
            rows = _spark.sql(sql).collect()
            return app.response_class(
                response=json.dumps(_rows_to_json(rows), default=_ser),
                status=200, mimetype="application/json"
            )
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    log.info("Spark JVM bridge listening on http://127.0.0.1:%d", BRIDGE_PORT)
    app.run(host="127.0.0.1", port=BRIDGE_PORT, threaded=True)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    global _spark

    from pyspark.sql import SparkSession

    # Resolve PostgreSQL JDBC jar — look next to this file's project root first,
    # then fall back to the SS proxy lib bundled in the same repo.
    _repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    pg_jar = os.path.join(
        _repo_root, "infra",
        "apache-shardingsphere-5.5.0-shardingsphere-proxy-bin", "lib",
        "postgresql-42.7.2.jar"
    )
    if not os.path.exists(pg_jar):
        # secondary: directly inside infra/
        pg_jar = os.path.join(_repo_root, "infra", "lib", "postgresql-42.7.2.jar")
    extra_jars = pg_jar if os.path.exists(pg_jar) else ""
    if extra_jars:
        log.info("PostgreSQL JDBC jar: %s", extra_jars)
    else:
        log.warning("PostgreSQL JDBC jar not found — JDBC reads will fail")

    log.info("Starting PySpark JVM session (local[3])…")
    builder = (
        SparkSession.builder
        .master("local[3]")
        .appName("FRTBSparkBridge")
        .config("spark.ui.enabled",                "false")
        .config("spark.driver.memory",             "2g")
        .config("spark.sql.shuffle.partitions",    "3")
        .config("spark.sql.adaptive.enabled",      "true")
    )
    if extra_jars:
        # Use extraClassPath rather than spark.jars — local mode doesn't need
        # jar distribution over RPC, and spark.jars causes a "Broken pipe" error.
        builder = builder.config("spark.driver.extraClassPath", extra_jars)
        builder = builder.config("spark.executor.extraClassPath", extra_jars)

    _spark = builder.getOrCreate()
    log.info("PySpark %s started (JVM)", _spark.version)

    with open(REGISTRY_PATH) as f:
        registry = yaml.safe_load(f)

    shard_creds = _build_shard_creds(registry)
    log.info("Registering datasets…")
    _register_datasets(_spark, registry, shard_creds)
    log.info("All views registered. Starting HTTP bridge…")

    _start_bridge()


if __name__ == "__main__":
    main()
