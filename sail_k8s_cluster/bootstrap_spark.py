#!/usr/bin/env python3
"""
sail_k8s_cluster — Spark JVM comparison bridge (local-cluster[3,1,1024]).
HTTP bridge on :8103
"""

import datetime
import decimal
import json
import logging
import os
import sys
from pathlib import Path

import yaml
from flask import Flask, jsonify, request
from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).parent))
from csm import get_secret

os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("spark.k8s.bridge")

BRIDGE_PORT   = int(os.environ.get("SPARK_BRIDGE_PORT", "8103"))
REGISTRY_PATH = Path(__file__).parent / "shard_registry.yaml"
PG_DRIVER     = "org.postgresql.Driver"


def _jdbc_url(host, port, db):
    return f"jdbc:postgresql://{host}:{port}/{db}"


def _build_creds(registry):
    cred_keys = registry.get("credentials", {})
    creds = {}
    for sid in registry.get("shards", {}):
        uk = cred_keys.get(f"{sid}_user_secret", f"FRTB_{sid.upper()}_DB_USER")
        pk = cred_keys.get(f"{sid}_pass_secret", f"FRTB_{sid.upper()}_DB_PASSWORD")
        creds[sid] = (get_secret(uk) or "rajeshganta", get_secret(pk) or "")
    return creds


def _register_datasets(spark, registry, creds):
    shards = registry.get("shards", {})
    for ds in registry.get("datasets", []):
        ds_name   = ds["name"]
        table     = ds["table"]
        shard_key = ds.get("shard_key")
        ds_shards = ds.get("shards", list(shards.keys()))
        num_parts = ds.get("num_partitions", 1)
        part_col  = ds.get("partition_column")
        part_lo   = ds.get("partition_lower_bound", 0)
        part_hi   = ds.get("partition_upper_bound", 100)

        union_parts = []
        for sid in ds_shards:
            sc = shards[sid]
            user, pw = creds[sid]
            view = f"{ds_name}_{sid}_view"
            reader = (spark.read.format("jdbc")
                .option("url",               _jdbc_url(sc["host"], sc["port"], sc["database"]))
                .option("dbtable",           table)
                .option("user",              user)
                .option("password",          pw)
                .option("driver",            PG_DRIVER)
                .option("numPartitions",     str(num_parts))
                .option("pushDownPredicate", "true"))
            if num_parts > 1 and part_col:
                reader = (reader
                    .option("partitionColumn", part_col)
                    .option("lowerBound",      str(part_lo))
                    .option("upperBound",      str(part_hi)))
            reader.load().createOrReplaceTempView(view)
            kr   = sc.get("key_range", {})
            filt = (f" WHERE {shard_key} >= DATE '{kr['from']}' AND {shard_key} <= DATE '{kr['to']}'"
                    if kr and shard_key else "")
            union_parts.append(f"SELECT * FROM {view}{filt}")

        if len(union_parts) == 1:
            spark.sql(f"CREATE OR REPLACE TEMP VIEW {ds_name} AS {union_parts[0]}")
        else:
            spark.sql(f"CREATE OR REPLACE TEMP VIEW {ds_name} AS " + "\nUNION ALL\n".join(union_parts))
        log.info("  View '%s' ready", ds_name)

    for bt in registry.get("broadcast_tables", []):
        sc = shards[bt.get("source_shard", list(shards.keys())[0])]
        user, pw = creds[bt.get("source_shard", list(shards.keys())[0])]
        (spark.read.format("jdbc")
            .option("url",      _jdbc_url(sc["host"], sc["port"], sc["database"]))
            .option("dbtable",  bt["table"])
            .option("user",     user)
            .option("password", pw)
            .option("driver",   PG_DRIVER)
            .load()
            .createOrReplaceTempView(bt["name"]))
        log.info("  Broadcast '%s' ready", bt["name"])


def _ser(obj):
    if isinstance(obj, decimal.Decimal): return float(obj)
    if isinstance(obj, (datetime.date, datetime.datetime)): return obj.isoformat()
    raise TypeError(type(obj))


def main():
    with open(REGISTRY_PATH) as f:
        registry = yaml.safe_load(f)
    creds = _build_creds(registry)

    # Find PostgreSQL JDBC jar (same location as sail_spark_cluster infra)
    _root = Path(__file__).parent.parent
    pg_jar = _root / "sail_spark_cluster" / "infra" / \
        "apache-shardingsphere-5.5.0-shardingsphere-proxy-bin" / "lib" / "postgresql-42.7.2.jar"
    if not pg_jar.exists():
        pg_jar = _root / "sail_spark_cluster" / "infra" / "lib" / "postgresql-42.7.2.jar"
    extra_jars = str(pg_jar) if pg_jar.exists() else ""
    if extra_jars:
        log.info("JDBC jar: %s", extra_jars)
    else:
        log.warning("PostgreSQL JDBC jar not found — JDBC reads will fail")

    log.info("Starting Spark JVM local-cluster[3,1,1024]…")
    builder = (SparkSession.builder
        .master("local-cluster[3, 1, 1024]")
        .appName("FRTBSparkK8sBridge")
        .config("spark.ui.enabled",             "false")
        .config("spark.driver.host",            "127.0.0.1")
        .config("spark.driver.bindAddress",     "127.0.0.1")
        .config("spark.local.ip",               "127.0.0.1")
        .config("spark.driver.memory",          "2g")
        .config("spark.executor.memory",        "1g")
        .config("spark.sql.shuffle.partitions", "3")
        .config("spark.sql.adaptive.enabled",   "true"))
    if extra_jars:
        builder = builder.config("spark.jars", extra_jars)

    spark = builder.getOrCreate()
    log.info("Spark %s started", spark.version)

    _register_datasets(spark, registry, creds)
    log.info("All views ready — bridge on :%d", BRIDGE_PORT)

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
            rows = spark.sql(sql).collect()
            return app.response_class(
                response=json.dumps([r.asDict() for r in rows], default=_ser),
                status=200, mimetype="application/json")
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    app.run(host="127.0.0.1", port=BRIDGE_PORT, threaded=True)


if __name__ == "__main__":
    main()
