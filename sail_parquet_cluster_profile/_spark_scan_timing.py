#!/usr/bin/env python3
"""
Run each isolation query alone in Spark, capture event log, extract per-stage
executor_run_time_ms so we have actual Spark scan time to compare with Sail's
DataSourceExec time_elapsed_scanning_total.

Outputs one JSON line per query:
  {"query": ..., "stage0_exec_ms": ..., "stage0_bytes_read": ..., "total_ms": ...}
"""
import os, sys, json, time, glob, shutil
from pathlib import Path

os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["PYSPARK_PYTHON"]  = sys.executable

DATA_DIR = "/Users/rajeshganta/work/sail_parquet/data"
VIEW     = "frtb_sa_capital_output"
EVLOG    = "/tmp/spark_scan_timing_evlog"

QUERIES = [
    ("scan_only_sum1col",   f"SELECT SUM(total) FROM {VIEW}"),
    ("compute_only_inmem",  f"SELECT SUM(v), COUNT(*) FROM (SELECT (id % 100) * 1.0 AS v FROM range(54880000))"),
    ("filtered_agg",        f"""SELECT legal_entity_group_id, SUM(total) AS total_capital,
                                 SUM(equities_delta) AS eq_delta, SUM(interest_rates_delta) AS ir_delta
                                 FROM {VIEW} WHERE legal_entity_group_id < 17
                                 GROUP BY legal_entity_group_id ORDER BY total_capital DESC LIMIT 20"""),
    ("full_agg",            f"""SELECT SUM(commodities_delta+commodities_vega+commodities_curvature) AS c,
                                 SUM(equities_delta+equities_vega+equities_curvature) AS e,
                                 SUM(interest_rates_delta+interest_rates_vega+interest_rates_curvature) AS r,
                                 SUM(foreign_exchange_delta+foreign_exchange_vega+foreign_exchange_curvature) AS fx
                                 FROM {VIEW}"""),
]

results = {}

for qid, sql in QUERIES:
    evlog_dir = f"{EVLOG}/{qid}"
    shutil.rmtree(evlog_dir, ignore_errors=True)
    os.makedirs(evlog_dir, exist_ok=True)

    from pyspark.sql import SparkSession
    spark = (SparkSession.builder
             .master("local-cluster[3,1,1024]")
             .appName(f"scan_timing_{qid}")
             .config("spark.driver.host",                "127.0.0.1")
             .config("spark.driver.bindAddress",         "127.0.0.1")
             .config("spark.sql.shuffle.partitions",     "3")
             .config("spark.sql.adaptive.enabled",       "true")
             .config("spark.sql.parquet.filterPushdown", "true")
             .config("spark.ui.enabled",                 "false")
             .config("spark.driver.memory",              "2g")
             .config("spark.executor.memory",            "1g")
             .config("spark.eventLog.enabled",           "true")
             .config("spark.eventLog.dir",               f"file://{evlog_dir}")
             .getOrCreate())

    spark.read.parquet(f"{DATA_DIR}/entity_hierarchy.parquet").createOrReplaceTempView("entity_hierarchy")
    spark.read.parquet(f"{DATA_DIR}/chorushierarchy.parquet").createOrReplaceTempView("chorushierarchy")
    spark.read.parquet(f"{DATA_DIR}/cobdate.parquet").createOrReplaceTempView("cobdate")
    spark.sql(f"""CREATE OR REPLACE TEMP VIEW {VIEW} AS
        SELECT * FROM parquet.`{DATA_DIR}/parquet1.parquet`
        UNION ALL SELECT * FROM parquet.`{DATA_DIR}/parquet2.parquet`""")

    # warmup
    try: spark.sql(sql).collect()
    except: pass

    # timed run
    t0 = time.time()
    spark.sql(sql).collect()
    total_ms = round((time.time() - t0) * 1000, 1)

    spark.stop()

    # parse event log (v2 uses subdirectories with numbered chunk files)
    import re
    stage_exec = {}
    stage_bytes = {}
    all_files = []
    for entry in glob.glob(f"{evlog_dir}/**", recursive=True):
        if os.path.isfile(entry):
            all_files.append(entry)
    for f in all_files:
        for line in open(f, errors="replace"):
            try:
                ev = json.loads(line)
                if not isinstance(ev, dict): continue
            except: continue
            if ev.get("Event") == "SparkListenerTaskEnd":
                sid = ev.get("Stage ID", -1)
                metrics = ev.get("Task Metrics", {})
                exec_ms = metrics.get("Executor Run Time", 0)
                br = metrics.get("Input Metrics", {}).get("Bytes Read", 0)
                stage_exec[sid] = stage_exec.get(sid, 0) + exec_ms
                stage_bytes[sid] = stage_bytes.get(sid, 0) + br

    # Stage 0 is the scan stage for all these queries
    s0_exec = stage_exec.get(0, 0)
    s0_bytes = stage_bytes.get(0, 0)

    results[qid] = {
        "total_ms":        total_ms,
        "stage0_exec_ms":  s0_exec,
        "stage0_bytes_mb": round(s0_bytes / 1e6, 1),
        "all_stages":      {str(k): {"exec_ms": v, "bytes_mb": round(stage_bytes.get(k,0)/1e6,1)}
                            for k, v in stage_exec.items()},
    }
    print(f"  {qid}: total={total_ms}ms  stage0_exec={s0_exec}ms  stage0_bytes={s0_bytes/1e6:.1f}MB", file=sys.stderr, flush=True)

print(json.dumps(results))
