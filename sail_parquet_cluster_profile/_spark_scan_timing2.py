#!/usr/bin/env python3
"""
Collect per-stage metrics from Spark's live REST API (localhost:4040)
while each query is running. This gives actual executor_run_time_ms and
bytes_read per stage — no event log parsing needed.
"""
import os, sys, json, time, urllib.request

os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["PYSPARK_PYTHON"]  = sys.executable

DATA_DIR = "/Users/rajeshganta/work/sail_parquet/data"
VIEW     = "frtb_sa_capital_output"

QUERIES = [
    ("scan_only_sum1col",  f"SELECT SUM(total) FROM {VIEW}"),
    ("compute_only_inmem", f"SELECT SUM(CAST((id % 100) * 1.0 AS DOUBLE)), COUNT(*) FROM range(54880000)"),
    ("filtered_agg",       f"""SELECT legal_entity_group_id, SUM(total), SUM(equities_delta), SUM(interest_rates_delta)
                                FROM {VIEW} WHERE legal_entity_group_id < 17
                                GROUP BY legal_entity_group_id ORDER BY 2 DESC LIMIT 20"""),
    ("full_agg",           f"""SELECT SUM(commodities_delta+commodities_vega+commodities_curvature),
                                SUM(equities_delta+equities_vega+equities_curvature),
                                SUM(interest_rates_delta+interest_rates_vega+interest_rates_curvature),
                                SUM(foreign_exchange_delta+foreign_exchange_vega+foreign_exchange_curvature)
                                FROM {VIEW}"""),
]

from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .master("local-cluster[3,1,1024]")
         .appName("spark_scan_timing2")
         .config("spark.driver.host",                "127.0.0.1")
         .config("spark.driver.bindAddress",         "127.0.0.1")
         .config("spark.sql.shuffle.partitions",     "3")
         .config("spark.sql.adaptive.enabled",       "true")
         .config("spark.sql.parquet.filterPushdown", "true")
         .config("spark.ui.enabled",                 "true")
         .config("spark.ui.port",                    "4041")
         .config("spark.driver.memory",              "2g")
         .config("spark.executor.memory",            "1g")
         .getOrCreate())

spark.read.parquet(f"{DATA_DIR}/entity_hierarchy.parquet").createOrReplaceTempView("entity_hierarchy")
spark.read.parquet(f"{DATA_DIR}/chorushierarchy.parquet").createOrReplaceTempView("chorushierarchy")
spark.read.parquet(f"{DATA_DIR}/cobdate.parquet").createOrReplaceTempView("cobdate")
spark.sql(f"""CREATE OR REPLACE TEMP VIEW {VIEW} AS
    SELECT * FROM parquet.`{DATA_DIR}/parquet1.parquet`
    UNION ALL SELECT * FROM parquet.`{DATA_DIR}/parquet2.parquet`""")

def get_stage_metrics(app_id):
    try:
        url = f"http://127.0.0.1:4041/api/v1/applications/{app_id}/stages?status=COMPLETE"
        with urllib.request.urlopen(url, timeout=5) as r:
            return json.loads(r.read())
    except Exception as e:
        return []

def get_app_id():
    return spark.sparkContext.applicationId

results = {}
app_id = get_app_id()
prev_stage_ids = set()

# warmup to stabilise
try:
    spark.sql(f"SELECT SUM(total) FROM {VIEW}").collect()
    spark.sql(f"SELECT SUM(total) FROM {VIEW}").collect()
except: pass

for qid, sql in QUERIES:
    # snapshot stages before
    before = {s["stageId"] for s in get_stage_metrics(app_id)}

    t0 = time.time()
    spark.sql(sql).collect()
    total_ms = round((time.time() - t0) * 1000, 1)

    time.sleep(0.3)  # let UI update
    all_stages = get_stage_metrics(app_id)
    new_stages = [s for s in all_stages if s["stageId"] not in before]

    stage_detail = {}
    for s in new_stages:
        sid = s["stageId"]
        stage_detail[str(sid)] = {
            "exec_ms":     s.get("executorRunTime", 0),
            "bytes_read":  s.get("inputBytes", 0),
            "bytes_mb":    round(s.get("inputBytes", 0) / 1e6, 1),
            "num_tasks":   s.get("numTasks", 0),
        }

    # scan stage = stage with most bytes read
    scan_stage = max(new_stages, key=lambda s: s.get("inputBytes", 0), default={})
    scan_exec_ms  = scan_stage.get("executorRunTime", 0) if scan_stage else 0
    scan_bytes_mb = round(scan_stage.get("inputBytes", 0) / 1e6, 1) if scan_stage else 0

    results[qid] = {
        "total_ms":       total_ms,
        "scan_exec_ms":   scan_exec_ms,
        "scan_bytes_mb":  scan_bytes_mb,
        "all_stages":     stage_detail,
    }
    print(f"  {qid}: total={total_ms}ms  scan_exec={scan_exec_ms}ms  scan_bytes={scan_bytes_mb}MB  new_stages={len(new_stages)}", file=sys.stderr, flush=True)

spark.stop()
print(json.dumps(results))
