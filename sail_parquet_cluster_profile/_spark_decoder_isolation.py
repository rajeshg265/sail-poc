#!/usr/bin/env python3
"""
Isolate the Parquet decoder as root cause vs task scheduling overhead.

Spark uses 74 tasks to scan 469 MB. Sail uses 3 workers.
If we force Spark to use ~3 tasks (maxPartitionBytes=512m), per-task overhead
shrinks by 25x. If scan time does NOT drop proportionally -> decoder is the bottleneck,
not task overhead. If it DOES drop -> task overhead was the cause.

Also measure raw OS read speed via Python to establish page-cache baseline.
"""
import os, sys, time, json, urllib.request

os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["PYSPARK_PYTHON"]  = sys.executable

DATA_DIR = "/Users/rajeshganta/work/sail_parquet/data"
VIEW     = "frtb_sa_capital_output"
SQL      = f"SELECT SUM(total) FROM {VIEW}"

# --- baseline: raw page-cache read speed ---
import mmap
t0 = time.perf_counter()
total_bytes = 0
for fname in ["parquet1.parquet", "parquet2.parquet"]:
    path = f"{DATA_DIR}/{fname}"
    with open(path, "rb") as f:
        with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as m:
            # sequential read to warm page cache and measure read BW
            chunk = 1 << 20  # 1 MB
            pos = 0
            while pos < len(m):
                _ = m[pos:pos+chunk]
                pos += chunk
            total_bytes += len(m)
raw_ms = round((time.perf_counter() - t0) * 1000, 1)
raw_gb_s = round(total_bytes / 1e9 / ((time.perf_counter() - t0)), 2)
print(f"  raw page-cache read: {total_bytes/1e6:.0f}MB in {raw_ms}ms = {total_bytes/1e9/(raw_ms/1000):.1f} GB/s", file=sys.stderr)

results = {"raw_page_cache": {"total_mb": round(total_bytes/1e6,0), "ms": raw_ms}}

from pyspark.sql import SparkSession

for label, max_part_bytes in [("74_tasks_default", "128m"), ("3_tasks_large_partitions", "2g")]:
    spark = (SparkSession.builder
             .master("local[3]")
             .appName(f"decoder_isolation_{label}")
             .config("spark.driver.host",                "127.0.0.1")
             .config("spark.driver.bindAddress",         "127.0.0.1")
             .config("spark.sql.shuffle.partitions",     "3")
             .config("spark.sql.adaptive.enabled",       "false")  # keep partition count fixed
             .config("spark.sql.files.maxPartitionBytes", max_part_bytes)
             .config("spark.sql.parquet.filterPushdown", "true")
             .config("spark.ui.enabled",                 "true")
             .config("spark.ui.port",                    "4042")
             .config("spark.driver.memory",              "2g")
             .getOrCreate())

    spark.sql(f"""CREATE OR REPLACE TEMP VIEW {VIEW} AS
        SELECT * FROM parquet.`{DATA_DIR}/parquet1.parquet`
        UNION ALL SELECT * FROM parquet.`{DATA_DIR}/parquet2.parquet`""")

    # count actual tasks used
    plan = spark.sql(SQL).explain(True)

    # warmup
    for _ in range(2):
        try: spark.sql(SQL).collect()
        except: pass

    # get task count from stage REST API
    app_id = spark.sparkContext.applicationId
    before = set()
    try:
        url = f"http://127.0.0.1:4042/api/v1/applications/{app_id}/stages?status=COMPLETE"
        with urllib.request.urlopen(url, timeout=3) as r:
            before = {s["stageId"] for s in json.loads(r.read())}
    except: pass

    times = []
    for _ in range(5):
        t0 = time.time()
        spark.sql(SQL).collect()
        times.append(round((time.time() - t0) * 1000, 1))

    times.sort()
    median = times[len(times)//2]

    # get stage info
    task_count = "?"
    exec_ms = "?"
    try:
        import time as _t; _t.sleep(0.3)
        url = f"http://127.0.0.1:4042/api/v1/applications/{app_id}/stages?status=COMPLETE"
        with urllib.request.urlopen(url, timeout=3) as r:
            stages = json.loads(r.read())
        new = [s for s in stages if s["stageId"] not in before and s.get("inputBytes",0) > 0]
        if new:
            scan_stage = max(new, key=lambda s: s.get("inputBytes",0))
            task_count = scan_stage.get("numTasks","?")
            exec_ms    = scan_stage.get("executorRunTime","?")
    except Exception as e:
        pass

    results[label] = {
        "median_ms": median, "runs": times,
        "tasks": task_count, "executor_run_ms": exec_ms
    }
    print(f"  {label}: median={median}ms tasks={task_count} exec_ms={exec_ms} runs={times}", file=sys.stderr, flush=True)
    spark.stop()

print(json.dumps(results))
