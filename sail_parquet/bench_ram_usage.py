#!/usr/bin/env python3
"""Measure actual RSS before and after each memory:// write."""
import os, time, psutil, statistics

os.environ["SAIL_MODE"] = "local"
os.environ["SAIL_EXECUTION__DEFAULT_PARALLELISM"] = "4"
os.environ["SAIL_SPARK__SESSION_TIMEOUT_SECS"] = "86400"

DATA_DIR = "/Users/rajeshganta/work/sail_parquet/data"
PARQUET  = f"parquet.`{DATA_DIR}/parquet_xl.parquet`"

from pysail.spark import SparkConnectServer
from pyspark.sql import SparkSession

print("Starting Sail ...", flush=True)
srv = SparkConnectServer(port=50056)
srv.start(background=True)
time.sleep(3)

spark = SparkSession.builder.remote("sc://localhost:50056").appName("ram_usage").getOrCreate()

def sail_rss_mb():
    return round(psutil.Process(os.getpid()).memory_info().rss / 1024**2, 1)

QUERIES = [
    ("by_node_date_scenario_10m", f"""SELECT node_id, cobdate, correlation_scenario_id, driving_scenario,
                   SUM(total) AS total_capital, SUM(equities_delta) AS equities_delta, SUM(rrao) AS rrao
            FROM {PARQUET}
            GROUP BY node_id, cobdate, correlation_scenario_id, driving_scenario""", "xl_by_node_date_scenario"),
]

print(f"\n{'query':25s}  {'rows':>12s}  {'RAM before':>12s}  {'RAM after':>12s}  {'delta':>10s}", flush=True)
print("-" * 75, flush=True)

for qid, sql, mem_path in QUERIES:
    before = sail_rss_mb()
    spark.sql(sql).write.mode("overwrite").parquet(f"memory:///{mem_path}")
    row_count = spark.read.parquet(f"memory:///{mem_path}").count()
    after = sail_rss_mb()
    delta = round(after - before, 1)
    print(f"{qid:25s}  {row_count:>12,}  {before:>11.1f}MB  {after:>11.1f}MB  {delta:>+9.1f}MB", flush=True)

srv.stop()
