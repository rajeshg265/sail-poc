#!/usr/bin/env python3
"""Measure actual RAM consumed by Demo 1 intermediate in memory catalog."""
import os, time, psutil

os.environ["SAIL_MODE"] = "local"
os.environ["SAIL_EXECUTION__DEFAULT_PARALLELISM"] = "4"
os.environ["SAIL_CATALOG__LIST"] = '[{name="memory_catalog",type="memory",initial_database=["default"]}]'
os.environ["SAIL_CATALOG__DEFAULT_CATALOG"] = "memory_catalog"

DATA_DIR    = "/Users/rajeshganta/work/sail_parquet/data"
PARQUET_XXL = f"parquet.`{DATA_DIR}/parquet_xxl.parquet`"

from pysail.spark import SparkConnectServer
from pyspark.sql import SparkSession

srv = SparkConnectServer(port=50056)
srv.start(background=True)
time.sleep(3)

spark = (SparkSession.builder
    .remote("sc://localhost:50056")
    .config("spark.sql.warehouse.dir", "memory:///warehouse")
    .appName("ram_demo1")
    .getOrCreate())

def rss_mb():
    import subprocess, re
    pid = os.getpid()
    # phys_footprint includes compressed memory on macOS — more accurate than RSS
    out = subprocess.check_output(["ps", "-o", "rss=,vsz=", "-p", str(pid)]).decode()
    rss_kb = int(out.split()[0])
    # Also get phys_footprint via vmmap
    try:
        vm = subprocess.check_output(["vmmap", "-summary", str(pid)], stderr=subprocess.DEVNULL).decode()
        for line in vm.splitlines():
            if "Physical footprint:" in line:
                val = re.search(r"([\d.]+)([MG])", line)
                if val:
                    n, unit = float(val.group(1)), val.group(2)
                    return round(n * 1024 if unit == "G" else n, 1)
    except Exception:
        pass
    return round(rss_kb / 1024, 1)

# Run one warmup query to let Sail fully initialize its engine
print("Warming up engine...", flush=True)
spark.sql(f"SELECT COUNT(*) FROM {PARQUET_XXL}").collect()

# Let RSS stabilize
time.sleep(5)
baseline = rss_mb()
print(f"Baseline RSS (engine stable): {baseline} MB", flush=True)

# Materialize the intermediate
SQL = (
    f"SELECT node_id, cobdate, correlation_scenario_id, driving_scenario,"
    f" SUM(total) AS total_capital,"
    f" SUM(equities_delta) AS eq_delta,"
    f" SUM(commodities_delta) AS commodities_delta,"
    f" SUM(interest_rates_delta) AS ir_delta,"
    f" SUM(foreign_exchange_delta) AS fx_delta,"
    f" SUM(equities_curvature) AS eq_curvature"
    f" FROM {PARQUET_XXL}"
    f" GROUP BY node_id, cobdate, correlation_scenario_id, driving_scenario"
)

print("Materializing intermediate into memory catalog...", flush=True)
spark.sql("DROP TABLE IF EXISTS memory_catalog.default.xxl_intermediate")
spark.sql(f"CREATE TABLE memory_catalog.default.xxl_intermediate AS {SQL}")

row_count = spark.sql("SELECT COUNT(*) AS n FROM memory_catalog.default.xxl_intermediate").collect()[0]["n"]

# Let RSS stabilize after materialization
time.sleep(5)
after = rss_mb()

print(f"After materialization RSS:     {after} MB", flush=True)
print(f"Rows cached:                   {row_count:,}", flush=True)
print(f"RAM delta:                     {after - baseline:+.1f} MB", flush=True)
print(f"Bytes per row:                 {round((after - baseline) * 1024**2 / row_count, 1)}", flush=True)

srv.stop()
