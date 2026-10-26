#!/usr/bin/env python3
"""
10M cached rows test — materialize by_node_date_scenario from parquet_xl into memory://,
then measure how fast repeat reads are from RAM.
"""
import os, time, statistics

os.environ["SAIL_MODE"] = "local"
os.environ["SAIL_EXECUTION__DEFAULT_PARALLELISM"] = "4"
os.environ["SAIL_SPARK__SESSION_TIMEOUT_SECS"] = "86400"

DATA_DIR = "/Users/rajeshganta/work/sail_parquet/data"
PARQUET  = f"parquet.`{DATA_DIR}/parquet_xl.parquet`"
N_RUNS   = 10

from pysail.spark import SparkConnectServer
from pyspark.sql import SparkSession

print("Starting Sail ...", flush=True)
srv = SparkConnectServer(port=50056)
srv.start(background=True)
time.sleep(3)

spark = SparkSession.builder.remote("sc://localhost:50056").appName("bench_10m").getOrCreate()

disk_sql = f"""SELECT node_id, cobdate, correlation_scenario_id, driving_scenario,
                   SUM(total) AS total_capital,
                   SUM(equities_delta+equities_vega+equities_curvature) AS equities_sa,
                   SUM(interest_rates_delta+interest_rates_vega+interest_rates_curvature) AS rates_sa,
                   SUM(rrao) AS rrao
            FROM {PARQUET}
            GROUP BY node_id, cobdate, correlation_scenario_id, driving_scenario"""

MEM = "xl_by_node_date_scenario"

# Materialize once into memory://
print(f"\nMaterializing into memory:///{MEM} ...", flush=True)
t0 = time.time()
spark.sql(disk_sql).write.mode("overwrite").parquet(f"memory:///{MEM}")
mat_ms = round((time.time() - t0) * 1000, 1)
row_count = spark.read.parquet(f"memory:///{MEM}").count()
arrow_mb  = round(row_count * 160 / 1024**2, 1)
print(f"  {row_count:,} rows cached in RAM in {mat_ms}ms (~{arrow_mb} MB)", flush=True)

# Benchmark repeat reads from memory://
print(f"\nReading from memory:// ({N_RUNS} runs) ...", flush=True)
spark.read.parquet(f"memory:///{MEM}").filter("total_capital > 0").limit(100).collect()  # warmup

times = []
for i in range(N_RUNS):
    t0 = time.time()
    spark.read.parquet(f"memory:///{MEM}").filter("total_capital > 0").limit(100).collect()
    times.append(round((time.time() - t0) * 1000, 1))
    print(f"  run {i+1:2d}/{N_RUNS}: {times[-1]:.0f}ms", flush=True)

med = round(statistics.median(times), 1)
p99 = sorted(times)[max(0, int(len(times)*0.99)-1)]

print(f"\n── Result ──", flush=True)
print(f"  cached rows : {row_count:,}  (~{arrow_mb} MB in RAM)", flush=True)
print(f"  median      : {med:.0f}ms", flush=True)
print(f"  p99         : {p99:.0f}ms", flush=True)
print(f"  (parquet_large by_node_date_scenario was 21,271ms median → speedup ~{round(21271/med)}x)", flush=True)

srv.stop()
