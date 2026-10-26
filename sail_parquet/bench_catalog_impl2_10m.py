#!/usr/bin/env python3
"""
10M-row memory:// cache test.
Caches 10M raw rows from parquet_large into memory://, then runs
full-scan aggregations against the cached RAM data vs reading from Parquet.
Both paths scan all 10M rows but return 1 row — measures pure scan speed.
"""
import os, time, statistics

os.environ["SAIL_MODE"] = "local"
os.environ["SAIL_EXECUTION__DEFAULT_PARALLELISM"] = "4"
os.environ["SAIL_SPARK__SESSION_TIMEOUT_SECS"] = "86400"

DATA_DIR = "/Users/rajeshganta/work/sail_parquet/data"
PARQUET  = f"parquet.`{DATA_DIR}/parquet_large.parquet`"
N_ROWS   = 10_000_000
N_RUNS   = 10

from pysail.spark import SparkConnectServer
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

print("Starting Sail ...", flush=True)
srv = SparkConnectServer(port=50056)
srv.start(background=True)
time.sleep(3)

spark = SparkSession.builder.remote("sc://localhost:50056").appName("impl2_10m").getOrCreate()

MEM_PATH = "l2_raw_10m"

# ── Step 1: Benchmark scanning 10M rows from Parquet ─────────────────────────

disk_sql = f"SELECT SUM(total) AS total_sum, SUM(equities_delta) AS eq_sum, COUNT(*) AS cnt FROM (SELECT * FROM {PARQUET} LIMIT {N_ROWS})"

print(f"\n── Parquet scan ({N_ROWS:,} rows) ──", flush=True)
spark.sql(disk_sql).collect()  # warmup

disk_times = []
for i in range(N_RUNS):
    t0 = time.time()
    spark.sql(disk_sql).collect()
    disk_times.append(round((time.time() - t0) * 1000, 1))
    print(f"  run {i+1:2d}/{N_RUNS}: {disk_times[-1]:.0f}ms", flush=True)

disk_med = round(statistics.median(disk_times), 1)
disk_p99 = sorted(disk_times)[max(0, int(len(disk_times)*0.99)-1)]
print(f"  → median={disk_med:.0f}ms  p99={disk_p99:.0f}ms", flush=True)

# ── Step 2: Materialize 10M rows into memory:// ───────────────────────────────

print(f"\n── Materializing {N_ROWS:,} rows into memory:///{MEM_PATH} ──", flush=True)
t0 = time.time()
spark.sql(f"SELECT * FROM {PARQUET} LIMIT {N_ROWS}").write.mode("overwrite").parquet(f"memory:///{MEM_PATH}")
mat_ms = round((time.time() - t0) * 1000, 1)

row_count = spark.read.parquet(f"memory:///{MEM_PATH}").count()
arrow_mb = round(row_count * 160 / 1024**2, 1)
print(f"  {row_count:,} rows in RAM in {mat_ms}ms  (~{arrow_mb} MB estimated)", flush=True)

# ── Step 3: Benchmark same aggregation against memory:// ─────────────────────

print(f"\n── memory:// scan ({row_count:,} rows) ──", flush=True)

def repeat():
    return (spark.read.parquet(f"memory:///{MEM_PATH}")
            .agg(F.sum("total").alias("total_sum"),
                 F.sum("equities_delta").alias("eq_sum"),
                 F.count("*").alias("cnt"))
            .collect())

repeat()  # warmup

cat_times = []
for i in range(N_RUNS):
    t0 = time.time()
    repeat()
    cat_times.append(round((time.time() - t0) * 1000, 1))
    print(f"  run {i+1:2d}/{N_RUNS}: {cat_times[-1]:.0f}ms", flush=True)

cat_med = round(statistics.median(cat_times), 1)
cat_p99 = sorted(cat_times)[max(0, int(len(cat_times)*0.99)-1)]
speedup = round(disk_med / cat_med, 1) if cat_med > 0 else 0
print(f"  → median={cat_med:.0f}ms  p99={cat_p99:.0f}ms", flush=True)

# ── Summary ───────────────────────────────────────────────────────────────────

print(f"\n── Summary ──", flush=True)
print(f"  rows cached in RAM : {row_count:,}  (~{arrow_mb} MB)", flush=True)
print(f"  parquet scan       : {disk_med:.0f}ms median", flush=True)
print(f"  memory:// scan     : {cat_med:.0f}ms median", flush=True)
print(f"  speedup            : {speedup}x", flush=True)

srv.stop()
