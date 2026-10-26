#!/usr/bin/env python3
"""
Implementation 2 XL — same as impl2 but parquet_xl (2800 nodes → ~10M cached rows).
N_DISK_RUNS=3 for expensive parquet queries, N_MEM_RUNS=10 for memory:// reads.
"""
import os, time, json, statistics

os.environ["SAIL_MODE"] = "local"
os.environ["SAIL_EXECUTION__DEFAULT_PARALLELISM"] = "4"
os.environ["SAIL_SPARK__SESSION_TIMEOUT_SECS"] = "86400"

DATA_DIR  = "/Users/rajeshganta/work/sail_parquet/data"
PARQUET   = f"parquet.`{DATA_DIR}/parquet_xl.parquet`"
N_DISK    = 3
N_MEM     = 10

from pysail.spark import SparkConnectServer
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

print("Starting Sail ...", flush=True)
srv = SparkConnectServer(port=50056)
srv.start(background=True)
time.sleep(3)

spark = SparkSession.builder.remote("sc://localhost:50056").appName("impl2_xl").getOrCreate()

spark.read.parquet(f"{DATA_DIR}/entity_hierarchy.parquet").createOrReplaceTempView("entity_hierarchy")
spark.read.parquet(f"{DATA_DIR}/cobdate.parquet").createOrReplaceTempView("cobdate")

QUERIES = [
    (
        "full_agg",
        "Full Scan SUM — 1 row result",
        f"""SELECT SUM(commodities_delta+commodities_vega+commodities_curvature) AS commodities_sa,
                   SUM(equities_delta+equities_vega+equities_curvature) AS equities_sa,
                   SUM(interest_rates_delta+interest_rates_vega+interest_rates_curvature) AS rates_sa,
                   SUM(foreign_exchange_delta+foreign_exchange_vega+foreign_exchange_curvature) AS fx_sa
            FROM {PARQUET}""",
        "xl_full_agg",
        lambda p: spark.read.parquet(f"memory:///{p}").collect(),
    ),
    (
        "by_node",
        "Total capital by node — 2,800 rows",
        f"""SELECT node_id,
                   SUM(total) AS total_capital, SUM(rrao) AS rrao, COUNT(*) AS row_count
            FROM {PARQUET}
            GROUP BY node_id ORDER BY total_capital DESC""",
        "xl_by_node",
        lambda p: spark.read.parquet(f"memory:///{p}").collect(),
    ),
    (
        "by_node_date_scenario",
        "Capital by node x date x scenario — ~10M rows",
        f"""SELECT node_id, cobdate, correlation_scenario_id, driving_scenario,
                   SUM(total) AS total_capital,
                   SUM(equities_delta+equities_vega+equities_curvature) AS equities_sa,
                   SUM(interest_rates_delta+interest_rates_vega+interest_rates_curvature) AS rates_sa,
                   SUM(rrao) AS rrao
            FROM {PARQUET}
            GROUP BY node_id, cobdate, correlation_scenario_id, driving_scenario""",
        "xl_by_node_date_scenario",
        lambda p: spark.read.parquet(f"memory:///{p}").filter("total_capital > 0").limit(100).collect(),
    ),
    (
        "join_entity",
        "JOIN entity_hierarchy — region/BU summary",
        f"""SELECT e.region, e.business_unit,
                   SUM(f.total) AS total_capital, COUNT(*) AS row_count
            FROM {PARQUET} f
            JOIN entity_hierarchy e ON f.legal_entity_group_id = e.legal_entity_group_id
            GROUP BY e.region, e.business_unit ORDER BY total_capital DESC""",
        "xl_join_entity",
        lambda p: spark.read.parquet(f"memory:///{p}").collect(),
    ),
]

results = {}

for qid, label, disk_sql, mem_path, repeat_fn in QUERIES:
    results[qid] = {"label": label}
    print(f"\n── {qid} ({label}) ──", flush=True)

    # Parquet baseline (fewer runs for expensive queries)
    spark.sql(disk_sql).collect()  # warmup

    disk_times = []
    for i in range(N_DISK):
        t0 = time.time()
        spark.sql(disk_sql).collect()
        disk_times.append(round((time.time() - t0) * 1000, 1))
        print(f"  parquet run {i+1}/{N_DISK}: {disk_times[-1]:.0f}ms", flush=True)

    disk_med = round(statistics.median(disk_times), 1)
    print(f"  parquet median={disk_med:.0f}ms", flush=True)
    results[qid]["parquet"] = {"median_ms": disk_med, "timings": disk_times}

    # Materialize into memory://
    t0 = time.time()
    spark.sql(disk_sql).write.mode("overwrite").parquet(f"memory:///{mem_path}")
    mat_ms = round((time.time() - t0) * 1000, 1)
    row_count = spark.read.parquet(f"memory:///{mem_path}").count()
    arrow_mb = round(row_count * 160 / 1024**2, 1)
    print(f"  materialized: {row_count:,} rows in RAM in {mat_ms}ms (~{arrow_mb} MB)", flush=True)
    results[qid]["materialization_ms"] = mat_ms
    results[qid]["cached_rows"] = row_count

    # memory:// reads
    repeat_fn(mem_path)  # warmup

    cat_times = []
    for i in range(N_MEM):
        t0 = time.time()
        repeat_fn(mem_path)
        cat_times.append(round((time.time() - t0) * 1000, 1))

    cat_med = round(statistics.median(cat_times), 1)
    speedup = round(disk_med / cat_med, 1) if cat_med > 0 else 0
    print(f"  memory://  median={cat_med:.0f}ms  speedup={speedup}x", flush=True)
    results[qid]["memory"] = {"median_ms": cat_med, "timings": cat_times}

with open("/tmp/catalog_impl2_xl_results.json", "w") as f:
    json.dump(results, f, indent=2)

print("\n── Summary ──", flush=True)
print(f"  {'query':25s}  {'parquet_med':>12s}  {'memory_med':>10s}  {'cached_rows':>12s}  {'speedup':>8s}", flush=True)
for qid, r in results.items():
    print(f"  {qid:25s}  {r['parquet']['median_ms']:>11.0f}ms  "
          f"{r['memory']['median_ms']:>9.0f}ms  "
          f"{r['cached_rows']:>12,}  "
          f"{round(r['parquet']['median_ms']/r['memory']['median_ms'],1):>7.1f}x", flush=True)

print("\nDone.", flush=True)
srv.stop()
