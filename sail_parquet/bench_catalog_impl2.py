#!/usr/bin/env python3
"""
Implementation 2 — Large aggregated results in memory (memory:// storage).
Uses parquet_large (27M rows, 800 nodes) to produce larger cached result sets.
Each query result is materialized once into memory://, then repeat queries read from RAM.
Proves: memory storage is faster even when cached result is large (thousands of rows).
"""
import os, time, json, statistics

os.environ["SAIL_MODE"] = "local"
os.environ["SAIL_EXECUTION__DEFAULT_PARALLELISM"] = "4"
os.environ["SAIL_SPARK__SESSION_TIMEOUT_SECS"] = "86400"

DATA_DIR = "/Users/rajeshganta/work/sail_parquet/data"
PARQUET  = f"parquet.`{DATA_DIR}/parquet_large.parquet`"
N_RUNS   = 10

from pysail.spark import SparkConnectServer
from pyspark.sql import SparkSession

print("Starting Sail ...", flush=True)
srv = SparkConnectServer(port=50056)
srv.start(background=True)
time.sleep(3)

spark = SparkSession.builder.remote("sc://localhost:50056").appName("catalog_impl2").getOrCreate()

# Lookup tables
spark.read.parquet(f"{DATA_DIR}/entity_hierarchy.parquet").createOrReplaceTempView("entity_hierarchy")
spark.read.parquet(f"{DATA_DIR}/chorushierarchy.parquet").createOrReplaceTempView("chorushierarchy")
spark.read.parquet(f"{DATA_DIR}/cobdate.parquet").createOrReplaceTempView("cobdate")

# Pattern:
# - disk_sql   → run against raw Parquet (expensive)
# - mem_path   → memory:// URI where result is stored as Arrow in RAM
# - repeat_fn  → lambda that reads from memory:// and collects (fast)

QUERIES = [
    (
        "full_agg",
        "Full Scan SUM — 1 row result",
        f"""SELECT SUM(commodities_delta+commodities_vega+commodities_curvature) AS commodities_sa,
                   SUM(equities_delta+equities_vega+equities_curvature)           AS equities_sa,
                   SUM(interest_rates_delta+interest_rates_vega+interest_rates_curvature) AS rates_sa,
                   SUM(foreign_exchange_delta+foreign_exchange_vega+foreign_exchange_curvature) AS fx_sa
            FROM {PARQUET}""",
        "l2_full_agg",
        lambda p: spark.read.parquet(f"memory:///{p}").collect(),
    ),
    (
        "by_node",
        "Total capital by node — 800 rows",
        f"""SELECT node_id,
                   SUM(commodities_delta+commodities_vega+commodities_curvature) AS commodities_sa,
                   SUM(equities_delta+equities_vega+equities_curvature)           AS equities_sa,
                   SUM(interest_rates_delta+interest_rates_vega+interest_rates_curvature) AS rates_sa,
                   SUM(foreign_exchange_delta+foreign_exchange_vega+foreign_exchange_curvature) AS fx_sa,
                   SUM(total) AS total_capital, SUM(rrao) AS rrao, COUNT(*) AS row_count
            FROM {PARQUET}
            GROUP BY node_id ORDER BY total_capital DESC""",
        "l2_by_node",
        lambda p: spark.read.parquet(f"memory:///{p}").collect(),
    ),
    (
        "by_node_entity",
        "Capital by node x entity — 40,000 rows",
        f"""SELECT node_id, legal_entity_group_id,
                   SUM(total) AS total_capital,
                   SUM(equities_delta+equities_vega+equities_curvature) AS equities_sa,
                   SUM(interest_rates_delta+interest_rates_vega+interest_rates_curvature) AS rates_sa,
                   SUM(rrao) AS rrao
            FROM {PARQUET}
            GROUP BY node_id, legal_entity_group_id ORDER BY total_capital DESC""",
        "l2_by_node_entity",
        lambda p: spark.read.parquet(f"memory:///{p}").filter("total_capital > 0").limit(100).collect(),
    ),
    (
        "by_node_date",
        "Capital by node x date — ~313K rows",
        f"""SELECT node_id, cobdate,
                   SUM(total) AS total_capital,
                   SUM(equities_delta) AS equities_delta,
                   SUM(interest_rates_delta) AS rates_delta,
                   SUM(rrao) AS rrao
            FROM {PARQUET}
            GROUP BY node_id, cobdate ORDER BY cobdate, total_capital DESC""",
        "l2_by_node_date",
        lambda p: spark.read.parquet(f"memory:///{p}").limit(100).collect(),
    ),
    (
        "by_node_date_scenario",
        "Capital by node x date x scenario — ~2.8M rows",
        f"""SELECT node_id, cobdate, correlation_scenario_id, driving_scenario,
                   SUM(total) AS total_capital,
                   SUM(commodities_delta+commodities_vega+commodities_curvature) AS commodities_sa,
                   SUM(equities_delta+equities_vega+equities_curvature) AS equities_sa,
                   SUM(interest_rates_delta+interest_rates_vega+interest_rates_curvature) AS rates_sa,
                   SUM(foreign_exchange_delta+foreign_exchange_vega+foreign_exchange_curvature) AS fx_sa,
                   SUM(rrao) AS rrao
            FROM {PARQUET}
            GROUP BY node_id, cobdate, correlation_scenario_id, driving_scenario""",
        "l2_by_node_date_scenario",
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
        "l2_join_entity",
        lambda p: spark.read.parquet(f"memory:///{p}").collect(),
    ),
    (
        "join_calendar",
        "JOIN cobdate — fiscal quarter summary",
        f"""SELECT d.fiscal_year, d.fiscal_quarter, SUM(f.total) AS total_capital
            FROM {PARQUET} f
            JOIN cobdate d ON f.cobdate = d.cobdate
            GROUP BY d.fiscal_year, d.fiscal_quarter
            ORDER BY d.fiscal_year, d.fiscal_quarter""",
        "l2_join_calendar",
        lambda p: spark.read.parquet(f"memory:///{p}").collect(),
    ),
]

results = {}

for qid, label, disk_sql, mem_path, repeat_fn in QUERIES:
    results[qid] = {"label": label}
    print(f"\n── {qid} ({label}) ──", flush=True)

    # 1. Benchmark raw Parquet (N_RUNS)
    try: spark.sql(disk_sql).collect()
    except Exception as e: print(f"  warmup error: {e}", flush=True)

    disk_times = []
    for _ in range(N_RUNS):
        t0 = time.time()
        spark.sql(disk_sql).collect()
        disk_times.append(round((time.time() - t0) * 1000, 1))

    disk_med = round(statistics.median(disk_times), 1)
    disk_p99 = sorted(disk_times)[max(0, int(len(disk_times)*0.99)-1)]
    print(f"  parquet       median={disk_med:6.0f}ms  p99={disk_p99:6.0f}ms", flush=True)
    results[qid]["parquet"] = {"median_ms": disk_med, "p99_ms": disk_p99, "timings": disk_times}

    # 2. Materialize result into memory:// (RAM)
    t0 = time.time()
    spark.sql(disk_sql).write.mode("overwrite").parquet(f"memory:///{mem_path}")
    mat_ms = round((time.time() - t0) * 1000, 1)
    row_count = spark.read.parquet(f"memory:///{mem_path}").count()
    arrow_mb = round(row_count * 160 / 1024**2, 1)
    print(f"  materialized: {row_count:,} rows in RAM in {mat_ms}ms (~{arrow_mb} MB)", flush=True)
    results[qid]["materialization_ms"] = mat_ms
    results[qid]["cached_rows"] = row_count
    results[qid]["cached_arrow_mb"] = arrow_mb

    # 3. Benchmark repeat reads from memory://
    try: repeat_fn(mem_path)
    except Exception as e: print(f"  warmup error: {e}", flush=True)

    cat_times = []
    for _ in range(N_RUNS):
        t0 = time.time()
        repeat_fn(mem_path)
        cat_times.append(round((time.time() - t0) * 1000, 1))

    cat_med = round(statistics.median(cat_times), 1)
    cat_p99 = sorted(cat_times)[max(0, int(len(cat_times)*0.99)-1)]
    speedup = round(disk_med / cat_med, 1) if cat_med > 0 else 0
    print(f"  memory://     median={cat_med:6.0f}ms  p99={cat_p99:6.0f}ms  speedup={speedup}x", flush=True)
    results[qid]["memory"] = {"median_ms": cat_med, "p99_ms": cat_p99, "timings": cat_times}

with open("/tmp/catalog_impl2_results.json", "w") as f:
    json.dump(results, f, indent=2)

print("\n── Summary ──", flush=True)
print(f"  {'query':20s}  {'parquet_med':>12s}  {'memory_med':>12s}  {'cached_rows':>12s}  {'speedup':>8s}", flush=True)
for qid, r in results.items():
    print(f"  {qid:20s}  {r['parquet']['median_ms']:>11.0f}ms  "
          f"{r['memory']['median_ms']:>11.0f}ms  "
          f"{r['cached_rows']:>12,}  "
          f"{round(r['parquet']['median_ms']/r['memory']['median_ms'],1):>7.1f}x", flush=True)

print("\nDone. Results: /tmp/catalog_impl2_results.json", flush=True)
srv.stop()
