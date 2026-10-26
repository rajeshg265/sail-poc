#!/usr/bin/env python3
"""
Implementation 1 — Screenshot approach:
- Run each FRTB query against raw Parquet (expensive, reads 27M rows)
- Store each small aggregated result in memory catalog as Arrow
- Run repeat queries against cached results (cheap, scan Arrow in memory)
- Prove: memory catalog repeat queries are faster than hitting Parquet every time
"""
import os, time, json, statistics

os.environ["SAIL_MODE"] = "local"
os.environ["SAIL_EXECUTION__DEFAULT_PARALLELISM"] = "4"
os.environ["SAIL_SPARK__SESSION_TIMEOUT_SECS"] = "86400"
os.environ["SAIL_CATALOG__LIST"] = '[{name="memory_catalog",type="memory",initial_database=["default"]}]'
os.environ["SAIL_CATALOG__DEFAULT_CATALOG"] = "memory_catalog"

DATA_DIR = "/Users/rajeshganta/work/sail_parquet/data"
PARQUET  = f"parquet.`{DATA_DIR}/parquet1.parquet`"
N_RUNS   = 10

from pysail.spark import SparkConnectServer
from pyspark.sql import SparkSession

print("Starting Sail ...", flush=True)
srv = SparkConnectServer(port=50056)
srv.start(background=True)
time.sleep(3)

spark = (SparkSession.builder
    .remote("sc://localhost:50056")
    .config("spark.sql.warehouse.dir", "memory:///warehouse")
    .appName("catalog_impl1")
    .getOrCreate())

# Lookup tables (disk, tiny — used in materialization JOINs only)
spark.read.parquet(f"{DATA_DIR}/entity_hierarchy.parquet").createOrReplaceTempView("entity_hierarchy")
spark.read.parquet(f"{DATA_DIR}/chorushierarchy.parquet").createOrReplaceTempView("chorushierarchy")
spark.read.parquet(f"{DATA_DIR}/cobdate.parquet").createOrReplaceTempView("cobdate")

# Each entry: (id, label, disk_sql, catalog_table, repeat_sql)
# disk_sql   → run against raw Parquet (expensive)
# catalog_table → where result is cached as Arrow
# repeat_sql → cheap query against cached Arrow result
QUERIES = [
    (
        "full_agg", "Full Scan SUM",
        f"""SELECT SUM(commodities_delta+commodities_vega+commodities_curvature) AS commodities_sa,
                   SUM(equities_delta+equities_vega+equities_curvature)           AS equities_sa,
                   SUM(interest_rates_delta+interest_rates_vega+interest_rates_curvature) AS rates_sa,
                   SUM(foreign_exchange_delta+foreign_exchange_vega+foreign_exchange_curvature) AS fx_sa
            FROM {PARQUET}""",
        "memory_catalog.default.frtb_full_agg",
        "SELECT * FROM memory_catalog.default.frtb_full_agg",
    ),
    (
        "grouped_agg", "Group By Entity/Scenario",
        f"""SELECT legal_entity_group_id, correlation_scenario_id, driving_scenario,
                   SUM(total) AS total_capital, COUNT(*) AS row_count
            FROM {PARQUET}
            GROUP BY legal_entity_group_id, correlation_scenario_id, driving_scenario
            ORDER BY total_capital DESC LIMIT 20""",
        "memory_catalog.default.frtb_grouped_agg",
        "SELECT * FROM memory_catalog.default.frtb_grouped_agg ORDER BY total_capital DESC",
    ),
    (
        "filtered_agg", "Filtered WHERE < 17",
        f"""SELECT legal_entity_group_id, SUM(total) AS total_capital,
                   SUM(equities_delta) AS eq_delta, SUM(interest_rates_delta) AS ir_delta
            FROM {PARQUET} WHERE legal_entity_group_id < 17
            GROUP BY legal_entity_group_id ORDER BY total_capital DESC LIMIT 20""",
        "memory_catalog.default.frtb_filtered_agg",
        "SELECT * FROM memory_catalog.default.frtb_filtered_agg WHERE total_capital > 0",
    ),
    (
        "join_entity", "JOIN entity_hierarchy",
        f"""SELECT e.region, e.business_unit, SUM(f.total) AS total_capital, COUNT(*) AS row_count
            FROM {PARQUET} f
            JOIN entity_hierarchy e ON f.legal_entity_group_id = e.legal_entity_group_id
            GROUP BY e.region, e.business_unit ORDER BY total_capital DESC""",
        "memory_catalog.default.frtb_join_entity",
        "SELECT * FROM memory_catalog.default.frtb_join_entity ORDER BY total_capital DESC",
    ),
    (
        "join_desk", "JOIN chorushierarchy",
        f"""SELECT c.desk_name, c.risk_pillar, SUM(f.total) AS total_capital, SUM(f.rrao) AS rrao_charge
            FROM {PARQUET} f
            JOIN chorushierarchy c ON f.node_id = c.node_id
            GROUP BY c.desk_name, c.risk_pillar ORDER BY total_capital DESC LIMIT 20""",
        "memory_catalog.default.frtb_join_desk",
        "SELECT * FROM memory_catalog.default.frtb_join_desk ORDER BY total_capital DESC",
    ),
    (
        "join_calendar", "JOIN cobdate",
        f"""SELECT d.fiscal_year, d.fiscal_quarter, SUM(f.total) AS total_capital
            FROM {PARQUET} f
            JOIN cobdate d ON f.cobdate = d.cobdate
            GROUP BY d.fiscal_year, d.fiscal_quarter
            ORDER BY d.fiscal_year, d.fiscal_quarter""",
        "memory_catalog.default.frtb_join_calendar",
        "SELECT * FROM memory_catalog.default.frtb_join_calendar ORDER BY fiscal_year, fiscal_quarter",
    ),
]

results = {}

for qid, label, disk_sql, catalog_table, repeat_sql in QUERIES:
    results[qid] = {"label": label}
    print(f"\n── {qid} ({label}) ──", flush=True)

    # ── 1. Benchmark raw Parquet (N_RUNS) ──────────────────────────────────────
    try: spark.sql(disk_sql).collect()
    except Exception as e: print(f"  warmup error: {e}", flush=True)

    disk_times = []
    for _ in range(N_RUNS):
        t0 = time.time()
        spark.sql(disk_sql).collect()
        disk_times.append(round((time.time() - t0) * 1000, 1))

    disk_med = round(statistics.median(disk_times), 1)
    disk_p99 = sorted(disk_times)[max(0, int(len(disk_times) * 0.99) - 1)]
    print(f"  parquet       median={disk_med:6.0f}ms  p99={disk_p99:6.0f}ms", flush=True)
    results[qid]["parquet"] = {"median_ms": disk_med, "p99_ms": disk_p99, "timings": disk_times}

    # ── 2. Materialize result into memory catalog ──────────────────────────────
    spark.sql(f"DROP TABLE IF EXISTS {catalog_table}")
    t0 = time.time()
    spark.sql(f"CREATE TABLE {catalog_table} AS {disk_sql}")
    mat_ms = round((time.time() - t0) * 1000, 1)
    row_count = spark.sql(f"SELECT COUNT(*) AS n FROM {catalog_table}").collect()[0]["n"]
    print(f"  materialized: {row_count} rows cached as Arrow in {mat_ms}ms", flush=True)
    results[qid]["materialization_ms"] = mat_ms
    results[qid]["cached_rows"] = row_count

    # ── 3. Benchmark repeat queries against memory catalog ─────────────────────
    try: spark.sql(repeat_sql).collect()
    except Exception as e: print(f"  warmup error: {e}", flush=True)

    cat_times = []
    for _ in range(N_RUNS):
        t0 = time.time()
        spark.sql(repeat_sql).collect()
        cat_times.append(round((time.time() - t0) * 1000, 1))

    cat_med = round(statistics.median(cat_times), 1)
    cat_p99 = sorted(cat_times)[max(0, int(len(cat_times) * 0.99) - 1)]
    speedup = round(disk_med / cat_med, 1) if cat_med > 0 else 0
    print(f"  memory_cat    median={cat_med:6.0f}ms  p99={cat_p99:6.0f}ms  speedup={speedup}x", flush=True)
    results[qid]["memory_catalog"] = {"median_ms": cat_med, "p99_ms": cat_p99, "timings": cat_times}

with open("/tmp/catalog_impl1_results.json", "w") as f:
    json.dump(results, f, indent=2)

print("\n── Summary ──", flush=True)
print(f"  {'query':15s}  {'parquet_med':>12s}  {'catalog_med':>12s}  {'cached_rows':>12s}  {'speedup':>8s}", flush=True)
for qid, r in results.items():
    print(f"  {qid:15s}  {r['parquet']['median_ms']:>11.0f}ms  "
          f"{r['memory_catalog']['median_ms']:>11.0f}ms  "
          f"{r['cached_rows']:>12,}  "
          f"{round(r['parquet']['median_ms']/r['memory_catalog']['median_ms'],1):>7.1f}x", flush=True)

print("\nDone. Results: /tmp/catalog_impl1_results.json", flush=True)
srv.stop()
