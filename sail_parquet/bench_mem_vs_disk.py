#!/usr/bin/env python3
"""
Benchmark: Sail memory catalog (in-memory Parquet) vs Sail disk Parquet.
Both use DataFusion. Memory catalog stores Parquet bytes in Rust memory — no disk I/O at query time.
"""
import os, time, json, statistics

os.environ["SAIL_MODE"] = "local"
os.environ["SAIL_EXECUTION__DEFAULT_PARALLELISM"] = "4"
os.environ["SAIL_SPARK__SESSION_TIMEOUT_SECS"] = "86400"
os.environ["SAIL_CATALOG__LIST"] = '[{name="memory_catalog",type="memory",initial_database=["default"]}]'
os.environ["SAIL_CATALOG__DEFAULT_CATALOG"] = "memory_catalog"

DATA_DIR = "/Users/rajeshganta/work/sail_parquet/data"
N_RUNS = 10

QUERIES = [
    ("full_agg", "Full Scan SUM",
     """SELECT SUM(commodities_delta+commodities_vega+commodities_curvature) AS commodities_sa,
               SUM(equities_delta+equities_vega+equities_curvature) AS equities_sa,
               SUM(interest_rates_delta+interest_rates_vega+interest_rates_curvature) AS rates_sa,
               SUM(foreign_exchange_delta+foreign_exchange_vega+foreign_exchange_curvature) AS fx_sa
        FROM {TABLE}"""),
    ("grouped_agg", "Group By Entity/Scenario",
     """SELECT legal_entity_group_id, correlation_scenario_id, driving_scenario,
               SUM(total) AS total_capital, COUNT(*) AS row_count
        FROM {TABLE}
        GROUP BY legal_entity_group_id, correlation_scenario_id, driving_scenario
        ORDER BY total_capital DESC LIMIT 20"""),
    ("filtered_agg", "Filtered WHERE < 17",
     """SELECT legal_entity_group_id, SUM(total) AS total_capital,
               SUM(equities_delta) AS eq_delta, SUM(interest_rates_delta) AS ir_delta
        FROM {TABLE} WHERE legal_entity_group_id < 17
        GROUP BY legal_entity_group_id ORDER BY total_capital DESC LIMIT 20"""),
    ("join_entity", "JOIN entity_hierarchy",
     """SELECT e.region, e.business_unit, SUM(f.total) AS total_capital, COUNT(*) AS row_count
        FROM {TABLE} f JOIN entity_hierarchy e ON f.legal_entity_group_id=e.legal_entity_group_id
        GROUP BY e.region, e.business_unit ORDER BY total_capital DESC"""),
    ("join_desk", "JOIN chorushierarchy",
     """SELECT c.desk_name, c.risk_pillar, SUM(f.total) AS total_capital, SUM(f.rrao) AS rrao_charge
        FROM {TABLE} f JOIN chorushierarchy c ON f.node_id=c.node_id
        GROUP BY c.desk_name, c.risk_pillar ORDER BY total_capital DESC LIMIT 20"""),
    ("join_calendar", "JOIN cobdate",
     """SELECT d.fiscal_year, d.fiscal_quarter, SUM(f.total) AS total_capital
        FROM {TABLE} f JOIN cobdate d ON f.cobdate=d.cobdate
        GROUP BY d.fiscal_year, d.fiscal_quarter ORDER BY d.fiscal_year, d.fiscal_quarter"""),
]

from pysail.spark import SparkConnectServer
from pyspark.sql import SparkSession

print("Starting Sail ...", flush=True)
srv = SparkConnectServer(port=50056)
srv.start(background=True)
time.sleep(3)

spark = SparkSession.builder.remote("sc://localhost:50056").appName("mem_bench").getOrCreate()

# Lookup tables (disk, tiny)
spark.read.parquet(f"{DATA_DIR}/entity_hierarchy.parquet").createOrReplaceTempView("entity_hierarchy")
spark.read.parquet(f"{DATA_DIR}/chorushierarchy.parquet").createOrReplaceTempView("chorushierarchy")
spark.read.parquet(f"{DATA_DIR}/cobdate.parquet").createOrReplaceTempView("cobdate")

# ── Load parquet1 into memory catalog as Arrow ───────────────────────────────
print("Materializing parquet1 into memory catalog (Arrow buffers) ...", flush=True)
t0 = time.time()
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS frtb_mem AS
    SELECT * FROM parquet.`{DATA_DIR}/parquet1.parquet`
""")
elapsed = round(time.time() - t0, 1)
n = spark.sql("SELECT COUNT(*) AS n FROM frtb_mem").collect()[0]["n"]
print(f"  frtb_mem: {n:,} rows materialized as Arrow in {elapsed}s", flush=True)

# Disk view
spark.read.parquet(f"{DATA_DIR}/parquet1.parquet").createOrReplaceTempView("frtb_disk")
print("  frtb_disk: parquet1 on disk", flush=True)

# ── Run benchmark ────────────────────────────────────────────────────────────
results = {}

for table_key, table_name in [("memory_catalog", "frtb_mem"), ("disk_parquet", "frtb_disk")]:
    results[table_key] = {}
    print(f"\n── {table_key} ({table_name}) ──", flush=True)
    for qid, qlabel, sql_tpl in QUERIES:
        sql = sql_tpl.replace("{TABLE}", table_name)
        try: spark.sql(sql).collect()
        except Exception as e: print(f"  warmup error {qid}: {e}", flush=True)

        times = []
        for _ in range(N_RUNS):
            t0 = time.time()
            spark.sql(sql).collect()
            times.append(round((time.time() - t0) * 1000, 1))

        p99 = sorted(times)[max(0, int(len(times)*0.99)-1)]
        med = round(statistics.median(times), 1)
        print(f"  {qid:15s}  median={med:6.0f}ms  p99={p99:6.0f}ms", flush=True)
        results[table_key][qid] = {"label": qlabel, "median_ms": med, "p99_ms": p99, "timings": times}

with open("/tmp/mem_vs_disk_results.json", "w") as f:
    json.dump(results, f, indent=2)
print("\nDone. Results: /tmp/mem_vs_disk_results.json", flush=True)
srv.stop()
