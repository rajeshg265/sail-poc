#!/usr/bin/env python3
"""
3-way benchmark: raw parquet vs memory catalog vs pre-aggregated parquet on disk

For each query:
  1. Cache result in memory_catalog → measure RAM delta
  2. Dump same result to parquet on disk → measure file size
  3. Run a final aggregation on top via all 3 paths → measure latency

  Path A: raw parquet_xl/parquet1 → full computation every time
  Path B: memory_catalog (Arrow in RAM) → aggregate on top
  Path C: pre-aggregated parquet on disk → aggregate on top

Demo 1: parquet_xl, intermediate = 3.28M rows (large cached result)
Demo 2: parquet1,  FRTB queries, intermediate = 1-20 rows (small cached result)
"""
import os, time, json, statistics, psutil, shutil

os.environ["SAIL_MODE"] = "local"
os.environ["SAIL_EXECUTION__DEFAULT_PARALLELISM"] = "4"
os.environ["SAIL_SPARK__SESSION_TIMEOUT_SECS"] = "86400"
os.environ["SAIL_CATALOG__LIST"] = '[{name="memory_catalog",type="memory",initial_database=["default"]}]'
os.environ["SAIL_CATALOG__DEFAULT_CATALOG"] = "memory_catalog"

DATA_DIR  = "/Users/rajeshganta/work/sail_parquet/data"
CACHE_DIR = f"{DATA_DIR}/cache_3way"
os.makedirs(CACHE_DIR, exist_ok=True)

PARQUET1    = f"parquet.`{DATA_DIR}/parquet1.parquet`"
PARQUET_XXL = f"parquet.`{DATA_DIR}/parquet_xxl.parquet`"
N_RUNS     = 10

from pysail.spark import SparkConnectServer
from pyspark.sql import SparkSession

print("Starting Sail...", flush=True)
srv = SparkConnectServer(port=50056)
srv.start(background=True)
time.sleep(3)

spark = (SparkSession.builder
    .remote("sc://localhost:50056")
    .config("spark.sql.warehouse.dir", "memory:///warehouse")
    .appName("bench_3way")
    .getOrCreate())

spark.read.parquet(f"{DATA_DIR}/entity_hierarchy.parquet").createOrReplaceTempView("entity_hierarchy")
spark.read.parquet(f"{DATA_DIR}/chorushierarchy.parquet").createOrReplaceTempView("chorushierarchy")
spark.read.parquet(f"{DATA_DIR}/cobdate.parquet").createOrReplaceTempView("cobdate")

def rss_mb():
    return round(psutil.Process(os.getpid()).memory_info().rss / 1024**2, 1)

def dir_size_mb(path):
    total = sum(
        os.path.getsize(os.path.join(dp, f))
        for dp, _, files in os.walk(path)
        for f in files
        if not f.startswith("_")
    )
    return round(total / 1024**2, 3)

def run_n(fn, n):
    try: fn()
    except: pass
    times = []
    for _ in range(n):
        t0 = time.time()
        fn()
        times.append(round((time.time() - t0) * 1000, 1))
    med = round(statistics.median(times), 1)
    p99 = sorted(times)[max(0, int(len(times) * 0.99) - 1)]
    return med, p99

def speedup(a, b):
    return round(a / b, 1) if b > 0 else 0.0

def run_query(label, source_sql, intermediate_table, final_on_catalog, final_on_parquet, cache_path, n_source):
    print(f"\n── {label} ──", flush=True)

    # ── Materialize intermediate into memory catalog ──────────────────────────
    spark.sql(f"DROP TABLE IF EXISTS {intermediate_table}")
    before = rss_mb()
    t0 = time.time()
    spark.sql(f"CREATE TABLE {intermediate_table} AS {source_sql}")
    mat_ms = round((time.time() - t0) * 1000, 1)
    after = rss_mb()
    rss_delta = round(after - before, 1)
    row_count = spark.sql(f"SELECT COUNT(*) AS n FROM {intermediate_table}").collect()[0]["n"]
    print(f"  cached: {row_count:,} rows in {mat_ms:.0f}ms  RSS {rss_delta:+.1f}MB", flush=True)

    # ── Write intermediate to disk parquet ────────────────────────────────────
    if os.path.exists(cache_path):
        shutil.rmtree(cache_path)
    spark.sql(source_sql).write.mode("overwrite").parquet(cache_path)
    fsize = dir_size_mb(cache_path)
    print(f"  disk parquet: {fsize}MB for {row_count:,} rows", flush=True)

    # ── Path A: raw source → final aggregation ────────────────────────────────
    path_a_sql = final_on_parquet.replace("__SOURCE__", f"({source_sql})")
    a_med, a_p99 = run_n(lambda: spark.sql(path_a_sql).collect(), n_source)
    print(f"  [A] raw parquet        median={a_med:>7.0f}ms  p99={a_p99:>7.0f}ms", flush=True)

    # ── Path B: memory catalog → final aggregation ────────────────────────────
    path_b_sql = final_on_catalog
    b_med, b_p99 = run_n(lambda: spark.sql(path_b_sql).collect(), N_RUNS)
    print(f"  [B] memory catalog     median={b_med:>7.0f}ms  p99={b_p99:>7.0f}ms  speedup={speedup(a_med,b_med)}x", flush=True)

    # ── Path C: pre-agg parquet on disk → final aggregation ──────────────────
    path_c_sql = final_on_parquet.replace("__SOURCE__", f"parquet.`{cache_path}`")
    c_med, c_p99 = run_n(lambda: spark.sql(path_c_sql).collect(), N_RUNS)
    print(f"  [C] pre-agg parquet    median={c_med:>7.0f}ms  p99={c_p99:>7.0f}ms  speedup={speedup(a_med,c_med)}x  file={fsize}MB", flush=True)

    return {
        "cached_rows": row_count, "mat_ms": mat_ms, "rss_delta_mb": rss_delta, "file_mb": fsize,
        "path_a": {"median_ms": a_med, "p99_ms": a_p99},
        "path_b": {"median_ms": b_med, "p99_ms": b_p99},
        "path_c": {"median_ms": c_med, "p99_ms": c_p99},
    }

results = {}

# ──────────────────────────────────────────────────────────────────────────────
# DEMO 1: parquet_xl — large intermediate (3.28M rows)
# ──────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 65, flush=True)
print("DEMO 1: Large Intermediate  (parquet_xxl → ~28M groups → ~4GB RAM)", flush=True)
print("=" * 65, flush=True)

# Intermediate: group by node_id + cobdate + corr + driving → ~28M rows, all risk columns
D1_SRC = (
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
# Final query on top: further aggregate by scenario only → 9 rows
D1_FINAL_CAT = (
    "SELECT correlation_scenario_id, driving_scenario, SUM(total_capital) AS grand_total"
    " FROM memory_catalog.default.xxl_intermediate"
    " GROUP BY correlation_scenario_id, driving_scenario"
    " ORDER BY correlation_scenario_id, driving_scenario"
)
D1_FINAL_PAR = (
    "SELECT correlation_scenario_id, driving_scenario, SUM(total_capital) AS grand_total"
    " FROM __SOURCE__"
    " GROUP BY correlation_scenario_id, driving_scenario"
    " ORDER BY correlation_scenario_id, driving_scenario"
)

results["demo1_xxl"] = run_query(
    label="xxl_by_node_date_scenario_driving → scenario_driving_total",
    source_sql=D1_SRC,
    intermediate_table="memory_catalog.default.xxl_intermediate",
    final_on_catalog=D1_FINAL_CAT,
    final_on_parquet=D1_FINAL_PAR,
    cache_path=f"{CACHE_DIR}/xxl_intermediate",
    n_source=N_RUNS,
)

# ──────────────────────────────────────────────────────────────────────────────
# DEMO 2: parquet1 — FRTB queries (1-20 rows each)
# ──────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 65, flush=True)
print("DEMO 2: FRTB Queries  (parquet1 → 1-20 rows each)", flush=True)
print("=" * 65, flush=True)

FRTB = [
    ("full_agg",
     f"""SELECT SUM(commodities_delta+commodities_vega+commodities_curvature) AS commodities_sa,
                SUM(equities_delta+equities_vega+equities_curvature) AS equities_sa,
                SUM(interest_rates_delta+interest_rates_vega+interest_rates_curvature) AS rates_sa,
                SUM(foreign_exchange_delta+foreign_exchange_vega+foreign_exchange_curvature) AS fx_sa
         FROM {PARQUET1}""",
     "memory_catalog.default.frtb_full_agg",
     "SELECT * FROM memory_catalog.default.frtb_full_agg",
     "SELECT * FROM __SOURCE__"),

    ("grouped_agg",
     f"""SELECT legal_entity_group_id, correlation_scenario_id, driving_scenario,
                SUM(total) AS total_capital, COUNT(*) AS row_count
         FROM {PARQUET1}
         GROUP BY legal_entity_group_id, correlation_scenario_id, driving_scenario
         ORDER BY total_capital DESC LIMIT 20""",
     "memory_catalog.default.frtb_grouped_agg",
     "SELECT * FROM memory_catalog.default.frtb_grouped_agg ORDER BY total_capital DESC",
     "SELECT * FROM __SOURCE__ ORDER BY total_capital DESC"),

    ("filtered_agg",
     f"""SELECT legal_entity_group_id, SUM(total) AS total_capital,
                SUM(equities_delta) AS eq_delta, SUM(interest_rates_delta) AS ir_delta
         FROM {PARQUET1} WHERE legal_entity_group_id < 17
         GROUP BY legal_entity_group_id ORDER BY total_capital DESC LIMIT 20""",
     "memory_catalog.default.frtb_filtered_agg",
     "SELECT * FROM memory_catalog.default.frtb_filtered_agg WHERE total_capital > 0",
     "SELECT * FROM __SOURCE__ WHERE total_capital > 0"),

    ("join_entity",
     f"""SELECT e.region, e.business_unit, SUM(f.total) AS total_capital, COUNT(*) AS row_count
         FROM {PARQUET1} f
         JOIN entity_hierarchy e ON f.legal_entity_group_id = e.legal_entity_group_id
         GROUP BY e.region, e.business_unit ORDER BY total_capital DESC""",
     "memory_catalog.default.frtb_join_entity",
     "SELECT * FROM memory_catalog.default.frtb_join_entity ORDER BY total_capital DESC",
     "SELECT * FROM __SOURCE__ ORDER BY total_capital DESC"),

    ("join_desk",
     f"""SELECT c.desk_name, c.risk_pillar, SUM(f.total) AS total_capital, SUM(f.rrao) AS rrao_charge
         FROM {PARQUET1} f
         JOIN chorushierarchy c ON f.node_id = c.node_id
         GROUP BY c.desk_name, c.risk_pillar ORDER BY total_capital DESC LIMIT 20""",
     "memory_catalog.default.frtb_join_desk",
     "SELECT * FROM memory_catalog.default.frtb_join_desk ORDER BY total_capital DESC",
     "SELECT * FROM __SOURCE__ ORDER BY total_capital DESC"),

    ("join_calendar",
     f"""SELECT d.fiscal_year, d.fiscal_quarter, SUM(f.total) AS total_capital
         FROM {PARQUET1} f
         JOIN cobdate d ON f.cobdate = d.cobdate
         GROUP BY d.fiscal_year, d.fiscal_quarter
         ORDER BY d.fiscal_year, d.fiscal_quarter""",
     "memory_catalog.default.frtb_join_calendar",
     "SELECT * FROM memory_catalog.default.frtb_join_calendar ORDER BY fiscal_year, fiscal_quarter",
     "SELECT * FROM __SOURCE__ ORDER BY fiscal_year, fiscal_quarter"),
]

results["demo2"] = {}
for qid, src_sql, cat_table, final_cat, final_par in FRTB:
    results["demo2"][qid] = run_query(
        label=qid,
        source_sql=src_sql,
        intermediate_table=cat_table,
        final_on_catalog=final_cat,
        final_on_parquet=final_par,
        cache_path=f"{CACHE_DIR}/{qid}",
        n_source=N_RUNS,
    )

# ──────────────────────────────────────────────────────────────────────────────
# DEMO 3: parquet_xxl — large JOIN result cached in memory
# ──────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 65, flush=True)
print("DEMO 3: Large JOIN Result  (parquet_xxl JOIN cobdate → fiscal period)", flush=True)
print("=" * 65, flush=True)

# Intermediate: JOIN parquet_xxl with cobdate (1:1 on cobdate → fiscal_year/quarter)
# keeps ~26M rows enriched with fiscal period — demonstrates JOIN result caching without row explosion
D3_SRC = (
    f"SELECT f.node_id, f.cobdate, d.fiscal_year, d.fiscal_quarter,"
    f" f.correlation_scenario_id, f.driving_scenario,"
    f" SUM(f.total) AS total_capital,"
    f" SUM(f.equities_delta) AS eq_delta,"
    f" SUM(f.interest_rates_delta) AS ir_delta,"
    f" SUM(f.foreign_exchange_delta) AS fx_delta"
    f" FROM {PARQUET_XXL} f"
    f" JOIN cobdate d ON f.cobdate = d.cobdate"
    f" GROUP BY f.node_id, f.cobdate, d.fiscal_year, d.fiscal_quarter,"
    f" f.correlation_scenario_id, f.driving_scenario"
)
# Final query on top: aggregate by fiscal quarter + scenario → ~24 rows
D3_FINAL_CAT = (
    "SELECT fiscal_year, fiscal_quarter, correlation_scenario_id,"
    " SUM(total_capital) AS grand_total, SUM(eq_delta) AS eq_total, SUM(ir_delta) AS ir_total"
    " FROM memory_catalog.default.xxl_join_intermediate"
    " GROUP BY fiscal_year, fiscal_quarter, correlation_scenario_id"
    " ORDER BY fiscal_year, fiscal_quarter, correlation_scenario_id"
)
D3_FINAL_PAR = (
    "SELECT fiscal_year, fiscal_quarter, correlation_scenario_id,"
    " SUM(total_capital) AS grand_total, SUM(eq_delta) AS eq_total, SUM(ir_delta) AS ir_total"
    " FROM __SOURCE__"
    " GROUP BY fiscal_year, fiscal_quarter, correlation_scenario_id"
    " ORDER BY fiscal_year, fiscal_quarter, correlation_scenario_id"
)

results["demo3"] = run_query(
    label="xxl JOIN entity_hierarchy → region/scenario total",
    source_sql=D3_SRC,
    intermediate_table="memory_catalog.default.xxl_join_intermediate",
    final_on_catalog=D3_FINAL_CAT,
    final_on_parquet=D3_FINAL_PAR,
    cache_path=f"{CACHE_DIR}/xxl_join_intermediate",
    n_source=N_RUNS,
)

# ──────────────────────────────────────────────────────────────────────────────
# SUMMARY
# ──────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 65, flush=True)
print("SUMMARY", flush=True)
print("=" * 65, flush=True)

print("\nDEMO 1 — Large Intermediate (parquet_xxl → ~26M rows)", flush=True)
r = results["demo1_xxl"]
print(f"  cached rows={r['cached_rows']:,}  disk={r['file_mb']}MB", flush=True)
print(f"  {'':25s}  {'median':>8s}  {'p99':>8s}  {'speedup':>8s}", flush=True)
print(f"  {'A raw parquet_xxl':25s}  {r['path_a']['median_ms']:>7.0f}ms  {r['path_a']['p99_ms']:>7.0f}ms", flush=True)
print(f"  {'B memory catalog':25s}  {r['path_b']['median_ms']:>7.0f}ms  {r['path_b']['p99_ms']:>7.0f}ms  {speedup(r['path_a']['median_ms'],r['path_b']['median_ms']):>7.1f}x", flush=True)
print(f"  {'C pre-agg parquet disk':25s}  {r['path_c']['median_ms']:>7.0f}ms  {r['path_c']['p99_ms']:>7.0f}ms  {speedup(r['path_a']['median_ms'],r['path_c']['median_ms']):>7.1f}x", flush=True)

print("\nDEMO 2 — FRTB Queries (parquet1 → 1-20 rows)", flush=True)
print(f"  {'query':15s}  {'rows':>6s}  {'disk':>8s}  {'A(raw)':>8s}  {'B(mem)':>8s}  {'B spdup':>7s}  {'C(disk)':>8s}  {'C spdup':>7s}", flush=True)
for qid, r in results["demo2"].items():
    print(f"  {qid:15s}  {r['cached_rows']:>6}  {r['file_mb']:>6.3f}MB"
          f"  {r['path_a']['median_ms']:>7.0f}ms  {r['path_b']['median_ms']:>7.0f}ms"
          f"  {speedup(r['path_a']['median_ms'],r['path_b']['median_ms']):>6.1f}x"
          f"  {r['path_c']['median_ms']:>7.0f}ms"
          f"  {speedup(r['path_a']['median_ms'],r['path_c']['median_ms']):>6.1f}x", flush=True)

print("\nDEMO 3 — Large JOIN Result (parquet_xxl JOIN cobdate → fiscal period)", flush=True)
r = results["demo3"]
print(f"  cached rows={r['cached_rows']:,}  disk={r['file_mb']}MB", flush=True)
print(f"  {'':25s}  {'median':>8s}  {'p99':>8s}  {'speedup':>8s}", flush=True)
print(f"  {'A raw JOIN':25s}  {r['path_a']['median_ms']:>7.0f}ms  {r['path_a']['p99_ms']:>7.0f}ms", flush=True)
print(f"  {'B memory catalog':25s}  {r['path_b']['median_ms']:>7.0f}ms  {r['path_b']['p99_ms']:>7.0f}ms  {speedup(r['path_a']['median_ms'],r['path_b']['median_ms']):>7.1f}x", flush=True)
print(f"  {'C pre-agg parquet disk':25s}  {r['path_c']['median_ms']:>7.0f}ms  {r['path_c']['p99_ms']:>7.0f}ms  {speedup(r['path_a']['median_ms'],r['path_c']['median_ms']):>7.1f}x", flush=True)

with open("/tmp/bench_3way_results.json", "w") as f:
    json.dump(results, f, indent=2)

print("\nDone. Results: /tmp/bench_3way_results.json", flush=True)
srv.stop()
