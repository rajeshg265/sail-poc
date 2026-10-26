#!/usr/bin/env python3
import os, time, statistics, psutil, shutil

os.environ["SAIL_MODE"] = "local"
os.environ["SAIL_EXECUTION__DEFAULT_PARALLELISM"] = "4"
os.environ["SAIL_SPARK__SESSION_TIMEOUT_SECS"] = "86400"
os.environ["SAIL_CATALOG__LIST"] = '[{name="memory_catalog",type="memory",initial_database=["default"]}]'
os.environ["SAIL_CATALOG__DEFAULT_CATALOG"] = "memory_catalog"

DATA_DIR  = "/Users/rajeshganta/work/sail_parquet/data"
CACHE_DIR = f"{DATA_DIR}/cache_3way"
PARQUET_XXL = f"parquet.`{DATA_DIR}/parquet_xxl.parquet`"
N = 10

from pysail.spark import SparkConnectServer
from pyspark.sql import SparkSession

srv = SparkConnectServer(port=50056)
srv.start(background=True)
time.sleep(3)

spark = (SparkSession.builder
    .remote("sc://localhost:50056")
    .config("spark.sql.warehouse.dir", "memory:///warehouse")
    .appName("bench_case1_rerun")
    .getOrCreate())

def rss_mb():
    return round(psutil.Process(os.getpid()).memory_info().rss / 1024**2, 1)

def run_n(fn, n):
    try: fn()
    except: pass
    times = []
    for _ in range(n):
        t0 = time.time()
        fn()
        times.append(round((time.time() - t0) * 1000, 1))
    times_sorted = sorted(times)
    med = round(statistics.median(times), 1)
    p99 = times_sorted[max(0, int(len(times) * 0.99) - 1)]
    return med, p99, times

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

print("=== Case 1 Re-run ===", flush=True)

# Materialize
spark.sql("DROP TABLE IF EXISTS memory_catalog.default.xxl_intermediate")
before = rss_mb()
t0 = time.time()
spark.sql(f"CREATE TABLE memory_catalog.default.xxl_intermediate AS {D1_SRC}")
mat_ms = round((time.time() - t0) * 1000)
rss_delta = round(rss_mb() - before, 1)
rows = spark.sql("SELECT COUNT(*) AS n FROM memory_catalog.default.xxl_intermediate").collect()[0]["n"]
print(f"  cached: {rows:,} rows in {mat_ms}ms  RSS {rss_delta:+.1f}MB", flush=True)

cache_path = f"{CACHE_DIR}/xxl_intermediate"

# Path A
path_a_sql = D1_FINAL_PAR.replace("__SOURCE__", f"({D1_SRC})")
a_med, a_p99, a_times = run_n(lambda: spark.sql(path_a_sql).collect(), N)
print(f"  [A] raw      median={a_med:>7.0f}ms  p99={a_p99:>7.0f}ms  all={[round(x) for x in a_times]}", flush=True)

# Path B
b_med, b_p99, b_times = run_n(lambda: spark.sql(D1_FINAL_CAT).collect(), N)
print(f"  [B] mem cat  median={b_med:>7.0f}ms  p99={b_p99:>7.0f}ms  all={[round(x) for x in b_times]}", flush=True)

# Path C
path_c_sql = D1_FINAL_PAR.replace("__SOURCE__", f"parquet.`{cache_path}`")
c_med, c_p99, c_times = run_n(lambda: spark.sql(path_c_sql).collect(), N)
print(f"  [C] disk par median={c_med:>7.0f}ms  p99={c_p99:>7.0f}ms  all={[round(x) for x in c_times]}", flush=True)

print(f"\n  B speedup vs A: {round(a_med/b_med,1)}x", flush=True)
print(f"  C speedup vs A: {round(a_med/c_med,1)}x", flush=True)
print(f"  B vs C diff: {round(b_med-c_med,1)}ms  ({round((b_med-c_med)/a_med*100,1)}% of A)", flush=True)

srv.stop()
