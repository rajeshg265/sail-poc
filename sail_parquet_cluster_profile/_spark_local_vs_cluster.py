#!/usr/bin/env python3
"""
Compare Spark local[3] (single JVM, all threads in-process) vs
local-cluster[3,1,1024] (3 separate executor JVMs with Netty RPC).
Difference isolates inter-JVM serialization + process overhead.
"""
import os, sys, time, json

os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["PYSPARK_PYTHON"]  = sys.executable

DATA_DIR = "/Users/rajeshganta/work/sail_parquet/data"
VIEW     = "frtb_sa_capital_output"

SQL = {
    "scan_only_sum1col":  f"SELECT SUM(total) FROM {VIEW}",
    "compute_only_inmem": f"SELECT SUM(CAST((id % 100) * 1.0 AS DOUBLE)), COUNT(*) FROM range(54880000)",
    "filtered_agg":       f"""SELECT legal_entity_group_id, SUM(total), SUM(equities_delta), SUM(interest_rates_delta)
                               FROM {VIEW} WHERE legal_entity_group_id < 17
                               GROUP BY legal_entity_group_id ORDER BY 2 DESC LIMIT 20""",
    "full_agg":           f"""SELECT SUM(commodities_delta+commodities_vega+commodities_curvature),
                               SUM(equities_delta+equities_vega+equities_curvature),
                               SUM(interest_rates_delta+interest_rates_vega+interest_rates_curvature),
                               SUM(foreign_exchange_delta+foreign_exchange_vega+foreign_exchange_curvature)
                               FROM {VIEW}""",
}

results = {}

for mode_name, master in [("local3", "local[3]"), ("local_cluster", "local-cluster[3,1,1024]")]:
    from pyspark.sql import SparkSession
    spark = (SparkSession.builder
             .master(master)
             .appName(f"local_vs_cluster_{mode_name}")
             .config("spark.driver.host",                "127.0.0.1")
             .config("spark.driver.bindAddress",         "127.0.0.1")
             .config("spark.sql.shuffle.partitions",     "3")
             .config("spark.sql.adaptive.enabled",       "true")
             .config("spark.sql.parquet.filterPushdown", "true")
             .config("spark.ui.enabled",                 "false")
             .config("spark.driver.memory",              "2g")
             .config("spark.executor.memory",            "1g")
             .getOrCreate())

    spark.sql(f"""CREATE OR REPLACE TEMP VIEW {VIEW} AS
        SELECT * FROM parquet.`{DATA_DIR}/parquet1.parquet`
        UNION ALL SELECT * FROM parquet.`{DATA_DIR}/parquet2.parquet`""")

    # two warmup runs
    for _ in range(2):
        try: spark.sql(SQL["scan_only_sum1col"]).collect()
        except: pass

    mode_results = {}
    for qname, sql in SQL.items():
        times = []
        for _ in range(5):
            t0 = time.time()
            spark.sql(sql).collect()
            times.append(round((time.time() - t0) * 1000, 1))
        times.sort()
        median = times[len(times)//2]
        mode_results[qname] = {"median_ms": median, "runs": times}
        print(f"  {mode_name} {qname}: {median}ms {times}", file=sys.stderr, flush=True)

    results[mode_name] = mode_results
    spark.stop()

# print ratio: local_cluster / local3
print("\n=== local-cluster overhead factor ===", file=sys.stderr)
for q in SQL:
    lc = results["local_cluster"][q]["median_ms"]
    l3 = results["local3"][q]["median_ms"]
    print(f"  {q}: local[3]={l3}ms  local-cluster={lc}ms  overhead={lc/l3:.2f}x", file=sys.stderr)

print(json.dumps(results))
