#!/usr/bin/env python3
"""
Spark worker for explain collection — runs in subprocess to avoid SPARK_REMOTE contamination.
Outputs JSON to stdout.
"""
import os, sys, json, time, re
from pathlib import Path

DATA_DIR      = "/Users/rajeshganta/work/sail_parquet/data"
COMBINED_VIEW = "frtb_sa_capital_output"

QUERIES = [
    ("scan_only_count",
     f"SELECT COUNT(*) FROM {COMBINED_VIEW}"),
    ("scan_only_sum1col",
     f"SELECT SUM(total) FROM {COMBINED_VIEW}"),
    ("compute_only_inmem",
     f"SELECT SUM(v), COUNT(*) FROM (SELECT (id % 100) * 1.0 AS v FROM range(54880000))"),
    ("filtered_agg",
     f"""SELECT legal_entity_group_id, SUM(total) AS total_capital,
              SUM(equities_delta) AS eq_delta, SUM(interest_rates_delta) AS ir_delta
         FROM {COMBINED_VIEW} WHERE legal_entity_group_id < 17
         GROUP BY legal_entity_group_id ORDER BY total_capital DESC LIMIT 20"""),
    ("full_agg",
     f"""SELECT SUM(commodities_delta+commodities_vega+commodities_curvature) AS commodities_sa,
              SUM(equities_delta+equities_vega+equities_curvature) AS equities_sa,
              SUM(interest_rates_delta+interest_rates_vega+interest_rates_curvature) AS rates_sa,
              SUM(foreign_exchange_delta+foreign_exchange_vega+foreign_exchange_curvature) AS fx_sa
         FROM {COMBINED_VIEW}"""),
    ("grouped_agg",
     f"""SELECT legal_entity_group_id, correlation_scenario_id, driving_scenario,
              SUM(total) AS total_capital, COUNT(*) AS row_count
         FROM {COMBINED_VIEW}
         GROUP BY legal_entity_group_id, correlation_scenario_id, driving_scenario
         ORDER BY total_capital DESC LIMIT 20"""),
]

os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["PYSPARK_PYTHON"]  = sys.executable

from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .master("local-cluster[3,1,1024]")
         .appName("explain_collect_spark")
         .config("spark.driver.host",                "127.0.0.1")
         .config("spark.driver.bindAddress",         "127.0.0.1")
         .config("spark.sql.shuffle.partitions",     "3")
         .config("spark.sql.adaptive.enabled",       "true")
         .config("spark.sql.parquet.filterPushdown", "true")
         .config("spark.ui.enabled",                 "false")
         .config("spark.driver.memory",              "2g")
         .config("spark.executor.memory",            "1g")
         .getOrCreate())

spark.read.parquet(f"{DATA_DIR}/entity_hierarchy.parquet").createOrReplaceTempView("entity_hierarchy")
spark.read.parquet(f"{DATA_DIR}/chorushierarchy.parquet").createOrReplaceTempView("chorushierarchy")
spark.read.parquet(f"{DATA_DIR}/cobdate.parquet").createOrReplaceTempView("cobdate")
spark.sql(f"""CREATE OR REPLACE TEMP VIEW {COMBINED_VIEW} AS
    SELECT * FROM parquet.`{DATA_DIR}/parquet1.parquet`
    UNION ALL
    SELECT * FROM parquet.`{DATA_DIR}/parquet2.parquet`""")

explain_analyze = {}
explain_formatted = {}

for qid, sql in QUERIES:
    print(f"  spark {qid} ...", file=sys.stderr, flush=True)
    # Warmup (execute once)
    try:
        spark.sql(sql).collect()
    except Exception as e:
        print(f"  warmup failed: {e}", file=sys.stderr)

    # EXPLAIN ANALYZE — executes the query and returns plan with metrics
    try:
        rows = spark.sql(f"EXPLAIN ANALYZE {sql}").collect()
        explain_analyze[qid] = "\n".join(str(r[0]) for r in rows)
    except Exception as e:
        explain_analyze[qid] = f"FAILED: {e}"

    # EXPLAIN FORMATTED — static plan with pushed filters, schema, etc.
    try:
        rows = spark.sql(f"EXPLAIN FORMATTED {sql}").collect()
        explain_formatted[qid] = "\n".join(str(r[0]) for r in rows)
    except Exception as e:
        explain_formatted[qid] = f"FAILED: {e}"

    print(f"    ea={len(explain_analyze[qid])} chars  fmt={len(explain_formatted[qid])} chars",
          file=sys.stderr, flush=True)

spark.stop()
print(json.dumps({"explain_analyze": explain_analyze, "explain_formatted": explain_formatted}))
