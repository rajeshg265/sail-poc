#!/usr/bin/env python3
"""Measure actual RSS for Implementation 1 with memory:///warehouse."""
import os, time, psutil

os.environ["SAIL_MODE"] = "local"
os.environ["SAIL_EXECUTION__DEFAULT_PARALLELISM"] = "4"
os.environ["SAIL_CATALOG__LIST"] = '[{name="memory_catalog",type="memory",initial_database=["default"]}]'
os.environ["SAIL_CATALOG__DEFAULT_CATALOG"] = "memory_catalog"

DATA_DIR = "/Users/rajeshganta/work/sail_parquet/data"
PARQUET  = f"parquet.`{DATA_DIR}/parquet1.parquet`"

from pysail.spark import SparkConnectServer
from pyspark.sql import SparkSession

srv = SparkConnectServer(port=50056)
srv.start(background=True)
time.sleep(3)

spark = (SparkSession.builder
    .remote("sc://localhost:50056")
    .config("spark.sql.warehouse.dir", "memory:///warehouse")
    .appName("ram_impl1")
    .getOrCreate())

spark.read.parquet(f"{DATA_DIR}/entity_hierarchy.parquet").createOrReplaceTempView("entity_hierarchy")
spark.read.parquet(f"{DATA_DIR}/chorushierarchy.parquet").createOrReplaceTempView("chorushierarchy")
spark.read.parquet(f"{DATA_DIR}/cobdate.parquet").createOrReplaceTempView("cobdate")

def rss_mb():
    return round(psutil.Process(os.getpid()).memory_info().rss / 1024**2, 1)

QUERIES = [
    ("full_agg",     f"SELECT SUM(commodities_delta+commodities_vega+commodities_curvature) AS commodities_sa, SUM(equities_delta+equities_vega+equities_curvature) AS equities_sa, SUM(interest_rates_delta+interest_rates_vega+interest_rates_curvature) AS rates_sa, SUM(foreign_exchange_delta+foreign_exchange_vega+foreign_exchange_curvature) AS fx_sa FROM {PARQUET}",          "memory_catalog.default.frtb_full_agg"),
    ("grouped_agg",  f"SELECT legal_entity_group_id, correlation_scenario_id, driving_scenario, SUM(total) AS total_capital, COUNT(*) AS row_count FROM {PARQUET} GROUP BY legal_entity_group_id, correlation_scenario_id, driving_scenario ORDER BY total_capital DESC LIMIT 20", "memory_catalog.default.frtb_grouped_agg"),
    ("filtered_agg", f"SELECT legal_entity_group_id, SUM(total) AS total_capital, SUM(equities_delta) AS eq_delta, SUM(interest_rates_delta) AS ir_delta FROM {PARQUET} WHERE legal_entity_group_id < 17 GROUP BY legal_entity_group_id ORDER BY total_capital DESC LIMIT 20", "memory_catalog.default.frtb_filtered_agg"),
    ("join_entity",  f"SELECT e.region, e.business_unit, SUM(f.total) AS total_capital, COUNT(*) AS row_count FROM {PARQUET} f JOIN entity_hierarchy e ON f.legal_entity_group_id = e.legal_entity_group_id GROUP BY e.region, e.business_unit ORDER BY total_capital DESC", "memory_catalog.default.frtb_join_entity"),
    ("join_desk",    f"SELECT c.desk_name, c.risk_pillar, SUM(f.total) AS total_capital, SUM(f.rrao) AS rrao_charge FROM {PARQUET} f JOIN chorushierarchy c ON f.node_id = c.node_id GROUP BY c.desk_name, c.risk_pillar ORDER BY total_capital DESC LIMIT 20", "memory_catalog.default.frtb_join_desk"),
    ("join_calendar",f"SELECT d.fiscal_year, d.fiscal_quarter, SUM(f.total) AS total_capital FROM {PARQUET} f JOIN cobdate d ON f.cobdate = d.cobdate GROUP BY d.fiscal_year, d.fiscal_quarter ORDER BY d.fiscal_year, d.fiscal_quarter", "memory_catalog.default.frtb_join_calendar"),
]

print(f"\n{'query':15s}  {'rows':>6s}  {'before':>10s}  {'after':>10s}  {'delta':>8s}", flush=True)
print("-" * 60, flush=True)

for qid, sql, table in QUERIES:
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    before = rss_mb()
    spark.sql(f"CREATE TABLE {table} AS {sql}")
    row_count = spark.sql(f"SELECT COUNT(*) AS n FROM {table}").collect()[0]["n"]
    after = rss_mb()
    print(f"{qid:15s}  {row_count:>6}  {before:>9.1f}MB  {after:>9.1f}MB  {after-before:>+7.1f}MB", flush=True)

srv.stop()
