#!/usr/bin/env python3
"""
Standalone benchmark: Sail Arrow (in-RAM) vs Sail Parquet (disk)
Same DataFusion engine, same 27.37M rows, no HTTP bridge overhead.
"""
import os, time, json, statistics

os.environ["SAIL_MODE"] = "local"
os.environ["SAIL_EXECUTION__DEFAULT_PARALLELISM"] = "4"
os.environ["SAIL_SPARK__SESSION_TIMEOUT_SECS"] = "86400"

DATA_DIR = "/Users/rajeshganta/work/sail_parquet/data"

COLS = [
    "cobdate", "legal_entity_group_id", "correlation_scenario_id",
    "driving_scenario", "node_id",
    "commodities_delta", "commodities_vega", "commodities_curvature",
    "equities_delta", "equities_vega", "equities_curvature",
    "interest_rates_delta", "interest_rates_vega", "interest_rates_curvature",
    "foreign_exchange_delta", "foreign_exchange_vega", "foreign_exchange_curvature",
    "total", "rrao"
]

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
     """SELECT e.region, e.business_unit, SUM(f.total) AS total_capital,
               SUM(f.equities_delta) AS eq_delta, COUNT(*) AS row_count
        FROM {TABLE} f JOIN entity_hierarchy e ON f.legal_entity_group_id=e.legal_entity_group_id
        GROUP BY e.region, e.business_unit ORDER BY total_capital DESC"""),
    ("join_desk", "JOIN chorushierarchy",
     """SELECT c.desk_name, c.risk_pillar, SUM(f.total) AS total_capital,
               SUM(f.rrao) AS rrao_charge
        FROM {TABLE} f JOIN chorushierarchy c ON f.node_id=c.node_id
        GROUP BY c.desk_name, c.risk_pillar ORDER BY total_capital DESC LIMIT 20"""),
    ("join_calendar", "JOIN cobdate",
     """SELECT d.fiscal_year, d.fiscal_quarter, SUM(f.total) AS total_capital,
               SUM(f.commodities_delta) AS commodities_delta
        FROM {TABLE} f JOIN cobdate d ON f.cobdate=d.cobdate
        GROUP BY d.fiscal_year, d.fiscal_quarter ORDER BY d.fiscal_year, d.fiscal_quarter"""),
]

N_RUNS = 10

import pyarrow as pa
import pyarrow.parquet as pq
from pysail.spark import SparkConnectServer
from pyspark.sql import SparkSession
from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition

# ── Load Arrow table into RAM ────────────────────────────────────────────────
print("Loading parquet1 into RAM ...", flush=True)
t0 = time.time()
arrow_table = pq.read_table(f"{DATA_DIR}/parquet1.parquet", columns=COLS)
load_s = round(time.time() - t0, 1)
size_gb = arrow_table.nbytes / 1024**3
print(f"  {len(arrow_table):,} rows  {size_gb:.2f} GB  loaded in {load_s}s", flush=True)

N = 4
rpp = len(arrow_table) // N
import __main__
__main__._BATCHES = [
    arrow_table.slice(i * rpp, rpp if i < N-1 else len(arrow_table) - i*rpp)
    for i in range(N)
]
__main__._SCHEMA = arrow_table.schema
__main__._N = N
del arrow_table

# ── Start Sail ───────────────────────────────────────────────────────────────
print("Starting Sail ...", flush=True)
srv = SparkConnectServer(port=50056)
srv.start(background=True)
time.sleep(3)
spark = SparkSession.builder.remote("sc://localhost:50056").appName("arrow_bench").getOrCreate()

# Register lookup tables (tiny, disk is fine)
spark.read.parquet(f"{DATA_DIR}/entity_hierarchy.parquet").createOrReplaceTempView("entity_hierarchy")
spark.read.parquet(f"{DATA_DIR}/chorushierarchy.parquet").createOrReplaceTempView("chorushierarchy")
spark.read.parquet(f"{DATA_DIR}/cobdate.parquet").createOrReplaceTempView("cobdate")

# Arrow DataSource
class _Reader(DataSourceReader):
    def partitions(self):
        import __main__; return [InputPartition(i) for i in range(__main__._N)]
    def read(self, partition):
        import __main__
        yield from __main__._BATCHES[partition.value].to_batches(max_chunksize=65536)

class _DS(DataSource):
    @classmethod
    def name(cls): return "frtb_arrow"
    def schema(self): import __main__; return __main__._SCHEMA
    def reader(self, schema): return _Reader()

spark.dataSource.register(_DS)
spark.sql("CREATE TABLE IF NOT EXISTS frtb_arrow USING frtb_arrow")

# Parquet disk view
spark.read.parquet(f"{DATA_DIR}/parquet1.parquet").createOrReplaceTempView("frtb_disk")
print("Tables ready.", flush=True)

# ── Benchmark ────────────────────────────────────────────────────────────────
results = {}

for table_key, table_name in [("sail_arrow", "frtb_arrow"), ("sail_disk", "frtb_disk")]:
    results[table_key] = {}
    print(f"\n── {table_key} ({table_name}) ──", flush=True)

    for qid, qlabel, sql_tpl in QUERIES:
        sql = sql_tpl.replace("{TABLE}", table_name)
        # warmup
        try: spark.sql(sql).collect()
        except: pass

        times = []
        for _ in range(N_RUNS):
            t0 = time.time()
            spark.sql(sql).collect()
            times.append(round((time.time() - t0) * 1000, 1))

        p99 = sorted(times)[max(0, int(len(times)*0.99)-1)]
        med = round(statistics.median(times), 1)
        print(f"  {qid:15s} p99={p99:6.0f}ms  median={med:6.0f}ms  timings={times}", flush=True)
        results[table_key][qid] = {"label": qlabel, "median_ms": med, "p99_ms": p99, "timings": times}

# ── Output JSON ──────────────────────────────────────────────────────────────
out = "/tmp/arrow_vs_parquet_results.json"
with open(out, "w") as f:
    json.dump(results, f, indent=2)
print(f"\nResults written to {out}", flush=True)

srv.stop()
