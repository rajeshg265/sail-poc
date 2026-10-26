#!/usr/bin/env python3
"""
Parquet local-cluster benchmark: Sail (3 workers) vs Spark (3 executors)

Same 6 FRTB queries as the local benchmark, run against parquet1.parquet + parquet2.parquet
(54.88M rows, ~9 GB total). Only the execution mode changes: local-cluster with 3 workers/executors.

Sail : SAIL_MODE=local-cluster, 3 workers, SparkConnect :50057
Spark: local-cluster[3,1,1024], SPARK_LOCAL_IP=127.0.0.1

Data: /Users/rajeshganta/work/sail_parquet/data/ (read-only, shared with local benchmark)

Usage:
    python bench_cluster.py
    python bench_cluster.py --sail-only
    python bench_cluster.py --spark-only

Results saved to results/results.json
"""

import os
import sys
import json
import time
import statistics
import argparse
import subprocess
from pathlib import Path

DATA_DIR    = "/Users/rajeshganta/work/sail_parquet/data"
RESULTS_DIR = Path(__file__).parent / "results"
RESULTS_DIR.mkdir(exist_ok=True)

COMBINED_VIEW = "frtb_sa_capital_output"
N_RUNS        = 10

FRTB_QUERIES = [
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

    ("filtered_agg",
     f"""SELECT legal_entity_group_id, SUM(total) AS total_capital,
                SUM(equities_delta) AS eq_delta, SUM(interest_rates_delta) AS ir_delta
         FROM {COMBINED_VIEW} WHERE legal_entity_group_id < 17
         GROUP BY legal_entity_group_id ORDER BY total_capital DESC LIMIT 20"""),

    ("join_entity",
     f"""SELECT e.region, e.business_unit, SUM(f.total) AS total_capital, COUNT(*) AS row_count
         FROM {COMBINED_VIEW} f
         JOIN entity_hierarchy e ON f.legal_entity_group_id = e.legal_entity_group_id
         GROUP BY e.region, e.business_unit ORDER BY total_capital DESC"""),

    ("join_desk",
     f"""SELECT c.desk_name, c.risk_pillar, SUM(f.total) AS total_capital, SUM(f.rrao) AS rrao_charge
         FROM {COMBINED_VIEW} f
         JOIN chorushierarchy c ON f.node_id = c.node_id
         GROUP BY c.desk_name, c.risk_pillar ORDER BY total_capital DESC LIMIT 20"""),

    ("join_calendar",
     f"""SELECT d.fiscal_year, d.fiscal_quarter, SUM(f.total) AS total_capital
         FROM {COMBINED_VIEW} f
         JOIN cobdate d ON f.cobdate = d.cobdate
         GROUP BY d.fiscal_year, d.fiscal_quarter
         ORDER BY d.fiscal_year, d.fiscal_quarter"""),
]


def run_n(spark, sql, n):
    """1 warmup run (ignored) then n timed runs. Returns (median_ms, p99_ms, all_times)."""
    try:
        spark.sql(sql).collect()
    except Exception:
        pass
    times = []
    for _ in range(n):
        t0 = time.time()
        spark.sql(sql).collect()
        times.append(round((time.time() - t0) * 1000, 1))
    med = round(statistics.median(times), 1)
    p99 = sorted(times)[max(0, int(len(times) * 0.99) - 1)]
    return med, p99, times


# ── Sail ──────────────────────────────────────────────────────────────────────

def bench_sail():
    print("\n" + "=" * 65, flush=True)
    print("SAIL  local-cluster  3 workers  SparkConnect :50057", flush=True)
    print("=" * 65, flush=True)

    os.environ["SAIL_MODE"]                           = "local-cluster"
    os.environ["SAIL_CLUSTER__WORKER_INITIAL_COUNT"]  = "3"
    os.environ["SAIL_CLUSTER__WORKER_MAX_COUNT"]      = "3"
    os.environ["SAIL_CLUSTER__WORKER_TASK_SLOTS"]     = "2"
    os.environ["SAIL_EXECUTION__DEFAULT_PARALLELISM"] = "3"
    os.environ["SAIL_SPARK__SESSION_TIMEOUT_SECS"]    = "86400"
    os.environ["SAIL_CLUSTER__SHUFFLE_BACKEND__FLIGHT__COMPRESSION"] = "none"

    from pysail.spark import SparkConnectServer
    from pyspark.sql import SparkSession

    srv = SparkConnectServer(port=50057)
    srv.start(background=True)
    time.sleep(5)

    spark = (SparkSession.builder
             .remote("sc://localhost:50057")
             .appName("bench_parquet_cluster_sail")
             .getOrCreate())

    spark.read.parquet(f"{DATA_DIR}/entity_hierarchy.parquet").createOrReplaceTempView("entity_hierarchy")
    spark.read.parquet(f"{DATA_DIR}/chorushierarchy.parquet").createOrReplaceTempView("chorushierarchy")
    spark.read.parquet(f"{DATA_DIR}/cobdate.parquet").createOrReplaceTempView("cobdate")
    spark.sql(f"""CREATE OR REPLACE TEMP VIEW {COMBINED_VIEW} AS
        SELECT * FROM parquet.`{DATA_DIR}/parquet1.parquet`
        UNION ALL
        SELECT * FROM parquet.`{DATA_DIR}/parquet2.parquet`""")
    print("Tables registered (parquet1 + parquet2 combined, 54.88M rows).", flush=True)

    results = {}
    for qid, sql in FRTB_QUERIES:
        print(f"\n  [{qid}]", flush=True)
        med, p99, times = run_n(spark, sql, N_RUNS)
        print(f"    median={med:>7.0f}ms  p99={p99:>7.0f}ms  all={[round(t) for t in times]}", flush=True)
        results[qid] = {"median_ms": med, "p99_ms": p99, "times_ms": times}

    spark.stop()
    srv.stop()
    return results


# ── Spark ─────────────────────────────────────────────────────────────────────

def bench_spark():
    """Run Spark in a subprocess to avoid pysail module-level state contamination."""
    print("\n" + "=" * 65, flush=True)
    print("SPARK  local-cluster[3,1,1024]  3 executors", flush=True)
    print("=" * 65, flush=True)

    import subprocess
    worker = Path(__file__).parent / "_bench_spark_worker.py"
    proc = subprocess.run(
        [sys.executable, str(worker)],
        capture_output=False,
        stdout=subprocess.PIPE,
        stderr=None,   # stderr flows through to terminal so progress is visible
        text=True,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"Spark worker exited with code {proc.returncode}")
    return json.loads(proc.stdout.strip())


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sail-only",  action="store_true")
    parser.add_argument("--spark-only", action="store_true")
    args = parser.parse_args()

    output = {}

    if not args.spark_only:
        output["sail"] = bench_sail()

    if not args.sail_only:
        output["spark"] = bench_spark()

    # Summary
    if "sail" in output and "spark" in output:
        print("\n" + "=" * 65, flush=True)
        print("SUMMARY  (Sail local-cluster[3] vs Spark local-cluster[3,1,1024])", flush=True)
        print("=" * 65, flush=True)
        print(f"  {'Query':<18}  {'Sail P99':>10}  {'Spark P99':>10}  {'Speedup':>9}", flush=True)
        print(f"  {'-'*18}  {'-'*10}  {'-'*10}  {'-'*9}", flush=True)
        for qid, _ in FRTB_QUERIES:
            s = output["sail"][qid]["p99_ms"]
            k = output["spark"][qid]["p99_ms"]
            ratio = round(k / s, 2) if s > 0 else 0
            winner = "Sail" if s < k else "Spark"
            print(f"  {qid:<18}  {s:>8.0f}ms  {k:>8.0f}ms  {ratio:>6.1f}x  ({winner})", flush=True)

    ts = time.strftime("%Y%m%d_%H%M%S")
    out_path = RESULTS_DIR / f"results_{ts}.json"
    with open(out_path, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\nResults saved to {out_path}", flush=True)


if __name__ == "__main__":
    main()
