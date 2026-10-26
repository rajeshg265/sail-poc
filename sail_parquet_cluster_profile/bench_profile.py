#!/usr/bin/env python3
"""
Parquet cluster profiling benchmark — Sail + Spark with full instrumentation.

Collects evidence to explain WHY Sail is faster:
  Spark side (via _bench_spark_profile_worker.py):
    - JVM GC pause count, total ms, max pause
    - Actual Parquet bytes read from disk per query
    - Shuffle write/read MB per query
    - Executor CPU time vs JVM GC time (gc% of executor time)
    - Isolation queries: scan-only, compute-only
    - Controlled variants: filtered_agg without predicate pushdown / without AQE

  Sail side:
    - EXPLAIN ANALYZE per query (per-operator row counts + timing)
    - Isolation queries: same scan-only and compute-only

Usage:
    python bench_profile.py
    python bench_profile.py --sail-only
    python bench_profile.py --spark-only

Results saved to results/profile_<timestamp>.json
"""
import os, sys, json, time, statistics, argparse, subprocess
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

ISOLATION_QUERIES = [
    ("scan_only_count",
     f"SELECT COUNT(*) FROM {COMBINED_VIEW}",
     "Pure Parquet scan — no aggregation"),
    ("scan_only_sum1col",
     f"SELECT SUM(total) FROM {COMBINED_VIEW}",
     "Single-column SUM — minimal compute after scan"),
    ("compute_only_inmem",
     f"SELECT SUM(v), COUNT(*) FROM (SELECT (id % 100) * 1.0 AS v FROM range(54880000))",
     "In-memory compute only — zero Parquet I/O"),
]


def run_n(spark, sql, n=N_RUNS):
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


def explain_analyze(spark, sql):
    """Run EXPLAIN ANALYZE and return the plan text."""
    try:
        rows = spark.sql(f"EXPLAIN ANALYZE {sql}").collect()
        return "\n".join(str(r[0]) for r in rows)
    except Exception as e:
        return f"EXPLAIN ANALYZE failed: {e}"


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
             .appName("bench_parquet_profile_sail")
             .getOrCreate())

    spark.read.parquet(f"{DATA_DIR}/entity_hierarchy.parquet").createOrReplaceTempView("entity_hierarchy")
    spark.read.parquet(f"{DATA_DIR}/chorushierarchy.parquet").createOrReplaceTempView("chorushierarchy")
    spark.read.parquet(f"{DATA_DIR}/cobdate.parquet").createOrReplaceTempView("cobdate")
    spark.sql(f"""CREATE OR REPLACE TEMP VIEW {COMBINED_VIEW} AS
        SELECT * FROM parquet.`{DATA_DIR}/parquet1.parquet`
        UNION ALL
        SELECT * FROM parquet.`{DATA_DIR}/parquet2.parquet`""")
    print("Tables registered (parquet1 + parquet2 combined, 54.88M rows).", flush=True)

    results       = {}
    explain_plans = {}
    isolation     = {}

    # Main queries
    print("\n[MAIN QUERIES]", flush=True)
    for qid, sql in FRTB_QUERIES:
        print(f"\n  [{qid}]", flush=True)
        med, p99, times = run_n(spark, sql)
        print(f"    median={med:>7.0f}ms  p99={p99:>7.0f}ms", flush=True)
        results[qid] = {"median_ms": med, "p99_ms": p99, "times_ms": times}
        plan = explain_analyze(spark, sql)
        explain_plans[qid] = plan
        print(f"    EXPLAIN ANALYZE collected ({len(plan)} chars)", flush=True)

    # Isolation queries
    print("\n[ISOLATION QUERIES]", flush=True)
    for qid, sql, desc in ISOLATION_QUERIES:
        print(f"\n  [{qid}]  {desc}", flush=True)
        med, p99, times = run_n(spark, sql, n=5)
        print(f"    median={med:>7.0f}ms  p99={p99:>7.0f}ms", flush=True)
        isolation[qid] = {"median_ms": med, "p99_ms": p99, "times_ms": times,
                           "description": desc,
                           "explain_analyze": explain_analyze(spark, sql)}

    spark.stop()
    srv.stop()

    return {
        "timing":          results,
        "explain_analyze": explain_plans,
        "isolation":       isolation,
    }


# ── Spark ─────────────────────────────────────────────────────────────────────

def bench_spark():
    print("\n" + "=" * 65, flush=True)
    print("SPARK  local-cluster[3,1,1024]  profiling worker", flush=True)
    print("=" * 65, flush=True)

    worker = Path(__file__).parent / "_bench_spark_profile_worker.py"
    clean_env = {k: v for k, v in os.environ.items()
                 if k not in ("SPARK_REMOTE", "SPARK_CONNECT_MODE",
                              "SPARK_CONNECT_MODE_ENABLED", "SPARK_API_MODE",
                              "SAIL_MODE", "SAIL_CLUSTER__WORKER_INITIAL_COUNT",
                              "SAIL_CLUSTER__WORKER_MAX_COUNT",
                              "SAIL_CLUSTER__WORKER_TASK_SLOTS",
                              "SAIL_EXECUTION__DEFAULT_PARALLELISM",
                              "SAIL_SPARK__SESSION_TIMEOUT_SECS",
                              "SAIL_CLUSTER__SHUFFLE_BACKEND__FLIGHT__COMPRESSION")}
    proc = subprocess.run(
        [sys.executable, str(worker)],
        stdout=subprocess.PIPE,
        stderr=None,
        text=True,
        env=clean_env,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"Spark profile worker exited with code {proc.returncode}")
    return json.loads(proc.stdout.strip())


# ── Summary ───────────────────────────────────────────────────────────────────

def print_summary(output):
    sail_t  = output.get("sail",  {}).get("timing",  {})
    spark_t = output.get("spark", {}).get("spark",   {})
    ev      = output.get("spark", {}).get("spark_event_metrics", {})
    gc      = output.get("spark", {}).get("spark_gc_summary", {})

    print("\n" + "=" * 80, flush=True)
    print("PROFILING SUMMARY", flush=True)
    print("=" * 80, flush=True)

    if sail_t and spark_t:
        print(f"\n  {'Query':<20} {'Sail P99':>10} {'Spark P99':>10} {'Speedup':>8} "
              f"{'Bytes Read':>12} {'GC ms':>8} {'GC%':>6} {'Shuffle W':>10}", flush=True)
        print(f"  {'-'*20} {'-'*10} {'-'*10} {'-'*8} {'-'*12} {'-'*8} {'-'*6} {'-'*10}", flush=True)
        for qid, _ in FRTB_QUERIES:
            sp99 = spark_t.get(qid, {}).get("p99_ms", 0)
            sap99 = sail_t.get(qid, {}).get("p99_ms", 0)
            ratio = round(sp99 / sap99, 1) if sap99 > 0 else 0
            em = ev.get(qid, {})
            br   = f"{em.get('bytes_read_mb', 0):.0f} MB"
            gcms = f"{em.get('jvm_gc_time_ms', 0):.0f}"
            gcpct= f"{em.get('gc_pct_of_executor', 0):.1f}%"
            sw   = f"{em.get('shuffle_write_mb', 0):.0f} MB"
            print(f"  {qid:<20} {sap99:>8.0f}ms {sp99:>8.0f}ms {ratio:>7.1f}x "
                  f"{br:>12} {gcms:>8} {gcpct:>6} {sw:>10}", flush=True)

    print(f"\n  JVM GC overall: {gc}", flush=True)

    spark_iso = output.get("spark", {}).get("spark_isolation", {})
    sail_iso  = output.get("sail",  {}).get("isolation",       {})
    if spark_iso or sail_iso:
        print("\n  ISOLATION QUERIES:", flush=True)
        for qid in ["scan_only_count", "scan_only_sum1col", "compute_only_inmem"]:
            sp = spark_iso.get(qid, {})
            sa = sail_iso.get(qid,  {})
            print(f"    {qid:<25}  Sail {sa.get('median_ms','-'):>7}ms  "
                  f"Spark {sp.get('median_ms','-'):>7}ms  — {sp.get('description','')}", flush=True)

    spark_ctrl = output.get("spark", {}).get("spark_controlled", {})
    sail_fa    = sail_t.get("filtered_agg", {})
    spark_fa   = spark_t.get("filtered_agg", {})
    if spark_ctrl:
        print(f"\n  CONTROLLED VARIANTS (filtered_agg baseline: "
              f"Sail {sail_fa.get('p99_ms','-')}ms / Spark {spark_fa.get('p99_ms','-')}ms):", flush=True)
        for vid, v in spark_ctrl.items():
            print(f"    {vid:<35}  Spark {v.get('p99_ms','-'):>7}ms  — {v.get('description','')}", flush=True)


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

    print_summary(output)

    ts = time.strftime("%Y%m%d_%H%M%S")
    out_path = RESULTS_DIR / f"profile_{ts}.json"
    with open(out_path, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\nResults saved to {out_path}", flush=True)


if __name__ == "__main__":
    main()
