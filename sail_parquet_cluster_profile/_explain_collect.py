#!/usr/bin/env python3
"""
Collect EXPLAIN ANALYZE plans from both Sail and Spark for the isolation queries
and key main queries. Saves raw plans + structured summary to results/explain_plans.txt
and results/explain_plans.json.

Run with:
    python _explain_collect.py              # both engines
    python _explain_collect.py --sail-only
    python _explain_collect.py --spark-only
"""
import os, sys, json, time, re, argparse, subprocess
from pathlib import Path

DATA_DIR    = "/Users/rajeshganta/work/sail_parquet/data"
RESULTS_DIR = Path(__file__).parent / "results"
COMBINED_VIEW = "frtb_sa_capital_output"

QUERIES = [
    # Isolation: these isolate the bottleneck
    ("scan_only_count",
     f"SELECT COUNT(*) FROM {COMBINED_VIEW}"),
    ("scan_only_sum1col",
     f"SELECT SUM(total) FROM {COMBINED_VIEW}"),
    ("compute_only_inmem",
     f"SELECT SUM(v), COUNT(*) FROM (SELECT (id % 100) * 1.0 AS v FROM range(54880000))"),
    # Main: best and worst speedup
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


def collect_sail_plans():
    print("\n=== SAIL EXPLAIN ANALYZE ===", flush=True)

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
             .appName("explain_collect_sail")
             .getOrCreate())

    spark.read.parquet(f"{DATA_DIR}/entity_hierarchy.parquet").createOrReplaceTempView("entity_hierarchy")
    spark.read.parquet(f"{DATA_DIR}/chorushierarchy.parquet").createOrReplaceTempView("chorushierarchy")
    spark.read.parquet(f"{DATA_DIR}/cobdate.parquet").createOrReplaceTempView("cobdate")
    spark.sql(f"""CREATE OR REPLACE TEMP VIEW {COMBINED_VIEW} AS
        SELECT * FROM parquet.`{DATA_DIR}/parquet1.parquet`
        UNION ALL
        SELECT * FROM parquet.`{DATA_DIR}/parquet2.parquet`""")

    plans = {}
    for qid, sql in QUERIES:
        print(f"  {qid} ...", flush=True)
        # Warmup
        try:
            spark.sql(sql).collect()
        except Exception:
            pass
        # Execute and explain
        try:
            rows = spark.sql(f"EXPLAIN ANALYZE {sql}").collect()
            plan = "\n".join(str(r[0]) for r in rows)
        except Exception as e:
            plan = f"FAILED: {e}"
        plans[qid] = plan
        print(f"    -> {len(plan)} chars", flush=True)

    spark.stop()
    srv.stop()
    return plans


def collect_spark_plans():
    worker = Path(__file__).parent / "_explain_spark_worker.py"
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
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True, env=clean_env,
    )
    if proc.returncode != 0:
        print("SPARK WORKER STDERR:", proc.stderr[-3000:], flush=True)
        raise RuntimeError(f"Spark worker exited {proc.returncode}")
    return json.loads(proc.stdout.strip())


def parse_sail_plan(plan_text):
    """Extract per-operator timing rows from DataFusion EXPLAIN ANALYZE output."""
    rows = []
    for line in plan_text.splitlines():
        # DataFusion lines look like:
        #   ParquetExec: ..., metrics=[output_rows=54880000, elapsed_compute=...ms, ...]
        m_rows = re.search(r'output_rows=(\d+)', line)
        m_time = re.search(r'elapsed_compute=([0-9.]+)([mu]?s)', line)
        m_bytes = re.search(r'bytes_scanned=(\d+)', line)
        if m_rows or m_time:
            operator = line.strip().split(',')[0].strip().split('(')[0].strip()
            rows.append({
                "operator": operator[:60],
                "output_rows": int(m_rows.group(1)) if m_rows else None,
                "elapsed_ms": _to_ms(m_time) if m_time else None,
                "bytes_scanned": int(m_bytes.group(1)) if m_bytes else None,
            })
    return rows


def _to_ms(m):
    val, unit = float(m.group(1)), m.group(2)
    if unit == "us" or unit == "µs":
        return round(val / 1000, 3)
    if unit == "ms":
        return round(val, 3)
    if unit == "s":
        return round(val * 1000, 3)
    return round(val, 3)


def parse_spark_plan_features(plan_text):
    """Extract key features from Spark EXPLAIN output."""
    features = {}
    features["vectorized_scan"] = "vectorized" in plan_text.lower()
    features["columnar_to_row"] = "ColumnarToRow" in plan_text
    features["row_to_columnar"] = "RowToColumnar" in plan_text
    features["whole_stage_codegen"] = "WholeStageCodegen" in plan_text
    features["file_scan_present"] = "FileScan" in plan_text or "ParquetScan" in plan_text
    # Look for pushed filters
    m = re.search(r'PushedFilters: \[(.*?)\]', plan_text)
    features["pushed_filters"] = m.group(1) if m else "none"
    # Look for selected columns (PartitionFilters / RequiredSchema)
    m = re.search(r'ReadSchema: struct<(.*?)>', plan_text)
    features["read_schema"] = m.group(1)[:200] if m else "unknown"
    # Rows output from explain analyze
    rows_list = re.findall(r'number of output rows: (\d+)', plan_text)
    features["output_rows_per_node"] = [int(x) for x in rows_list]
    # Per-node timing
    times = re.findall(r'time to collect: ([\d.]+) ms', plan_text)
    features["node_times_ms"] = [float(x) for x in times]
    return features


def print_and_save(sail_plans, spark_data):
    out_lines = []

    def w(s=""):
        out_lines.append(s)
        print(s, flush=True)

    known_timings = {
        "scan_only_count":    {"sail_median": 5,    "spark_median": 411},
        "scan_only_sum1col":  {"sail_median": 82,   "spark_median": 874},
        "compute_only_inmem": {"sail_median": 496,  "spark_median": 1359},
        "filtered_agg":       {"sail_median": 272,  "spark_median": 2445},
        "full_agg":           {"sail_median": 3134, "spark_median": 5735},
        "grouped_agg":        {"sail_median": 521,  "spark_median": 3217},
    }

    structured = {}

    for qid, _ in QUERIES:
        t = known_timings.get(qid, {})
        sail_med = t.get("sail_median", "?")
        spark_med = t.get("spark_median", "?")
        speedup = round(spark_med / sail_med, 1) if isinstance(sail_med, int) and sail_med > 0 else "?"

        w()
        w("=" * 72)
        w(f"QUERY: {qid}  |  Sail median {sail_med}ms  Spark median {spark_med}ms  ({speedup}×)")
        w("=" * 72)

        # --- SAIL ---
        w("\n--- SAIL EXPLAIN ANALYZE ---")
        sail_plan = sail_plans.get(qid, "")
        w(sail_plan)
        sail_ops = parse_sail_plan(sail_plan)

        w("\n  Sail operator summary:")
        for op in sail_ops:
            parts = [f"  {op['operator']:<45}"]
            if op["output_rows"] is not None:
                parts.append(f"rows={op['output_rows']:>12,}")
            if op["elapsed_ms"] is not None:
                parts.append(f"elapsed={op['elapsed_ms']:>10.1f}ms")
            if op["bytes_scanned"] is not None:
                parts.append(f"bytes_scanned={op['bytes_scanned']:>12,}")
            w("".join(parts))

        # --- SPARK ---
        spark_plan_ea  = spark_data.get("explain_analyze", {}).get(qid, "")
        spark_plan_fmt = spark_data.get("explain_formatted", {}).get(qid, "")

        w("\n--- SPARK EXPLAIN ANALYZE ---")
        w(spark_plan_ea)

        w("\n--- SPARK EXPLAIN FORMATTED ---")
        w(spark_plan_fmt)

        spark_feats = parse_spark_plan_features(spark_plan_fmt or spark_plan_ea)
        w("\n  Spark plan features:")
        for k, v in spark_feats.items():
            w(f"    {k}: {v}")

        structured[qid] = {
            "sail_median_ms": sail_med,
            "spark_median_ms": spark_med,
            "speedup": speedup,
            "sail_operators": sail_ops,
            "spark_features": spark_feats,
        }

    txt_path = RESULTS_DIR / "explain_plans.txt"
    json_path = RESULTS_DIR / "explain_plans.json"
    txt_path.write_text("\n".join(out_lines))
    json_path.write_text(json.dumps(structured, indent=2))
    print(f"\nSaved: {txt_path}", flush=True)
    print(f"Saved: {json_path}", flush=True)
    return structured


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--sail-only",  action="store_true")
    parser.add_argument("--spark-only", action="store_true")
    args = parser.parse_args()

    sail_plans  = {}
    spark_data  = {}

    if not args.spark_only:
        sail_plans = collect_sail_plans()

    if not args.sail_only:
        spark_data = collect_spark_plans()

    print_and_save(sail_plans, spark_data)
