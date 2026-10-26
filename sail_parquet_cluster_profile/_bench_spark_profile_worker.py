#!/usr/bin/env python3
"""
Spark profiling worker — subprocess-isolated, same queries as bench_cluster.py.

Additions over _bench_spark_worker.py:
  - GC logging (JVM -Xlog:gc*)
  - Spark event log (per-stage timing, bytes read, shuffle bytes, JVM GC time per task)
  - Isolation queries: scan-only COUNT(*) and in-memory compute-only
  - Controlled variants for filtered_agg: no predicate pushdown, no AQE
  - Parses event log after run; emits rich JSON to stdout
"""
import os, sys, json, time, re, glob, statistics, io
from pathlib import Path
try:
    import zstandard as zstd
    _HAS_ZSTD = True
except ImportError:
    _HAS_ZSTD = False

DATA_DIR      = "/Users/rajeshganta/work/sail_parquet/data"
RESULTS_DIR   = Path(__file__).parent / "results"
EVENTLOG_DIR  = str(RESULTS_DIR / "spark_eventlog")
GC_LOG        = str(RESULTS_DIR / "spark_gc.log")
N_RUNS        = 10
VIEW          = "frtb_sa_capital_output"

os.makedirs(EVENTLOG_DIR, exist_ok=True)

FRTB_QUERIES = [
    ("full_agg",
     f"""SELECT SUM(commodities_delta+commodities_vega+commodities_curvature) AS commodities_sa,
                SUM(equities_delta+equities_vega+equities_curvature) AS equities_sa,
                SUM(interest_rates_delta+interest_rates_vega+interest_rates_curvature) AS rates_sa,
                SUM(foreign_exchange_delta+foreign_exchange_vega+foreign_exchange_curvature) AS fx_sa
         FROM {VIEW}"""),
    ("grouped_agg",
     f"""SELECT legal_entity_group_id, correlation_scenario_id, driving_scenario,
                SUM(total) AS total_capital, COUNT(*) AS row_count
         FROM {VIEW}
         GROUP BY legal_entity_group_id, correlation_scenario_id, driving_scenario
         ORDER BY total_capital DESC LIMIT 20"""),
    ("filtered_agg",
     f"""SELECT legal_entity_group_id, SUM(total) AS total_capital,
                SUM(equities_delta) AS eq_delta, SUM(interest_rates_delta) AS ir_delta
         FROM {VIEW} WHERE legal_entity_group_id < 17
         GROUP BY legal_entity_group_id ORDER BY total_capital DESC LIMIT 20"""),
    ("join_entity",
     f"""SELECT e.region, e.business_unit, SUM(f.total) AS total_capital, COUNT(*) AS row_count
         FROM {VIEW} f
         JOIN entity_hierarchy e ON f.legal_entity_group_id = e.legal_entity_group_id
         GROUP BY e.region, e.business_unit ORDER BY total_capital DESC"""),
    ("join_desk",
     f"""SELECT c.desk_name, c.risk_pillar, SUM(f.total) AS total_capital, SUM(f.rrao) AS rrao_charge
         FROM {VIEW} f
         JOIN chorushierarchy c ON f.node_id = c.node_id
         GROUP BY c.desk_name, c.risk_pillar ORDER BY total_capital DESC LIMIT 20"""),
    ("join_calendar",
     f"""SELECT d.fiscal_year, d.fiscal_quarter, SUM(f.total) AS total_capital
         FROM {VIEW} f
         JOIN cobdate d ON f.cobdate = d.cobdate
         GROUP BY d.fiscal_year, d.fiscal_quarter
         ORDER BY d.fiscal_year, d.fiscal_quarter"""),
]

# Isolation queries
ISOLATION_QUERIES = [
    ("scan_only_count",
     f"SELECT COUNT(*) FROM {VIEW}",
     "Pure scan — no aggregation. Measures Parquet read + minimal compute."),
    ("scan_only_sum1col",
     f"SELECT SUM(total) FROM {VIEW}",
     "Single-column SUM — minimal compute after scan. Isolates scan vs multi-col compute."),
    ("compute_only_inmem",
     f"""SELECT SUM(v), COUNT(*) FROM (
           SELECT (id % 100) * 1.0 AS v FROM range(54880000)
         )""",
     "In-memory compute only — no Parquet I/O. Isolates pure aggregation cost."),
]

# Controlled variants for filtered_agg — quantify each optimization's contribution
CONTROLLED_VARIANTS = [
    ("filtered_agg_no_pushdown",
     f"""SELECT legal_entity_group_id, SUM(total) AS total_capital,
                SUM(equities_delta) AS eq_delta, SUM(interest_rates_delta) AS ir_delta
         FROM {VIEW} WHERE legal_entity_group_id < 17
         GROUP BY legal_entity_group_id ORDER BY total_capital DESC LIMIT 20""",
     {"spark.sql.parquet.filterPushdown": "false"},
     "filtered_agg with filterPushdown=false — quantifies predicate pushdown value"),
    ("filtered_agg_no_aqe",
     f"""SELECT legal_entity_group_id, SUM(total) AS total_capital,
                SUM(equities_delta) AS eq_delta, SUM(interest_rates_delta) AS ir_delta
         FROM {VIEW} WHERE legal_entity_group_id < 17
         GROUP BY legal_entity_group_id ORDER BY total_capital DESC LIMIT 20""",
     {"spark.sql.adaptive.enabled": "false"},
     "filtered_agg with AQE=false — quantifies adaptive query execution value"),
]

os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["PYSPARK_PYTHON"]  = sys.executable

from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .master("local-cluster[3,1,1024]")
         .appName("bench_parquet_profile_spark")
         .config("spark.driver.host",                "127.0.0.1")
         .config("spark.driver.bindAddress",         "127.0.0.1")
         .config("spark.sql.shuffle.partitions",     "3")
         .config("spark.sql.adaptive.enabled",       "true")
         .config("spark.sql.parquet.filterPushdown", "true")
         .config("spark.ui.enabled",                 "false")
         .config("spark.driver.memory",              "2g")
         .config("spark.executor.memory",            "1g")
         # GC logging — captures pause count, pause duration, GC type per executor
         .config("spark.executor.extraJavaOptions",
                 f"-Xlog:gc*:file={GC_LOG}:time,uptime:filecount=1,filesize=100m")
         # Spark event log — per-stage timing, task metrics, shuffle bytes, JVM GC time
         .config("spark.eventLog.enabled",           "true")
         .config("spark.eventLog.dir",               f"file://{EVENTLOG_DIR}")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

spark.read.parquet(f"{DATA_DIR}/entity_hierarchy.parquet").createOrReplaceTempView("entity_hierarchy")
spark.read.parquet(f"{DATA_DIR}/chorushierarchy.parquet").createOrReplaceTempView("chorushierarchy")
spark.read.parquet(f"{DATA_DIR}/cobdate.parquet").createOrReplaceTempView("cobdate")
spark.read.parquet(
    f"{DATA_DIR}/parquet1.parquet",
    f"{DATA_DIR}/parquet2.parquet",
).createOrReplaceTempView(VIEW)
print("Tables registered (parquet1 + parquet2 combined, 54.88M rows).", file=sys.stderr, flush=True)


def run_timed(sql, n=N_RUNS):
    """1 warmup + n timed runs. Returns (median_ms, p99_ms, times_ms, t_start_epoch_ms, t_end_epoch_ms)."""
    try:
        spark.sql(sql).collect()
    except Exception:
        pass
    times = []
    t_start = int(time.time() * 1000)
    for _ in range(n):
        t0 = time.time()
        spark.sql(sql).collect()
        times.append(round((time.time() - t0) * 1000, 1))
    t_end = int(time.time() * 1000)
    med = round(statistics.median(times), 1)
    p99 = sorted(times)[max(0, int(len(times) * 0.99) - 1)]
    return med, p99, times, t_start, t_end


# ── Main benchmark queries ────────────────────────────────────────────────────
print("\n[MAIN QUERIES]", file=sys.stderr, flush=True)
results = {}
query_timestamps = {}
for qid, sql in FRTB_QUERIES:
    print(f"  [{qid}]", file=sys.stderr, flush=True)
    med, p99, times, ts, te = run_timed(sql)
    print(f"    median={med:>7.0f}ms  p99={p99:>7.0f}ms", file=sys.stderr, flush=True)
    results[qid] = {"median_ms": med, "p99_ms": p99, "times_ms": times}
    query_timestamps[qid] = (ts, te)

# ── Isolation queries ─────────────────────────────────────────────────────────
print("\n[ISOLATION QUERIES]", file=sys.stderr, flush=True)
isolation_results = {}
for qid, sql, desc in ISOLATION_QUERIES:
    print(f"  [{qid}]", file=sys.stderr, flush=True)
    med, p99, times, ts, te = run_timed(sql, n=5)
    print(f"    median={med:>7.0f}ms  p99={p99:>7.0f}ms  — {desc}", file=sys.stderr, flush=True)
    isolation_results[qid] = {"median_ms": med, "p99_ms": p99, "times_ms": times,
                               "description": desc}
    query_timestamps[qid] = (ts, te)

# ── Controlled variants ───────────────────────────────────────────────────────
print("\n[CONTROLLED VARIANTS — filtered_agg]", file=sys.stderr, flush=True)
variant_results = {}
for qid, sql, extra_conf, desc in CONTROLLED_VARIANTS:
    # Apply the config change temporarily
    for k, v in extra_conf.items():
        spark.conf.set(k, v)
    print(f"  [{qid}]", file=sys.stderr, flush=True)
    med, p99, times, ts, te = run_timed(sql, n=5)
    print(f"    median={med:>7.0f}ms  p99={p99:>7.0f}ms  — {desc}", file=sys.stderr, flush=True)
    variant_results[qid] = {"median_ms": med, "p99_ms": p99, "times_ms": times,
                             "description": desc, "config_change": extra_conf}
    query_timestamps[qid] = (ts, te)
    # Restore
    for k, v in extra_conf.items():
        spark.conf.set(k, "true")

spark.stop()

# ── Parse event log ───────────────────────────────────────────────────────────
def parse_eventlog(eventlog_dir, query_timestamps):
    """
    Parse Spark event log to extract per-query:
      - bytes_read_mb  (actual Parquet bytes read from disk)
      - shuffle_write_mb
      - shuffle_read_mb
      - executor_run_time_ms  (sum across tasks, excludes scheduler overhead)
      - jvm_gc_time_ms        (JVM GC time inside tasks)
      - stage_count
      - stages detail
    """
    # Support both flat event logs and v2 rolling dirs (eventlog_v2_app-*/events_*)
    files = []
    for entry in glob.glob(f"{eventlog_dir}/*"):
        if os.path.isfile(entry):
            files.append(entry)
        elif os.path.isdir(entry):
            files.extend(f for f in glob.glob(f"{entry}/*") if os.path.isfile(f))
    if not files:
        return {}

    logfile = sorted(files, key=os.path.getsize)[-1]

    # Collect task metrics grouped by stage_id
    task_metrics = {}   # stage_id -> aggregated metrics
    stage_info   = {}   # stage_id -> {submit_time, complete_time, name}

    def _open_log(path):
        if path.endswith(".zstd") and _HAS_ZSTD:
            dctx = zstd.ZstdDecompressor()
            with open(path, "rb") as raw_f:
                data = dctx.stream_reader(raw_f).read()
            return io.StringIO(data.decode("utf-8", errors="replace"))
        return open(path, errors="replace")

    with _open_log(logfile) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                ev = json.loads(line)
            except json.JSONDecodeError:
                continue

            event = ev.get("Event", "")

            if event == "SparkListenerStageSubmitted":
                si = ev.get("Stage Info", {})
                sid = si.get("Stage ID")
                if sid is not None:
                    stage_info[sid] = {
                        "name": si.get("Stage Name", ""),
                        "submit_time": si.get("Submission Time", 0),
                        "complete_time": 0,
                    }

            elif event == "SparkListenerStageCompleted":
                si = ev.get("Stage Info", {})
                sid = si.get("Stage ID")
                if sid is not None:
                    if sid not in stage_info:
                        stage_info[sid] = {"name": si.get("Stage Name", ""),
                                           "submit_time": si.get("Submission Time", 0)}
                    stage_info[sid]["complete_time"] = si.get("Completion Time", 0)

            elif event == "SparkListenerTaskEnd":
                si    = ev.get("Stage ID")
                tm    = ev.get("Task Metrics", {})
                if si is None or not tm:
                    continue
                if si not in task_metrics:
                    task_metrics[si] = {
                        "bytes_read": 0, "shuffle_write_bytes": 0,
                        "shuffle_read_bytes": 0, "executor_run_time_ms": 0,
                        "jvm_gc_time_ms": 0, "task_count": 0,
                    }
                m = task_metrics[si]
                m["bytes_read"]           += tm.get("Input Metrics", {}).get("Bytes Read", 0)
                m["shuffle_write_bytes"]  += tm.get("Shuffle Write Metrics", {}).get("Shuffle Bytes Written", 0)
                m["shuffle_read_bytes"]   += (
                    tm.get("Shuffle Read Metrics", {}).get("Remote Bytes Read", 0) +
                    tm.get("Shuffle Read Metrics", {}).get("Local Bytes Read", 0)
                )
                m["executor_run_time_ms"] += tm.get("Executor Run Time", 0)
                m["jvm_gc_time_ms"]       += tm.get("JVM GC Time", 0)
                m["task_count"]           += 1

    # Correlate stages to queries using timestamps
    query_metrics = {}
    for qid, (t_start, t_end) in query_timestamps.items():
        # A stage belongs to this query if it submitted after query start and before query end + 10s buffer
        matched_stages = [
            sid for sid, info in stage_info.items()
            if t_start <= info.get("submit_time", 0) <= t_end + 10_000
        ]
        agg = {
            "bytes_read_mb": 0, "shuffle_write_mb": 0, "shuffle_read_mb": 0,
            "executor_run_time_ms": 0, "jvm_gc_time_ms": 0,
            "stage_count": len(matched_stages), "stages": [],
        }
        for sid in matched_stages:
            tm = task_metrics.get(sid, {})
            agg["bytes_read_mb"]           += tm.get("bytes_read", 0) / 1024**2
            agg["shuffle_write_mb"]        += tm.get("shuffle_write_bytes", 0) / 1024**2
            agg["shuffle_read_mb"]         += tm.get("shuffle_read_bytes", 0) / 1024**2
            agg["executor_run_time_ms"]    += tm.get("executor_run_time_ms", 0)
            agg["jvm_gc_time_ms"]          += tm.get("jvm_gc_time_ms", 0)
            si = stage_info.get(sid, {})
            agg["stages"].append({
                "id": sid, "name": si.get("name", ""),
                "duration_ms": si.get("complete_time", 0) - si.get("submit_time", 0),
                "task_count": tm.get("task_count", 0),
            })
        agg["bytes_read_mb"]        = round(agg["bytes_read_mb"], 1)
        agg["shuffle_write_mb"]     = round(agg["shuffle_write_mb"], 1)
        agg["shuffle_read_mb"]      = round(agg["shuffle_read_mb"], 1)
        agg["gc_pct_of_executor"]   = (
            round(agg["jvm_gc_time_ms"] / agg["executor_run_time_ms"] * 100, 1)
            if agg["executor_run_time_ms"] > 0 else 0
        )
        query_metrics[qid] = agg

    return query_metrics


def parse_gc_log(gc_log_path):
    """Parse JVM GC log. Returns {total_pauses, total_pause_ms, max_pause_ms, pauses:[]}."""
    if not os.path.exists(gc_log_path):
        return {"error": "gc log not found"}
    pauses = []
    # Match lines like: [0.123s] GC(0) Pause Young ... 3.456ms
    pattern = re.compile(r"Pause\s+\w+.*?([\d.]+)ms\s*$")
    with open(gc_log_path, errors="replace") as f:
        for line in f:
            m = pattern.search(line)
            if m:
                pauses.append(float(m.group(1)))
    if not pauses:
        return {"total_pauses": 0, "total_pause_ms": 0, "max_pause_ms": 0}
    return {
        "total_pauses":   len(pauses),
        "total_pause_ms": round(sum(pauses), 1),
        "max_pause_ms":   round(max(pauses), 1),
        "mean_pause_ms":  round(sum(pauses) / len(pauses), 1),
    }


# Run parsers
print("\n[PARSING PROFILING DATA]", file=sys.stderr, flush=True)
event_metrics = parse_eventlog(EVENTLOG_DIR, query_timestamps)
gc_summary    = parse_gc_log(GC_LOG)
print(f"  GC log: {gc_summary}", file=sys.stderr, flush=True)

# ── Emit final JSON ───────────────────────────────────────────────────────────
output = {
    "spark": results,
    "spark_isolation":        isolation_results,
    "spark_controlled":       variant_results,
    "spark_event_metrics":    event_metrics,
    "spark_gc_summary":       gc_summary,
}
print(json.dumps(output))
