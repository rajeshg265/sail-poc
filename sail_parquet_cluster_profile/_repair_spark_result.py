#!/usr/bin/env python3
"""
Combines timing data from log files with event log metrics into profile_<ts>.json.
Run after a benchmark where event log parsing failed but timing completed.
"""
import os, sys, json, time, re, glob, statistics, io
from pathlib import Path

try:
    import zstandard as zstd
    _HAS_ZSTD = True
except ImportError:
    _HAS_ZSTD = False

RESULTS_DIR = Path(__file__).parent / "results"

SPARK_QUERY_ORDER = [
    "full_agg", "grouped_agg", "filtered_agg",
    "join_entity", "join_desk", "join_calendar",
]
ISOLATION_ORDER = [
    ("scan_only_count",   "Pure scan — no aggregation. Measures Parquet read + minimal compute."),
    ("scan_only_sum1col", "Single-column SUM — minimal compute after scan."),
    ("compute_only_inmem","In-memory compute only — no Parquet I/O."),
]
CONTROLLED_ORDER = [
    ("filtered_agg_no_pushdown",
     "filtered_agg with filterPushdown=false — quantifies predicate pushdown value"),
    ("filtered_agg_no_aqe",
     "filtered_agg with AQE=false — quantifies adaptive query execution value"),
]


def parse_timing_from_log(logpath, section_start_marker=None, section_end_marker=None):
    """Extract median/p99 lines from a log file, optionally scoped to a section."""
    with open(logpath, "rb") as f:
        raw = f.read()
    text = re.sub(rb'\r', b'\n', raw).decode("utf-8", errors="replace")
    lines = text.split("\n")

    if section_start_marker:
        in_section = False
        filtered = []
        for l in lines:
            if section_start_marker in l:
                in_section = True
            if section_end_marker and section_end_marker in l:
                in_section = False
            if in_section:
                filtered.append(l)
        lines = filtered

    median_lines = [l for l in lines if "median=" in l and "p99=" in l and l.strip()]
    results = []
    for l in median_lines:
        m = re.search(r'median=\s*([\d.]+)ms\s+p99=\s*([\d.]+)ms', l)
        if m:
            results.append((float(m.group(1)), float(m.group(2))))
    return results


def parse_eventlog(eventlog_dir):
    """Parse Spark event log (supports .zstd) for per-stage bytes/shuffle/GC metrics."""
    files = []
    for entry in glob.glob(f"{eventlog_dir}/*"):
        if os.path.isfile(entry):
            files.append(entry)
        elif os.path.isdir(entry):
            files.extend(f for f in glob.glob(f"{entry}/*") if os.path.isfile(f))
    if not files:
        print("  No event log files found.")
        return {}

    # Use the largest file (completed run)
    logfile = sorted(files, key=os.path.getsize)[-1]
    print(f"  Using event log: {logfile} ({os.path.getsize(logfile)//1024}KB)")

    def open_log(path):
        if path.endswith(".zstd") and _HAS_ZSTD:
            dctx = zstd.ZstdDecompressor()
            with open(path, "rb") as raw_f:
                data = dctx.stream_reader(raw_f).read()
            return io.StringIO(data.decode("utf-8", errors="replace"))
        return open(path, errors="replace")

    task_metrics = {}
    stage_info   = {}

    with open_log(logfile) as f:
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
                if sid is not None and sid in stage_info:
                    stage_info[sid]["complete_time"] = si.get("Completion Time", 0)

            elif event == "SparkListenerTaskEnd":
                si = ev.get("Stage ID")
                tm = ev.get("Task Metrics", {})
                if si is None or tm is None:
                    continue
                inp    = tm.get("Input Metrics", {})
                out    = tm.get("Output Metrics", {})
                shuf_w = tm.get("Shuffle Write Metrics", {})
                shuf_r = tm.get("Shuffle Read Metrics", {})
                if si not in task_metrics:
                    task_metrics[si] = {
                        "bytes_read": 0, "bytes_written": 0,
                        "shuffle_write_bytes": 0, "shuffle_read_bytes": 0,
                        "executor_run_time_ms": 0, "jvm_gc_time_ms": 0,
                    }
                task_metrics[si]["bytes_read"]          += inp.get("Bytes Read", 0)
                task_metrics[si]["bytes_written"]       += out.get("Bytes Written", 0)
                task_metrics[si]["shuffle_write_bytes"] += shuf_w.get("Shuffle Bytes Written", 0)
                task_metrics[si]["shuffle_read_bytes"]  += shuf_r.get("Remote Bytes Read", 0) + shuf_r.get("Local Bytes Read", 0)
                task_metrics[si]["executor_run_time_ms"]+= tm.get("Executor Run Time", 0)
                task_metrics[si]["jvm_gc_time_ms"]      += tm.get("JVM GC Time", 0)

    # Aggregate across all stages (all stages belong to same app run)
    totals = {
        "bytes_read_mb": sum(v["bytes_read"] for v in task_metrics.values()) / 1e6,
        "shuffle_write_mb": sum(v["shuffle_write_bytes"] for v in task_metrics.values()) / 1e6,
        "shuffle_read_mb": sum(v["shuffle_read_bytes"] for v in task_metrics.values()) / 1e6,
        "executor_run_time_ms": sum(v["executor_run_time_ms"] for v in task_metrics.values()),
        "jvm_gc_time_ms": sum(v["jvm_gc_time_ms"] for v in task_metrics.values()),
        "stage_count": len(task_metrics),
    }
    if totals["executor_run_time_ms"] > 0:
        totals["gc_pct_of_executor"] = round(
            totals["jvm_gc_time_ms"] / totals["executor_run_time_ms"] * 100, 2
        )
    return {"overall": totals, "per_stage": {str(k): v for k, v in task_metrics.items()}}


def parse_gc_log(gc_log_path):
    """Parse JVM GC log for pause count, total ms, max pause ms."""
    if not os.path.exists(gc_log_path):
        return {}
    pauses = []
    with open(gc_log_path, errors="replace") as f:
        for line in f:
            m = re.search(r'GC\((\d+)\).*Pause.*?([\d.]+)ms', line)
            if m:
                pauses.append(float(m.group(2)))
    if not pauses:
        return {}
    return {
        "pause_count": len(pauses),
        "total_ms": round(sum(pauses), 1),
        "max_ms": round(max(pauses), 1),
        "p99_ms": round(sorted(pauses)[max(0, int(len(pauses)*0.99)-1)], 1),
    }


# ── Extract Sail timing from profile_run.log ──────────────────────────────────

sail_log = RESULTS_DIR / "profile_run.log"
sail_timings = parse_timing_from_log(
    sail_log,
    section_start_marker="SAIL  local-cluster",
    section_end_marker="SPARK  local-cluster",
)
print(f"Sail timing lines found: {len(sail_timings)}")

sail_timing, sail_iso = {}, {}
for i, qid in enumerate(SPARK_QUERY_ORDER):
    if i < len(sail_timings):
        sail_timing[qid] = {"median_ms": sail_timings[i][0], "p99_ms": sail_timings[i][1]}
for i, (qid, desc) in enumerate(ISOLATION_ORDER):
    idx = len(SPARK_QUERY_ORDER) + i
    if idx < len(sail_timings):
        sail_iso[qid] = {
            "median_ms": sail_timings[idx][0],
            "p99_ms": sail_timings[idx][1],
            "description": desc,
        }

# ── Extract Spark timing from spark_profile_run.log ───────────────────────────

spark_log = RESULTS_DIR / "spark_profile_run.log"
spark_timings = parse_timing_from_log(spark_log)
print(f"Spark timing lines found: {len(spark_timings)}")

spark_timing, iso_timing, ctrl_timing = {}, {}, {}
for i, qid in enumerate(SPARK_QUERY_ORDER):
    if i < len(spark_timings):
        spark_timing[qid] = {"median_ms": spark_timings[i][0], "p99_ms": spark_timings[i][1]}
for i, (qid, desc) in enumerate(ISOLATION_ORDER):
    idx = len(SPARK_QUERY_ORDER) + i
    if idx < len(spark_timings):
        iso_timing[qid] = {
            "median_ms": spark_timings[idx][0],
            "p99_ms": spark_timings[idx][1],
            "description": desc,
        }
for i, (vid, desc) in enumerate(CONTROLLED_ORDER):
    idx = len(SPARK_QUERY_ORDER) + len(ISOLATION_ORDER) + i
    if idx < len(spark_timings):
        ctrl_timing[vid] = {
            "median_ms": spark_timings[idx][0],
            "p99_ms": spark_timings[idx][1],
            "description": desc,
        }

# ── Parse event log ───────────────────────────────────────────────────────────

print("Parsing event log...")
event_metrics = parse_eventlog(str(RESULTS_DIR / "spark_eventlog"))

print("Parsing GC log...")
gc_summary = parse_gc_log(str(RESULTS_DIR / "spark_gc.log"))
print(f"  gc: {gc_summary}")

# ── Save combined result ──────────────────────────────────────────────────────

output = {
    "sail": {
        "timing":    sail_timing,
        "isolation": sail_iso,
    },
    "spark": {
        "spark":              spark_timing,
        "spark_isolation":    iso_timing,
        "spark_controlled":   ctrl_timing,
        "spark_event_metrics": event_metrics,
        "spark_gc_summary":   gc_summary,
    },
}

ts = time.strftime("%Y%m%d_%H%M%S")
out_path = RESULTS_DIR / f"profile_{ts}.json"
with open(out_path, "w") as f:
    json.dump(output, f, indent=2)
print(f"\nSaved: {out_path}")

# ── Print summary ─────────────────────────────────────────────────────────────

print(f"\n{'Query':<20} {'Sail P99':>10} {'Spark P99':>10} {'Speedup':>8}")
print(f"  {'-'*20} {'-'*10} {'-'*10} {'-'*8}")
for qid in SPARK_QUERY_ORDER:
    sa = sail_timing.get(qid, {}).get("p99_ms", 0)
    sp = spark_timing.get(qid, {}).get("p99_ms", 0)
    ratio = round(sp / sa, 1) if sa > 0 else 0
    print(f"  {qid:<20} {sa:>8.0f}ms {sp:>8.0f}ms {ratio:>6.1f}x")

print(f"\nISOLATION (Spark P99):")
for qid, _ in ISOLATION_ORDER:
    sa = sail_iso.get(qid, {}).get("p99_ms", "-")
    sp = iso_timing.get(qid, {}).get("p99_ms", "-")
    print(f"  {qid:<25}  Sail {sa}ms  Spark {sp}ms")

if event_metrics:
    ov = event_metrics.get("overall", {})
    print(f"\nEvent log overall: bytes_read={ov.get('bytes_read_mb',0):.0f}MB  "
          f"shuffle_w={ov.get('shuffle_write_mb',0):.0f}MB  "
          f"gc%={ov.get('gc_pct_of_executor',0):.1f}%  "
          f"stages={ov.get('stage_count',0)}")
