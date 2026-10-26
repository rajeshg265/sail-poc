# sail_parquet_cluster_profile — POC 7: Parquet cluster profiling and root cause analysis

## What this tests

Deep profiling of the `sail_parquet_cluster` results to understand *why* Sail
underperforms in cluster mode. Captures JVM GC metrics, Parquet bytes read,
shuffle bytes, executor CPU vs GC time, and EXPLAIN ANALYZE plans from both
engines.

**Finding:** Confirmed root cause is pysail 0.7.2 `SELECT *` with no column
pruning — Sail reads and shuffles all 50+ columns even for queries that only
need 2-3. Spark's AQE and column projection limit data transfer correctly.

## Prerequisites

| Dependency | Version |
|------------|---------|
| Python     | 3.13+   |
| pysail     | 0.7.2   |
| pyspark    | 4.2.0   |

Install Python dependencies:
```bash
pip install pysail==0.7.2 pyspark==4.2.0 pyarrow
```

## Setup

Data is shared with `sail_parquet`. Generate it if not already done:
```bash
cd ../sail_parquet
python infra/generate_data.py
```

Or set `BENCHMARK_DATA_DIR` environment variable:
```bash
export BENCHMARK_DATA_DIR=/path/to/your/data
```

## Run

**Full profile benchmark** (Sail + Spark with all instrumentation):
```bash
python bench_profile.py
python bench_profile.py --sail-only
python bench_profile.py --spark-only
```

**Collect EXPLAIN ANALYZE plans:**
```bash
python _explain_collect.py
```

**Isolate Parquet decoder overhead:**
```bash
python _spark_decoder_isolation.py
```

**Compare Spark local[3] vs local-cluster[3,1,1024]:**
```bash
python _spark_local_vs_cluster.py
```

**Capture per-stage scan timing via Spark REST API:**
```bash
python _spark_scan_timing2.py
```

Results are saved to `results/` (not in git).

## Key files

| File | Purpose |
|------|---------|
| `bench_profile.py` | Full profiling benchmark with GC + shuffle + explain metrics |
| `_bench_spark_profile_worker.py` | Spark subprocess worker with GC logging and event log |
| `_explain_collect.py` | Collects EXPLAIN ANALYZE plans from both engines |
| `_spark_decoder_isolation.py` | Isolates Parquet decoder vs task scheduling as root cause |
| `_spark_local_vs_cluster.py` | Compares Spark local[3] vs local-cluster[3,1,1024] overhead |
| `_spark_scan_timing.py` | Per-stage executor timing via Spark event log |
| `_spark_scan_timing2.py` | Per-stage timing via Spark live REST API (localhost:4040) |
| `_repair_spark_result.py` | Utility: merges timing log + event log when event log parsing fails |

## Known issues

- Spark event log parsing requires the app to complete normally; use `_repair_spark_result.py`
  if the event log is incomplete.
- Sail EXPLAIN ANALYZE output format changed between pysail versions.
- `_spark_scan_timing2.py` requires Spark UI enabled (`spark.ui.enabled=true`).
