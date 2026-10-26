# sail_parquet — POC 5: Sail vs Spark on Parquet files (local mode)

## What this tests

Sail (pysail SparkConnect, `local` mode) vs Spark JVM (`local[*]`), both reading
FRTB SA Capital data directly from Parquet files — no PostgreSQL, no JDBC.

**Result: Sail wins 2.3–7.3×.** This is the key POC that proves DataFusion is
genuinely faster than the Spark JVM when IO is not the bottleneck. Removing
JDBC isolates the query engine performance.

Data: 54.88M rows, ~9 GB across `parquet1.parquet` + `parquet2.parquet`.

## Prerequisites

| Dependency | Version |
|------------|---------|
| Python     | 3.13+   |
| pysail     | 0.7.2   |
| pyspark    | 4.2.0   |

Install Python dependencies:
```bash
pip install pysail==0.7.2 pyspark==4.2.0 pyarrow pandas flask
```

## Setup

### 1. Generate data

Run once to produce all Parquet files under `data/`:

```bash
python infra/generate_data.py       # parquet1.parquet + parquet2.parquet (~9 GB total)
python infra/generate_xl.py         # parquet_xl.parquet (optional, larger dataset)
python infra/generate_xxl.py        # parquet_xxl.parquet (optional, extra large)
```

Generate lookup tables (entity_hierarchy, chorushierarchy, cobdate):
```bash
python infra/generate_data.py       # lookup tables are included in this script
```

### 2. Run the benchmark

**Main 3-way benchmark** (raw parquet vs memory catalog vs pre-aggregated):
```bash
python bench_3way.py
```

**Standard Sail vs Spark benchmark:**
```bash
python bootstrap.py          # starts Sail SparkConnect :50056
python bootstrap_spark.py    # runs same queries against Spark
```

**Other benchmark variants:**
```bash
python bench_case1_rerun.py         # repeatable single-query comparison
python bench_arrow_vs_parquet.py    # Arrow IPC vs Parquet format comparison
python bench_mem_vs_disk.py         # memory catalog vs disk parquet
python bench_ram_usage.py           # RAM consumption tracking
```

### 3. UI server (optional)

```bash
python ui_server.py     # opens interactive query UI at http://localhost:8095
```

## Key files

| File | Purpose |
|------|---------|
| `bench_3way.py` | 3-way benchmark: raw parquet / memory catalog / pre-aggregated parquet |
| `bootstrap.py` | Starts Sail SparkConnect :50056, registers Parquet views |
| `bootstrap_spark.py` | Runs same FRTB queries against Spark local[*] |
| `infra/generate_data.py` | Generates parquet1+parquet2 (54.88M rows) + lookup tables |
| `infra/generate_xl.py` | Generates parquet_xl (larger dataset) |
| `ui_server.py` | Flask UI for interactive queries |

## Data files (not in git — generate locally)

| File | Size | Rows |
|------|------|------|
| `data/parquet1.parquet` | ~5 GB | 27.44M |
| `data/parquet2.parquet` | ~4 GB | 27.44M |
| `data/entity_hierarchy.parquet` | small | — |
| `data/chorushierarchy.parquet` | small | — |
| `data/cobdate.parquet` | small | — |

## Known issues

- pysail 0.7.2 `SELECT *` (no column pruning) is slower than explicit column lists,
  but in local mode the effect is within-process memory copy — less impactful than cluster.
- `cache()` is a no-op in pysail 0.7.2 — results are not pinned in memory.
- Date pushdown bug: applies to JDBC paths only; Parquet path is unaffected.
