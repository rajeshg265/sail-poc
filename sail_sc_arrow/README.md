# sail_sc_arrow — POC 3: Sail + Arrow HTTP Bridge over PostgreSQL

## What this tests

Sail SparkConnect with an Arrow HTTP bridge replacing JDBC as the data transport,
vs Spark JVM with JDBC. Data still lives in PostgreSQL (sharded via ShardingSphere
+ pgpool), but the Sail side reads each shard via connectorX (PG → Arrow IPC)
into Arrow files, then registers them as DataFusion external tables.

**Result:** FlightSQL mode is incompatible with ShardingSphere 5.5.0 (protocol
mismatch). The HTTP bridge (SAIL_ENGINE=http) works as a workaround but adds
round-trip overhead. This POC established the Arrow IPC path as a viable
alternative to JDBC — later used as the basis for the Parquet benchmarks.

## Architecture

```
PostgreSQL frtb_shard1 (:5440) ← pgpool :6432
PostgreSQL frtb_shard2 (:5441) ← pgpool :6433
              |
    connectorX (PG → Arrow IPC)
              |
    Arrow files /tmp/sail_flightsql/<view>/<shard>.arrow
              |
    Sail SparkConnect :50051  (DataFusion EXTERNAL TABLE STORED AS ARROW)
    + HTTP bridge :8080
              |
    Java QueryService (POST /query)
```

Two engine modes:
- `SAIL_ENGINE=http` (default) — HTTP bridge → SparkConnect (works)
- `SAIL_ENGINE=flightsql` — FlightSQL :32010 (incompatible with ShardingSphere 5.5.0)

## Prerequisites

| Dependency          | Version          |
|---------------------|------------------|
| Python              | 3.13+            |
| pysail              | 0.7.2            |
| pyspark             | 4.2.0            |
| Java                | 17               |
| Maven               | 3.9+             |
| PostgreSQL          | 16               |
| pgpool-II           | 4.7.2            |
| ShardingSphere      | 5.5.0 (in `infra/`) |

Install Python dependencies:
```bash
pip install pysail==0.7.2 pyspark==4.2.0 psycopg2-binary connectorx pyarrow \
            adbc-driver-flightsql flask pyyaml
```

## Setup

### 1. Start PostgreSQL shards

```bash
psql -U postgres -f infra/setup_db.sql
```

### 2. Create schema and seed data

```bash
cd infra
python seed_data.py
python setup_datasets.py
python setup_cluster_db.py
```

### 3. Start pgpool

```bash
cd infra
bash start_pgpool.sh          # shard pgpools :6432/:6433
bash start_pgpool_cluster.sh  # cluster pgpools :6440/:6441
```

> **macOS note:** `sudo sysctl -w kern.sysv.shmmax=134217728` required before pgpool.

### 4. Extract and start ShardingSphere Proxy

```bash
cd infra
tar -xzf shardingsphere-proxy.tar.gz
bash start_shardingsphere.sh
```

### 5. Configure secrets

```bash
cp .secrets.example .secrets
# edit .secrets with your PostgreSQL credentials
```

### 6. Build the Java QueryService

```bash
cd java
bash build_and_run.sh
```

### 7. Start Sail

```bash
cd sail
SAIL_ENGINE=http python bootstrap.py    # HTTP bridge mode (recommended)
```

### 8. Run benchmark or open UI

```
http://localhost:8080
```

## Key files

| File | Purpose |
|------|---------|
| `sail/bootstrap.py` | Starts Sail + HTTP bridge; reads PG shards via connectorX into Arrow files |
| `sail/ui_server.py` | Flask UI; refreshes Arrow files from pgpool before each query |
| `sail/shard_registry.yaml` | Shard topology |
| `infra/start_pgpool.sh` | Starts shard pgpools :6432/:6433 |
| `infra/start_pgpool_cluster.sh` | Starts cluster pgpools :6440/:6441 |

## Known issues

- FlightSQL (`SAIL_ENGINE=flightsql`) is incompatible with ShardingSphere 5.5.0 —
  use `SAIL_ENGINE=http` (default).
- connectorX reads entire shard on every query refresh — no incremental update.
- pysail 0.7.2 date pushdown bug applies to SparkConnect path; Arrow path bypasses it.
