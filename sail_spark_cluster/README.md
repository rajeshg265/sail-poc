# sail_spark_cluster — POC 2: Sail vs Spark over PostgreSQL/JDBC (local-cluster mode)

## What this tests

Sail (pysail SparkConnect, `local-cluster` mode, 3 workers) vs Spark JVM
(`local-cluster[3,1,1024]`, 3 executors), both querying FRTB SA Capital data
stored in PostgreSQL via ShardingSphere proxy + pgpool.

**Result:** Spark still wins. Adding cluster mode to the JDBC stack doesn't help —
the bottleneck remains PostgreSQL row serialization over JDBC. Additionally,
Sail's `SELECT *` in local-cluster mode pulls all columns even when only a few
are needed (no column pruning in pysail 0.7.2).

## Architecture

```
PostgreSQL frtb_shard1 (TimescaleDB :5440) ← pgpool :6440
PostgreSQL frtb_shard2 (TimescaleDB :5441) ← pgpool :6441
                          |
              ShardingSphere Proxy :3308
                          |
     Sail local-cluster :50055 (3 DataFusion workers)
     + HTTP bridge :8096
          |
     Java QueryService (POST /query)
```

## Prerequisites

| Dependency        | Version          |
|-------------------|------------------|
| Python            | 3.13+            |
| pysail            | 0.7.2            |
| pyspark           | 4.2.0            |
| Java              | 17               |
| Maven             | 3.9+             |
| PostgreSQL        | 16               |
| pgpool-II         | 4.7.2            |
| ShardingSphere    | 5.5.0 (included in `infra/`) |

Install Python dependencies:
```bash
pip install pysail==0.7.2 pyspark==4.2.0 psycopg2-binary connectorx pyarrow flask pyyaml
```

## Setup

### 1. Start PostgreSQL shards

Two PostgreSQL instances must be running:
- `frtb_shard1` on port 5440
- `frtb_shard2` on port 5441

```bash
psql -U postgres -f infra/setup_db.sql
```

### 2. Create schema and seed data

```bash
cd infra
python seed_data.py
python setup_datasets.py
python setup_cluster_db.py     # additional cluster-mode setup
```

### 3. Start pgpool

```bash
cd infra
bash start_pgpool_cluster.sh   # starts pgpool_shard1 (:6440) and pgpool_shard2 (:6441)
```

> **macOS note:** `sudo sysctl -w kern.sysv.shmmax=134217728` required before starting pgpool.

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

### 7. Start Sail in local-cluster mode

```bash
cd sail
SAIL_WORKER_COUNT=3 SAIL_CLUSTER_MODE=local-cluster python bootstrap.py
```

### 8. Run the benchmark

Open the UI at `http://localhost:8096`, or run the Spark side via `sail/bootstrap_spark.py`.

## Key files

| File | Purpose |
|------|---------|
| `sail/bootstrap.py` | Starts Sail in local-cluster mode (3 workers), HTTP bridge :8096 |
| `sail/bootstrap_spark.py` | Starts Spark local-cluster[3,1,1024] |
| `sail/shard_registry.yaml` | Shard topology |
| `infra/start_pgpool_cluster.sh` | Starts pgpool on cluster ports :6440/:6441 |
| `infra/setup_cluster_db.py` | Cluster-specific DB setup |

## Known issues

- `SELECT *` (no column pruning) in pysail 0.7.2 causes Sail to read all columns over JDBC,
  amplifying the IO bottleneck in cluster mode.
- pgpool `num_init_children` must be ≥ 10 on macOS.
- pysail 0.7.2 date pushdown bug: dates sent as epoch integers. Workaround applied in bootstrap.
