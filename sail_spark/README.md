# sail_spark — POC 1: Sail vs Spark over PostgreSQL/JDBC (local mode)

## What this tests

Sail (pysail SparkConnect) vs Spark JVM, both querying FRTB SA Capital data stored in PostgreSQL,
sharded via ShardingSphere proxy + pgpool.

**Result:** Spark wins. The bottleneck is JDBC — both engines spend most time reading rows
from PostgreSQL over the wire. Sail's DataFusion engine provides no advantage when IO dominates.

## Architecture

```
PostgreSQL frtb_shard1 (TimescaleDB :5440) ← pgpool :6432
PostgreSQL frtb_shard2 (TimescaleDB :5441) ← pgpool :6433
                          |
              ShardingSphere Proxy :3307
                          |
          ┌───────────────┴───────────────┐
     Sail :50051 (SparkConnect)     Spark JVM :8104
     + HTTP bridge :8080             (local mode)
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
pip install pysail==0.7.2 pyspark==4.2.0 psycopg2-binary connectorx pyarrow flask
```

## Setup

### 1. Start PostgreSQL shards

Two PostgreSQL instances must be running:
- `frtb_shard1` on port 5440
- `frtb_shard2` on port 5441

```bash
# Create databases (run once)
psql -U postgres -f infra/setup_db.sql
```

### 2. Create schema and seed data

```bash
cd infra
python seed_data.py          # seeds frtb_shard1 and frtb_shard2
python setup_datasets.py     # creates lookup tables (entity_hierarchy, chorushierarchy, cobdate)
```

### 3. Start pgpool

Manages connection pooling to each shard.

```bash
cd infra
bash start_pgpool.sh         # starts pgpool_shard1 (:6432) and pgpool_shard2 (:6433)
```

> **macOS note:** pgpool requires `shmmax` to be raised. Run once:
> `sudo sysctl -w kern.sysv.shmmax=134217728`

### 4. Extract and start ShardingSphere Proxy

```bash
cd infra
tar -xzf shardingsphere-proxy.tar.gz
bash start_shardingsphere.sh    # starts ShardingSphere on :3307
```

### 5. Configure secrets

Copy `.secrets.example` to `.secrets` and fill in PostgreSQL credentials:
```bash
cp .secrets.example .secrets
# edit .secrets with your PostgreSQL username/password
```

### 6. Build the Java QueryService

```bash
cd java
bash build_and_run.sh
```

### 7. Start Sail

```bash
cd sail
python bootstrap.py           # starts SparkConnect :50051 + HTTP bridge :8080
```

### 8. Run the benchmark

From the `sail/` directory, open the UI:
```
http://localhost:8080
```

Or run Spark directly via `sail/bootstrap_spark.py` for the Spark side.

## Key files

| File | Purpose |
|------|---------|
| `sail/bootstrap.py` | Starts Sail SparkConnect + HTTP bridge, registers shards from `shard_registry.yaml` |
| `sail/bootstrap_spark.py` | Starts Spark JVM side |
| `sail/shard_registry.yaml` | Shard topology — edit to add/remove shards or datasets |
| `sail/ui_server.py` | Flask UI for running queries interactively |
| `infra/start_shardingsphere.sh` | Starts ShardingSphere proxy |
| `infra/start_pgpool.sh` | Starts pgpool for both shards |
| `infra/seed_data.py` | Seeds FRTB SA Capital data into PostgreSQL shards |

## Known issues

- pysail 0.7.2 date pushdown bug: date predicates sent as epoch integers, not date strings.
  Workaround applied in `bootstrap.py` — dates cast to `TEXT` before comparison.
- ShardingSphere + FlightSQL incompatible. HTTP bridge used instead (SAIL_ENGINE=http default).
- pgpool `num_init_children` must be set to at least 10 on macOS.
