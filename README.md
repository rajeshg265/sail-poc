# sail-poc — Sail vs Spark POC Collection

A series of proof-of-concept benchmarks comparing [Sail](https://github.com/lakehq/sail)
(pysail 0.7.2, DataFusion-backed SparkConnect) against Apache Spark JVM across
different data access patterns and deployment modes.

## The Story

Each POC answers one question. Together they trace a root-cause investigation:

| POC | Mode | Data source | Winner | Why |
|-----|------|-------------|--------|-----|
| [sail_spark](sail_spark/) | local | PostgreSQL / JDBC | Spark | JDBC is the bottleneck; engine doesn't matter |
| [sail_spark_cluster](sail_spark_cluster/) | local-cluster (3w) | PostgreSQL / JDBC | Spark | Same — cluster mode doesn't help when IO dominates |
| [sail_sc_arrow](sail_sc_arrow/) | local | PostgreSQL / Arrow HTTP bridge | — | FlightSQL incompatible with ShardingSphere; established Arrow IPC path |
| [sail_k8s_cluster](sail_k8s_cluster/) | K8s cluster (3 pods) | PostgreSQL / JDBC | Spark 1.4–2.5× | SELECT * (no column pruning) amplified over pod network |
| [sail_parquet](sail_parquet/) | local | Parquet files | **Sail 2.3–7.3×** | Removes JDBC; DataFusion faster than Spark JVM on pure compute |
| [sail_parquet_cluster](sail_parquet_cluster/) | local-cluster (3w) | Parquet files | Spark | SELECT * shuffles all columns via Arrow Flight even for aggregations |
| [sail_parquet_cluster_profile](sail_parquet_cluster_profile/) | local-cluster (3w) | Parquet files | — | Deep profiling confirming SELECT * / no column pruning as root cause |

**Key finding:** Sail's DataFusion engine is 2.3–7.3× faster than Spark when JDBC
is removed and queries use explicit column projections. The pysail 0.7.2 limitation
is `SELECT *` with no column pruning — this is the single root cause for every case
where Spark wins in cluster mode.

The final, published benchmark (column-pruned FRTB queries on Parquet, cluster mode)
is at [sail-parquet-benchmark](https://github.com/rajeshg265/sail-parquet-benchmark).

## Common prerequisites

| Dependency     | Version   |
|----------------|-----------|
| Python         | 3.13+     |
| pysail         | 0.7.2     |
| pyspark        | 4.2.0     |
| Java           | 17        |
| Maven          | 3.9+      |
| PostgreSQL     | 16        |
| pgpool-II      | 4.7.2     |

Base install:
```bash
pip install pysail==0.7.2 pyspark==4.2.0 pyarrow psycopg2-binary connectorx flask pyyaml
```

JDBC/PostgreSQL POCs additionally require:
- ShardingSphere Proxy 5.5.0 (bundled in each `infra/` directory, extract before use)
- TimescaleDB extension for PostgreSQL

Parquet POCs require only Python packages — no database needed.

## Data

**PostgreSQL POCs** (`sail_spark`, `sail_spark_cluster`, `sail_sc_arrow`, `sail_k8s_cluster`):
- Run `infra/seed_data.py` inside each POC after creating the schema with `infra/setup_db.sql`

**Parquet POCs** (`sail_parquet`, `sail_parquet_cluster`, `sail_parquet_cluster_profile`):
- Run `sail_parquet/infra/generate_data.py` once — all Parquet POCs read from the same `data/` directory
- Data files (~9 GB) are not in git

## macOS notes

- pgpool requires raised shared memory: `sudo sysctl -w kern.sysv.shmmax=134217728`
- pgpool `num_init_children` must be ≥ 10
- Sail and PySpark cannot share a Python process — subprocess workers are used where needed
