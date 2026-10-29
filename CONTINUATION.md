# Continuation — Where Things Stand and What to Build Next

This document captures the full state of the Sail POC as of 2026-10-29, including
what has been proven, what the acceptance criteria are, and exactly what remains to
be built before the POC can be declared complete.

---

## What Has Been Proven

### Parquet benchmark (the key result)

Sail DataFusion is **2.3–7.3× faster** than Spark JVM on FRTB OLAP queries when
data is read directly from Parquet files (no JDBC). This is the core claim of the POC.

- 54.88M rows / ~9 GB
- 8 queries: full_agg, grouped_agg, filtered_agg, join_entity, join_desk, join_calendar,
  scan_only_count, scan_only_sum1col
- Sail local-cluster (3 workers) vs Spark local-cluster[3,1,1024]
- Published analysis: https://github.com/rajeshg265/sail-parquet-benchmark
- Shuffle proof (Arrow Flight gRPC evidence): `sail-parquet-benchmark/shuffle_proof.html`

### JDBC benchmarks

When data is in PostgreSQL (the production topology), Spark currently wins:

| Mode | Winner | Margin | Root cause |
|------|--------|--------|------------|
| local (PostgreSQL/JDBC) | Spark | — | JDBC bottleneck |
| local-cluster (PostgreSQL/JDBC) | Spark | — | JDBC + SELECT * |
| K8s cluster (PostgreSQL/JDBC) | Spark | 1.4–2.5× | SELECT * (all 33 cols over network) |
| local (Parquet) | **Sail** | **2.3–7.3×** | DataFusion faster on pure compute |
| local-cluster (Parquet, column-pruned) | **Sail** | varies | DataFusion + Arrow Flight shuffle |

**Single root cause for all Sail underperformance on JDBC:** pysail 0.7.2 connectorX
issues `SELECT *` on every JDBC read — all columns fetched regardless of query projection.
Spark's JDBC driver pushes column selection to PostgreSQL. Fix: either upgrade pysail
(check if later versions fix column pruning) or switch to a non-JDBC data path.

---

## Acceptance Criteria Status

### FR1 — Cross-Dataset Joins & Analytics ✅ COMPLETE
- Cross-dataset join (FRTB + equity/credit/macro + SDS hierarchy): correct
- Complex OLAP (TopN, GROUP BY, aggregations): correct
- SUM/AVG/COUNT/MIN/MAX: correct across shards and joined datasets
- CASE/WHEN in aggregations: correct
- Validated by integration tests: `sail_spark/tests/test_integration.py` (16 tests)

### FR2 — Distributed Query Execution ✅ COMPLETE
- Sail connected to all configured TimescaleDB shards via PostgreSQL connectors
- Logical views abstract physical shard topology via UNION ALL
- Partition pruning: date-range shards fan out only to relevant shards
- Broadcast tables (chorushierarchy, cobdate, vw_pillar_node_mapping) replicated to all shards
- Query Service issues logical SQL against views with no shard awareness
- EXPLAIN endpoint available at `/api/explain`

### NFR3 — Performance & Memory Efficiency ⚠️ PARTIALLY DONE
- **Done:** Parquet benchmark proves 2.3–7.3× speedup over Spark (removes JDBC bottleneck)
- **Gap:** Benchmark against production data volume in PostgreSQL topology needs column-pruning fix first
- **Gap:** Side-by-side 1-worker vs 2-worker comparison not yet formally run

### NFR4 — Horizontal Scalability for Data Volume ⚠️ PARTIALLY DONE
- **Done:** Shard registry (shard_registry.yaml) — adding a shard requires only a YAML entry + restart
- **Done:** View generation automatic via bootstrap.py
- **Gap:** No scaling test: 2→4→8 shards with regression validation

### NFR5 — Horizontal Scalability for Compute Performance ⚠️ ARCHITECTURE DONE
- **Done:** Sail local-cluster mode (3 workers, Arrow Flight shuffle): `sail_spark_cluster/`
- **Done:** Sail K8s cluster (3 worker pods, Arrow Flight over pod network): `sail_k8s_cluster/`
- **Gap:** Formal scaling benchmark: 1 worker → 2 workers → 4 workers → 8 workers,
  measuring latency reduction per step
- **Note:** Docker Desktop K8s is slower than bare-metal K8s due to VM overhead — not
  an architecture flaw. On real cluster nodes (BCP) performance would be significantly better.

### BCP 2.0 Deployment ❌ NOT STARTED
- Sail + pgpool deployed on BCP cluster nodes
- Multiple Sail workers on BCP infrastructure
- Everything currently runs on local macOS with Docker Desktop K8s

---

## What to Build Next (Priority Order)

### 1. Fix the SELECT * / column pruning problem (BLOCKER for JDBC performance)

The single biggest gap. Options:
- **Check if pysail ≥ 0.7.3 fixes connectorX column projection** — if yes, upgrade and
  re-run JDBC benchmarks. This is the cheapest fix.
- **Switch JDBC path to direct DataFusion PostgreSQL connector** — investigate whether
  Sail's FlightSQL DataFusion session supports `CREATE EXTERNAL TABLE` with a native
  PostgreSQL connection string (bypasses connectorX entirely). This is the cleanest fix.
  See `sail_sc_arrow/sail/bootstrap.py` for the current Arrow IPC bridge approach.
- **Explicit column lists in SQL** — workaround: rewrite FRTB queries to name specific
  columns instead of `SELECT *`. Confirmed to work in the Parquet benchmark.

### 2. FlightSQL path (replaces HTTP bridge)

Current production path: HTTP bridge (Flask) → SparkConnect. This works but adds latency.
Target: direct FlightSQL (`SAIL_ENGINE=flightsql`) for lower overhead.

Blockers (as of pysail 0.7.2):
- ShardingSphere 5.5.0 is FlightSQL-incompatible. **If moving to Parquet/DataFusion-native
  data source (removing ShardingSphere), this blocker disappears.**
- Arrow FlightSQL JDBC driver v15.0.2 always uses `CreatePreparedStatement`, which Sail
  0.7.2 doesn't implement. Python ADBC (`adbc-driver-flightsql`) works but has session
  isolation with SparkConnect.
- Session isolation: SparkConnect and FlightSQL DataFusion contexts are separate — registered
  views/tables in one are not visible in the other.

Path forward:
1. Investigate whether Sail's FlightSQL DataFusion session supports `CREATE EXTERNAL TABLE`
   with a native PG connection string (if yes, session isolation is not an issue — register
   directly in FlightSQL session)
2. If not, register views in both sessions at startup

### 3. Scaling benchmark (NFR5 gap)

Run the same FRTB Parquet queries with 1, 2, 4, and 8 Sail workers and measure P99 latency.
Expected near-linear scaling. Use `sail_parquet_cluster/bench_cluster.py` as the base —
it already supports `SAIL_CLUSTER__WORKER_INITIAL_COUNT` env var.

### 4. BCP deployment

Production deployment target. Requires:
- Sail binary on BCP nodes
- pgpool instances on same BCP servers (or co-located with TimescaleDB)
- Sail worker count configurable via env or shard registry
- Start/stop scripts adapted from `start_sail.command` for Linux systemd

---

## Infrastructure Notes

### Database topology (macOS local)
- TimescaleDB 2.30.1 in Docker: 6 containers
  - FRTB shards: ports 5440, 5441, 5442
  - Domain DBs: ports 5443, 5444, 5445
- pgpool-II (Homebrew): 3 instances on 6432, 6433, 6434
- ShardingSphere 5.5.0: port 3307

### macOS-specific fixes
- pgpool shared memory: `sudo sysctl -w kern.sysv.shmmax=268435456 kern.sysv.shmall=131072`
- pgpool `num_init_children` must be ≥ 10
- `kill -9` leaves orphaned shared memory — always use SIGTERM; `start_sail.command` cleans up

### pysail 0.7.2 known bugs
| Bug | Symptom | Workaround |
|-----|---------|------------|
| SELECT * (no column pruning) | All columns read over JDBC regardless of projection | Use explicit column lists |
| Date pushdown (JDBC path) | DATE literals sent as epoch integers | Cast to TEXT in SQL, or use HTTP bridge |
| cache() no-op | `df.cache()` does nothing | No workaround in 0.7.2 |
| WORKER_TASK_SLOTS math | Effective parallelism = SLOTS × WORKERS | Set SLOTS=2, WORKERS=3 for 6 parallel tasks |

### Key version pins
```
pysail==0.7.2
pyspark==4.2.0
Python 3.13.3
Java 17.0.11
pgpool-II 4.7.2
PostgreSQL 16
ShardingSphere 5.5.0
```

---

## References

| Resource | Location |
|----------|----------|
| Published benchmark + analysis | https://github.com/rajeshg265/sail-parquet-benchmark |
| Sail vs Spark analysis artifact | https://claude.ai/artifact/1TkK1aX6MucAZfzvmaocN7 |
| Shuffle proof artifact | https://claude.ai/artifact/6Kqok8wPJnSsXYadG68nxJ |
| K8s benchmark artifact | https://claude.ai/artifact/CayoEVHgXp3QhzTYSSRDQf |
| Sail GitHub | https://github.com/lakehq/sail |
| pysail PyPI | https://pypi.org/project/pysail/ |
