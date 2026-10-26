# sail_k8s_cluster — POC 4: Sail vs Spark on Kubernetes (cluster mode)

## What this tests

Sail running inside a Kubernetes cluster (driver pod + 3 worker pods) vs Spark
also in K8s (driver + 3 executors), both querying FRTB SA Capital data from
PostgreSQL via pgpool. Arrow Flight gRPC stays within the pod network.

**Result:** Spark wins 1.4–2.5×. Root cause is the same as the local-cluster
POCs — `SELECT *` with no column pruning in pysail 0.7.2 causes Sail to
transfer all 50+ columns over the network even for aggregation-only queries.

## Architecture

```
macOS host
  pgpool :6440 → TimescaleDB frtb_shard1 :5440
  pgpool :6441 → TimescaleDB frtb_shard2 :5441
       |  (LAN IP — reachable from K8s pods)
       |
Kubernetes cluster (local, e.g. Docker Desktop / minikube)
  sail-driver pod  :50060 (SparkConnect, port-forwarded to macOS)
    ├── sail-worker-0
    ├── sail-worker-1
    └── sail-worker-2
  spark-driver pod :50061
    ├── spark-executor-0
    ├── spark-executor-1
    └── spark-executor-2
       |
macOS  kubectl port-forward → :50060 / :50061
       python bootstrap.py   (drives benchmark via SparkConnect)
```

## Prerequisites

| Dependency        | Version                    |
|-------------------|----------------------------|
| Python            | 3.13+                      |
| pysail            | 0.7.2                      |
| pyspark           | 4.2.0                      |
| kubectl           | any recent                 |
| Docker            | with local registry        |
| PostgreSQL        | 16                         |
| pgpool-II         | 4.7.2                      |
| Kubernetes        | Docker Desktop / minikube  |

Install Python dependencies:
```bash
pip install pysail==0.7.2 pyspark==4.2.0 psycopg2-binary pyarrow flask pyyaml
```

## Setup

### 1. Start a local Kubernetes cluster

Enable Kubernetes in Docker Desktop, or start minikube:
```bash
minikube start
```

### 2. Start a local Docker registry (for pod images)

```bash
docker run -d -p 5000:5000 --name registry registry:2
```

### 3. Build and push the Sail worker image

```bash
docker build -f Dockerfile.worker -t host.docker.internal:5000/sail-worker-k8s:0.7.2 .
docker push host.docker.internal:5000/sail-worker-k8s:0.7.2
```

### 4. Apply K8s RBAC and deploy pods

```bash
kubectl apply -f k8s/rbac.yaml
kubectl apply -f k8s/sail-driver-deployment.yaml
kubectl apply -f k8s/spark-driver-deployment.yaml
```

### 5. Start PostgreSQL shards and pgpool on macOS

```bash
# pgpool must listen on your LAN IP (not localhost) so K8s pods can reach it
# Edit shard_registry.yaml: set host to your machine's LAN IP
bash start.sh
```

### 6. Configure secrets

```bash
cp .secrets.example .secrets   # if present; or set env vars directly
```

### 7. Run the benchmark

```bash
python bootstrap.py        # Sail side (port-forwards :50060 and runs queries)
python bootstrap_spark.py  # Spark side (port-forwards :50061 and runs queries)
```

Results are printed to stdout and saved via `csm.py`.

## Key files

| File | Purpose |
|------|---------|
| `bootstrap.py` | Drives Sail benchmark: port-forwards, registers views, runs queries |
| `bootstrap_spark.py` | Drives Spark benchmark side |
| `Dockerfile.worker` | Docker image for Sail worker pods (based on pysail 0.7.2) |
| `Dockerfile.spark` | Docker image for Spark executor pods |
| `k8s/sail-driver-deployment.yaml` | Sail driver + 3 worker pods |
| `k8s/spark-driver-deployment.yaml` | Spark driver + 3 executor pods |
| `k8s/rbac.yaml` | RBAC for driver pod to create/list worker pods |
| `shard_registry.yaml` | Shard topology (set host to LAN IP for K8s access) |
| `csm.py` | Credential/secret manager |

## Known issues

- `MY_POD_IP` env var must be set in the Sail driver pod for correct Arrow Flight binding.
- `spark.jars` must be set to include the PostgreSQL JDBC driver inside the Spark executor image.
- pysail 0.7.2 `SELECT *` (no column pruning) is the root cause of Sail's underperformance.
- pgpool must bind to the machine's LAN IP, not `127.0.0.1`, so pods can connect.
