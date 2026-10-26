#!/usr/bin/env bash
# Create TimescaleDB Docker containers for sail_spark_cluster.
# Run once before setup_cluster_db.py.
set -e

DB_USER="rajeshganta"

echo "Creating cluster_shard1 container (port 5450)…"
docker run -d \
  --name timescale-cluster-shard1 \
  -e POSTGRES_USER="$DB_USER" \
  -e POSTGRES_DB=cluster_shard1 \
  -e POSTGRES_HOST_AUTH_METHOD=trust \
  -p 5450:5432 \
  timescale/timescaledb:latest-pg16

echo "Creating cluster_shard2 container (port 5451)…"
docker run -d \
  --name timescale-cluster-shard2 \
  -e POSTGRES_USER="$DB_USER" \
  -e POSTGRES_DB=cluster_shard2 \
  -e POSTGRES_HOST_AUTH_METHOD=trust \
  -p 5451:5432 \
  timescale/timescaledb:latest-pg16

echo ""
echo "Waiting for PostgreSQL to be ready…"
for port in 5450 5451; do
  for i in $(seq 1 30); do
    if pg_isready -h 127.0.0.1 -p "$port" -U "$DB_USER" -q 2>/dev/null; then
      echo "  port $port: ready"
      break
    fi
    sleep 1
    [ "$i" -eq 30 ] && echo "  port $port: timed out" && exit 1
  done
done

echo ""
echo "Containers ready. Run: python3 infra/setup_cluster_db.py"
