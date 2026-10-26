#!/usr/bin/env python3
"""Generate synthetic FRTB SA capital output data for two shards."""
import psycopg2
import random
from datetime import date, timedelta
import sys

random.seed(42)

LEGAL_ENTITIES = [f"LE_{i:03d}" for i in range(1, 51)]   # 50 legal entities
NODE_IDS = [f"NODE_{i:04d}" for i in range(1, 201)]
SCENARIOS = ["LOW", "MEDIUM", "HIGH"]
BATCH_PREFIX = "BATCH"

DB_USER = "rajeshganta"
DB_HOST = "localhost"

def gen_row(cobdate, batch_seq):
    le_idx = random.randint(0, len(LEGAL_ENTITIES) - 1)
    node_idx = random.randint(0, len(NODE_IDS) - 1)
    sign = 1 if random.random() > 0.3 else -1
    scale = random.uniform(1_000, 10_000_000)
    return (
        cobdate,
        f"{BATCH_PREFIX}_{batch_seq:06d}",
        NODE_IDS[node_idx],
        random.randint(1, 5),
        f"/root/{NODE_IDS[node_idx]}",
        random.randint(1, 10),
        random.random() > 0.5,
        random.randint(0, 3),
        le_idx + 1,
        random.randint(1, 4),
        random.randint(1, 3),
        random.choice(SCENARIOS),
        random.randint(1, 3),
        sign * random.uniform(0, scale),
        sign * random.uniform(0, scale),
        sign * random.uniform(0, scale),
        sign * random.uniform(0, scale),
        sign * random.uniform(0, scale),
        sign * random.uniform(0, scale),
        sign * random.uniform(0, scale),
        sign * random.uniform(0, scale),
        sign * random.uniform(0, scale),
        sign * random.uniform(0, scale),
        sign * random.uniform(0, scale),
        sign * random.uniform(0, scale),
        sign * random.uniform(0, scale),
        sign * random.uniform(0, scale),
        sign * random.uniform(0, scale),
        sign * random.uniform(0, scale),
        sign * random.uniform(0, scale),
        sign * random.uniform(0, scale),
        sign * random.uniform(0, scale),
        sign * random.uniform(0, scale),
    )

def seed_shard(db_name, start_date, end_date, rows_per_date):
    conn = psycopg2.connect(host=DB_HOST, dbname=db_name, user=DB_USER)
    cur = conn.cursor()

    INSERT_SQL = """
        INSERT INTO public.frtb_sa_capital_output VALUES (
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
        )
    """

    current = start_date
    batch_seq = 0
    total = 0
    while current < end_date:
        rows = [gen_row(current, batch_seq + i) for i in range(rows_per_date)]
        cur.executemany(INSERT_SQL, rows)
        conn.commit()
        total += len(rows)
        batch_seq += rows_per_date
        current += timedelta(days=1)
        if total % 50000 == 0:
            print(f"  {db_name}: {total:,} rows inserted...")

    cur.close()
    conn.close()
    print(f"  {db_name}: done — {total:,} rows total")

if __name__ == "__main__":
    # ~300 business days * 200 rows/day ≈ 60k rows per shard (fast for POC)
    # Increase rows_per_date for larger volume
    ROWS_PER_DATE = 200

    print("Seeding shard 1 (2024-01-01 to 2025-06-30)...")
    seed_shard("frtb_shard1", date(2024, 1, 1), date(2025, 7, 1), ROWS_PER_DATE)

    print("Seeding shard 2 (2025-07-01 to 2026-09-25)...")
    seed_shard("frtb_shard2", date(2025, 7, 1), date(2026, 9, 25), ROWS_PER_DATE)

    print("Done.")
