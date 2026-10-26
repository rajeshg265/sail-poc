#!/usr/bin/env python3
"""
Create and seed frtb_shard3 (2027-01-01 → 2027-12-31).
Also extends the cobdate broadcast table in shard1 to cover 2027.
Safe to re-run.
"""
import psycopg2
import random
from datetime import date, timedelta

random.seed(77)

DB_USER = "rajeshganta"
DB_HOST = "localhost"

SCENARIOS   = ["LOW", "MEDIUM", "HIGH"]
N_ENTITIES  = 50
ROWS_PER_DATE = 200   # ~73k rows for frtb_sa_capital_output

START = date(2027, 1, 1)
END   = date(2027, 12, 31)

# ── Create frtb_shard3 DB ─────────────────────────────────────────────────────

def create_db():
    conn = psycopg2.connect(host=DB_HOST, dbname="postgres", user=DB_USER)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM pg_database WHERE datname='frtb_shard3'")
    if cur.fetchone():
        print("frtb_shard3 already exists — skipping CREATE DATABASE")
    else:
        cur.execute("CREATE DATABASE frtb_shard3")
        print("Created frtb_shard3")
    cur.close()
    conn.close()

# ── Schema ────────────────────────────────────────────────────────────────────

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS public.frtb_sa_capital_output (
    cobdate                        DATE NOT NULL,
    batch_id                       TEXT,
    node_id                        TEXT,
    node_id_level                  INTEGER,
    node_id_path                   TEXT,
    scope_id                       INTEGER,
    hedge_flag                     BOOLEAN,
    projection_index               INTEGER,
    legal_entity_group_id          INTEGER,
    consolidation_type_id          INTEGER,
    allocation_type_id             INTEGER,
    driving_scenario               TEXT,
    correlation_scenario_id        INTEGER,
    commodities_curvature          DOUBLE PRECISION,
    commodities_delta              DOUBLE PRECISION,
    commodities_vega               DOUBLE PRECISION,
    csrnonsec_curvature            DOUBLE PRECISION,
    csrnonsec_delta                DOUBLE PRECISION,
    csrnonsec_vega                 DOUBLE PRECISION,
    csrsecnonctp_delta             DOUBLE PRECISION,
    csrsecnonctp_jump_to_default   DOUBLE PRECISION,
    equities_curvature             DOUBLE PRECISION,
    equities_delta                 DOUBLE PRECISION,
    equities_vega                  DOUBLE PRECISION,
    foreign_exchange_curvature     DOUBLE PRECISION,
    foreign_exchange_delta         DOUBLE PRECISION,
    foreign_exchange_vega          DOUBLE PRECISION,
    interest_rates_curvature       DOUBLE PRECISION,
    interest_rates_delta           DOUBLE PRECISION,
    interest_rates_vega            DOUBLE PRECISION,
    nonsec_jump_to_default         DOUBLE PRECISION,
    rrao                           DOUBLE PRECISION,
    total                          DOUBLE PRECISION
) PARTITION BY RANGE (cobdate);

CREATE TABLE IF NOT EXISTS frtb_sa_capital_output_2027
    PARTITION OF public.frtb_sa_capital_output
    FOR VALUES FROM ('2027-01-01') TO ('2028-01-01');

CREATE INDEX ON public.frtb_sa_capital_output (cobdate);
CREATE INDEX ON public.frtb_sa_capital_output (legal_entity_group_id, cobdate);

DROP TABLE IF EXISTS public.equity_risk_measure CASCADE;
CREATE TABLE public.equity_risk_measure (
    cobdate                  DATE             NOT NULL,
    legal_entity_group_id    INTEGER          NOT NULL,
    correlation_scenario_id  INTEGER          NOT NULL,
    driving_scenario         TEXT             NOT NULL,
    equity_sensitivity       DOUBLE PRECISION,
    equity_delta             DOUBLE PRECISION,
    equity_vega              DOUBLE PRECISION,
    equity_curvature         DOUBLE PRECISION,
    total_equity_risk        DOUBLE PRECISION
);
CREATE INDEX ON public.equity_risk_measure (cobdate);
CREATE INDEX ON public.equity_risk_measure (legal_entity_group_id, cobdate);

CREATE TABLE IF NOT EXISTS public.cobdate (
    cobdate         DATE     NOT NULL PRIMARY KEY,
    is_business_day BOOLEAN  NOT NULL DEFAULT TRUE,
    fiscal_year     SMALLINT NOT NULL,
    fiscal_quarter  SMALLINT NOT NULL,
    fiscal_month    SMALLINT NOT NULL
);
"""

# ── Seed frtb_sa_capital_output ───────────────────────────────────────────────

NODE_IDS = [f"NODE_{i:04d}" for i in range(1, 201)]

def gen_frtb_row(cobdate, batch_seq):
    sign  = 1 if random.random() > 0.3 else -1
    scale = random.uniform(1_000, 10_000_000)
    return (
        cobdate, f"BATCH_{batch_seq:06d}",
        NODE_IDS[random.randint(0, 199)], random.randint(1, 5),
        f"/root/{NODE_IDS[random.randint(0,199)]}",
        random.randint(1, 10), random.random() > 0.5, random.randint(0, 3),
        random.randint(1, N_ENTITIES),
        random.randint(1, 4), random.randint(1, 3),
        random.choice(SCENARIOS), random.randint(1, 3),
        *[sign * random.uniform(0, scale) for _ in range(20)],
    )

def seed_frtb(conn):
    cur = conn.cursor()
    SQL = """INSERT INTO public.frtb_sa_capital_output VALUES (
        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
    current, batch_seq, total = START, 0, 0
    while current <= END:
        rows = [gen_frtb_row(current, batch_seq + i) for i in range(ROWS_PER_DATE)]
        cur.executemany(SQL, rows)
        conn.commit()
        total += ROWS_PER_DATE
        batch_seq += ROWS_PER_DATE
        current += timedelta(days=1)
        if total % 10000 == 0:
            print(f"  frtb_sa_capital_output: {total:,} rows...", end="\r")
    cur.close()
    print(f"  frtb_sa_capital_output: {total:,} rows done")

# ── Seed equity_risk_measure ──────────────────────────────────────────────────

def seed_equity(conn):
    cur = conn.cursor()
    SQL = """INSERT INTO public.equity_risk_measure
        (cobdate,legal_entity_group_id,correlation_scenario_id,driving_scenario,
         equity_sensitivity,equity_delta,equity_vega,equity_curvature,total_equity_risk)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
    current, total, batch = START, 0, []
    while current <= END:
        for le_id in range(1, N_ENTITIES + 1):
            for corr in range(1, 4):
                sign  = 1 if random.random() > 0.3 else -1
                scale = random.uniform(500, 5_000_000)
                s, d, v, c = [sign * random.uniform(0, scale * f) for f in (1, 0.8, 0.5, 0.3)]
                batch.append((current, le_id, corr, random.choice(SCENARIOS), s, d, v, c, s+d+v+c))
        if len(batch) >= 10_000:
            cur.executemany(SQL, batch)
            conn.commit()
            total += len(batch)
            batch = []
        current += timedelta(days=1)
    if batch:
        cur.executemany(SQL, batch)
        conn.commit()
        total += len(batch)
    cur.close()
    print(f"  equity_risk_measure: {total:,} rows done")

# ── Seed cobdate in shard3 ────────────────────────────────────────────────────

def seed_cobdate_shard3(conn):
    cur = conn.cursor()
    current, rows = START, []
    while current <= END:
        q = ((current.month - 1) // 3) + 1
        rows.append((current, current.weekday() < 5, current.year, q, current.month))
        current += timedelta(days=1)
    cur.executemany(
        "INSERT INTO public.cobdate VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING", rows)
    conn.commit()
    cur.close()
    print(f"  cobdate (shard3): {len(rows)} rows done")

# ── Extend cobdate in shard1 to cover 2027 ────────────────────────────────────

def extend_cobdate_shard1():
    conn = psycopg2.connect(host=DB_HOST, dbname="frtb_shard1", user=DB_USER)
    cur  = conn.cursor()
    current, rows = START, []
    while current <= END:
        q = ((current.month - 1) // 3) + 1
        rows.append((current, current.weekday() < 5, current.year, q, current.month))
        current += timedelta(days=1)
    cur.executemany(
        "INSERT INTO public.cobdate VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING", rows)
    conn.commit()
    cur.close()
    conn.close()
    print(f"  cobdate in shard1 extended with {len(rows)} rows for 2027")

# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=== Setting up frtb_shard3 (2027-01-01 → 2027-12-31) ===\n")

    create_db()

    conn3 = psycopg2.connect(host=DB_HOST, dbname="frtb_shard3", user=DB_USER)
    conn3.cursor().execute(SCHEMA_SQL)
    conn3.commit()
    print("Schema created in frtb_shard3\n")

    print("Seeding frtb_sa_capital_output...")
    seed_frtb(conn3)

    print("Seeding equity_risk_measure...")
    seed_equity(conn3)

    print("Seeding cobdate in shard3...")
    seed_cobdate_shard3(conn3)

    conn3.close()

    print("\nExtending cobdate in shard1 to cover 2027...")
    extend_cobdate_shard1()

    print("\n=== frtb_shard3 setup complete ===")
