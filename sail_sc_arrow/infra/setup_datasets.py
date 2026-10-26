#!/usr/bin/env python3
"""
Create and seed synthetic secondary datasets for the Sail POC:
  - equity_risk_measure  (sharded by cobdate, same ranges as frtb_sa_capital_output)
  - entity_hierarchy     (broadcast dimension: identical copy in both shards)

Safe to re-run — DROPs and recreates tables each time.
"""
import psycopg2
import random
from datetime import date, timedelta

random.seed(99)

DB_USER = "rajeshganta"
DB_HOST = "localhost"
SHARD1_DB = "frtb_shard1"
SHARD2_DB = "frtb_shard2"

SCENARIOS = ["LOW", "MEDIUM", "HIGH"]
N_ENTITIES = 50
N_CORR     = 3


# ── DDL ───────────────────────────────────────────────────────────────────────

DDL_EQUITY = """
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
"""

DDL_HIERARCHY = """
DROP TABLE IF EXISTS public.entity_hierarchy CASCADE;
CREATE TABLE public.entity_hierarchy (
    legal_entity_group_id  INTEGER  PRIMARY KEY,
    entity_name            TEXT     NOT NULL,
    region                 TEXT     NOT NULL,
    business_unit          TEXT     NOT NULL,
    level2_name            TEXT     NOT NULL,
    level3_name            TEXT     NOT NULL
);
"""


# ── Hierarchy master data (50 entities, deterministic) ────────────────────────

def _build_hierarchy():
    rows = []
    groups = [
        ("EMEA", "IB",       "Europe North",          ["UK", "Germany", "Netherlands", "Sweden"]),
        ("EMEA", "WM",       "Europe South",           ["France", "Spain", "Italy", "Belgium"]),
        ("APAC", "IB",       "Asia Pacific Core",      ["Japan", "Singapore", "Hong Kong", "Australia"]),
        ("APAC", "RETAIL",   "Asia Pacific Emerging",  ["China", "India", "South Korea", "Thailand"]),
        ("AMER", "IB",       "Americas North",         ["US East", "US West", "Canada", "Mexico"]),
        ("AMER", "TREASURY", "Americas South",         ["Brazil", "Argentina", "Chile", "Colombia"]),
    ]
    eid = 1
    # Assign roughly 8-9 entities per group to reach 50 total
    per_group = N_ENTITIES // len(groups)
    remainder = N_ENTITIES % len(groups)
    for g_idx, (region, bu, l2, l3_list) in enumerate(groups):
        count = per_group + (1 if g_idx < remainder else 0)
        for i in range(count):
            l3 = l3_list[i % len(l3_list)]
            rows.append((eid, f"Entity_{eid:03d}", region, bu, l2, l3))
            eid += 1
    return rows

ENTITY_ROWS = _build_hierarchy()
assert len(ENTITY_ROWS) == N_ENTITIES, f"Expected 50, got {len(ENTITY_ROWS)}"


# ── Seed equity_risk_measure ──────────────────────────────────────────────────

def seed_equity(db_name: str, start_date: date, end_date: date):
    conn = psycopg2.connect(host=DB_HOST, dbname=db_name, user=DB_USER)
    cur  = conn.cursor()
    cur.execute(DDL_EQUITY)
    conn.commit()

    SQL = """
        INSERT INTO public.equity_risk_measure
            (cobdate, legal_entity_group_id, correlation_scenario_id, driving_scenario,
             equity_sensitivity, equity_delta, equity_vega, equity_curvature, total_equity_risk)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    current = start_date
    total   = 0
    batch   = []

    while current <= end_date:
        for le_id in range(1, N_ENTITIES + 1):
            for corr in range(1, N_CORR + 1):
                scen  = random.choice(SCENARIOS)
                sign  = 1 if random.random() > 0.3 else -1
                scale = random.uniform(500, 5_000_000)
                sens  = sign * random.uniform(0, scale)
                delta = sign * random.uniform(0, scale * 0.8)
                vega  = sign * random.uniform(0, scale * 0.5)
                curv  = sign * random.uniform(0, scale * 0.3)
                total_eq = sens + delta + vega + curv
                batch.append((current, le_id, corr, scen, sens, delta, vega, curv, total_eq))

        if len(batch) >= 10_000:
            cur.executemany(SQL, batch)
            conn.commit()
            total += len(batch)
            batch = []
            print(f"    {db_name}: {total:,} rows...", end="\r")

        current += timedelta(days=1)

    if batch:
        cur.executemany(SQL, batch)
        conn.commit()
        total += len(batch)

    cur.close()
    conn.close()
    print(f"  {db_name}: equity_risk_measure — {total:,} rows inserted")


# ── Seed entity_hierarchy (broadcast) ────────────────────────────────────────

def seed_hierarchy(db_name: str):
    conn = psycopg2.connect(host=DB_HOST, dbname=db_name, user=DB_USER)
    cur  = conn.cursor()
    cur.execute(DDL_HIERARCHY)
    conn.commit()
    cur.executemany(
        "INSERT INTO public.entity_hierarchy VALUES (%s,%s,%s,%s,%s,%s)",
        ENTITY_ROWS,
    )
    conn.commit()
    cur.close()
    conn.close()
    print(f"  {db_name}: entity_hierarchy     — {len(ENTITY_ROWS)} rows inserted")


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("Setting up secondary datasets...\n")

    print("Shard 1 (2024-01-01 → 2025-06-30):")
    seed_equity(SHARD1_DB, date(2024, 1, 1), date(2025, 6, 30))
    seed_hierarchy(SHARD1_DB)

    print("\nShard 2 (2025-07-01 → 2026-09-25):")
    seed_equity(SHARD2_DB, date(2025, 7, 1), date(2026, 9, 25))
    seed_hierarchy(SHARD2_DB)

    print("\nDone.")
