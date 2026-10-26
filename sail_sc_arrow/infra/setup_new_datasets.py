#!/usr/bin/env python3
"""
Seed credit_risk_measure, macro_risk_measure (sharded) and chorushierarchy
(broadcast) into all three FRTB shards.

Safe to re-run — DROPs and recreates tables each time.
"""
import psycopg2
import random
from datetime import date, timedelta

random.seed(42)

DB_USER = "rajeshganta"
DB_HOST = "localhost"

SHARDS = [
    ("frtb_shard1", date(2024, 1, 1),  date(2025, 6, 30)),
    ("frtb_shard2", date(2025, 7, 1),  date(2026, 12, 31)),
    ("frtb_shard3", date(2027, 1, 1),  date(2027, 12, 31)),
]

SCENARIOS     = ["LOW", "MEDIUM", "HIGH"]
N_ENTITIES    = 50
N_CORR        = 3
N_NODES       = 200   # matches frtb_sa_capital_output


# ── DDL ───────────────────────────────────────────────────────────────────────

DDL_CREDIT = """
DROP TABLE IF EXISTS public.credit_risk_measure CASCADE;
CREATE TABLE public.credit_risk_measure (
    cobdate                  DATE             NOT NULL,
    legal_entity_group_id    INTEGER          NOT NULL,
    correlation_scenario_id  INTEGER          NOT NULL,
    driving_scenario         TEXT             NOT NULL,
    credit_sensitivity       DOUBLE PRECISION,
    credit_delta             DOUBLE PRECISION,
    credit_jump_to_default   DOUBLE PRECISION,
    credit_curvature         DOUBLE PRECISION,
    total_credit_risk        DOUBLE PRECISION
);
CREATE INDEX ON public.credit_risk_measure (cobdate);
CREATE INDEX ON public.credit_risk_measure (legal_entity_group_id, cobdate);
"""

DDL_MACRO = """
DROP TABLE IF EXISTS public.macro_risk_measure CASCADE;
CREATE TABLE public.macro_risk_measure (
    cobdate                  DATE             NOT NULL,
    legal_entity_group_id    INTEGER          NOT NULL,
    correlation_scenario_id  INTEGER          NOT NULL,
    driving_scenario         TEXT             NOT NULL,
    ir_sensitivity           DOUBLE PRECISION,
    fx_sensitivity           DOUBLE PRECISION,
    commodities_sensitivity  DOUBLE PRECISION,
    total_macro_risk         DOUBLE PRECISION
);
CREATE INDEX ON public.macro_risk_measure (cobdate);
CREATE INDEX ON public.macro_risk_measure (legal_entity_group_id, cobdate);
"""

DDL_CHORUS = """
DROP TABLE IF EXISTS public.chorushierarchy CASCADE;
CREATE TABLE public.chorushierarchy (
    node_id          TEXT     NOT NULL PRIMARY KEY,
    node_name        TEXT     NOT NULL,
    parent_node_id   TEXT,
    node_level       INTEGER  NOT NULL,
    desk_name        TEXT     NOT NULL,
    trading_book     TEXT     NOT NULL,
    risk_pillar      TEXT     NOT NULL,
    sds_unit_id      TEXT     NOT NULL
);
"""

# ── Hierarchy master data ──────────────────────────────────────────────────────

DESKS = [
    "Rates Trading", "FX Trading", "Equity Derivatives",
    "Credit Trading", "Commodities", "Structured Products",
    "Emerging Markets", "Prime Finance", "Macro Trading",
    "Flow Credit",
]
PILLARS = ["Delta Risk", "Vega Risk", "Curvature Risk", "JTD Risk", "RRAO"]
BOOKS   = ["BB_FLOW", "BB_EXOTICS", "BB_HEDGE", "BB_STRUCTURED", "BB_MM"]


def _build_chorushierarchy():
    rows = []
    for i in range(1, N_NODES + 1):
        node_id    = f"NODE_{i:04d}"
        level      = ((i - 1) % 5) + 1
        parent     = f"NODE_{max(1, i - 5):04d}" if level > 1 else None
        desk       = DESKS[(i - 1) % len(DESKS)]
        book       = BOOKS[(i - 1) % len(BOOKS)]
        pillar     = PILLARS[(i - 1) % len(PILLARS)]
        sds_unit   = f"SDS_{((i - 1) // 10) + 1:03d}"
        node_name  = f"{desk} L{level} Node {i}"
        rows.append((node_id, node_name, parent, level, desk, book, pillar, sds_unit))
    return rows

CHORUS_ROWS = _build_chorushierarchy()


# ── Seed helpers ───────────────────────────────────────────────────────────────

def _insert_batched(cur, conn, sql, rows, label):
    total = 0
    batch = []
    for row in rows:
        batch.append(row)
        if len(batch) >= 10_000:
            cur.executemany(sql, batch)
            conn.commit()
            total += len(batch)
            batch = []
            print(f"    {label}: {total:,} rows...", end="\r")
    if batch:
        cur.executemany(sql, batch)
        conn.commit()
        total += len(batch)
    print(f"  {label}: {total:,} rows inserted")
    return total


def seed_credit(db_name: str, start_date: date, end_date: date):
    conn = psycopg2.connect(host=DB_HOST, dbname=db_name, user=DB_USER)
    cur  = conn.cursor()
    cur.execute(DDL_CREDIT)
    conn.commit()

    SQL = """INSERT INTO public.credit_risk_measure
        (cobdate, legal_entity_group_id, correlation_scenario_id, driving_scenario,
         credit_sensitivity, credit_delta, credit_jump_to_default, credit_curvature,
         total_credit_risk)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

    rows = []
    d = start_date
    while d <= end_date:
        for le_id in range(1, N_ENTITIES + 1):
            for corr in range(1, N_CORR + 1):
                scen  = random.choice(SCENARIOS)
                sign  = 1 if random.random() > 0.25 else -1
                scale = random.uniform(100, 2_000_000)
                sens  = sign * random.uniform(0, scale)
                delta = sign * random.uniform(0, scale * 0.6)
                jtd   = abs(random.uniform(0, scale * 0.4))
                curv  = sign * random.uniform(0, scale * 0.2)
                total = sens + delta + jtd + curv
                rows.append((d, le_id, corr, scen, sens, delta, jtd, curv, total))
        d += timedelta(days=1)

    _insert_batched(cur, conn, SQL, rows, f"{db_name}/credit_risk_measure")
    cur.close(); conn.close()


def seed_macro(db_name: str, start_date: date, end_date: date):
    conn = psycopg2.connect(host=DB_HOST, dbname=db_name, user=DB_USER)
    cur  = conn.cursor()
    cur.execute(DDL_MACRO)
    conn.commit()

    SQL = """INSERT INTO public.macro_risk_measure
        (cobdate, legal_entity_group_id, correlation_scenario_id, driving_scenario,
         ir_sensitivity, fx_sensitivity, commodities_sensitivity, total_macro_risk)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"""

    rows = []
    d = start_date
    while d <= end_date:
        for le_id in range(1, N_ENTITIES + 1):
            for corr in range(1, N_CORR + 1):
                scen   = random.choice(SCENARIOS)
                sign   = 1 if random.random() > 0.35 else -1
                scale  = random.uniform(200, 3_000_000)
                ir     = sign * random.uniform(0, scale * 0.5)
                fx     = sign * random.uniform(0, scale * 0.3)
                commod = sign * random.uniform(0, scale * 0.2)
                total  = ir + fx + commod
                rows.append((d, le_id, corr, scen, ir, fx, commod, total))
        d += timedelta(days=1)

    _insert_batched(cur, conn, SQL, rows, f"{db_name}/macro_risk_measure")
    cur.close(); conn.close()


def seed_chorushierarchy(db_name: str):
    conn = psycopg2.connect(host=DB_HOST, dbname=db_name, user=DB_USER)
    cur  = conn.cursor()
    cur.execute(DDL_CHORUS)
    conn.commit()
    cur.executemany(
        "INSERT INTO public.chorushierarchy VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
        CHORUS_ROWS,
    )
    conn.commit()
    cur.close(); conn.close()
    print(f"  {db_name}/chorushierarchy: {len(CHORUS_ROWS)} rows inserted")


# ── Main ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("Setting up new datasets (credit, macro, chorushierarchy)...\n")
    for db_name, start, end in SHARDS:
        print(f"{db_name} ({start} → {end}):")
        seed_credit(db_name, start, end)
        seed_macro(db_name, start, end)
        seed_chorushierarchy(db_name)
        print()
    print("Done.")
