#!/usr/bin/env python3
"""
Seed domain databases per the spec topology:
  equity_db   → equity_risk_measure   (all dates 2024-2027, single domain DB)
  credit_db   → credit_risk_measure   (all dates 2024-2027, single domain DB)
  macro_db    → macro_risk_measure    (all dates 2024-2027, single domain DB)
  frtb_shard1 → vw_pillar_node_mapping (broadcast, node→pillar mapping)

Also removes equity/credit/macro tables from frtb_shard1/2/3 (they were
incorrectly co-located there; they now live in their own domain databases).

Safe to re-run — DROPs and recreates tables each time.
"""
import psycopg2
import random
from datetime import date, timedelta

random.seed(42)

DB_USER = "rajeshganta"
DB_HOST = "127.0.0.1"

# TimescaleDB Docker container ports
SHARD_PORTS = {"frtb_shard1": 5440, "frtb_shard2": 5441, "frtb_shard3": 5442,
               "equity_db": 5443, "credit_db": 5444, "macro_db": 5445}
FRTB_SHARDS = ["frtb_shard1", "frtb_shard2", "frtb_shard3"]

# Domain databases cover ALL dates — no time sharding
DOMAIN_START = date(2024, 1, 1)
DOMAIN_END   = date(2027, 12, 31)

SCENARIOS  = ["LOW", "MEDIUM", "HIGH"]
N_ENTITIES = 50
N_CORR     = 3
N_NODES    = 200

RISK_PILLARS = [
    ("EQUITY",       "Equity Risk",             "equities"),
    ("RATES",        "Interest Rate Risk",       "interest_rates"),
    ("FX",           "Foreign Exchange Risk",    "foreign_exchange"),
    ("COMMODITIES",  "Commodities Risk",         "commodities"),
    ("CSR_NONSEC",   "CSR Non-Securitisation",  "csrnonsec"),
    ("CSR_SEC",      "CSR Sec Non-CTP",         "csrsecnonctp"),
    ("RRAO",         "Residual Risk Add-On",     "rrao"),
]
SENSITIVITY_TYPES = ["DELTA", "VEGA", "CURVATURE", "JTD"]


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

DDL_PILLAR_MAPPING = """
DROP TABLE IF EXISTS public.vw_pillar_node_mapping CASCADE;
CREATE TABLE public.vw_pillar_node_mapping (
    node_id               TEXT NOT NULL,
    pillar_code           TEXT NOT NULL,
    pillar_name           TEXT NOT NULL,
    primary_sensitivity   TEXT NOT NULL,
    capital_column_prefix TEXT NOT NULL,
    pillar_weight         DOUBLE PRECISION NOT NULL DEFAULT 1.0,
    PRIMARY KEY (node_id, pillar_code)
);
"""


# ── Seed helpers ───────────────────────────────────────────────────────────────

def _connect(db_name):
    return psycopg2.connect(host=DB_HOST, dbname=db_name, user=DB_USER)


def _insert_batched(cur, conn, sql, rows, label):
    total, batch = 0, []
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


# ── equity_db ─────────────────────────────────────────────────────────────────

def seed_equity_db():
    conn = _connect("equity_db")
    cur  = conn.cursor()
    cur.execute(DDL_EQUITY)
    conn.commit()

    SQL = """INSERT INTO public.equity_risk_measure
        (cobdate, legal_entity_group_id, correlation_scenario_id, driving_scenario,
         equity_sensitivity, equity_delta, equity_vega, equity_curvature, total_equity_risk)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

    rows, d = [], DOMAIN_START
    while d <= DOMAIN_END:
        for le in range(1, N_ENTITIES + 1):
            for corr in range(1, N_CORR + 1):
                scen  = random.choice(SCENARIOS)
                sign  = 1 if random.random() > 0.3 else -1
                scale = random.uniform(500, 5_000_000)
                s = sign * random.uniform(0, scale)
                dl = sign * random.uniform(0, scale * 0.8)
                v = sign * random.uniform(0, scale * 0.5)
                c = sign * random.uniform(0, scale * 0.3)
                rows.append((d, le, corr, scen, s, dl, v, c, s + dl + v + c))
        d += timedelta(days=1)

    _insert_batched(cur, conn, SQL, rows, "equity_db/equity_risk_measure")
    cur.close(); conn.close()


# ── credit_db ─────────────────────────────────────────────────────────────────

def seed_credit_db():
    conn = _connect("credit_db")
    cur  = conn.cursor()
    cur.execute(DDL_CREDIT)
    conn.commit()

    SQL = """INSERT INTO public.credit_risk_measure
        (cobdate, legal_entity_group_id, correlation_scenario_id, driving_scenario,
         credit_sensitivity, credit_delta, credit_jump_to_default, credit_curvature, total_credit_risk)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

    rows, d = [], DOMAIN_START
    while d <= DOMAIN_END:
        for le in range(1, N_ENTITIES + 1):
            for corr in range(1, N_CORR + 1):
                scen  = random.choice(SCENARIOS)
                sign  = 1 if random.random() > 0.25 else -1
                scale = random.uniform(100, 2_000_000)
                s  = sign * random.uniform(0, scale)
                dl = sign * random.uniform(0, scale * 0.6)
                jtd = abs(random.uniform(0, scale * 0.4))
                c  = sign * random.uniform(0, scale * 0.2)
                rows.append((d, le, corr, scen, s, dl, jtd, c, s + dl + jtd + c))
        d += timedelta(days=1)

    _insert_batched(cur, conn, SQL, rows, "credit_db/credit_risk_measure")
    cur.close(); conn.close()


# ── macro_db ──────────────────────────────────────────────────────────────────

def seed_macro_db():
    conn = _connect("macro_db")
    cur  = conn.cursor()
    cur.execute(DDL_MACRO)
    conn.commit()

    SQL = """INSERT INTO public.macro_risk_measure
        (cobdate, legal_entity_group_id, correlation_scenario_id, driving_scenario,
         ir_sensitivity, fx_sensitivity, commodities_sensitivity, total_macro_risk)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"""

    rows, d = [], DOMAIN_START
    while d <= DOMAIN_END:
        for le in range(1, N_ENTITIES + 1):
            for corr in range(1, N_CORR + 1):
                scen   = random.choice(SCENARIOS)
                sign   = 1 if random.random() > 0.35 else -1
                scale  = random.uniform(200, 3_000_000)
                ir     = sign * random.uniform(0, scale * 0.5)
                fx     = sign * random.uniform(0, scale * 0.3)
                commod = sign * random.uniform(0, scale * 0.2)
                rows.append((d, le, corr, scen, ir, fx, commod, ir + fx + commod))
        d += timedelta(days=1)

    _insert_batched(cur, conn, SQL, rows, "macro_db/macro_risk_measure")
    cur.close(); conn.close()


# ── vw_pillar_node_mapping in frtb_shard1 (broadcast source) ──────────────────

def seed_pillar_mapping():
    conn = _connect("frtb_shard1")
    cur  = conn.cursor()
    cur.execute(DDL_PILLAR_MAPPING)
    conn.commit()

    rows = []
    for i in range(1, N_NODES + 1):
        node_id = f"NODE_{i:04d}"
        # Each node maps to a primary pillar (cycle through pillars)
        pillar_code, pillar_name, col_prefix = RISK_PILLARS[(i - 1) % len(RISK_PILLARS)]
        # Primary sensitivity type cycles through DELTA/VEGA/CURVATURE/JTD
        sens_type = SENSITIVITY_TYPES[(i - 1) % len(SENSITIVITY_TYPES)]
        weight = round(0.5 + random.random() * 0.5, 4)
        rows.append((node_id, pillar_code, pillar_name, sens_type, col_prefix, weight))

    cur.executemany(
        "INSERT INTO public.vw_pillar_node_mapping VALUES (%s,%s,%s,%s,%s,%s)",
        rows,
    )
    conn.commit()
    cur.close(); conn.close()
    print(f"  frtb_shard1/vw_pillar_node_mapping: {len(rows)} rows inserted")


# ── Drop wrongly co-located tables from frtb shards ───────────────────────────

def drop_domain_tables_from_frtb_shards():
    tables = ["equity_risk_measure", "credit_risk_measure", "macro_risk_measure"]
    for db in FRTB_SHARDS:
        conn = _connect(db)
        cur  = conn.cursor()
        for t in tables:
            cur.execute(f"DROP TABLE IF EXISTS public.{t} CASCADE")
        conn.commit()
        cur.close(); conn.close()
        print(f"  {db}: dropped {', '.join(tables)}")


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=== Setting up domain databases ===\n")

    print("equity_db (all dates 2024-2027):")
    seed_equity_db()

    print("\ncredit_db (all dates 2024-2027):")
    seed_credit_db()

    print("\nmacro_db (all dates 2024-2027):")
    seed_macro_db()

    print("\nvw_pillar_node_mapping (broadcast → frtb_shard1):")
    seed_pillar_mapping()

    print("\nRemoving domain tables from FRTB shards (were incorrectly co-located):")
    drop_domain_tables_from_frtb_shards()

    print("\nDone.")
