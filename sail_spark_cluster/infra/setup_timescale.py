#!/usr/bin/env python3
"""
Migrate all POC databases to TimescaleDB Docker containers.

Topology:
  port 5440  frtb_shard1  → frtb_sa_capital_output  HYPERTABLE (cobdate, 2024-01-01 – 2025-06-30)
  port 5441  frtb_shard2  → frtb_sa_capital_output  HYPERTABLE (cobdate, 2025-07-01 – 2026-12-31)
  port 5442  frtb_shard3  → frtb_sa_capital_output  HYPERTABLE (cobdate, 2027-01-01 – 2027-12-31)
  port 5440  frtb_shard1  → broadcast tables (cobdate, entity_hierarchy,
                             chorushierarchy, vw_pillar_node_mapping)
  port 5443  equity_db    → equity_risk_measure  HYPERTABLE
  port 5444  credit_db    → credit_risk_measure  HYPERTABLE
  port 5445  macro_db     → macro_risk_measure   HYPERTABLE
"""
import psycopg2
import random
from datetime import date, timedelta

random.seed(42)

DB_USER = "rajeshganta"
DB_HOST = "127.0.0.1"

SHARDS = {
    "frtb_shard1": {"port": 5440, "from": date(2024, 1, 1),  "to": date(2025, 6, 30)},
    "frtb_shard2": {"port": 5441, "from": date(2025, 7, 1),  "to": date(2026, 12, 31)},
    "frtb_shard3": {"port": 5442, "from": date(2027, 1, 1),  "to": date(2027, 12, 31)},
}

DOMAIN_DBS = {
    "equity_db": 5443,
    "credit_db": 5444,
    "macro_db":  5445,
}

DOMAIN_START = date(2024, 1, 1)
DOMAIN_END   = date(2027, 12, 31)

SCENARIOS  = ["LOW", "MEDIUM", "HIGH"]
N_ENTITIES = 50
N_CORR     = 3
N_NODES    = 200

RISK_PILLARS = [
    ("EQUITY",      "Equity Risk",            "equities"),
    ("RATES",       "Interest Rate Risk",      "interest_rates"),
    ("FX",          "Foreign Exchange Risk",   "foreign_exchange"),
    ("COMMODITIES", "Commodities Risk",        "commodities"),
    ("CSR_NONSEC",  "CSR Non-Securitisation",  "csrnonsec"),
    ("CSR_SEC",     "CSR Sec Non-CTP",         "csrsecnonctp"),
    ("RRAO",        "Residual Risk Add-On",    "rrao"),
]
SENSITIVITY_TYPES = ["DELTA", "VEGA", "CURVATURE", "JTD"]


def conn(dbname, port):
    return psycopg2.connect(host=DB_HOST, port=port, dbname=dbname, user=DB_USER)


def insert_batched(cur, c, sql, rows, label):
    total, batch = 0, []
    for row in rows:
        batch.append(row)
        if len(batch) >= 10_000:
            cur.executemany(sql, batch)
            c.commit()
            total += len(batch)
            batch = []
            print(f"    {label}: {total:,} rows...", end="\r")
    if batch:
        cur.executemany(sql, batch)
        c.commit()
        total += len(batch)
    print(f"  {label}: {total:,} rows inserted")


# ── FRTB SA capital output (hypertable per shard) ─────────────────────────────

DDL_FRTB = """
DROP TABLE IF EXISTS public.frtb_sa_capital_output CASCADE;
CREATE TABLE public.frtb_sa_capital_output (
    cobdate                       DATE             NOT NULL,
    legal_entity_group_id         INTEGER          NOT NULL,
    correlation_scenario_id       INTEGER          NOT NULL,
    driving_scenario              TEXT             NOT NULL,
    node_id                       TEXT,
    batch_id                      TEXT,
    scope_id                      INTEGER,
    hedge_flag                    BOOLEAN,
    interest_rates_delta          DOUBLE PRECISION,
    interest_rates_vega           DOUBLE PRECISION,
    interest_rates_curvature      DOUBLE PRECISION,
    equities_delta                DOUBLE PRECISION,
    equities_vega                 DOUBLE PRECISION,
    equities_curvature            DOUBLE PRECISION,
    foreign_exchange_delta        DOUBLE PRECISION,
    foreign_exchange_vega         DOUBLE PRECISION,
    foreign_exchange_curvature    DOUBLE PRECISION,
    commodities_delta             DOUBLE PRECISION,
    commodities_vega              DOUBLE PRECISION,
    commodities_curvature         DOUBLE PRECISION,
    csrnonsec_delta               DOUBLE PRECISION,
    csrnonsec_vega                DOUBLE PRECISION,
    csrnonsec_curvature           DOUBLE PRECISION,
    csrsecnonctp_delta            DOUBLE PRECISION,
    csrsecnonctp_jump_to_default  DOUBLE PRECISION,
    nonsec_jump_to_default        DOUBLE PRECISION,
    rrao                          DOUBLE PRECISION,
    total                         DOUBLE PRECISION
);
SELECT create_hypertable('public.frtb_sa_capital_output', 'cobdate', chunk_time_interval => INTERVAL '1 month');
CREATE INDEX ON public.frtb_sa_capital_output (legal_entity_group_id, cobdate);
CREATE INDEX ON public.frtb_sa_capital_output (node_id, cobdate);
"""

def seed_frtb_shard(shard_name, port, date_from, date_to):
    print(f"\n{shard_name} (port {port}, {date_from} – {date_to}):")
    c = conn(shard_name, port)
    cur = c.cursor()
    cur.execute(DDL_FRTB)
    c.commit()

    SQL = """INSERT INTO public.frtb_sa_capital_output
        (cobdate, legal_entity_group_id, correlation_scenario_id, driving_scenario,
         node_id, batch_id, scope_id, hedge_flag,
         interest_rates_delta, interest_rates_vega, interest_rates_curvature,
         equities_delta, equities_vega, equities_curvature,
         foreign_exchange_delta, foreign_exchange_vega, foreign_exchange_curvature,
         commodities_delta, commodities_vega, commodities_curvature,
         csrnonsec_delta, csrnonsec_vega, csrnonsec_curvature,
         csrsecnonctp_delta, csrsecnonctp_jump_to_default,
         nonsec_jump_to_default, rrao, total)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

    rows, d = [], date_from
    while d <= date_to:
        for le in range(1, N_ENTITIES + 1):
            for corr in range(1, N_CORR + 1):
                scen  = random.choice(SCENARIOS)
                node  = f"NODE_{random.randint(1, N_NODES):04d}"
                sign  = 1 if random.random() > 0.3 else -1
                scale = random.uniform(1_000, 10_000_000)
                def c_(w): return sign * random.uniform(0, scale * w)
                ir_d, ir_v, ir_c   = c_(0.3), c_(0.15), c_(0.08)
                eq_d, eq_v, eq_c   = c_(0.25), c_(0.12), c_(0.06)
                fx_d, fx_v, fx_c   = c_(0.2),  c_(0.10), c_(0.05)
                co_d, co_v, co_c   = c_(0.15), c_(0.08), c_(0.04)
                cn_d, cn_v, cn_c   = c_(0.1),  c_(0.05), c_(0.03)
                cs_d, cs_jtd       = c_(0.08), abs(c_(0.04))
                ns_jtd             = abs(c_(0.03))
                rrao_v             = abs(c_(0.02))
                total = (ir_d+ir_v+ir_c + eq_d+eq_v+eq_c + fx_d+fx_v+fx_c +
                         co_d+co_v+co_c + cn_d+cn_v+cn_c + cs_d+cs_jtd + ns_jtd + rrao_v)
                rows.append((
                    d, le, corr, scen, node, f"BATCH_{d.strftime('%Y%m%d')}", corr, random.random() > 0.8,
                    ir_d, ir_v, ir_c, eq_d, eq_v, eq_c, fx_d, fx_v, fx_c,
                    co_d, co_v, co_c, cn_d, cn_v, cn_c, cs_d, cs_jtd, ns_jtd, rrao_v, total
                ))
        d += timedelta(days=1)

    insert_batched(cur, c, SQL, rows, f"{shard_name}/frtb_sa_capital_output")
    cur.close(); c.close()


# ── Broadcast tables (all in frtb_shard1) ────────────────────────────────────

DDL_COBDATE = """
DROP TABLE IF EXISTS public.cobdate CASCADE;
CREATE TABLE public.cobdate (
    cobdate        DATE PRIMARY KEY,
    fiscal_year    INTEGER,
    fiscal_quarter INTEGER,
    fiscal_month   INTEGER,
    is_month_end   BOOLEAN,
    is_quarter_end BOOLEAN,
    is_year_end    BOOLEAN
);
"""

def seed_cobdate(port):
    # Build rows once
    rows, d = [], date(2024, 1, 1)
    end = date(2027, 12, 31)
    while d <= end:
        q = (d.month - 1) // 3 + 1
        last_day = (date(d.year, d.month % 12 + 1, 1) - timedelta(days=1)) if d.month < 12 else date(d.year, 12, 31)
        rows.append((d, d.year, q, d.month,
                     d == last_day, d.month in (3,6,9,12) and d == last_day, d.month == 12 and d == last_day))
        d += timedelta(days=1)

    # SS BROADCAST requires cobdate in every datasource — seed all FRTB shards and domain DBs
    targets = [
        ("frtb_shard1", SHARDS["frtb_shard1"]["port"]),
        ("frtb_shard2", SHARDS["frtb_shard2"]["port"]),
        ("frtb_shard3", SHARDS["frtb_shard3"]["port"]),
        ("equity_db",   DOMAIN_DBS["equity_db"]),
        ("credit_db",   DOMAIN_DBS["credit_db"]),
        ("macro_db",    DOMAIN_DBS["macro_db"]),
    ]
    for db_name, db_port in targets:
        print(f"\nbroadcast cobdate → {db_name}:{db_port}")
        c = conn(db_name, db_port)
        cur = c.cursor()
        cur.execute(DDL_COBDATE)
        c.commit()
        cur.executemany("INSERT INTO public.cobdate VALUES (%s,%s,%s,%s,%s,%s,%s)", rows)
        c.commit()
        print(f"  cobdate: {len(rows):,} rows inserted")
        cur.close(); c.close()


DDL_ENTITY_HIERARCHY = """
DROP TABLE IF EXISTS public.entity_hierarchy CASCADE;
CREATE TABLE public.entity_hierarchy (
    legal_entity_group_id INTEGER PRIMARY KEY,
    legal_entity_name     TEXT,
    region                TEXT,
    business_unit         TEXT,
    reporting_currency    TEXT
);
"""

REGIONS = ["EMEA", "APAC", "AMER", "LATAM"]
BUS     = ["Investment Banking", "Markets", "Wealth Management", "Corporate Banking"]

def seed_entity_hierarchy(port):
    print("frtb_shard1 broadcast: entity_hierarchy")
    c = conn("frtb_shard1", port)
    cur = c.cursor()
    cur.execute(DDL_ENTITY_HIERARCHY)
    c.commit()
    rows = [(i, f"Legal Entity {i}", random.choice(REGIONS),
             random.choice(BUS), random.choice(["USD","GBP","EUR","JPY"]))
            for i in range(1, N_ENTITIES + 1)]
    cur.executemany("INSERT INTO public.entity_hierarchy VALUES (%s,%s,%s,%s,%s)", rows)
    c.commit()
    print(f"  entity_hierarchy: {len(rows)} rows inserted")
    cur.close(); c.close()


DDL_CHORUSHIERARCHY = """
DROP TABLE IF EXISTS public.chorushierarchy CASCADE;
CREATE TABLE public.chorushierarchy (
    node_id      TEXT PRIMARY KEY,
    desk_name    TEXT,
    trading_book TEXT,
    risk_pillar  TEXT
);
"""

DESKS   = ["Rates Desk", "Equity Desk", "FX Desk", "Credit Desk", "Commodities Desk"]
BOOKS   = ["Trading Book A", "Trading Book B", "Banking Book"]
PILLARS = ["GIRR", "CSR", "EQUITY", "FX", "COMMODITY", "RRAO"]

def seed_chorushierarchy(port):
    print("frtb_shard1 broadcast: chorushierarchy")
    c = conn("frtb_shard1", port)
    cur = c.cursor()
    cur.execute(DDL_CHORUSHIERARCHY)
    c.commit()
    rows = [(f"NODE_{i:04d}", random.choice(DESKS),
             random.choice(BOOKS), random.choice(PILLARS))
            for i in range(1, N_NODES + 1)]
    cur.executemany("INSERT INTO public.chorushierarchy VALUES (%s,%s,%s,%s)", rows)
    c.commit()
    print(f"  chorushierarchy: {len(rows)} rows inserted")
    cur.close(); c.close()


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

def seed_pillar_mapping(port):
    print("frtb_shard1 broadcast: vw_pillar_node_mapping")
    c = conn("frtb_shard1", port)
    cur = c.cursor()
    cur.execute(DDL_PILLAR_MAPPING)
    c.commit()
    rows = []
    for i in range(1, N_NODES + 1):
        pc, pn, col = RISK_PILLARS[(i - 1) % len(RISK_PILLARS)]
        st = SENSITIVITY_TYPES[(i - 1) % len(SENSITIVITY_TYPES)]
        rows.append((f"NODE_{i:04d}", pc, pn, st, col, round(0.5 + random.random() * 0.5, 4)))
    cur.executemany("INSERT INTO public.vw_pillar_node_mapping VALUES (%s,%s,%s,%s,%s,%s)", rows)
    c.commit()
    print(f"  vw_pillar_node_mapping: {len(rows)} rows inserted")
    cur.close(); c.close()


# ── Domain databases (hypertables) ────────────────────────────────────────────

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
SELECT create_hypertable('public.equity_risk_measure', 'cobdate', chunk_time_interval => INTERVAL '3 months');
CREATE INDEX ON public.equity_risk_measure (legal_entity_group_id, cobdate);
"""

def seed_equity_db():
    print("\nequity_db (port 5443):")
    c = conn("equity_db", 5443)
    cur = c.cursor()
    cur.execute(DDL_EQUITY)
    c.commit()
    SQL = """INSERT INTO public.equity_risk_measure
        (cobdate, legal_entity_group_id, correlation_scenario_id, driving_scenario,
         equity_sensitivity, equity_delta, equity_vega, equity_curvature, total_equity_risk)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
    rows, d = [], DOMAIN_START
    while d <= DOMAIN_END:
        for le in range(1, N_ENTITIES + 1):
            for corr in range(1, N_CORR + 1):
                scen = random.choice(SCENARIOS)
                sign = 1 if random.random() > 0.3 else -1
                scale = random.uniform(500, 5_000_000)
                s, dl, v, cv = (sign * random.uniform(0, scale * w) for w in [1, 0.8, 0.5, 0.3])
                rows.append((d, le, corr, scen, s, dl, v, cv, s + dl + v + cv))
        d += timedelta(days=1)
    insert_batched(cur, c, SQL, rows, "equity_db/equity_risk_measure")
    cur.close(); c.close()


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
SELECT create_hypertable('public.credit_risk_measure', 'cobdate', chunk_time_interval => INTERVAL '3 months');
CREATE INDEX ON public.credit_risk_measure (legal_entity_group_id, cobdate);
"""

def seed_credit_db():
    print("\ncredit_db (port 5444):")
    c = conn("credit_db", 5444)
    cur = c.cursor()
    cur.execute(DDL_CREDIT)
    c.commit()
    SQL = """INSERT INTO public.credit_risk_measure
        (cobdate, legal_entity_group_id, correlation_scenario_id, driving_scenario,
         credit_sensitivity, credit_delta, credit_jump_to_default, credit_curvature, total_credit_risk)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
    rows, d = [], DOMAIN_START
    while d <= DOMAIN_END:
        for le in range(1, N_ENTITIES + 1):
            for corr in range(1, N_CORR + 1):
                scen = random.choice(SCENARIOS)
                sign = 1 if random.random() > 0.25 else -1
                scale = random.uniform(100, 2_000_000)
                s, dl = sign * random.uniform(0, scale), sign * random.uniform(0, scale * 0.6)
                jtd   = abs(random.uniform(0, scale * 0.4))
                cv    = sign * random.uniform(0, scale * 0.2)
                rows.append((d, le, corr, scen, s, dl, jtd, cv, s + dl + jtd + cv))
        d += timedelta(days=1)
    insert_batched(cur, c, SQL, rows, "credit_db/credit_risk_measure")
    cur.close(); c.close()


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
SELECT create_hypertable('public.macro_risk_measure', 'cobdate', chunk_time_interval => INTERVAL '3 months');
CREATE INDEX ON public.macro_risk_measure (legal_entity_group_id, cobdate);
"""

def seed_macro_db():
    print("\nmacro_db (port 5445):")
    c = conn("macro_db", 5445)
    cur = c.cursor()
    cur.execute(DDL_MACRO)
    c.commit()
    SQL = """INSERT INTO public.macro_risk_measure
        (cobdate, legal_entity_group_id, correlation_scenario_id, driving_scenario,
         ir_sensitivity, fx_sensitivity, commodities_sensitivity, total_macro_risk)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"""
    rows, d = [], DOMAIN_START
    while d <= DOMAIN_END:
        for le in range(1, N_ENTITIES + 1):
            for corr in range(1, N_CORR + 1):
                scen = random.choice(SCENARIOS)
                sign = 1 if random.random() > 0.35 else -1
                scale = random.uniform(200, 3_000_000)
                ir, fx, com = (sign * random.uniform(0, scale * w) for w in [0.5, 0.3, 0.2])
                rows.append((d, le, corr, scen, ir, fx, com, ir + fx + com))
        d += timedelta(days=1)
    insert_batched(cur, c, SQL, rows, "macro_db/macro_risk_measure")
    cur.close(); c.close()


# ── Verify hypertables ────────────────────────────────────────────────────────

def verify():
    print("\n=== Hypertable verification ===")
    checks = [
        ("frtb_shard1", 5440, "frtb_sa_capital_output"),
        ("frtb_shard2", 5441, "frtb_sa_capital_output"),
        ("frtb_shard3", 5442, "frtb_sa_capital_output"),
        ("equity_db",   5443, "equity_risk_measure"),
        ("credit_db",   5444, "credit_risk_measure"),
        ("macro_db",    5445, "macro_risk_measure"),
    ]
    for db, port, table in checks:
        c = conn(db, port)
        cur = c.cursor()
        cur.execute("""
            SELECT h.table_name, COUNT(ch.chunk_name) AS chunks,
                   pg_size_pretty(hypertable_size(format('public.%I', h.table_name)::regclass)) AS size
            FROM timescaledb_information.hypertables h
            JOIN timescaledb_information.chunks ch
              ON h.hypertable_name = ch.hypertable_name
            WHERE h.table_name = %s
            GROUP BY h.table_name
        """, (table,))
        row = cur.fetchone()
        cur2 = c.cursor()
        cur2.execute(f"SELECT COUNT(*) FROM public.{table}")
        n = cur2.fetchone()[0]
        if row:
            print(f"  {db}.{table}: {n:,} rows, {row[1]} chunks, {row[2]}")
        else:
            print(f"  {db}.{table}: {n:,} rows (hypertable info not available)")
        cur.close(); cur2.close(); c.close()


if __name__ == "__main__":
    print("=== Seeding TimescaleDB containers ===")

    # FRTB shards
    for shard, cfg in SHARDS.items():
        seed_frtb_shard(shard, cfg["port"], cfg["from"], cfg["to"])

    # Broadcast / reference tables
    shard1_port = SHARDS["frtb_shard1"]["port"]
    seed_cobdate(shard1_port)          # seeds all 6 datasources (SS BROADCAST requirement)
    seed_entity_hierarchy(shard1_port)
    seed_chorushierarchy(shard1_port)
    seed_pillar_mapping(shard1_port)

    # Domain databases
    seed_equity_db()
    seed_credit_db()
    seed_macro_db()

    verify()
    print("\nDone.")
